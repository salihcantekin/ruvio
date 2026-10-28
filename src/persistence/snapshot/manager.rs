//! The checkpoint manager's methods: planning, writing, publishing and loading checkpoint generations.

use super::*;

impl CheckpointManager {
    pub(crate) fn new(config: PersistenceConfig, limits: ProtocolLimits) -> Self {
        Self {
            config,
            limits,
            last_generation: std::sync::Mutex::new(None),
        }
    }

    /// A generation newer than `floor`, every published one and every round directory ever
    /// started. A round that failed after `CURRENT` may already name it, so it is not reused.
    pub(crate) fn allocate_generation(&self, floor: u64) -> Result<u64, PersistenceError> {
        let mut last = self
            .last_generation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let known = match *last {
            Some(known) => known,
            None => self.highest_generation_on_disk()?,
        };
        let next = known
            .max(floor)
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Failed("checkpoint generation exhausted".into()))?;

        *last = Some(next);
        Ok(next)
    }

    fn highest_generation_on_disk(&self) -> Result<u64, PersistenceError> {
        let mut highest = self.current_generation()?.unwrap_or(0);

        if let Ok(entries) = fs::read_dir(self.config.data_dir.join("checkpoints")) {
            for entry in entries.flatten() {
                if let Some(generation) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u64>().ok())
                {
                    highest = highest.max(generation);
                }
            }
        }

        Ok(highest)
    }

    /// The generation `CURRENT` names, if any.
    fn current_generation(&self) -> Result<Option<u64>, PersistenceError> {
        match fs::read(self.config.data_dir.join("CURRENT")) {
            Ok(bytes) => decode_current(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Reads the manifest and each snapshot header, not the entries. Recovery then streams
    /// only the files its shard owns, so memory follows one shard's data, not the whole
    /// checkpoint.
    pub(crate) fn plan(&self) -> Result<Option<CheckpointPlan>, PersistenceError> {
        let current_path = self.config.data_dir.join("CURRENT");
        let current = match fs::read(&current_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let generation = decode_current(&current)?;
        let directory = self
            .config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"));
        let manifest = decode_manifest(&fs::read(directory.join("MANIFEST"))?, generation)?;
        let mut created_at = 0u64;
        let mut ids: Vec<u32> = manifest.iter().map(|item| item.shard_id).collect();

        ids.sort_unstable();
        if ids
            .iter()
            .enumerate()
            .any(|(expected, id)| *id as usize != expected)
        {
            return Err(PersistenceError::Corruption(
                "checkpoint shard set is not contiguous".into(),
            ));
        }

        for item in &manifest {
            let mut header = [0u8; SNAPSHOT_HEADER_LEN];
            let mut file = fs::File::open(directory.join(&item.file_name))?;

            std::io::Read::read_exact(&mut file, &mut header)?;

            if &header[..8] != SNAPSHOT_MAGIC
                || crc32c::crc32c(&header[..60]) != u32_at(&header, 60)?
            {
                return Err(PersistenceError::Corruption(
                    "invalid snapshot header".into(),
                ));
            }

            created_at = created_at.max(u64_at(&header, 48)?);
        }

        Ok(Some(CheckpointPlan {
            generation,
            created_at: UnixMillis::new(created_at),
            directory,
            items: manifest,
            limits: self.limits,
            maximum_bytes: self.config.snapshot_max_bytes,
        }))
    }

    pub(crate) fn load(&self) -> Result<Option<Checkpoint>, PersistenceError> {
        self.load_with_budget(None)
    }

    #[cfg(test)]
    pub(crate) fn load_bounded_history(
        &self,
        max_bytes: u64,
    ) -> Result<Option<Checkpoint>, PersistenceError> {
        self.load_with_budget(Some(max_bytes))
    }

    fn load_with_budget(
        &self,
        budget: Option<u64>,
    ) -> Result<Option<Checkpoint>, PersistenceError> {
        let current_path = self.config.data_dir.join("CURRENT");
        let current = match fs::read(&current_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let generation = decode_current(&current)?;
        let directory = self
            .config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"));
        let manifest_path = directory.join("MANIFEST");
        let mut remaining = budget.unwrap_or(u64::MAX);
        if budget.is_some() {
            let length = fs::metadata(&manifest_path)?.len();
            remaining = remaining.checked_sub(length).ok_or_else(|| {
                PersistenceError::Corruption("JSON validation snapshot budget exceeded".into())
            })?;
        }
        let manifest_bytes = fs::read(manifest_path)?;
        let manifest = decode_manifest(&manifest_bytes, generation)?;
        let mut shards = Vec::with_capacity(manifest.len());
        let mut created_at = 0u64;

        for item in manifest {
            let path = directory.join(&item.file_name);
            if budget.is_some() {
                let length = fs::metadata(&path)?.len();
                remaining = remaining.checked_sub(length).ok_or_else(|| {
                    PersistenceError::Corruption("JSON validation snapshot budget exceeded".into())
                })?;
            }
            let bytes = fs::read(&path)?;

            if bytes.len() as u64 != item.byte_length
                || blake3::hash(&bytes).as_bytes() != &item.digest
            {
                return Err(PersistenceError::Corruption(format!(
                    "snapshot digest mismatch: {}",
                    path.display()
                )));
            }

            let (snapshot, snapshot_created) = decode_snapshot(
                &bytes,
                generation,
                item.shard_id,
                self.limits,
                self.config.snapshot_max_bytes,
            )?;

            created_at = created_at.max(snapshot_created);

            if snapshot.last_sequence != item.last_sequence {
                return Err(PersistenceError::Corruption(
                    "manifest snapshot sequence mismatch".into(),
                ));
            }

            shards.push(snapshot);
        }

        shards.sort_by_key(|snapshot| snapshot.shard_id);

        for (expected, shard) in shards.iter().enumerate() {
            if shard.shard_id as usize != expected {
                return Err(PersistenceError::Corruption(
                    "checkpoint shard set is not contiguous".into(),
                ));
            }
        }

        Ok(Some(Checkpoint {
            generation,
            created_at: UnixMillis::new(created_at),
            shards,
        }))
    }

    pub(crate) fn publish(
        &self,
        generation: u64,
        shards: &[ShardCheckpoint],
    ) -> Result<(), PersistenceError> {
        if shards.is_empty() {
            return Err(PersistenceError::Failed(
                "checkpoint requires at least one shard".into(),
            ));
        }

        self.begin_round(generation)?;

        let mut files = Vec::with_capacity(shards.len());

        for shard in shards {
            files.push(self.write_shard_file(generation, shards.len() as u32, shard)?);
        }

        self.commit_round(generation, &files)
    }

    pub(super) fn generation_directory(&self, generation: u64) -> PathBuf {
        self.config
            .data_dir
            .join("checkpoints")
            .join(format!("{generation:020}"))
    }

    /// Creates the directory a checkpoint round writes its shard files into. A leftover
    /// directory of the same generation was never selected by `CURRENT`, so it is stale.
    pub(crate) fn begin_round(&self, generation: u64) -> Result<(), PersistenceError> {
        // Never clear the directory a published `CURRENT` points to.
        if self
            .current_generation()?
            .is_some_and(|current| current >= generation)
        {
            return Err(PersistenceError::Failed(format!(
                "checkpoint generation {generation} is already published"
            )));
        }

        let checkpoints = self.config.data_dir.join("checkpoints");

        if !checkpoints.exists() {
            crate::persistence::create_private_directory(&checkpoints)?;
            sync_directory(&self.config.data_dir)?;
        }
        let directory = self.generation_directory(generation);

        if let Some(last) = self
            .last_generation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            *last = (*last).max(generation);
        }

        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        fs::create_dir(&directory)?;
        // The round's directory entry must be durable before CURRENT can name it.
        sync_directory(&checkpoints)?;
        Ok(())
    }

    /// Removes the directory of a round that was abandoned before it was published. A
    /// published generation (or an older one) is never touched.
    pub(crate) fn discard_round(&self, generation: u64) -> Result<(), PersistenceError> {
        if self
            .current_generation()?
            .is_some_and(|current| current >= generation)
        {
            return Ok(());
        }

        match fs::remove_dir_all(self.generation_directory(generation)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Starts a streamed snapshot file for one shard of a round.
    pub(crate) fn shard_writer(
        &self,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        last_sequence: u64,
    ) -> Result<SnapshotWriter, PersistenceError> {
        SnapshotWriter::create(
            &self.generation_directory(generation),
            generation,
            shard_id,
            shard_count,
            last_sequence,
            self.config.snapshot_max_bytes,
        )
    }

    /// Writes an in-memory checkpoint as one shard file of a round.
    pub(crate) fn write_shard_file(
        &self,
        generation: u64,
        shard_count: u32,
        shard: &ShardCheckpoint,
    ) -> Result<ShardFile, PersistenceError> {
        let bytes = encode_snapshot(generation, shard_count, shard)?;

        if bytes.len() as u64 > self.config.snapshot_max_bytes {
            return Err(PersistenceError::Failed(
                "snapshot exceeds configured size limit".into(),
            ));
        }

        let file_name = format!("shard-{:04}.snap", shard.shard_id);

        #[cfg(test)]
        delay_shard_file(&self.config.data_dir, shard.shard_id);
        atomic_write_file(&self.generation_directory(generation), &file_name, &bytes)?;
        Ok(ShardFile {
            shard_id: shard.shard_id,
            last_sequence: shard.last_sequence,
            byte_length: bytes.len() as u64,
            digest: *blake3::hash(&bytes).as_bytes(),
            file_name,
        })
    }

    /// Copies shard `shard_id`'s file of published round `from` into round `to`, unchanged
    /// but for the generation in its header (and the checksums that cover it). A shard
    /// whose log failed cannot take a new file of memory that may hold refused writes; it
    /// hands each new round its last published one instead, so the other shards' rounds
    /// still publish and their logs are compacted. The copy keeps its snapshot point, so
    /// that shard's log is kept from there.
    pub(crate) fn carry_shard_file(
        &self,
        from: u64,
        to: u64,
        shard_id: u32,
    ) -> Result<ShardFile, PersistenceError> {
        use std::io::Read;

        let file_name = format!("shard-{shard_id:04}.snap");
        let source = self.generation_directory(from).join(&file_name);
        let mut input = File::open(&source)?;
        let length = input.metadata()?.len();
        let body_length = length
            .checked_sub((SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN) as u64)
            .ok_or_else(|| PersistenceError::Corruption("snapshot file is truncated".into()))?;
        let mut header = [0u8; SNAPSHOT_HEADER_LEN];

        input.read_exact(&mut header)?;
        if &header[..8] != SNAPSHOT_MAGIC
            || crc32c::crc32c(&header[..60]) != u32_at(&header, 60)?
            || u64_at(&header, 16)? != from
            || u32_at(&header, 24)? != shard_id
        {
            return Err(PersistenceError::Corruption(
                "carried snapshot header does not match its round".into(),
            ));
        }

        let directory = self.generation_directory(to);
        let temporary = directory.join(carry_temporary_name(&file_name));
        #[cfg(test)]
        if let Some((_, delay)) = CARRY_DELAYS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|(dir, _)| *dir == self.config.data_dir)
        {
            std::thread::sleep(*delay);
        }
        let copied = (|| -> Result<ShardFile, PersistenceError> {
            let mut output = crate::persistence::private_file_options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            let mut old_hash = blake3::Hasher::new();
            let mut new_hash = blake3::Hasher::new();

            old_hash.update(&header);
            header[16..24].copy_from_slice(&to.to_le_bytes());
            let checksum = crc32c::crc32c(&header[..60]);
            header[60..64].copy_from_slice(&checksum.to_le_bytes());
            new_hash.update(&header);
            output.write_all(&header)?;

            let mut buffer = vec![0u8; SNAPSHOT_READ_BUFFER];
            let mut left = body_length;

            while left > 0 {
                let take = left.min(buffer.len() as u64) as usize;

                input.read_exact(&mut buffer[..take])?;
                old_hash.update(&buffer[..take]);
                new_hash.update(&buffer[..take]);
                output.write_all(&buffer[..take])?;
                left -= take as u64;
            }

            let mut footer = [0u8; SNAPSHOT_FOOTER_LEN];

            input.read_exact(&mut footer)?;
            if &footer[..8] != SNAPSHOT_FOOTER_MAGIC
                || crc32c::crc32c(&footer[..56]) != u32_at(&footer, 56)?
                || footer[24..56] != *old_hash.finalize().as_bytes()
            {
                return Err(PersistenceError::Corruption(
                    "carried snapshot does not match its digest".into(),
                ));
            }

            footer[24..56].copy_from_slice(new_hash.finalize().as_bytes());
            let checksum = crc32c::crc32c(&footer[..56]);
            footer[56..60].copy_from_slice(&checksum.to_le_bytes());
            new_hash.update(&footer);
            output.write_all(&footer)?;
            output.sync_all()?;
            drop(output);
            fs::rename(&temporary, directory.join(&file_name))?;
            sync_directory(&directory)?;
            Ok(ShardFile {
                shard_id,
                last_sequence: u64_at(&header, 32)?,
                byte_length: length,
                digest: *new_hash.finalize().as_bytes(),
                file_name: file_name.clone(),
            })
        })();

        // A failed copy leaves no partial file behind (the round is retried with a new one).
        if copied.is_err() {
            let _ = fs::remove_file(&temporary);
        }

        copied
    }

    /// A file holding no entries at sequence 0, for a round of a shard whose log failed when
    /// no round was ever published: its log holds every record from the first, so it
    /// replays all of them over the empty file.
    pub(crate) fn empty_shard_file(
        &self,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
    ) -> Result<ShardFile, PersistenceError> {
        let shard = ShardCheckpoint {
            shard_id,
            last_sequence: 0,
            entries: Vec::new(),
        };
        let bytes = encode_snapshot(generation, shard_count, &shard)?;
        let file_name = format!("shard-{shard_id:04}.snap");
        let directory = self.generation_directory(generation);

        write_through_temporary(
            &directory.join(carry_temporary_name(&file_name)),
            &directory.join(&file_name),
            &bytes,
        )?;
        sync_directory(&directory)?;
        Ok(ShardFile {
            shard_id,
            last_sequence: 0,
            byte_length: bytes.len() as u64,
            digest: *blake3::hash(&bytes).as_bytes(),
            file_name,
        })
    }

    /// Commits a round: the manifest, then `CURRENT`. Only a complete set of shard files
    /// may be committed.
    pub(crate) fn commit_round(
        &self,
        generation: u64,
        files: &[ShardFile],
    ) -> Result<(), PersistenceError> {
        #[cfg(test)]
        if let Some((_, left)) = FAILING_COMMITS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter_mut()
            .find(|(dir, left)| *dir == self.config.data_dir && *left > 0)
        {
            *left -= 1;
            return Err(PersistenceError::Failed("injected commit failure".into()));
        }
        let directory = self.generation_directory(generation);
        let manifest: Vec<ManifestItem> = files
            .iter()
            .map(|file| ManifestItem {
                shard_id: file.shard_id,
                last_sequence: file.last_sequence,
                byte_length: file.byte_length,
                digest: file.digest,
                file_name: file.file_name.clone(),
            })
            .collect();
        let manifest_bytes = encode_manifest(generation, &manifest)?;

        super::crash::hit(super::crash::CrashSite::Manifest);
        atomic_write_file(&directory, "MANIFEST", &manifest_bytes)?;
        sync_directory(&directory)?;
        let current = encode_current(generation);

        atomic_write_file(&self.config.data_dir, "CURRENT", &current)?;
        #[cfg(test)]
        if let Some(error) = super::crash::take_io_fault(super::crash::CrashSite::Manifest) {
            return Err(error.into());
        }
        sync_directory(&self.config.data_dir)?;
        retain_previous_generations(&self.config.data_dir.join("checkpoints"), generation);
        Ok(())
    }

    pub(crate) fn persist_clock(&self, now: UnixMillis) -> Result<(), PersistenceError> {
        if self.config.profile == DurabilityProfile::Memory {
            return Ok(());
        }

        // Every shard persists the clock: serialize the read-modify-write and its shared temp
        // file, or two writers can lose the rename (ENOENT) or move the floor backwards.
        static CLOCK_WRITE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serialized = CLOCK_WRITE
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        crate::persistence::create_private_directory(&self.config.data_dir)?;
        let floor = self.load_clock()?.unwrap_or(UnixMillis::new(0));
        let value = UnixMillis::new(floor.get().max(now.get()));

        atomic_write_file(&self.config.data_dir, "CLOCK", &encode_clock(value.get()))?;
        Ok(())
    }

    pub(crate) fn load_clock(&self) -> Result<Option<UnixMillis>, PersistenceError> {
        let path = self.config.data_dir.join("CLOCK");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        Ok(Some(UnixMillis::new(decode_clock(&bytes)?)))
    }

    pub(crate) fn recovery_clock_floor(
        &self,
        checkpoint_created_at: UnixMillis,
    ) -> Result<UnixMillis, PersistenceError> {
        let persisted = self.load_clock()?.unwrap_or(UnixMillis::new(0));

        Ok(UnixMillis::new(
            persisted.get().max(checkpoint_created_at.get()),
        ))
    }
}

//! Segment preallocation: the background zero-fill thread and the platform file helpers it uses.

use super::*;

struct PrefillJob {
    file: File,
    start: u64,
    length: u64,
    sync_error: Option<Arc<SyncError>>,
    #[cfg(test)]
    fail_sync: bool,
}

#[derive(Default)]
struct PrefillState {
    job: Option<PrefillJob>,
    /// Outcome of the last job; `None` while it runs.
    result: Option<bool>,
    stop: bool,
}

struct PrefillShared {
    state: Mutex<PrefillState>,
    changed: std::sync::Condvar,
}

/// Extends a segment ahead of the write cursor and fills the new space with zeros on a helper thread,
/// so the shard never waits for it in steady state. One long-lived thread and one shared slot:
/// asking for a chunk allocates nothing on the shard's write path.
#[derive(Default)]
pub(super) struct Prefill {
    shared: Option<Arc<PrefillShared>>,
    /// Target size of the chunk being filled, if one is.
    pending: Option<u64>,
    /// Where a failed sync of a fill is reported: it is a sync of the live segment, so it
    /// also covers record pages (see `WalSink::sync_failed`).
    sync_error: Option<Arc<SyncError>>,
}

impl Drop for Prefill {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            shared.state.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
            shared.changed.notify_all();
        }
    }
}

impl Prefill {
    pub(super) fn reporting_to(sync_error: Arc<SyncError>) -> Self {
        Self {
            shared: None,
            pending: None,
            sync_error: Some(sync_error),
        }
    }

    /// Makes `[.., end)` ready (blocking only if the helper lags or this is the first chunk) and
    /// asks for the next chunk once less than `WAL_PREALLOC_AHEAD` is left.
    pub(super) fn reserve(
        &mut self,
        file: &File,
        prealloc_end: &mut u64,
        enabled: &mut bool,
        end: u64,
        cap: u64,
        write_pos: u64,
    ) {
        self.poll(prealloc_end, enabled, false);

        if end > *prealloc_end && *enabled {
            self.poll(prealloc_end, enabled, true);
        }

        if end > *prealloc_end && *enabled {
            self.request(file, *prealloc_end, end, cap, enabled);
            self.poll(prealloc_end, enabled, true);
        }

        if *enabled
            && self.pending.is_none()
            && *prealloc_end < cap
            && prealloc_end.saturating_sub(write_pos) < WAL_PREALLOC_AHEAD
        {
            self.request(file, *prealloc_end, *prealloc_end + 1, cap, enabled);
        }
    }

    fn request(&mut self, file: &File, start: u64, min_end: u64, cap: u64, enabled: &mut bool) {
        let target = start
            .max(min_end)
            .saturating_add(WAL_PREALLOC_CHUNK)
            .min(cap)
            .max(min_end);
        // A descriptor of its own (not a `dup`, which shares the error cursor): an error the
        // fill's sync reports is then also reported to the shard's next sync of the file.
        let Ok(clone) = reopen(file) else {
            *enabled = false;
            return;
        };

        if self.shared.is_none() {
            let shared = Arc::new(PrefillShared {
                state: Mutex::new(PrefillState::default()),
                changed: std::sync::Condvar::new(),
            });
            let worker = Arc::clone(&shared);

            if std::thread::Builder::new()
                .name("ruvio-wal-prefill".into())
                .spawn(move || prefill_thread(&worker))
                .is_err()
            {
                *enabled = false;
                return;
            }

            self.shared = Some(shared);
        }

        let Some(shared) = &self.shared else {
            return;
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        state.job = Some(PrefillJob {
            file: clone,
            start,
            length: target - start,
            sync_error: self.sync_error.clone(),
            #[cfg(test)]
            fail_sync: super::crash::take_io_fault(super::crash::CrashSite::PrefillSync).is_some(),
        });
        state.result = None;
        shared.changed.notify_all();
        self.pending = Some(target);
    }

    fn poll(&mut self, prealloc_end: &mut u64, enabled: &mut bool, block: bool) {
        let (Some(target), Some(shared)) = (self.pending, &self.shared) else {
            return;
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        let result = loop {
            if let Some(result) = state.result.take() {
                break result;
            }

            if !block {
                return;
            }

            state = shared
                .changed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        };

        drop(state);
        self.pending = None;

        if result {
            *prealloc_end = target;
        } else {
            // Not supported here or the disk refused: write as the file grows, like before.
            *enabled = false;
        }
    }

    /// Waits for a running fill so the caller may truncate the file.
    pub(super) fn finish(&mut self, prealloc_end: &mut u64) {
        let mut enabled = true;

        self.poll(prealloc_end, &mut enabled, true);
    }

    pub(super) fn reset(&mut self) {
        self.pending = None;
    }
}

fn prefill_thread(shared: &PrefillShared) {
    let zeros = vec![0u8; 1 << 20];

    loop {
        let job = {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

            loop {
                if state.stop {
                    return;
                }

                if let Some(job) = state.job.take() {
                    break job;
                }

                state = shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
            }
        };
        let ok = match fill_with_zeros(&job, &zeros) {
            Ok(filled) => filled,
            Err(error) => {
                if let Some(sync_error) = &job.sync_error {
                    sync_error.report_first(error);
                }
                false
            }
        };
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());

        state.result = Some(ok);
        shared.changed.notify_all();
    }
}

/// `fallocate`, then really write zeros so the blocks are "written" extents, then sync them once.
/// `Ok(false)`: preallocation is unsupported or refused. `Err`: the sync failed.
fn fill_with_zeros(job: &PrefillJob, zeros: &[u8]) -> Result<bool, String> {
    let (file, start, length) = (&job.file, job.start, job.length);

    if !preallocate(file, start, length) {
        return Ok(false);
    }

    // Unwritten (fallocate-only) extents make the first append's fdatasync wait on an ext4
    // extent-conversion commit, so the blocks are written here on the helper thread.
    let mut done = 0u64;

    while done < length {
        let piece = usize::try_from((length - done).min(zeros.len() as u64)).unwrap_or(zeros.len());

        if write_all_at(file, &zeros[..piece], start + done).is_err() {
            return Ok(false);
        }

        done += piece as u64;
    }

    #[cfg(test)]
    if job.fail_sync {
        return Err("injected prefill sync failure".into());
    }

    file.sync_data()
        .map(|()| true)
        .map_err(|error| error.to_string())
}

/// A new open file description of `file`, so its syncs keep their own error cursor.
#[cfg(target_os = "linux")]
fn reopen(file: &File) -> io::Result<File> {
    use std::{
        io::Write,
        os::{fd::AsRawFd, unix::ffi::OsStrExt},
    };

    // Formatted on the stack: a fill is requested from the write path, which allocates nothing.
    let mut path = [0u8; 32];
    let mut cursor = io::Cursor::new(&mut path[..]);
    write!(cursor, "/proc/self/fd/{}", file.as_raw_fd())?;
    let length = cursor.position() as usize;

    OpenOptions::new()
        .write(true)
        .open(std::ffi::OsStr::from_bytes(&path[..length]))
}

#[cfg(not(target_os = "linux"))]
fn reopen(file: &File) -> io::Result<File> {
    file.try_clone()
}

#[cfg(unix)]
pub(super) fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buffer, offset)
}

#[cfg(windows)]
pub(super) fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    let mut done = 0;

    while done < buffer.len() {
        match std::os::windows::fs::FileExt::seek_write(
            file,
            &buffer[done..],
            offset + done as u64,
        )? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            count => done += count,
        }
    }

    Ok(())
}

/// Extend `file` by `length` bytes from `offset` (size grows too). Linux only; `false` means unsupported or failed.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
pub(super) fn preallocate(file: &File, offset: u64, length: u64) -> bool {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn fallocate(fd: i32, mode: i32, offset: i64, length: i64) -> i32;
    }

    let (Ok(offset), Ok(length)) = (i64::try_from(offset), i64::try_from(length)) else {
        return false;
    };

    // SAFETY: `fallocate` only reads its integer arguments; the descriptor is valid for the
    // whole call because `file` is borrowed.
    unsafe { fallocate(file.as_raw_fd(), 0, offset, length) == 0 }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn preallocate(_file: &File, _offset: u64, _length: u64) -> bool {
    false
}

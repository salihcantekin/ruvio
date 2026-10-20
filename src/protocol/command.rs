use super::*;

impl<'a> Command<'a> {
    pub fn set(key: Bytes<'a>, value: Bytes<'a>) -> Self {
        Self::Set(key, value, SetOptions::default())
    }

    pub fn into_owned(self) -> Command<'static> {
        self.into_owned_measured().0
    }

    /// `into_owned`, plus how many argument bytes it copied.
    pub fn into_owned_measured(self) -> (Command<'static>, usize) {
        let copied = std::cell::Cell::new(0usize);
        let owned = |bytes: Bytes<'_>| -> Bytes<'static> {
            copied.set(copied.get().saturating_add(bytes.len()));
            Cow::Owned(bytes.into_owned())
        };
        let owned_vec = |values: Vec<Bytes<'_>>| -> Vec<Bytes<'static>> {
            values.into_iter().map(owned).collect()
        };
        let owned_lex = |bound: LexBound<'_>| -> LexBound<'static> {
            match bound {
                LexBound::NegInf => LexBound::NegInf,
                LexBound::PosInf => LexBound::PosInf,
                LexBound::Inclusive(bytes) => LexBound::Inclusive(owned(bytes)),
                LexBound::Exclusive(bytes) => LexBound::Exclusive(owned(bytes)),
            }
        };

        let command = match self {
            Self::Ping(message) => Command::Ping(message.map(owned)),
            Self::CommandMeta { kind, names } => Command::CommandMeta {
                kind,
                names: owned_vec(names),
            },
            Self::Wait {
                replicas,
                timeout_ms,
            } => Command::Wait {
                replicas,
                timeout_ms,
            },
            Self::Ruvio(message) => Command::Ruvio(message.map(owned)),
            Self::Hello {
                protocol,
                auth,
                client_name,
            } => Command::Hello {
                protocol,
                auth: auth.map(|(username, password)| (owned(username), owned(password))),
                client_name: client_name.map(owned),
            },
            Self::ClientGetName => Command::ClientGetName,
            Self::ClientId => Command::ClientId,
            Self::ClientList { ids } => Command::ClientList { ids },
            Self::ClientInfo => Command::ClientInfo,
            Self::ClientKill {
                id,
                addr,
                skip_me,
                legacy,
            } => Command::ClientKill {
                id,
                addr: addr.map(owned),
                skip_me,
                legacy,
            },
            Self::Slowlog(op) => Command::Slowlog(op),
            Self::LatencyLatest => Command::LatencyLatest,
            Self::Echo(message) => Command::Echo(owned(message)),
            Self::Object { kind, key } => Command::Object {
                kind,
                key: owned(key),
            },
            Self::MemoryUsage(key) => Command::MemoryUsage(owned(key)),
            Self::ClientSetName(name) => Command::ClientSetName(owned(name)),
            Self::ClientTracking(on) => Command::ClientTracking(on),
            Self::ShardPublish { channel, message } => Command::ShardPublish {
                channel: owned(channel),
                message: owned(message),
            },
            Self::ShardSubscribe(channels) => Command::ShardSubscribe(owned_vec(channels)),
            Self::ShardUnsubscribe(channels) => Command::ShardUnsubscribe(owned_vec(channels)),
            Self::Publish { channel, message } => Command::Publish {
                channel: owned(channel),
                message: owned(message),
            },
            Self::Subscribe(channels) => Command::Subscribe(owned_vec(channels)),
            Self::Unsubscribe(channels) => Command::Unsubscribe(owned_vec(channels)),
            Self::PSubscribe(patterns) => Command::PSubscribe(owned_vec(patterns)),
            Self::PUnsubscribe(patterns) => Command::PUnsubscribe(owned_vec(patterns)),
            Self::ClusterSlots => Command::ClusterSlots,
            Self::ClusterNodes => Command::ClusterNodes,
            Self::ClusterInfo => Command::ClusterInfo,
            Self::ClusterShards => Command::ClusterShards,
            Self::ClusterMyId => Command::ClusterMyId,
            Self::ClusterKeySlot(key) => Command::ClusterKeySlot(owned(key)),
            Self::ReadOnly => Command::ReadOnly,
            Self::ReadWrite => Command::ReadWrite,
            Self::ConfigGet(pattern) => Command::ConfigGet(owned(pattern)),
            Self::ConfigSet(pairs) => Command::ConfigSet(
                pairs
                    .into_iter()
                    .map(|(name, value)| (owned(name), owned(value)))
                    .collect(),
            ),
            Self::ConfigRewrite => Command::ConfigRewrite,
            Self::MoveSlots(target) => Command::MoveSlots(target),
            Self::Topology(map) => Command::Topology(map.map(owned)),
            Self::ReplicaOf { primary, force } => Command::ReplicaOf {
                primary: primary.map(|(host, port)| (owned(host), owned(port))),
                force,
            },
            Self::Role => Command::Role,
            Self::Info(message) => Command::Info(message.map(owned)),
            Self::Save => Command::Save,
            Self::Bgsave => Command::Bgsave,
            Self::Get(key) => Command::Get(owned(key)),
            Self::JsonGet { key, path } => Command::JsonGet {
                key: owned(key),
                path: owned(path),
            },
            Self::JsonMGet { keys, path } => Command::JsonMGet {
                keys: owned_vec(keys),
                path: owned(path),
            },
            Self::JsonSetIf {
                key,
                path,
                value,
                must_exist,
            } => Command::JsonSetIf {
                key: owned(key),
                path: owned(path),
                value: owned(value),
                must_exist,
            },
            Self::JsonSet { key, path, value } => Command::JsonSet {
                key: owned(key),
                path: owned(path),
                value: owned(value),
            },
            Self::JsonMerge { key, path, value } => Command::JsonMerge {
                key: owned(key),
                path: owned(path),
                value: owned(value),
            },
            Self::JsonDel { key, path } => Command::JsonDel {
                key: owned(key),
                path: owned(path),
            },
            Self::JsonIndex { op, args } => Command::JsonIndex {
                op,
                args: owned_vec(args),
            },
            Self::JsonOp {
                op,
                key,
                path,
                args,
            } => Command::JsonOp {
                op,
                key: owned(key),
                path: owned(path),
                args: owned_vec(args),
            },
            Self::Lease { key, ttl_ms } => Command::Lease {
                key: owned(key),
                ttl_ms,
            },
            Self::Release { key, token } => Command::Release {
                key: owned(key),
                token,
            },
            Self::Renew { key, token, ttl_ms } => Command::Renew {
                key: owned(key),
                token,
                ttl_ms,
            },
            Self::SetVersioned {
                key,
                value,
                expected,
            } => Command::SetVersioned {
                key: owned(key),
                value: owned(value),
                expected,
            },
            Self::Changes(after) => Command::Changes(after),
            Self::ChangesSubscribe(subscription) => Command::ChangesSubscribe(subscription),
            Self::Limit {
                key,
                max,
                window_ms,
                mode,
            } => Command::Limit {
                key: owned(key),
                max,
                window_ms,
                mode,
            },
            Self::Once { key, ttl_ms } => Command::Once {
                key: owned(key),
                ttl_ms,
            },
            Self::IdempotencyBegin {
                key,
                fingerprint,
                owner,
                ttl_ms,
            } => Command::IdempotencyBegin {
                key: owned(key),
                fingerprint: owned(fingerprint),
                owner: owned(owner),
                ttl_ms,
            },
            Self::IdempotencyComplete {
                key,
                fingerprint,
                owner,
                result,
            } => Command::IdempotencyComplete {
                key: owned(key),
                fingerprint: owned(fingerprint),
                owner: owned(owner),
                result: owned(result),
            },
            Self::SetIfEq {
                key,
                value,
                expected,
                options,
            } => Command::SetIfEq {
                key: owned(key),
                value: owned(value),
                expected: owned(expected),
                options,
            },
            Self::SetIfCmp {
                key,
                value,
                number,
                greater,
                options,
            } => Command::SetIfCmp {
                key: owned(key),
                value: owned(value),
                number,
                greater,
                options,
            },
            Self::DelIfEq { key, expected } => Command::DelIfEq {
                key: owned(key),
                expected: owned(expected),
            },
            Self::ListPushCapped {
                key,
                cap,
                values,
                back,
            } => Command::ListPushCapped {
                key: owned(key),
                cap,
                values: owned_vec(values),
                back,
            },
            Self::Take { key, count } => Command::Take {
                key: owned(key),
                count,
            },
            Self::Transfer {
                source,
                destination,
                amount,
                max,
            } => Command::Transfer {
                source: owned(source),
                destination: owned(destination),
                amount,
                max,
            },
            Self::HashTransfer {
                key,
                from,
                to,
                amount,
                max,
            } => Command::HashTransfer {
                key: owned(key),
                from: owned(from),
                to: owned(to),
                amount,
                max,
            },
            Self::GetEx { key, ttl_ms } => Command::GetEx {
                key: owned(key),
                ttl_ms,
            },
            Self::Set(key, value, options) => Command::Set(owned(key), owned(value), options),
            Self::Increment(key, delta) => Command::Increment(owned(key), delta),
            Self::IncrementCapped { key, delta, limit } => Command::IncrementCapped {
                key: owned(key),
                delta,
                limit,
            },
            Self::Semaphore { key, max, ttl_ms } => Command::Semaphore {
                key: owned(key),
                max,
                ttl_ms,
            },
            Self::MGet(keys) => Command::MGet(owned_vec(keys)),
            Self::MSet(pairs) => Command::MSet(
                pairs
                    .into_iter()
                    .map(|(key, value)| (owned(key), owned(value)))
                    .collect(),
            ),
            Self::GetSet(key, value) => Command::GetSet(owned(key), owned(value)),
            Self::Append(key, value) => Command::Append(owned(key), owned(value)),
            Self::SetNx(key, value) => Command::SetNx(owned(key), owned(value)),
            Self::GetDel(key) => Command::GetDel(owned(key)),
            Self::MSetNx(pairs) => Command::MSetNx(
                pairs
                    .into_iter()
                    .map(|(key, value)| (owned(key), owned(value)))
                    .collect(),
            ),
            Self::IncrementFloat(key, delta) => Command::IncrementFloat(owned(key), delta),
            Self::GetRange { key, start, end } => Command::GetRange {
                key: owned(key),
                start,
                end,
            },
            Self::SetRange { key, offset, value } => Command::SetRange {
                key: owned(key),
                offset,
                value: owned(value),
            },
            Self::SetBit { key, offset, value } => Command::SetBit {
                key: owned(key),
                offset,
                value,
            },
            Self::GetBit(key, offset) => Command::GetBit(owned(key), offset),
            Self::BitField {
                key,
                ops,
                read_only_name,
            } => Command::BitField {
                key: owned(key),
                ops,
                read_only_name,
            },
            Self::BitCount { key, range } => Command::BitCount {
                key: owned(key),
                range,
            },
            Self::BitPos { key, bit, range } => Command::BitPos {
                key: owned(key),
                bit,
                range,
            },
            Self::FillGet {
                key,
                fill_ms,
                wait_ms,
            } => Command::FillGet {
                key: owned(key),
                fill_ms,
                wait_ms,
            },
            Self::FillSet {
                key,
                value,
                ttl_ms,
                token,
                swr_ms,
            } => Command::FillSet {
                key: owned(key),
                value: owned(value),
                ttl_ms,
                token,
                swr_ms,
            },
            Self::PfAdd { key, elements } => Command::PfAdd {
                key: owned(key),
                elements: elements.into_iter().map(owned).collect(),
            },
            Self::PfCount(keys) => {
                Command::PfCount(keys.into_iter().map(owned).collect::<Vec<_>>().into())
            }
            Self::PfMerge {
                destination,
                sources,
            } => Command::PfMerge {
                destination: owned(destination),
                sources: sources.into_iter().map(owned).collect(),
            },
            Self::BitOp {
                operation,
                destination,
                keys,
            } => Command::BitOp {
                operation,
                destination: owned(destination),
                keys: keys.into_iter().map(owned).collect(),
            },
            Self::StrLen(key) => Command::StrLen(owned(key)),
            Self::Exists(keys) => Command::Exists(owned_vec(keys.into_vec()).into()),
            Self::Type(key) => Command::Type(owned(key)),
            Self::Rename {
                source,
                destination,
            } => Command::Rename {
                source: owned(source),
                destination: owned(destination),
            },
            Self::Keys(pattern) => Command::Keys(owned(pattern)),
            Self::RandomKey => Command::RandomKey,
            Self::Touch(keys) => Command::Touch(owned_vec(keys.into_vec()).into()),
            Self::Dump(key) => Command::Dump(owned(key)),
            Self::Restore {
                key,
                ttl,
                payload,
                replace,
                absolute,
            } => Command::Restore {
                key: owned(key),
                ttl,
                payload: owned(payload),
                replace,
                absolute,
            },
            Self::RenameNx {
                source,
                destination,
            } => Command::RenameNx {
                source: owned(source),
                destination: owned(destination),
            },
            Self::ExpireTime { key, millis } => Command::ExpireTime {
                key: owned(key),
                millis,
            },
            Self::Copy {
                source,
                destination,
                replace,
            } => Command::Copy {
                source: owned(source),
                destination: owned(destination),
                replace,
            },
            Self::Scan {
                cursor,
                pattern,
                count,
                type_name,
                slot,
            } => Command::Scan {
                cursor,
                pattern: pattern.map(owned),
                count,
                type_name: type_name.map(owned),
                slot,
            },
            Self::DbSize => Command::DbSize,
            Self::Del(keys) => Command::Del(owned_vec(keys.into_vec()).into()),
            Self::ListPush(key, values) => Command::ListPush(owned(key), owned_vec(values)),
            Self::ListPushBack(key, values) => Command::ListPushBack(owned(key), owned_vec(values)),
            Self::ListPop { key, count } => Command::ListPop {
                key: owned(key),
                count,
            },
            Self::ListPopBack { key, count } => Command::ListPopBack {
                key: owned(key),
                count,
            },
            Self::ListLen(key) => Command::ListLen(owned(key)),
            Self::ListIndex(key, index) => Command::ListIndex(owned(key), index),
            Self::ListTrim(key, start, stop) => Command::ListTrim(owned(key), start, stop),
            Self::ListBlock {
                keys,
                timeout_ms,
                back,
            } => Command::ListBlock {
                keys: owned_vec(keys),
                timeout_ms,
                back,
            },
            Self::ListRange(key, start, stop) => Command::ListRange(owned(key), start, stop),
            Self::ListInsert {
                key,
                before,
                pivot,
                value,
            } => Command::ListInsert {
                key: owned(key),
                before,
                pivot: owned(pivot),
                value: owned(value),
            },
            Self::ListSet { key, index, value } => Command::ListSet {
                key: owned(key),
                index,
                value: owned(value),
            },
            Self::ListRemove { key, count, value } => Command::ListRemove {
                key: owned(key),
                count,
                value: owned(value),
            },
            Self::ListPos {
                key,
                element,
                rank,
                count,
                max_len,
            } => Command::ListPos {
                key: owned(key),
                element: owned(element),
                rank,
                count,
                max_len,
            },
            Self::ListPushX { key, values, back } => Command::ListPushX {
                key: owned(key),
                values: owned_vec(values),
                back,
            },
            Self::ListMove {
                source,
                destination,
                from_left,
                to_left,
                block,
            } => Command::ListMove {
                source: owned(source),
                destination: owned(destination),
                from_left,
                to_left,
                block,
            },
            Self::ListMPop {
                keys,
                from_left,
                count,
                block,
            } => Command::ListMPop {
                keys: owned_vec(keys),
                from_left,
                count,
                block,
            },
            Self::SetAdd(key, members) => Command::SetAdd(owned(key), owned_vec(members)),
            Self::SetRemove(key, members) => Command::SetRemove(owned(key), owned_vec(members)),
            Self::SetContains(key, member) => Command::SetContains(owned(key), owned(member)),
            Self::SetCard(key) => Command::SetCard(owned(key)),
            Self::SetMembers(key) => Command::SetMembers(owned(key)),
            Self::SetPop { key, count } => Command::SetPop {
                key: owned(key),
                count,
            },
            Self::SetRandom { key, count } => Command::SetRandom {
                key: owned(key),
                count,
            },
            Self::SetAlgebra {
                keys,
                op,
                destination,
            } => Command::SetAlgebra {
                keys: owned_vec(keys),
                op,
                destination: destination.map(owned),
            },
            Self::SetMove {
                source,
                destination,
                member,
            } => Command::SetMove {
                source: owned(source),
                destination: owned(destination),
                member: owned(member),
            },
            Self::HashSet { key, pairs } => Command::HashSet {
                key: owned(key),
                pairs: pairs
                    .into_iter()
                    .map(|(field, value)| (owned(field), owned(value)))
                    .collect(),
            },
            Self::HashMSet { key, pairs } => Command::HashMSet {
                key: owned(key),
                pairs: pairs
                    .into_iter()
                    .map(|(field, value)| (owned(field), owned(value)))
                    .collect(),
            },
            Self::HashDelete { key, fields } => Command::HashDelete {
                key: owned(key),
                fields: owned_vec(fields),
            },
            Self::HashGet { key, field } => Command::HashGet {
                key: owned(key),
                field: owned(field),
            },
            Self::HashMGet { key, fields } => Command::HashMGet {
                key: owned(key),
                fields: owned_vec(fields),
            },
            Self::HashLen(key) => Command::HashLen(owned(key)),
            Self::HashGetAll(key) => Command::HashGetAll(owned(key)),
            Self::HashExists { key, field } => Command::HashExists {
                key: owned(key),
                field: owned(field),
            },
            Self::HashKeys(key) => Command::HashKeys(owned(key)),
            Self::HashValues(key) => Command::HashValues(owned(key)),
            Self::HashIncrement { key, field, delta } => Command::HashIncrement {
                key: owned(key),
                field: owned(field),
                delta,
            },
            Self::HashSetNx { key, field, value } => Command::HashSetNx {
                key: owned(key),
                field: owned(field),
                value: owned(value),
            },
            Self::HashStrLen { key, field } => Command::HashStrLen {
                key: owned(key),
                field: owned(field),
            },
            Self::HashScan {
                key,
                cursor,
                pattern,
                count,
            } => Command::HashScan {
                key: owned(key),
                cursor,
                pattern: pattern.map(owned),
                count,
            },
            Self::CollectionScan {
                kind,
                key,
                cursor,
                pattern,
                count,
            } => Command::CollectionScan {
                kind,
                key: owned(key),
                cursor,
                pattern: pattern.map(owned),
                count,
            },
            Self::SortedSetAdd {
                key,
                pairs,
                options,
            } => Command::SortedSetAdd {
                key: owned(key),
                pairs: pairs
                    .into_iter()
                    .map(|(score, member)| (score, owned(member)))
                    .collect(),
                options,
            },
            Self::SortedSetRange {
                key,
                start,
                stop,
                with_scores,
                reverse,
            } => Command::SortedSetRange {
                key: owned(key),
                start,
                stop,
                with_scores,
                reverse,
            },
            Self::SortedSetRangeByScore {
                key,
                min,
                max,
                with_scores,
                reverse,
                limit,
                via_zrange,
            } => Command::SortedSetRangeByScore {
                key: owned(key),
                min,
                max,
                with_scores,
                reverse,
                limit,
                via_zrange,
            },
            Self::SortedSetRangeByLex {
                key,
                min,
                max,
                with_scores,
                reverse,
                limit,
                invocation,
            } => Command::SortedSetRangeByLex {
                key: owned(key),
                min: owned_lex(min),
                max: owned_lex(max),
                with_scores,
                reverse,
                limit,
                invocation,
            },
            Self::SortedSetRemove { key, members } => Command::SortedSetRemove {
                key: owned(key),
                members: owned_vec(members),
            },
            Self::SortedSetIncr { key, delta, member } => Command::SortedSetIncr {
                key: owned(key),
                delta,
                member: owned(member),
            },
            Self::SortedSetCard(key) => Command::SortedSetCard(owned(key)),
            Self::SetMIsMember { key, members } => Command::SetMIsMember {
                key: owned(key),
                members: owned_vec(members),
            },
            Self::SetInterCard { keys, limit } => Command::SetInterCard {
                keys: owned_vec(keys),
                limit,
            },
            Self::SortedSetCount { key, min, max } => Command::SortedSetCount {
                key: owned(key),
                min,
                max,
            },
            Self::SortedSetLexCount { key, min, max } => Command::SortedSetLexCount {
                key: owned(key),
                min: owned_lex(min),
                max: owned_lex(max),
            },
            Self::SortedSetMScore { key, members } => Command::SortedSetMScore {
                key: owned(key),
                members: owned_vec(members),
            },
            Self::SortedSetAlgebra {
                op,
                keys,
                weights,
                aggregate,
                with_scores,
                destination,
            } => Command::SortedSetAlgebra {
                op,
                keys: owned_vec(keys),
                weights,
                aggregate,
                with_scores,
                destination: destination.map(owned),
            },
            Self::SortedSetRandMember {
                key,
                count,
                with_scores,
            } => Command::SortedSetRandMember {
                key: owned(key),
                count,
                with_scores,
            },
            Self::HashRandField {
                key,
                count,
                with_values,
            } => Command::HashRandField {
                key: owned(key),
                count,
                with_values,
            },
            Self::HashIncrementFloat { key, field, delta } => Command::HashIncrementFloat {
                key: owned(key),
                field: owned(field),
                delta,
            },
            Self::SortedSetScore { key, member } => Command::SortedSetScore {
                key: owned(key),
                member: owned(member),
            },
            Self::SortedSetRank {
                key,
                member,
                reverse,
            } => Command::SortedSetRank {
                key: owned(key),
                member: owned(member),
                reverse,
            },
            Self::SortedSetPop { key, count, max } => Command::SortedSetPop {
                key: owned(key),
                count,
                max,
            },
            Self::SortedSetPopByScore {
                key,
                min,
                max,
                count,
                with_scores,
            } => Command::SortedSetPopByScore {
                key: owned(key),
                min,
                max,
                count,
                with_scores,
            },
            Self::SortedSetMoveDue {
                source,
                destination,
                max,
                count,
            } => Command::SortedSetMoveDue {
                source: owned(source),
                destination: owned(destination),
                max,
                count,
            },
            Self::SortedSetBlockPop {
                keys,
                timeout_ms,
                max,
            } => Command::SortedSetBlockPop {
                keys: owned_vec(keys),
                timeout_ms,
                max,
            },
            Self::SortedSetRemoveRange { key, range } => Command::SortedSetRemoveRange {
                key: owned(key),
                range: match range {
                    SortedRemoveRange::Rank(start, stop) => SortedRemoveRange::Rank(start, stop),
                    SortedRemoveRange::Score(min, max) => SortedRemoveRange::Score(min, max),
                    SortedRemoveRange::Lex(min, max) => {
                        SortedRemoveRange::Lex(owned_lex(min), owned_lex(max))
                    }
                },
            },
            Self::HashExpire {
                key,
                time,
                condition,
                fields,
                name,
            } => Command::HashExpire {
                key: owned(key),
                time,
                condition,
                fields: owned_vec(fields),
                name,
            },
            Self::HashTtl { key, reply, fields } => Command::HashTtl {
                key: owned(key),
                reply,
                fields: owned_vec(fields),
            },
            Self::HashPersist { key, fields } => Command::HashPersist {
                key: owned(key),
                fields: owned_vec(fields),
            },
            Self::GeoAdd {
                key,
                members,
                options,
            } => Command::GeoAdd {
                key: owned(key),
                members: members
                    .into_iter()
                    .map(|(longitude, latitude, member)| (longitude, latitude, owned(member)))
                    .collect(),
                options,
            },
            Self::GeoDist {
                key,
                from,
                to,
                unit,
            } => Command::GeoDist {
                key: owned(key),
                from: owned(from),
                to: owned(to),
                unit,
            },
            Self::GeoPos { key, members } => Command::GeoPos {
                key: owned(key),
                members: owned_vec(members),
            },
            Self::GeoHash { key, members } => Command::GeoHash {
                key: owned(key),
                members: owned_vec(members),
            },
            Self::GeoSearch {
                key,
                center,
                area,
                unit,
                descending,
                ordered,
                count,
                any,
                with_dist,
                with_hash,
                with_coord,
                store,
                query,
            } => Command::GeoSearch {
                key: owned(key),
                center: match center {
                    GeoCenter::Member(member) => GeoCenter::Member(owned(member)),
                    GeoCenter::Coordinates(longitude, latitude) => {
                        GeoCenter::Coordinates(longitude, latitude)
                    }
                },
                area,
                unit,
                descending,
                ordered,
                count,
                any,
                with_dist,
                with_hash,
                with_coord,
                store: store.map(|store| GeoStore {
                    destination: owned(store.destination),
                    distance: store.distance,
                }),
                query,
            },
            Self::BloomAddMany(key, items) => Command::BloomAddMany(owned(key), owned_vec(items)),
            Self::BloomContainsMany(key, items) => {
                Command::BloomContainsMany(owned(key), owned_vec(items))
            }
            Self::BloomInsert {
                key,
                error_rate,
                capacity,
                expansion,
                no_create,
                items,
            } => Command::BloomInsert {
                key: owned(key),
                error_rate,
                capacity,
                expansion,
                no_create,
                items: owned_vec(items),
            },
            Self::BloomInfo(key, field) => Command::BloomInfo(owned(key), field),
            Self::BloomCard(key) => Command::BloomCard(owned(key)),
            Self::BloomReserve(key, error_rate, capacity, expansion) => {
                Command::BloomReserve(owned(key), error_rate, capacity, expansion)
            }

            Self::BloomAdd(key, value) => Command::BloomAdd(owned(key), owned(value)),
            Self::BloomContains(key, value) => Command::BloomContains(owned(key), owned(value)),
            Self::StreamAdd {
                key,
                id,
                max_len,
                min_id,
                delay_ms,
                nomkstream,
                fields,
            } => Command::StreamAdd {
                key: owned(key),
                id,
                max_len,
                min_id,
                delay_ms,
                nomkstream,
                fields: fields
                    .into_iter()
                    .map(|(field, value)| (owned(field), owned(value)))
                    .collect(),
            },
            Self::StreamLen(key) => Command::StreamLen(owned(key)),
            Self::StreamInfo { key, group, kind } => Command::StreamInfo {
                key: owned(key),
                group: group.map(owned),
                kind,
            },
            Self::StreamRange {
                key,
                start,
                end,
                count,
                reverse,
            } => Command::StreamRange {
                key: owned(key),
                start,
                end,
                count,
                reverse,
            },
            Self::StreamRead { count, block, keys } => Command::StreamRead {
                count,
                block,
                keys: keys
                    .into_iter()
                    .map(|(key, cursor)| (owned(key), cursor))
                    .collect(),
            },
            Self::StreamDelete { key, ids } => Command::StreamDelete {
                key: owned(key),
                ids,
            },
            Self::StreamTrim { key, max_len } => Command::StreamTrim {
                key: owned(key),
                max_len,
            },
            Self::StreamTrimMinId { key, id } => Command::StreamTrimMinId {
                key: owned(key),
                id,
            },
            Self::StreamSetId { key, id } => Command::StreamSetId {
                key: owned(key),
                id,
            },
            Self::StreamGroupCreate {
                key,
                group,
                start,
                mkstream,
            } => Command::StreamGroupCreate {
                key: owned(key),
                group: owned(group),
                start,
                mkstream,
            },
            Self::StreamGroupRead {
                key,
                group,
                consumer,
                count,
                block,
                noack,
            } => Command::StreamGroupRead {
                key: owned(key),
                group: owned(group),
                consumer: owned(consumer),
                count,
                block,
                noack,
            },
            Self::StreamGroupDestroy { key, group } => Command::StreamGroupDestroy {
                key: owned(key),
                group: owned(group),
            },
            Self::StreamGroupSetId { key, group, start } => Command::StreamGroupSetId {
                key: owned(key),
                group: owned(group),
                start,
            },
            Self::StreamGroupDelConsumer {
                key,
                group,
                consumer,
            } => Command::StreamGroupDelConsumer {
                key: owned(key),
                group: owned(group),
                consumer: owned(consumer),
            },
            Self::StreamGroupCreateConsumer {
                key,
                group,
                consumer,
            } => Command::StreamGroupCreateConsumer {
                key: owned(key),
                group: owned(group),
                consumer: owned(consumer),
            },
            Self::StreamAck { key, group, ids } => Command::StreamAck {
                key: owned(key),
                group: owned(group),
                ids,
            },
            Self::StreamPending { key, group } => Command::StreamPending {
                key: owned(key),
                group: owned(group),
            },
            Self::StreamPendingRange {
                key,
                group,
                min_idle,
                start,
                end,
                count,
                consumer,
            } => Command::StreamPendingRange {
                key: owned(key),
                group: owned(group),
                min_idle,
                start,
                end,
                count,
                consumer: consumer.map(owned),
            },
            Self::StreamClaimPending {
                key,
                group,
                consumer,
                min_idle,
                ids,
                options,
            } => Command::StreamClaimPending {
                key: owned(key),
                group: owned(group),
                consumer: owned(consumer),
                min_idle,
                ids,
                options,
            },
            Self::StreamAutoclaim {
                key,
                group,
                consumer,
                min_idle,
                start,
                count,
                just_id,
            } => Command::StreamAutoclaim {
                key: owned(key),
                group: owned(group),
                consumer: owned(consumer),
                min_idle,
                start,
                count,
                just_id,
            },
            Self::Expire {
                key,
                milliseconds,
                condition,
            } => Command::Expire {
                key: owned(key),
                milliseconds,
                condition,
            },
            Self::PExpireAt {
                key,
                unix_millis,
                condition,
            } => Command::PExpireAt {
                key: owned(key),
                unix_millis,
                condition,
            },
            Self::CreateTtl { ttl_ms, inner } => {
                let (inner, bytes) = (*inner).into_owned_measured();
                copied.set(copied.get().saturating_add(bytes));
                Command::CreateTtl {
                    ttl_ms,
                    inner: Box::new(inner),
                }
            }
            Self::Ttl(key) => Command::Ttl(owned(key)),
            Self::PTtl(key) => Command::PTtl(owned(key)),
            Self::Persist(key) => Command::Persist(owned(key)),
            Self::Auth { username, password } => Command::Auth {
                username: username.map(owned),
                password: owned(password),
            },
            Self::Acl(command) => Command::Acl(command.into_owned()),
            Self::Multi => Command::Multi,
            Self::Exec => Command::Exec,
            Self::Discard => Command::Discard,
            Self::Watch(keys) => Command::Watch(owned_vec(keys)),
            Self::Unwatch => Command::Unwatch,
            Self::Eval { script, keys, args } => Command::Eval {
                script: owned(script),
                keys: owned_vec(keys),
                args: owned_vec(args),
            },
            Self::EvalSha { sha, keys, args } => Command::EvalSha {
                sha: owned(sha),
                keys: owned_vec(keys),
                args: owned_vec(args),
            },
            Self::ScriptLoad(script) => Command::ScriptLoad(owned(script)),
            Self::ScriptExists(shas) => Command::ScriptExists(owned_vec(shas)),
            Self::ScriptFlush => Command::ScriptFlush,
            Self::ScriptKill => Command::ScriptKill,
            Self::FunctionLoad { name, body } => Command::FunctionLoad {
                name: owned(name),
                body: owned(body),
            },
            Self::FunctionList => Command::FunctionList,
            Self::FunctionDelete(name) => Command::FunctionDelete(owned(name)),
            Self::FCall { name, keys, args } => Command::FCall {
                name: owned(name),
                keys: owned_vec(keys),
                args: owned_vec(args),
            },
            Self::Select(index) => Command::Select(index),
            Self::FlushDb => Command::FlushDb,
            Self::FlushAll => Command::FlushAll,
            Self::SwapDb(left, right) => Command::SwapDb(left, right),
            Self::Move { key, db } => Command::Move {
                key: owned(key),
                db,
            },
        };

        (command, copied.get())
    }
}

pub(crate) fn command_mutates_store(command: &Command<'_>) -> bool {
    if let Command::BitField { ops, .. } = command {
        return ops.iter().any(BitFieldOp::writes);
    }
    if let Command::JsonOp { op, .. } = command {
        return op.writes();
    }
    if let Command::JsonIndex { op, .. } = command {
        return op.writes();
    }

    !matches!(
        command,
        Command::Ping(_)
            | Command::CommandMeta { .. }
            | Command::Wait { .. }
            | Command::Ruvio(_)
            | Command::Changes(_)
            | Command::ChangesSubscribe(_)
            | Command::Hello { .. }
            | Command::ClientGetName
            | Command::ClientSetName(_)
            | Command::ClientTracking(_)
            | Command::ShardPublish { .. }
            | Command::ShardSubscribe(_)
            | Command::ShardUnsubscribe(_)
            | Command::Publish { .. }
            | Command::Subscribe(_)
            | Command::Unsubscribe(_)
            | Command::PSubscribe(_)
            | Command::PUnsubscribe(_)
            | Command::ClusterSlots
            | Command::ClusterNodes
            | Command::ClusterInfo
            | Command::ClusterShards
            | Command::ClusterMyId
            | Command::ClusterKeySlot(_)
            | Command::ReadOnly
            | Command::ReadWrite
            | Command::ConfigGet(_)
            | Command::Info(_)
            | Command::Save
            | Command::ConfigSet(_)
            | Command::ConfigRewrite
            | Command::ReplicaOf { .. }
            | Command::MoveSlots(_)
            | Command::Topology(_)
            | Command::Role
            | Command::Bgsave
            | Command::Get(_)
            | Command::JsonGet { .. }
            | Command::JsonMGet { .. }
            | Command::MGet(_)
            | Command::StrLen(_)
            | Command::GetBit(..)
            | Command::ListPos { .. }
            | Command::GetRange { .. }
            | Command::PfCount(_)
            | Command::BitCount { .. }
            | Command::BitPos { .. }
            | Command::Exists(_)
            | Command::Keys(_)
            | Command::ClientId
            | Command::ClientList { .. }
            | Command::ClientInfo
            | Command::Slowlog(_)
            | Command::ClientKill { .. }
            | Command::LatencyLatest
            | Command::Echo(_)
            | Command::Object { .. }
            | Command::MemoryUsage(_)
            | Command::RandomKey
            | Command::Touch(_)
            | Command::Dump(_)
            | Command::ExpireTime { .. }
            | Command::Type(_)
            | Command::Scan { .. }
            | Command::DbSize
            | Command::ListRange(_, _, _)
            | Command::ListLen(_)
            | Command::ListIndex(_, _)
            | Command::SetContains(_, _)
            | Command::SetCard(_)
            | Command::SetMembers(_)
            | Command::SetRandom { .. }
            | Command::SetAlgebra {
                destination: None,
                ..
            }
            | Command::SortedSetAlgebra {
                destination: None,
                ..
            }
            | Command::HashGet { .. }
            | Command::HashMGet { .. }
            | Command::HashLen(_)
            | Command::HashGetAll(_)
            | Command::HashExists { .. }
            | Command::HashKeys(_)
            | Command::HashValues(_)
            | Command::HashStrLen { .. }
            | Command::HashScan { .. }
            | Command::CollectionScan { .. }
            | Command::HashTtl { .. }
            | Command::SortedSetRange { .. }
            | Command::SortedSetRangeByScore { .. }
            | Command::SortedSetRangeByLex { .. }
            | Command::SortedSetCard(_)
            | Command::SetMIsMember { .. }
            | Command::SetInterCard { .. }
            | Command::SortedSetCount { .. }
            | Command::SortedSetLexCount { .. }
            | Command::SortedSetMScore { .. }
            | Command::SortedSetRandMember { .. }
            | Command::HashRandField { .. }
            | Command::SortedSetScore { .. }
            | Command::SortedSetRank { .. }
            | Command::GeoDist { .. }
            | Command::GeoPos { .. }
            | Command::GeoHash { .. }
            | Command::GeoSearch { store: None, .. }
            | Command::BloomContains(_, _)
            | Command::BloomContainsMany(_, _)
            | Command::BloomInfo(_, _)
            | Command::BloomCard(_)
            | Command::StreamLen(_)
            | Command::StreamInfo { .. }
            | Command::StreamRange { .. }
            | Command::StreamRead { .. }
            | Command::StreamPending { .. }
            | Command::StreamPendingRange { .. }
            | Command::Ttl(_)
            | Command::PTtl(_)
            | Command::Auth { .. }
            | Command::Acl(_)
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch(_)
            | Command::Unwatch
            | Command::ScriptLoad(_)
            | Command::ScriptExists(_)
            | Command::ScriptFlush
            | Command::ScriptKill
            | Command::FunctionLoad { .. }
            | Command::FunctionList
            | Command::FunctionDelete(_)
            | Command::Select(_)
    )
}

/// Commands that may park the connection until data arrives or their timeout passes. Every
/// place that treats waiting specially (forwarding, `MULTI`, scripts, slowlog, the WAL group)
/// starts from this one list, so a new blocking command is handled everywhere at once.
pub(crate) fn command_may_block(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::ListBlock { .. }
            | Command::ListMove { block: Some(_), .. }
            | Command::ListMPop { block: Some(_), .. }
            | Command::FillGet {
                wait_ms: Some(_),
                ..
            }
            | Command::SortedSetBlockPop { .. }
            | Command::StreamRead { block: Some(_), .. }
            | Command::StreamGroupRead { block: Some(_), .. }
    )
}

pub(crate) fn command_stays_process_local(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Acl(AclCommand::SetUser { .. })
            | Command::Acl(AclCommand::Load)
            | Command::Acl(AclCommand::Save)
            | Command::FunctionLoad { .. }
            | Command::FunctionDelete(_)
            | Command::ScriptLoad(_)
            | Command::ScriptFlush
    )
}

pub(crate) fn hot_command_mutates_store(command: &HotCommand<'_>) -> bool {
    matches!(
        command,
        HotCommand::Set(..)
            | HotCommand::SetEx(..)
            | HotCommand::Delete(_)
            | HotCommand::Increment(_, _)
    )
}

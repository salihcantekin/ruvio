//! Fails when a command in `docs/compatibility-matrix.json` has no typed method.
//!
//! `clients/testdata/command_methods.json` maps every matrix name to the typed method(s) that
//! send it (the `"rust"` field: one name or a list of names). The test checks that the file and
//! the matrix list the same commands and that each method is a real `pub fn` of the crate;
//! `methods_exist` makes the compiler check the same names.

use std::collections::BTreeMap;
use std::path::Path;

use ruvio_client::Client;

/// Reads a JSON file relative to the crate, or `None` outside the repository (for example a
/// published crate), where there is nothing to check.
fn read_json(relative: &str) -> Option<serde_json::Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    let Ok(text) = std::fs::read_to_string(&path) else {
        eprintln!("skipping: {} not found", path.display());

        return None;
    };

    Some(serde_json::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display())))
}

fn matrix_names() -> Vec<String> {
    let Some(matrix) = read_json("../../docs/compatibility-matrix.json") else {
        return Vec::new();
    };

    matrix["commands"]
        .as_array()
        .expect("commands array")
        .iter()
        .map(|command| command["name"].as_str().expect("name").to_owned())
        .collect()
}

/// Command name to the Rust method(s) that send it.
fn command_methods() -> BTreeMap<String, Vec<String>> {
    let Some(file) = read_json("../testdata/command_methods.json") else {
        return BTreeMap::new();
    };

    file["commands"]
        .as_object()
        .expect("commands object")
        .iter()
        .map(|(name, entry)| {
            let methods = match &entry["rust"] {
                serde_json::Value::String(method) => vec![method.clone()],
                serde_json::Value::Array(list) => list
                    .iter()
                    .map(|method| method.as_str().expect("method name").to_owned())
                    .collect(),
                _ => Vec::new(),
            };

            (name.clone(), methods)
        })
        .collect()
}

fn crate_source() -> String {
    let mut source = String::new();
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

    for entry in std::fs::read_dir(directory).expect("src") {
        let path = entry.expect("entry").path();

        if path.extension().is_some_and(|extension| extension == "rs") {
            source.push_str(&std::fs::read_to_string(path).expect("source"));
        }
    }

    source
}

#[test]
fn every_matrix_command_has_a_typed_method() {
    let names = matrix_names();

    if names.is_empty() {
        return;
    }

    let table = command_methods();
    let missing: Vec<&String> = names
        .iter()
        .filter(|name| !table.contains_key(name.as_str()))
        .collect();
    let stale: Vec<&String> = table.keys().filter(|name| !names.contains(name)).collect();

    assert!(
        missing.is_empty(),
        "matrix commands without a typed method: {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "command_methods.json rows not in the matrix: {stale:?}"
    );
    assert_eq!(table.len(), names.len(), "duplicate matrix commands");

    let source = crate_source();

    for (name, methods) in &table {
        assert!(!methods.is_empty(), "{name} maps to no Rust method");

        for method in methods {
            assert!(
                source.contains(&format!("pub fn {method}("))
                    || source.contains(&format!("pub fn {method}<")),
                "{name}: method {method} is not a pub fn of the crate"
            );
        }
    }

    println!(
        "typed coverage: {}/{} matrix commands",
        table.len(),
        names.len()
    );
}

#[allow(dead_code)]
fn methods_exist() {
    let _ = Client::acl_cat;
    let _ = Client::acl_getuser;
    let _ = Client::acl_list;
    let _ = Client::acl_load;
    let _ = Client::acl_save;
    let _ = Client::acl_setuser;
    let _ = Client::acl_users;
    let _ = Client::acl_whoami;
    let _ = Client::append;
    let _ = Client::auth;
    let _ = Client::auth_user;
    let _ = Client::bf_add;
    let _ = Client::bf_card;
    let _ = Client::bf_exists;
    let _ = Client::bf_info;
    let _ = Client::bf_info_field;
    let _ = Client::bf_insert;
    let _ = Client::bf_madd;
    let _ = Client::bf_mexists;
    let _ = Client::bf_reserve;
    let _ = Client::bf_reserve_with;
    let _ = Client::bgsave;
    let _ = Client::bitcount;
    let _ = Client::bitcount_range;
    let _ = Client::bitfield;
    let _ = Client::bitfield_ro;
    let _ = Client::bitop;
    let _ = Client::bitpos;
    let _ = Client::bitpos_range;
    let _ = Client::blmove;
    let _ = Client::blmpop;
    let _ = Client::blpop;
    let _ = Client::blpop_key;
    let _ = Client::brpop;
    let _ = Client::brpop_key;
    let _ = Client::bzpopmax;
    let _ = Client::bzpopmin;
    let _ = Client::changes;
    let _ = Client::changes_start;
    let _ = Client::client_getname;
    let _ = Client::client_id;
    let _ = Client::client_info;
    let _ = Client::client_kill_addr;
    let _ = Client::client_kill_id;
    let _ = Client::client_list;
    let _ = Client::client_setname;
    let _ = Client::client_tracking;
    let _ = Client::client_durability;
    let _ = Client::cluster_info;
    let _ = Client::cluster_keyslot;
    let _ = Client::cluster_myid;
    let _ = Client::cluster_nodes;
    let _ = Client::cluster_shards;
    let _ = Client::cluster_slots;
    let _ = Client::command_count;
    let _ = Client::command_docs;
    let _ = Client::command_info;
    let _ = Client::command_list;
    let _ = Client::config_get;
    let _ = Client::config_rewrite;
    let _ = Client::config_set;
    let _ = Client::copy;
    let _ = Client::dbsize;
    let _ = Client::decr;
    let _ = Client::decr_by;
    let _ = Client::del;
    let _ = Client::del_key;
    let _ = Client::delex;
    let _ = Client::discard;
    let _ = Client::dump;
    let _ = Client::echo;
    let _ = Client::eval;
    let _ = Client::evalsha;
    let _ = Client::exec;
    let _ = Client::exists;
    let _ = Client::exists_key;
    let _ = Client::expire;
    let _ = Client::expire_at;
    let _ = Client::expire_time;
    let _ = Client::fcall;
    let _ = Client::fill_get;
    let _ = Client::fill_get_wait;
    let _ = Client::fill_set;
    let _ = Client::flushall;
    let _ = Client::flushdb;
    let _ = Client::function_delete;
    let _ = Client::function_list;
    let _ = Client::function_load;
    let _ = Client::geo_add;
    let _ = Client::geo_add_with;
    let _ = Client::geo_dist;
    let _ = Client::geo_hash;
    let _ = Client::geo_pos;
    let _ = Client::geo_radius;
    let _ = Client::geo_radius_by_member;
    let _ = Client::geo_search;
    let _ = Client::geo_search_store;
    let _ = Client::geo_search_with;
    let _ = Client::get;
    let _ = Client::get_string;
    let _ = Client::getbit;
    let _ = Client::getdel;
    let _ = Client::getex;
    let _ = Client::getrange;
    let _ = Client::getset;
    let _ = Client::hdel;
    let _ = Client::hello;
    let _ = Client::hexists;
    let _ = Client::hexpire_at;
    let _ = Client::hexpire_with;
    let _ = Client::hexpiretime;
    let _ = Client::hget;
    let _ = Client::hgetall;
    let _ = Client::hincr_by;
    let _ = Client::hincr_by_float;
    let _ = Client::hkeys;
    let _ = Client::hlen;
    let _ = Client::hmget;
    let _ = Client::hpersist;
    let _ = Client::hpexpire;
    let _ = Client::hpexpiretime;
    let _ = Client::hpttl;
    let _ = Client::hmset;
    let _ = Client::hrandfield;
    let _ = Client::hrandfield_count;
    let _ = Client::hrandfield_with_values;
    let _ = Client::hscan;
    let _ = Client::hscan_iter;
    let _ = Client::hset;
    let _ = Client::hsetnx;
    let _ = Client::hstrlen;
    let _ = Client::httl;
    let _ = Client::hvals;
    let _ = Client::idem_begin;
    let _ = Client::idem_abort;
    let _ = Client::idem_complete;
    let _ = Client::idempotency_abort;
    let _ = Client::idempotency_begin;
    let _ = Client::idempotency_complete;
    let _ = Client::incr;
    let _ = Client::incr_by;
    let _ = Client::incr_by_float;
    let _ = Client::incr_by_max;
    let _ = Client::info;
    let _ = Client::jmerge;
    let _ = Client::json_arrappend;
    let _ = Client::json_arrappend_many;
    let _ = Client::json_arrindex;
    let _ = Client::json_arrinsert;
    let _ = Client::json_arrlen;
    let _ = Client::json_arrpop;
    let _ = Client::json_arrtrim;
    let _ = Client::json_clear;
    let _ = Client::json_del;
    let _ = Client::json_find;
    let _ = Client::json_forget;
    let _ = Client::json_get;
    let _ = Client::json_index_create;
    let _ = Client::json_index_drop;
    let _ = Client::json_index_list;
    let _ = Client::json_merge;
    let _ = Client::json_mget;
    let _ = Client::json_numincrby;
    let _ = Client::json_nummultby;
    let _ = Client::json_objkeys;
    let _ = Client::json_objlen;
    let _ = Client::json_set;
    let _ = Client::json_set_with;
    let _ = Client::json_strappend;
    let _ = Client::json_strlen;
    let _ = Client::json_toggle;
    let _ = Client::json_type;
    let _ = Client::key_type;
    let _ = Client::keys;
    let _ = Client::latency_latest;
    let _ = Client::lease;
    let _ = Client::lease_guard;
    let _ = Client::limit;
    let _ = Client::limit_sliding;
    let _ = Client::limit_sliding_with_retry;
    let _ = Client::limit_tokens;
    let _ = Client::limit_tokens_with_retry;
    let _ = Client::lindex;
    let _ = Client::linsert;
    let _ = Client::llen;
    let _ = Client::lmove;
    let _ = Client::lmpop;
    let _ = Client::lpop;
    let _ = Client::lpop_count;
    let _ = Client::lpos;
    let _ = Client::lpos_count;
    let _ = Client::lpos_with;
    let _ = Client::lpush;
    let _ = Client::lpushcap;
    let _ = Client::lpushx;
    let _ = Client::lrange;
    let _ = Client::lrem;
    let _ = Client::lset;
    let _ = Client::ltrim;
    let _ = Client::maybe_seen;
    let _ = Client::memory_usage;
    let _ = Client::memory_usage_samples;
    let _ = Client::mget;
    let _ = Client::move_key;
    let _ = Client::mset;
    let _ = Client::msetnx;
    let _ = Client::multi;
    let _ = Client::object_encoding;
    let _ = Client::object_freq;
    let _ = Client::object_idletime;
    let _ = Client::once;
    let _ = Client::persist;
    let _ = Client::pexpire;
    let _ = Client::pexpire_time;
    let _ = Client::pfadd;
    let _ = Client::pfcount;
    let _ = Client::pfmerge;
    let _ = Client::ping;
    let _ = Client::ping_message;
    let _ = Client::psetex;
    let _ = Client::psubscribe;
    let _ = Client::pttl;
    let _ = Client::publish;
    let _ = Client::punsubscribe;
    let _ = Client::push_recent;
    let _ = Client::random_key;
    let _ = Client::rate_limit;
    let _ = Client::readonly;
    let _ = Client::readwrite;
    let _ = Client::release;
    let _ = Client::release_lock;
    let _ = Client::rename;
    let _ = Client::rename_nx;
    let _ = Client::restore;
    let _ = Client::rpop;
    let _ = Client::rpop_count;
    let _ = Client::rpoplpush;
    let _ = Client::rpush;
    let _ = Client::rpushcap;
    let _ = Client::rpushx;
    let _ = Client::ruvio;
    let _ = Client::move_slots;
    let _ = Client::move_slots_status;
    let _ = Client::sadd;
    let _ = Client::save;
    let _ = Client::scan;
    let _ = Client::scan_iter;
    let _ = Client::scard;
    let _ = Client::script_exists;
    let _ = Client::script_flush;
    let _ = Client::script_kill;
    let _ = Client::script_load;
    let _ = Client::sdiff;
    let _ = Client::sdiffstore;
    let _ = Client::seen_before;
    let _ = Client::select;
    let _ = Client::semaphore;
    let _ = Client::semaphore_permit;
    let _ = Client::set;
    let _ = Client::set_and_get;
    let _ = Client::set_bytes;
    let _ = Client::set_ex;
    let _ = Client::set_exat;
    let _ = Client::set_expires_at;
    let _ = Client::set_if_eq;
    let _ = Client::set_if_eq_keep_ttl;
    let _ = Client::set_if_greater;
    let _ = Client::set_if_less;
    let _ = Client::set_keep_ttl;
    let _ = Client::set_with;
    let _ = Client::setbit;
    let _ = Client::setex;
    let _ = Client::setnx;
    let _ = Client::setrange;
    let _ = Client::setv;
    let _ = Client::sinter;
    let _ = Client::sintercard;
    let _ = Client::sinterstore;
    let _ = Client::sismember;
    let _ = Client::slowlog_get;
    let _ = Client::slowlog_len;
    let _ = Client::slowlog_reset;
    let _ = Client::smembers;
    let _ = Client::smismember;
    let _ = Client::smove;
    let _ = Client::spop;
    let _ = Client::spop_count;
    let _ = Client::spublish;
    let _ = Client::srandmember;
    let _ = Client::srandmember_count;
    let _ = Client::srem;
    let _ = Client::sscan;
    let _ = Client::sscan_iter;
    let _ = Client::ssubscribe;
    let _ = Client::strlen;
    let _ = Client::subscribe;
    let _ = Client::subscriber;
    let _ = Client::substr;
    let _ = Client::sunion;
    let _ = Client::sunionstore;
    let _ = Client::sunsubscribe;
    let _ = Client::swapdb;
    let _ = Client::take;
    let _ = Client::transfer;
    let _ = Client::htransfer;
    let _ = Client::touch;
    let _ = Client::ttl;
    let _ = Client::unlink;
    let _ = Client::unlink_key;
    let _ = Client::unsubscribe;
    let _ = Client::unwatch;
    let _ = Client::wait;
    let _ = Client::watch;
    let _ = Client::watch_key;
    let _ = Client::xack;
    let _ = Client::xadd;
    let _ = Client::xadd_delay;
    let _ = Client::xadd_maxlen;
    let _ = Client::xadd_nomkstream;
    let _ = Client::xautoclaim;
    let _ = Client::xclaim;
    let _ = Client::xclaim_ids;
    let _ = Client::xdel;
    let _ = Client::xgroup_create;
    let _ = Client::xgroup_create_consumer;
    let _ = Client::xgroup_delconsumer;
    let _ = Client::xgroup_destroy;
    let _ = Client::xgroup_setid;
    let _ = Client::xinfo_consumers;
    let _ = Client::xinfo_groups;
    let _ = Client::xinfo_stream;
    let _ = Client::xlen;
    let _ = Client::xpending;
    let _ = Client::xpending_summary;
    let _ = Client::xrange;
    let _ = Client::xread;
    let _ = Client::xreadgroup;
    let _ = Client::xrevrange;
    let _ = Client::xsetid;
    let _ = Client::xtrim_maxlen;
    let _ = Client::xtrim_minid;
    let _ = Client::xtrim;
    let _ = Client::xadd_trim;
    let _ = Client::info_section;
    let _ = Client::client_list_type;
    let _ = Client::zadd;
    let _ = Client::zadd_incr;
    let _ = Client::zadd_with;
    let _ = Client::zcard;
    let _ = Client::zcount;
    let _ = Client::zdiff;
    let _ = Client::zdiff_with_scores;
    let _ = Client::zdiffstore;
    let _ = Client::zincr_by;
    let _ = Client::zinter;
    let _ = Client::zinter_with_scores;
    let _ = Client::zinterstore;
    let _ = Client::zlexcount;
    let _ = Client::zmscore;
    let _ = Client::zpopmax;
    let _ = Client::zpopmin;
    let _ = Client::zpopbyscore;
    let _ = Client::zmovedue;
    let _ = Client::zrandmember;
    let _ = Client::zrandmember_count;
    let _ = Client::zrandmember_with_scores;
    let _ = Client::zrange;
    let _ = Client::zrange_by_lex;
    let _ = Client::zrange_by_score;
    let _ = Client::zrange_by_score_with_scores;
    let _ = Client::zrange_query;
    let _ = Client::zrange_query_with_scores;
    let _ = Client::zrange_with_scores;
    let _ = Client::zrangebylex;
    let _ = Client::zrank;
    let _ = Client::zrem;
    let _ = Client::zremrangebylex;
    let _ = Client::zremrangebyrank;
    let _ = Client::zremrangebyscore;
    let _ = Client::zrevrange;
    let _ = Client::zrevrange_with_scores;
    let _ = Client::zrevrangebylex;
    let _ = Client::zrevrank;
    let _ = Client::zscan;
    let _ = Client::zscan_iter;
    let _ = Client::zscore;
    let _ = Client::zunion;
    let _ = Client::zunion_with_scores;
    let _ = Client::zunionstore;
    let _ = Client::expire_if;
    let _ = Client::pexpire_if;
    let _ = Client::expire_at_if;
    let _ = Client::pexpire_at_if;
    let _ = Client::incr_with_ttl;
    let _ = Client::decr_with_ttl;
    let _ = Client::incr_by_with_ttl;
    let _ = Client::decr_by_with_ttl;
    let _ = Client::incr_by_max_with_ttl;
    let _ = Client::incr_by_float_with_ttl;
    let _ = Client::hincr_by_with_ttl;
    let _ = Client::zincr_by_with_ttl;
    let _ = Client::setbit_with_ttl;
    let _ = Client::xadd_with_ttl;
    let _ = Client::xadd_trim_with_ttl;
    let _ = Client::json_set_with_ttl;
}

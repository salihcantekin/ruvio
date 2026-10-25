use super::*;

/// Keyless commands a script may call. Every other keyless command is refused, and
/// each one the parser builds is in exactly one of the two lists: a new keyless command
/// fails here until someone decides. Redis refuses `CLIENT`, `FUNCTION` and `FCALL` in
/// scripts; `JSON.INDEX` and `CHANGES SUBSCRIBE` act beyond the script's shard and call.
const KEYLESS_IN_SCRIPTS: &[&str] = &[
    "Changes",
    "CommandMeta",
    "DbSize",
    "Echo",
    "Keys",
    "LatencyLatest",
    "Ping",
    "RandomKey",
    "Ruvio",
    "Scan",
    "Slowlog",
    "StreamGroupRead",
];
const KEYLESS_REFUSED_IN_SCRIPTS: &[&str] = &[
    "Acl",
    "Auth",
    "Bgsave",
    "ChangesSubscribe",
    "ClientGetName",
    "ClientId",
    "ClientInfo",
    "ClientKill",
    "ClientList",
    "ClientSetName",
    "ClientTracking",
    "ClusterInfo",
    "ClusterKeySlot",
    "ClusterMyId",
    "ClusterNodes",
    "ClusterShards",
    "ClusterSlots",
    "ConfigGet",
    "ConfigRewrite",
    "ConfigSet",
    "ReplicaOf",
    "Role",
    "Discard",
    "Eval",
    "EvalSha",
    "Exec",
    "FCall",
    "FlushAll",
    "FlushDb",
    "FunctionDelete",
    "FunctionList",
    "FunctionLoad",
    "Hello",
    "Info",
    "JsonIndex",
    "Multi",
    "PSubscribe",
    "PUnsubscribe",
    "Publish",
    "ReadOnly",
    "ReadWrite",
    "Save",
    "ScriptExists",
    "ScriptFlush",
    "ScriptKill",
    "ScriptLoad",
    "Select",
    "ShardPublish",
    "ShardSubscribe",
    "ShardUnsubscribe",
    "Subscribe",
    "SwapDb",
    "Unsubscribe",
    "Unwatch",
    "Wait",
];

#[test]
fn every_keyless_command_is_decided_for_scripts() {
    let mut undecided = Vec::new();
    let mut wrong = Vec::new();

    for (variant, command) in crate::protocol::consistency::keyless_forms() {
        let allowed = KEYLESS_IN_SCRIPTS.contains(&variant.as_str());

        if !allowed && !KEYLESS_REFUSED_IN_SCRIPTS.contains(&variant.as_str()) {
            undecided.push(variant);
        } else if scriptable(&command) != allowed {
            wrong.push(variant);
        }
    }

    assert!(
        undecided.is_empty(),
        "keyless commands with no script decision: {undecided:?}"
    );
    assert!(
        wrong.is_empty(),
        "scriptable disagrees with the lists: {wrong:?}"
    );
}
use crate::{KvShard, mutation::NoopMutationSink};

fn owned(value: &[u8]) -> Bytes<'static> {
    Cow::Owned(value.to_vec())
}

struct RefusingSink;

impl MutationSink for RefusingSink {
    type Error = ();

    fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
        Err(())
    }
}

/// `ruvio.call` meets the key and value byte limits a client's command meets.
#[test]
fn a_script_cannot_write_a_value_over_the_value_limit() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::with_payload_limits(crate::protocol::ProtocolLimits {
        max_value_bytes: 16,
        ..crate::protocol::ProtocolLimits::default()
    });
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        roomy_limits(),
        b"return ruvio.call('SET', KEYS[1], string.rep('x', 100))",
        &[owned(b"big")],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(response, Response::Error(b"ERR value is too large"));
    assert_eq!(shard.get(b"big"), None);
}

#[test]
fn a_script_hands_blocked_clients_nothing_unless_its_writes_are_recorded() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut waiters = crate::list_waiters::ListWaiters::new(4);
    let slot = crate::list_waiters::EncodedReplySlot::default();
    waiters
        .register(
            vec![crate::database::tagged(0, b"q")],
            None,
            slot.clone(),
            Vec::new(),
        )
        .unwrap();
    let body = b"ruvio.call('RPUSH', KEYS[1], 'a', 'b'); return 1";
    let refused = execute_eval(
        &mut shard,
        &mut RefusingSink,
        &mut waiters,
        &mut cache,
        ScriptLimits::default(),
        body,
        &[owned(b"q")],
        &[],
        0,
    );

    assert!(refused.is_err());
    assert_eq!(slot.take(), None);
    assert_eq!(waiters.len(), 1, "the client is still parked");

    let failed = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut waiters,
        &mut cache,
        ScriptLimits::default(),
        b"ruvio.call('RPUSH', KEYS[1], 'a'); return ruvio.call('NOSUCHCOMMAND')",
        &[owned(b"q")],
        &[],
        0,
    )
    .unwrap();

    assert!(matches!(
        failed,
        Response::Error(_) | Response::OwnedError(_)
    ));
    assert_eq!(slot.take(), None, "a script that failed hands out nothing");
    assert_eq!(waiters.len(), 1);
}

#[test]
fn a_repeated_script_keeps_one_compiled_function() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let body = b"local n = 0 for i = 1, 2 do n = n + i end return n";

    for expected in [3, 3] {
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[],
            &[],
            0,
        )
        .unwrap();

        assert_eq!(response, Response::Integer(expected));
    }

    assert_eq!(cache.vm.as_ref().unwrap().compiled.len(), 1);
    let first = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"return KEYS[1]",
        &[owned(b"a")],
        &[],
        0,
    )
    .unwrap();
    let second = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"return KEYS[1]",
        &[owned(b"b")],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(first, Response::Bulk(Some(Cow::Borrowed(b"a"))));
    assert_eq!(second, Response::Bulk(Some(Cow::Borrowed(b"b"))));
}

#[test]
fn a_script_returning_a_cyclic_or_deeply_nested_table_fails_without_crashing() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut run = |body: &[u8]| {
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[],
            &[],
            0,
        )
        .unwrap()
    };

    for body in [
        b"local t = {} t[1] = t return t" as &[u8],
        b"local t = {} for i = 1, 100 do t[i] = t end return t",
        b"local t = {} for i = 1, 100000 do t = {t} end return t",
        b"local t = {} t[1] = {err = t} t[2] = t return t",
    ] {
        assert_eq!(
            run(body),
            Response::Error(STACK_LIMIT_ERROR),
            "{}",
            String::from_utf8_lossy(body)
        );
    }

    // Shared, not cyclic: 2^40 leaves from 41 tables.
    assert_eq!(
        run(b"local t = {'leaf'} for i = 1, 40 do t = {t, t} end return t"),
        Response::Error(b"ERR Lua reply is too large")
    );

    // Nesting up to the limit is still a reply, and it can be encoded.
    let nest = |tables: usize| {
        format!("local t = {{7}} for i = 2, {tables} do t = {{t}} end return t").into_bytes()
    };

    assert_eq!(
        run(&nest(MAX_REPLY_DEPTH + 1)),
        Response::Error(STACK_LIMIT_ERROR)
    );
    let deep = run(&nest(MAX_REPLY_DEPTH));
    let mut tables = 0;
    let mut level = &deep;

    while let Response::Exec(items) = level {
        level = &items[0];
        tables += 1;
    }

    assert_eq!((tables, level), (MAX_REPLY_DEPTH, &Response::Integer(7)));
    let mut wire = Vec::new();
    crate::engine::write_response(&mut wire, deep).unwrap();
    assert!(wire.ends_with(b":7\r\n"));
    assert_eq!(
        run(b"return 'still running'"),
        Response::Bulk(Some(owned(b"still running")))
    );
}

#[test]
fn a_deeply_nested_reply_is_refused_when_pushed_to_lua() {
    // Dropping 20 000 nested replies recurses too; give it room.
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            let lua = Lua::new();
            let mut nested = Response::Integer(1);

            for _ in 0..20_000 {
                nested = Response::Exec(vec![nested]);
            }

            let mut shallow = Response::Integer(1);

            for _ in 0..MAX_REPLY_DEPTH {
                shallow = Response::Map(vec![(owned(b"k"), shallow)]);
            }

            // SAFETY: no memory limit is set on this state.
            #[allow(unsafe_code)]
            lua.exec_raw_lua(|raw| unsafe {
                let state = raw.state();
                let top = ffi::lua_gettop(state);
                let mut budget = LuaBudget::new(&lua, state);

                assert_eq!(
                    push_reply(state, nested, &mut budget),
                    Err(PushFailure::Refused(STACK_LIMIT_ERROR))
                );
                assert_eq!(ffi::lua_gettop(state), top, "the partial reply is popped");
                assert_eq!(push_reply(state, shallow, &mut budget), Ok(()));
                ffi::lua_settop(state, top);
            });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn scripts_cannot_change_what_later_scripts_run() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut run = |body: &str| {
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body.as_bytes(),
            &[owned(b"{t}k")],
            &[owned(b"arg")],
            0,
        )
        .unwrap()
    };
    let readonly = Response::Error(b"ERR Attempt to modify a readonly table");
    let victim = "return ruvio.call('PING')";

    // Compiled and cached before the attempts, so the cached function is checked too.
    assert_eq!(run(victim), Response::Simple(b"PONG"));

    for attack in [
        "ruvio.call = function() return 'hijacked' end",
        "ruvio = {call = function() return 'hijacked' end}",
        "_G.ruvio = {call = function() return 'hijacked' end}",
        "hijacked = true",
        "KEYS = {'evil'}",
        "string.rep = function() return 'hijacked' end",
        "table.insert = nil",
        "math.floor = nil",
        "_G = {}",
    ] {
        // Twice: the second run takes the cached function.
        for _ in 0..2 {
            assert_eq!(run(attack), readonly, "{attack}");
        }
    }

    for attack in [
        "setmetatable(_G, nil)",
        "setmetatable(ruvio, {})",
        "getmetatable('').__index.rep = nil",
        "setfenv(0, {})",
        "getfenv(0).ruvio = {}",
    ] {
        assert!(
            matches!(run(attack), Response::Error(_)),
            "{attack} succeeded"
        );
    }

    // Raw writes are refused too; on a script's own tables they still work.
    for attack in [
        "rawset(ruvio, 'call', function() return 'hijacked' end)",
        "rawset(_G, 'KEYS', {'evil'})",
        "table.insert(_G, 1)",
        "table.insert(string, 1, 'x')",
        "table.remove(ruvio)",
        "table.sort(_G)",
    ] {
        assert_eq!(run(attack), readonly, "{attack}");
    }

    assert_eq!(
        run(
            "local t = {3, 1} rawset(t, 'k', 'v') table.insert(t, 2) table.insert(t, 1, 0) \
                 table.remove(t, 1) table.sort(t) return {t[1], t[2], t[3], t.k}"
        ),
        Response::Exec(vec![
            Response::Integer(1),
            Response::Integer(2),
            Response::Integer(3),
            Response::Bulk(Some(owned(b"v"))),
        ])
    );

    assert_eq!(run(victim), Response::Simple(b"PONG"));
    assert_eq!(
        run("return {KEYS[1], ARGV[1], type(hijacked), string.rep('ab', 2), #_G}"),
        Response::Exec(vec![
            Response::Bulk(Some(owned(b"{t}k"))),
            Response::Bulk(Some(owned(b"arg"))),
            Response::Bulk(Some(owned(b"nil"))),
            Response::Bulk(Some(owned(b"abab"))),
            Response::Integer(0),
        ])
    );
    // Ordinary scripts: locals, their own tables and metatables, library calls.
    assert_eq!(
        run(
            "local t = setmetatable({}, {__index = function(_, k) return k .. '!' end}) \
                 t.x = 1 local ok = pcall(error, 'e') \
                 return {t.x, t.y, tostring(ok), ('abc'):upper(), table.concat({1, 2}, ','), \
                 math.floor(2.5), string.format('%d', 3), select('#', unpack({1, 2}))}"
        ),
        Response::Exec(vec![
            Response::Integer(1),
            Response::Bulk(Some(owned(b"y!"))),
            Response::Bulk(Some(owned(b"false"))),
            Response::Bulk(Some(owned(b"ABC"))),
            Response::Bulk(Some(owned(b"1,2"))),
            Response::Integer(2),
            Response::Bulk(Some(owned(b"3"))),
            Response::Integer(2),
        ])
    );
    assert_eq!(
        run("local ok, e = pcall(function() undeclared = 1 end) \
                 return string.find(e, 'Attempt to modify a readonly table', 1, true) ~= nil"),
        Response::Integer(1)
    );
}

#[test]
fn a_wrapper_left_by_one_user_cannot_run_under_the_next_users_acl() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut rules = crate::acl::AclRules::none();

    rules.commands.insert("set");
    rules.key_patterns.push(b"*".to_vec());
    let mut run = |body: &[u8], acl: Option<crate::acl::AclRules>, shard: &mut KvShard| {
        cache.restrict_next_script(acl.map(std::sync::Arc::new));
        execute_eval(
            shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[owned(b"{t}k")],
            &[],
            0,
        )
        .unwrap()
    };

    // A restricted user tries to leave a wrapper that writes a second key when the
    // next, unrestricted script calls `ruvio.call`.
    run(
        b"local call = ruvio.call \
              ruvio.call = function(...) call('SET', KEYS[1], 'planted') return call(...) end",
        Some(rules.clone()),
        &mut shard,
    );
    assert_eq!(
        run(b"return ruvio.call('GET', KEYS[1])", None, &mut shard),
        Response::Bulk(None)
    );
    assert_eq!(shard.get(b"{t}k"), None);
}

#[test]
fn a_long_built_pattern_is_refused_instead_of_overflowing_the_stack() {
    let run = |body: &[u8]| eval_body(body, ScriptLimits::default());

    assert_eq!(
        run(b"return #string.match(string.rep('a', 200000), string.rep('a?', 200000))"),
        Response::Error(b"ERR pattern too complex")
    );
    assert_eq!(
        run(b"return #(string.rep('a', 200000)):gsub(string.rep('a-', 100000), '')"),
        Response::Error(b"ERR pattern too complex")
    );
    // Short patterns, long literal ones and plain finds are untouched.
    assert_eq!(
            run(b"local s = string.rep('ab', 1000) \
                  return {string.match('hello', 'h(.-)o'), string.find(s, s, 1, true), \
                  string.find(s, string.rep('ab', 500) .. '$'), select(2, string.gsub(s, 'a', 'c'))}"),
            Response::Exec(vec![
                Response::Bulk(Some(owned(b"ell"))),
                Response::Integer(1),
                Response::Integer(1001),
                Response::Integer(1000),
            ])
        );
}

#[test]
fn a_huge_call_reply_is_refused_while_it_is_built() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    // Distinct items: Lua 5.1 interns every string, so equal ones would share memory.
    let items: Vec<Vec<u8>> = (0..20u8).map(|i| vec![b'a' + i; 2 << 20]).collect();

    shard.list_push_back(b"{t}list", &items).unwrap();
    PEAK_CALL_MEMORY.set(0);
    // 100 MiB of the script's own leaves about 28 MiB for a 40 MiB reply.
    let refused = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        roomy_limits(),
        b"local pad = {} for i = 1, 100 do pad[i] = string.rep('x', 2^20 - 64) .. i end \
              local ok, items = pcall(ruvio.call, 'LRANGE', KEYS[1], 0, -1) \
              return {tostring(ok), #pad}",
        &[owned(b"{t}list")],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(refused, Response::Error(MEMORY_LIMIT_ERROR));
    assert!(PEAK_CALL_MEMORY.get() > 100 << 20, "the call never ran");
    assert!(
        PEAK_CALL_MEMORY.get() <= SCRIPT_MEMORY_LIMIT,
        "the reply was built past the limit: {} bytes",
        PEAK_CALL_MEMORY.get()
    );
    assert_eq!(
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            roomy_limits(),
            b"return #ruvio.call('LRANGE', KEYS[1], 0, -1)",
            &[owned(b"{t}list")],
            &[],
            0,
        )
        .unwrap(),
        Response::Integer(20)
    );
}

#[test]
fn a_command_whose_reply_passes_the_memory_limit_is_not_kept() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    // Distinct items: Lua 5.1 interns every string, so equal ones would share memory.
    let items: Vec<Vec<u8>> = (0..20u8).map(|i| vec![b'a' + i; 2 << 20]).collect();

    shard.list_push_back(b"{t}list", &items).unwrap();
    // The pop runs, its 40 MiB reply does not fit: the script fails as a whole even
    // though it caught the error, so the pop is undone rather than kept unreported.
    let reply = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        roomy_limits(),
        b"local pad = {} for i = 1, 100 do pad[i] = string.rep('x', 2^20 - 64) .. i end \
              pcall(ruvio.call, 'LPOP', KEYS[1], 20) \
              ruvio.pcall('SET', KEYS[2], 'after') \
              return 'survived'",
        &[owned(b"{t}list"), owned(b"{t}other")],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(reply, Response::Error(MEMORY_LIMIT_ERROR));
    assert_eq!(shard.list_len(b"{t}list").unwrap(), 20);
    assert_eq!(shard.get(b"{t}other"), None);
}

#[test]
fn lua_global_is_named_ruvio() {
    assert_eq!(
        eval_body(b"return type(ruvio.call)", ScriptLimits::default()),
        Response::Bulk(Some(Cow::Borrowed(b"function")))
    );
    assert_eq!(
        eval_body(b"return type(redis)", ScriptLimits::default()),
        Response::Bulk(Some(Cow::Borrowed(b"nil")))
    );
}

#[test]
fn large_json_root_reply_is_a_lua_string() {
    let lua = Lua::new();
    let bytes = serde_json::to_vec(&serde_json::json!({
        "padding": "x".repeat(crate::json::TREE_THRESHOLD),
        "value": "line\n\"quoted\"",
    }))
    .unwrap();
    let tree = crate::json::TreeDocument::from_bytes(&bytes).unwrap();
    // SAFETY: no memory limit is set on this state.
    #[allow(unsafe_code)]
    let value = lua.exec_raw_lua(|raw| unsafe {
        let mut budget = LuaBudget::new(&lua, raw.state());

        push_reply(raw.state(), Response::JsonTree(&tree), &mut budget).unwrap();
        raw.pop::<Value>().unwrap()
    });
    let Value::String(result) = value else {
        panic!("expected Lua string");
    };
    let parsed: serde_json::Value = serde_json::from_slice(result.as_bytes().as_ref()).unwrap();
    assert_eq!(parsed["value"], "line\n\"quoted\"");
}

#[test]
fn a_script_past_the_memory_limit_fails_and_the_vm_keeps_working() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut run = |body: &[u8]| {
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            roomy_limits(),
            body,
            &[],
            &[],
            0,
        )
        .unwrap()
    };

    assert_eq!(
        run(b"return #string.rep('x', 2^30)"),
        Response::Error(b"ERR Lua script exceeded the memory limit")
    );
    // Doubling a string takes few instructions but exponential memory.
    assert_eq!(
        run(b"local s = 'x' for i = 1, 40 do s = s .. s end return #s"),
        Response::Error(b"ERR Lua script exceeded the memory limit")
    );
    assert_eq!(
        run(b"local t = {} for i = 1, 200 do t[i] = string.rep('y', 2^20) .. i end return #t"),
        Response::Error(b"ERR Lua script exceeded the memory limit")
    );
    assert_eq!(
        run(b"return #string.rep('x', 2^20)"),
        Response::Integer(1 << 20)
    );
}

#[test]
fn replies_a_script_keeps_count_toward_the_memory_limit() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let keys: Vec<Bytes<'_>> = (0..10)
        .map(|i| owned(format!("{{b}}big{i}").as_bytes()))
        .collect();

    for (i, key) in keys.iter().enumerate() {
        shard.set_bytes(key, &vec![b'a' + i as u8; 15 << 20]);
    }

    // Ten distinct 15 MiB replies held in locals: the script allocates nothing itself.
    let mut body = String::new();
    for i in 1..=10 {
        body.push_str(&format!("local v{i} = ruvio.call('GET', KEYS[{i}]) "));
    }
    body.push_str("return 1");
    let mut run = |body: &[u8], keys: &[Bytes<'_>]| {
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            keys,
            &[],
            0,
        )
        .unwrap()
    };

    assert_eq!(
        run(body.as_bytes(), &keys),
        Response::Error(b"ERR Lua script exceeded the memory limit")
    );
    assert_eq!(
        run(b"return #ruvio.call('GET', KEYS[1])", &keys[..1]),
        Response::Integer(15 << 20)
    );
    // No finalizers for scripts: one could raise inside the host's own calls.
    assert_eq!(
        run(b"return type(newproxy)", &[]),
        Response::Bulk(Some(owned(b"nil")))
    );
}

#[test]
fn scripts_obey_the_acl_of_the_user_running_them() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut rules = crate::acl::AclRules::none();

    rules.commands.insert("get");
    rules.key_patterns.push(b"app:*".to_vec());

    let run = |body: &[u8], key: &[u8], cache: &mut ScriptCache, shard: &mut KvShard<_, _>| {
        cache.restrict_next_script(Some(std::sync::Arc::new(rules.clone())));
        execute_eval(
            shard,
            &mut NoopMutationSink,
            &mut (),
            cache,
            ScriptLimits::default(),
            body,
            &[Cow::Owned(key.to_vec())],
            &[],
            0,
        )
        .unwrap()
    };

    let denied = run(
        b"return ruvio.call('SET', KEYS[1], 'v')",
        b"app:1",
        &mut cache,
        &mut shard,
    );
    assert!(
        matches!(&denied, Response::Error(m) if m.starts_with(b"NOPERM"))
            || matches!(&denied, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
        "{denied:?}"
    );
    assert!(
        shard.get(b"app:1").is_none(),
        "a refused SET writes nothing"
    );

    let wrong_key = run(
        b"return ruvio.call('GET', KEYS[1])",
        b"other:1",
        &mut cache,
        &mut shard,
    );
    assert!(
        matches!(&wrong_key, Response::Error(m) if m.starts_with(b"NOPERM"))
            || matches!(&wrong_key, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
        "{wrong_key:?}"
    );

    let allowed = run(
        b"return ruvio.call('GET', KEYS[1])",
        b"app:1",
        &mut cache,
        &mut shard,
    );
    assert_eq!(allowed, Response::Bulk(None));
}

#[test]
fn pcall_returns_every_refusal_as_an_error_table_and_call_raises_it() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut rules = crate::acl::AclRules::none();

    rules.commands.insert("get");
    rules.key_patterns.push(b"app:*".to_vec());
    shard.set_bytes(b"app:1", b"v");
    shard.set_bytes(b"other:1", b"v");

    let mut run = |body: &str, key: &[u8]| {
        cache.restrict_next_script(Some(std::sync::Arc::new(rules.clone())));
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body.as_bytes(),
            &[Cow::Owned(key.to_vec())],
            &[],
            0,
        )
        .unwrap()
    };
    // The table's `err` field, read inside the script; `nil` when pcall did not return one.
    let probe = |call: &str| {
        format!(
            "local r = ruvio.pcall({call}); if type(r) == 'table' and r.err then return 'err:' .. r.err end; return type(r)"
        )
    };

    for (call, key, expected) in [
        ("'SET', KEYS[1], 'x'", &b"app:1"[..], "err:NOPERM"),
        ("'GET', KEYS[1]", b"other:1", "err:NOPERM"),
        (
            "'GET', 'app:2'",
            b"app:1",
            "err:ERR script attempted to access undeclared keys",
        ),
        (
            "'BLPOP', KEYS[1], '0'",
            b"app:1",
            "err:ERR command not allowed from script source",
        ),
        ("'GET'", b"app:1", "err:ERR"),
        ("'LPUSH', KEYS[1], 'x'", b"app:1", "err:NOPERM"),
    ] {
        let reply = run(&probe(call), key);
        let text = match &reply {
            Response::Bulk(Some(text)) => String::from_utf8_lossy(text).into_owned(),
            other => format!("{other:?}"),
        };

        assert!(text.starts_with(expected), "pcall({call}) gave {text}");

        // `call` raises the same refusal: the script fails with an error reply.
        let raised = run(&format!("return ruvio.call({call})"), key);
        assert!(
            matches!(raised, Response::Error(_) | Response::OwnedError(_)),
            "call({call}) did not raise: {raised:?}"
        );
    }

    // An error table returned as the script's result is the client's error reply.
    let returned = run("return ruvio.pcall('SET', KEYS[1], 'x')", b"app:1");
    assert!(
        matches!(&returned, Response::Error(m) if m.starts_with(b"NOPERM"))
            || matches!(&returned, Response::OwnedError(m) if m.starts_with(b"NOPERM")),
        "{returned:?}"
    );
    assert_eq!(shard.get(b"app:1"), Some(&b"v"[..]), "a refused SET wrote");
}

#[test]
fn a_call_refused_under_maxmemory_fails_the_script_with_the_oom_error() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();

    shard.set_memory_limit(Some(48));
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"return ruvio.call('SET', KEYS[1], ARGV[1])",
        &[Cow::Borrowed(b"k".as_slice())],
        &[Cow::Owned(vec![7; 128])],
        0,
    )
    .unwrap();

    assert_eq!(response, Response::Error(crate::engine::OOM_ERROR));
    assert!(shard.is_empty());
}

#[test]
fn a_script_on_a_full_noeviction_store_can_delete_but_not_grow() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();

    shard.set_eviction_policy(crate::EvictionPolicy::NoEviction);
    shard.set_bytes(b"old", &[1; 256]);
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() - 1));
    let mut eval = |shard: &mut KvShard, body: &[u8], key: &'static [u8]| {
        execute_eval(
            shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[Cow::Borrowed(key)],
            &[Cow::Owned(vec![7; 256])],
            0,
        )
        .unwrap()
        .into_owned()
    };

    assert_eq!(
        eval(
            &mut shard,
            b"return ruvio.call('SET', KEYS[1], ARGV[1])",
            b"new"
        ),
        Response::Error(crate::engine::OOM_ERROR)
    );
    assert_eq!(
        eval(&mut shard, b"return ruvio.call('DEL', KEYS[1])", b"old"),
        Response::Integer(1)
    );
    assert!(shard.is_empty());
}

#[test]
fn eval_runs_ruvio_call_and_caches_the_sha() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let body = b"return ruvio.call('SET', KEYS[1], ARGV[1])";
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        body,
        &[owned(b"{t}k")],
        &[owned(b"v")],
        0,
    )
    .unwrap();

    assert_eq!(response, Response::Simple(b"OK"));
    assert_eq!(shard.get(b"{t}k"), Some(&b"v"[..]));
    let sha = hex_sha(&script_sha1(body));

    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            Command::EvalSha {
                sha: owned(sha.as_bytes()),
                keys: vec![owned(b"{t}k")],
                args: vec![owned(b"v2")],
            },
            0
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"{t}k"), Some(&b"v2"[..]));
}

#[test]
fn eval_sees_a_sorted_set_pop_as_member_score_pairs() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();

    shard
        .sorted_set_add_pairs(b"z", &[(2.5, b"b"), (1.0, b"a"), (3.0, b"c")])
        .unwrap();

    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"return ruvio.call('ZPOPMIN', KEYS[1], 2)",
        &[owned(b"z")],
        &[],
        0,
    )
    .unwrap();
    let bulk = |value: &[u8]| Response::Bulk(Some(Cow::Owned(value.to_vec())));

    assert_eq!(
        response,
        Response::Exec(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2.5")])
    );
    assert_eq!(shard.sorted_set_card(b"z"), Ok(1));
}

#[test]
fn eval_without_keys_runs_and_cross_slot_keys_are_refused() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut eval = |keys: &[Bytes<'static>]| {
        execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return 7",
            keys,
            &[],
            0,
        )
        .unwrap()
    };

    assert_eq!(eval(&[]), Response::Integer(7));
    assert_eq!(
        eval(&[owned(b"{a}1"), owned(b"{b}2")]),
        Response::Error(b"CROSSSLOT Keys in request do not hash to the same slot")
    );
}

#[test]
fn eval_sees_user_keys_and_writes_to_the_selected_database() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"ruvio.call('SET', KEYS[1], ARGV[1]); return {KEYS[1], ruvio.call('DBSIZE')}",
        &[owned(b"k")],
        &[owned(b"v")],
        2,
    )
    .unwrap();

    assert_eq!(
        response,
        Response::Exec(vec![
            Response::Bulk(Some(Cow::Owned(b"k".to_vec()))),
            Response::Integer(1),
        ])
    );
    shard.enter(0);
    assert_eq!(shard.get(b"k"), None);
    shard.enter(2);
    assert_eq!(shard.get(b"k"), Some(&b"v"[..]));
}

#[test]
fn eval_rejects_undeclared_keys_and_rolls_back_failed_scripts() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"{t}k", b"old");
    let mut cache = ScriptCache::default();
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"ruvio.call('SET', KEYS[1], 'new'); return ruvio.call('GET', 'other')",
        &[owned(b"{t}k")],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(
        response,
        Response::Error(b"ERR script attempted to access undeclared keys")
    );
    assert_eq!(shard.get(b"{t}k"), Some(&b"old"[..]));
}

#[test]
fn script_load_exists_flush_and_missing_sha() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let loaded = execute_script_command(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        Command::ScriptLoad(owned(b"return 1")),
        0,
    )
    .unwrap();
    let Response::Bulk(Some(sha)) = loaded else {
        panic!("expected SHA bulk");
    };

    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            Command::ScriptExists(vec![sha.clone(), owned(&[b'0'; 40])]),
            0
        )
        .unwrap(),
        Response::Exec(vec![Response::Integer(1), Response::Integer(0)])
    );
    execute_script_command(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        Command::ScriptFlush,
        0,
    )
    .unwrap();
    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            Command::EvalSha {
                sha,
                keys: vec![],
                args: vec![],
            },
            0
        )
        .unwrap(),
        Response::Error(b"NOSCRIPT No matching script. Please use EVAL.")
    );
}

#[test]
fn persist_failure_restores_script_keys() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"{t}k", b"old");
    let mut cache = ScriptCache::default();

    struct FailingSink;
    impl MutationSink for FailingSink {
        type Error = &'static str;
        fn record(&mut self, _mutation: Mutation<'_>) -> Result<(), Self::Error> {
            Err("persistence unavailable")
        }
    }

    assert_eq!(
        execute_eval(
            &mut shard,
            &mut FailingSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"return ruvio.call('SET', KEYS[1], ARGV[1])",
            &[owned(b"{t}k")],
            &[owned(b"new")],
            0
        ),
        Err("persistence unavailable")
    );
    assert_eq!(shard.get(b"{t}k"), Some(&b"old"[..]));
}

#[test]
fn rollback_preserves_absolute_deadlines_in_the_selected_database() {
    use crate::time::{TestClock, UnixMillis};

    struct FailingSink;
    impl MutationSink for FailingSink {
        type Error = &'static str;
        fn record(&mut self, _: Mutation<'_>) -> Result<(), Self::Error> {
            Err("persistence unavailable")
        }
    }

    for script_error in [false, true] {
        let mut shard = KvShard::with_clock(TestClock::new(1_000));

        shard.enter(2);
        shard.set_bytes(b"{t}expires", b"old");
        shard.expire_at(b"{t}expires", UnixMillis::new(5_000));
        shard.set_bytes(b"{t}persistent", b"old");
        shard.set_bytes(b"{t}expired", b"old");
        shard.expire_at(b"{t}expired", UnixMillis::new(999));
        let body = if script_error {
            b"ruvio.call('SET', KEYS[1], 'new'); ruvio.call('PEXPIRE', KEYS[1], 9000); ruvio.call('PEXPIRE', KEYS[2], 9000); ruvio.call('SET', KEYS[3], 'new', 'PX', 9000); error('abort')" as &[u8]
        } else {
            b"ruvio.call('SET', KEYS[1], 'new'); ruvio.call('PEXPIRE', KEYS[1], 9000); ruvio.call('PEXPIRE', KEYS[2], 9000); return ruvio.call('SET', KEYS[3], 'new', 'PX', 9000)"
        };

        let response = execute_eval(
            &mut shard,
            &mut FailingSink,
            &mut (),
            &mut ScriptCache::default(),
            ScriptLimits::default(),
            body,
            &[
                owned(b"{t}expires"),
                owned(b"{t}persistent"),
                owned(b"{t}missing"),
                owned(b"{t}expired"),
            ],
            &[],
            2,
        );

        if script_error {
            assert!(matches!(
                response,
                Ok(Response::Error(_) | Response::OwnedError(_))
            ));
        } else {
            assert_eq!(response, Err("persistence unavailable"));
        }

        assert_eq!(shard.get(b"{t}expires"), Some(&b"old"[..]));
        assert_eq!(
            shard.expires_at(b"{t}expires"),
            Some(UnixMillis::new(5_000))
        );
        assert_eq!(shard.pttl(b"{t}expires"), 4_000);
        assert_eq!(shard.pttl(b"{t}persistent"), -1);
        assert_eq!(shard.get(b"{t}missing"), None);
        assert_eq!(shard.get(b"{t}expired"), None);
        shard.enter(0);
        assert_eq!(shard.get(b"{t}expires"), None);
    }
}

/// Limits for tests that build large values on purpose: they reach the memory limit
/// before the quota or the deadline.
fn roomy_limits() -> ScriptLimits {
    ScriptLimits {
        max_instructions: 100_000_000,
        max_time_ms: 60_000,
        ..ScriptLimits::default()
    }
}

fn eval_body(body: &[u8], limits: ScriptLimits) -> Response<'static> {
    execute_eval(
        &mut KvShard::new(),
        &mut NoopMutationSink,
        &mut (),
        &mut ScriptCache::default(),
        limits,
        body,
        &[owned(b"{t}k")],
        &[],
        0,
    )
    .unwrap()
}

#[test]
fn redis_is_an_alias_of_the_ruvio_table_and_is_read_only() {
    let run = |body: &[u8]| eval_body(body, ScriptLimits::default());

    assert_eq!(
        run(b"redis.call('SET', KEYS[1], 'v') return redis.call('GET', KEYS[1])"),
        Response::Bulk(Some(std::borrow::Cow::Borrowed(b"v"))),
    );
    assert_eq!(
        run(b"local r = redis.pcall('NOPE') return r.err ~= nil and 1 or 0"),
        Response::Integer(1)
    );
    assert_eq!(
        run(b"return redis.status_reply('PONG')"),
        run(b"return ruvio.status_reply('PONG')")
    );
    // Like `ruvio`, the table cannot be changed by a script.
    assert!(matches!(
        run(b"redis.call = function() end"),
        Response::OwnedError(_) | Response::Error(_)
    ));
}

#[test]
fn sandbox_rejects_host_escape_and_admin_calls() {
    for body in [
        b"return os.execute('id')" as &[u8],
        b"return io.open('/etc/passwd')",
        b"return loadfile('/etc/passwd')",
        b"return require('os')",
        b"return ruvio.call('AUTH', 'x')",
        b"return ruvio.call('SAVE')",
        b"return ruvio.call('CONFIG', 'GET', '*')",
        b"return ruvio.call('ACL', 'WHOAMI')",
        b"return ruvio.call('CLUSTER', 'SLOTS')",
    ] {
        let response = eval_body(body, ScriptLimits::default());

        assert!(
            matches!(response, Response::Error(_) | Response::OwnedError(_)),
            "script {body:?} should not succeed: {response:?}"
        );

        if body.starts_with(b"return ruvio.call") {
            assert_eq!(
                response,
                Response::Error(b"ERR command not allowed from script source")
            );
        }
    }
}

/// Runs `body` on its own thread and fails the test if it has not finished within
/// `seconds`, instead of hanging the suite.
fn eval_within(seconds: u64, body: &'static [u8], limits: ScriptLimits) -> Response<'static> {
    let (sender, receiver) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let _ = sender.send(eval_body(body, limits));
    });

    match receiver.recv_timeout(std::time::Duration::from_secs(seconds)) {
        Ok(response) => response,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
            "script still running after {seconds}s: {}",
            String::from_utf8_lossy(body)
        ),
        Err(error) => panic!("script thread failed: {error}"),
    }
}

/// Lua 5.1 computes the count in `int`. Compiled with optimization, as in release
/// builds, its overflow check is folded away and these bounds crashed the process; an
/// unoptimized build happens to catch them.
#[test]
fn unpack_with_extreme_bounds_fails_instead_of_crashing() {
    for body in [
        b"return {pcall(unpack, {}, -2147483648, -1)}" as &'static [u8],
        b"return {pcall(unpack, {}, -1073741824, 1073741824)}",
        b"return {pcall(unpack, {}, -1e10, 1e10)}",
        b"return {pcall(unpack, {}, 1, 1e6)}",
    ] {
        let response = eval_within(30, body, ScriptLimits::default());
        let Response::Exec(items) = &response else {
            panic!("{response:?}");
        };

        assert_eq!(items.first(), Some(&Response::Bulk(None)), "{response:?}");
        assert!(
            matches!(items.get(1), Some(Response::Bulk(Some(message)))
                    if message.ends_with(b"too many results to unpack")),
            "{response:?}"
        );
    }

    // Ordinary ranges keep Lua 5.1's results.
    assert_eq!(
        eval_within(
            30,
            b"local t = {1, 2, 3} \
                  return {select('#', unpack(t)), select('#', unpack(t, -1, 1)), \
                  select('#', unpack(t, 3, 1)), unpack(t, 2), \
                  select('#', unpack({}, 2147483647, 2147483647))}",
            ScriptLimits::default(),
        ),
        Response::Exec(vec![
            Response::Integer(3),
            Response::Integer(3),
            Response::Integer(0),
            Response::Integer(2),
            Response::Integer(1),
        ])
    );
}

#[test]
fn an_endless_loop_inside_a_coroutine_or_pcall_hits_the_quota() {
    let limits = ScriptLimits {
        max_instructions: 100_000,
        ..ScriptLimits::default()
    };

    for body in [
        b"coroutine.wrap(function() while true do end end)()" as &'static [u8],
        b"local co = coroutine.create(function() while true do end end) \
              return coroutine.resume(co)",
        b"while true do pcall(function() while true do end end) end",
        b"while true do pcall(coroutine.wrap(function() while true do end end)) end",
        b"while true do xpcall(function() while true do end end, function(e) return e end) end",
        b"local co = coroutine.wrap(function() \
                  while true do coroutine.resume(coroutine.create(function() \
                      while true do end end)) end end) \
              while true do pcall(co) end",
    ] {
        assert_eq!(
            eval_within(30, body, limits),
            Response::Error(b"ERR Lua instruction quota exceeded"),
            "{}",
            String::from_utf8_lossy(body)
        );
    }

    // The next script gets a fresh quota, and catching ordinary errors still works.
    assert_eq!(
        eval_within(30, b"return {pcall(error, 'x', 0)}", limits),
        Response::Exec(vec![
            Response::Bulk(None),
            Response::Bulk(Some(owned(b"x")))
        ])
    );
}

#[test]
fn a_backtracking_pattern_hits_the_quota_instead_of_holding_the_shard() {
    // The default quota, with time enough that a loaded debug build hits it first.
    let quota_only = ScriptLimits {
        max_time_ms: 60_000,
        ..ScriptLimits::default()
    };
    for body in [
        b"return string.find(string.rep('a', 40), string.rep('a*', 20) .. 'b')" as &'static [u8],
        b"return string.match(string.rep('a', 100000), '.-.-b')",
        b"return string.gsub(string.rep('a', 100000), '.-b', '')",
        b"for m in string.gmatch(string.rep('a', 100000), '.-b') do end",
        b"while true do pcall(string.find, string.rep('a', 40), string.rep('a*', 20) .. 'b') end",
    ] {
        assert_eq!(
            eval_within(60, body, quota_only),
            Response::Error(b"ERR Lua instruction quota exceeded"),
            "{}",
            String::from_utf8_lossy(body)
        );
    }

    // Ordinary matching over a large subject fits in the default budget.
    assert_eq!(
        eval_within(
            60,
            b"return select(2, string.gsub(string.rep('a b ', 262144), '%s+', ' '))",
            quota_only
        ),
        Response::Integer(524_288)
    );

    // A smaller quota gives fewer steps.
    let tight = ScriptLimits {
        max_instructions: 1_000,
        ..ScriptLimits::default()
    };

    assert_eq!(
        eval_within(
            30,
            b"return string.find(string.rep('a', 1000), '.-b')",
            tight
        ),
        Response::Error(b"ERR Lua instruction quota exceeded")
    );
    assert_eq!(
        eval_within(
            30,
            b"return string.find(string.rep('a', 100), '.-b')",
            tight
        ),
        Response::Bulk(None)
    );
}

#[test]
fn library_functions_that_loop_in_c_return_promptly() {
    let run = |body: &'static [u8]| eval_within(10, body, ScriptLimits::default());

    // C appends the empty string two billion times.
    assert_eq!(
        run(b"local n = 0 for i = 1, 20 do n = n + #string.rep('', 2^31 - 1) end return n"),
        Response::Integer(0)
    );
    assert_eq!(
        run(b"return {string.rep('ab', 3), string.rep('x', 0), string.rep('x', -1)}"),
        Response::Exec(vec![
            Response::Bulk(Some(owned(b"ababab"))),
            Response::Bulk(Some(owned(b""))),
            Response::Bulk(Some(owned(b""))),
        ])
    );
    // C moves every slot from the end of the table down to the position.
    assert_eq!(
        run(b"local ok, e = pcall(table.insert, {}, -2^31 + 2, 1) return {tostring(ok), e}"),
        Response::Exec(vec![
            Response::Bulk(Some(owned(b"false"))),
            Response::Bulk(Some(owned(
                b"bad argument #2 to '?' (position out of bounds)"
            ))),
        ])
    );
    assert_eq!(
        run(b"local t = {1} table.insert(t, 0, 'z') return t[0]"),
        Response::Bulk(Some(owned(b"z")))
    );
    assert_eq!(
        run(b"local t = {1, 3} table.insert(t, 2, 2) table.insert(t, 4) return t"),
        Response::Exec(vec![
            Response::Integer(1),
            Response::Integer(2),
            Response::Integer(3),
            Response::Integer(4),
        ])
    );
    // C's plain search compares the pattern at every position: quadratic.
    assert_eq!(
        run(b"local s = string.rep('a', 1000000) \
                  return {string.find(s, string.rep('a', 500000) .. 'b', 1, true), \
                  string.find(s .. 'b', string.rep('a', 500000) .. 'b', 1, true)}"),
        Response::Exec(vec![
            Response::Bulk(None),
            Response::Integer(500001),
            Response::Integer(1000001),
        ])
    );
}

#[test]
fn coroutines_are_charged_for_the_instructions_the_hook_misses() {
    let limits = ScriptLimits {
        max_instructions: 100_000,
        ..ScriptLimits::default()
    };

    // About a million instructions, nearly all in the first thousand of fresh
    // coroutines, which the hook never counts.
    assert_eq!(
        eval_within(
            30,
            b"for i = 1, 1000 do coroutine.wrap(function() for j = 1, 450 do end end)() end \
                  return 'done'",
            limits
        ),
        Response::Error(b"ERR Lua instruction quota exceeded")
    );
    assert_eq!(
        eval_within(
            30,
            b"local n = 0 for i = 1, 20 do n = n + coroutine.wrap(function() return i end)() end \
                  return n",
            limits
        ),
        Response::Integer(210)
    );
}

#[test]
fn work_that_grows_with_data_is_stopped_by_the_quota_or_the_deadline() {
    let limits = ScriptLimits {
        max_time_ms: 200,
        ..ScriptLimits::default()
    };

    // Each costs a few instructions and up to 16 MiB of copying.
    for body in [
        b"local s = string.rep('x', 2^20) while true do local t = s .. s end" as &'static [u8],
        b"local s = string.rep('x', 2^20) \
              while true do pcall(function() local t = s .. s end) end",
    ] {
        assert_eq!(
            eval_within(30, body, limits),
            Response::Error(b"ERR Lua script time limit exceeded"),
            "{}",
            String::from_utf8_lossy(body)
        );
    }

    // Library functions charge the bytes they build or scan: the quota stops these
    // even with time to spare.
    let slow = ScriptLimits {
        max_time_ms: 60_000,
        ..ScriptLimits::default()
    };

    for body in [
        b"for i = 1, 200 do local t = string.rep('x', 2^24) end" as &'static [u8],
        b"local s = string.rep('x', 2^24) for i = 1, 100 do local u = s:upper() end",
        b"local s = string.rep('x', 2^24) for i = 1, 100 do local u = s:reverse() end",
        b"local t = {} for i = 1, 2^16 do t[i] = 'x' end \
              while true do local s = table.concat(t) end",
        b"local t = {} for i = 1, 50000 do t[i] = i end while true do table.sort(t) end",
        b"local t = {} for i = 1, 2^16 do t[i] = i end \
              while true do table.remove(t, 1) table.insert(t, 1, 0) end",
        b"local s = string.rep('x', 2^24) while true do local u = s:sub(2) end",
        b"local s = string.rep('x', 2^24) while true do local u = string.format('%s', s) end",
        b"local s = string.rep('x', 2^24) while true do local u = s:find('y', 1, true) end",
    ] {
        assert_eq!(
            eval_within(30, body, slow),
            Response::Error(b"ERR Lua instruction quota exceeded"),
            "{}",
            String::from_utf8_lossy(body)
        );
    }

    assert_eq!(
        eval_within(
            30,
            b"return #string.rep('ab', 2^20) + #string.upper(string.rep('x', 2^20))",
            limits
        ),
        Response::Integer(3 * 1024 * 1024)
    );
}

#[test]
fn an_instruction_quota_above_four_billion_is_not_truncated() {
    let limits = ScriptLimits {
        max_instructions: (1 << 32) + 1_000,
        ..ScriptLimits::default()
    };

    assert_eq!(
        eval_within(30, b"for i = 1, 5000 do end return 'done'", limits),
        Response::Bulk(Some(owned(b"done")))
    );
}

/// Runs `body` with a 200 ms deadline and checks that one of the limits stopped it
/// within a few seconds: one VM instruction or library call on a large value must not
/// run on long past the deadline.
fn assert_stopped_promptly(body: &'static [u8]) {
    assert_stopped_within(body, ScriptLimits::default().max_instructions, 4);
}

/// Asserts `body` fails with a quota or time error within `bound` seconds, with a
/// 200 ms deadline and `max_instructions`.
fn assert_stopped_within(body: &'static [u8], max_instructions: usize, bound: u64) {
    let limits = ScriptLimits {
        max_instructions,
        max_time_ms: 200,
        ..ScriptLimits::default()
    };
    let started = std::time::Instant::now();
    let response = eval_within(30, body, limits);
    let elapsed = started.elapsed();

    assert!(
        matches!(
            response,
            Response::Error(
                b"ERR Lua instruction quota exceeded"
                    | b"ERR Lua script time limit exceeded"
                    | b"ERR Lua ruvio.call quota exceeded"
            )
        ),
        "{}: {response:?}",
        String::from_utf8_lossy(body)
    );
    assert!(
        elapsed < std::time::Duration::from_secs(bound),
        "{} ran {elapsed:?}",
        String::from_utf8_lossy(body)
    );
}

#[test]
fn an_xpcall_handler_cannot_outlive_the_quota() {
    for body in [
        b"return xpcall(function() while true do end end, function(e) while true do end end)"
            as &'static [u8],
        b"return xpcall(function() error('x') end, function(e) while true do end end)",
        b"return xpcall(function() local s = string.rep('x', 2^30) end, \
              function(e) while true do end end)",
        b"while true do xpcall(function() while true do end end, function(e) return e end) end",
    ] {
        assert_stopped_promptly(body);
    }

    // An ordinary handler still runs.
    assert_eq!(
            eval_within(
                30,
                b"return {xpcall(function() error('x', 0) end, function(e) return 'handled ' .. e end)}",
                ScriptLimits::default()
            ),
            Response::Exec(vec![Response::Bulk(None), Response::Bulk(Some(owned(b"handled x")))])
        );
}

#[test]
fn a_table_length_far_past_its_entries_is_charged_before_it_is_read() {
    // About 30 entries whose length, found by binary search, is 671,088,640.
    for body in [
        b"local t = {} t[1] = 1 t[2] = 1 t[4] = 1 local j = 5 \
              while j < 2^30 do t[j] = 1 j = j * 2 end table.sort(t)" as &'static [u8],
        b"local t = {} t[1] = 1 t[2] = 1 t[4] = 1 local j = 5 \
              while j < 2^30 do t[j] = 1 j = j * 2 end \
              return xpcall(function() error(t) end, table.sort)",
    ] {
        // The default quota is spent before the scan starts.
        let started = std::time::Instant::now();
        assert_eq!(
            eval_within(30, body, ScriptLimits::default()),
            Response::Error(b"ERR Lua instruction quota exceeded"),
            "{}",
            String::from_utf8_lossy(body)
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        // With quota to spare the deadline stops the scan, which unchecked took
        // about 12 s in a release build.
        assert_stopped_within(body, usize::MAX, 10);
    }
}

#[test]
fn large_values_cannot_hold_the_shard_past_the_deadline() {
    for body in [
            // The replacement is read once per match.
            b"local s = string.rep('y', 2^20) return #s:gsub('', string.rep('%0', 2^19))"
                as &'static [u8],
            // Each comparison reads both strings.
            b"local s = string.rep('x', 2^25) local t = {} for i = 1, 500 do t[i] = s end \
              table.sort(t) return #t",
            // One VM instruction each.
            b"local s = string.rep('x', 2^25) local n = 0 while true do if s < s then n = n + 1 end end",
            b"local s = string.rep('x', 2^24) while true do local t = s .. s end",
            // Library calls that read a large argument.
            b"local s = string.rep('1', 2^25) local n = 0 \
              while true do n = n + (tonumber(s) and 1 or 0) end",
            b"local sep = string.rep('x', 2^24) while true do local u = table.concat({'', ''}, sep) end",
            b"local t = {} for i = 1, 100000 do t['k' .. i] = i end \
              while true do local m = table.maxn(t) end",
            // A command per call, few instructions between.
            b"for b = 1, 20 do local t = {} for i = 1, 5000 do t[i] = i end \
              ruvio.call('RPUSH', KEYS[1], unpack(t)) end \
              while true do local r = ruvio.call('LRANGE', KEYS[1], 0, -1) end",
        ] {
            assert_stopped_promptly(body);
        }

    assert_eq!(
        eval_within(
            30,
            b"local t = {} for i = 1, 100 do t[i] = i end t[1000] = 1 t.x = 5 \
                  return {table.maxn(t), table.maxn({}), table.concat({1, 'a', 2}, '-')}",
            ScriptLimits::default()
        ),
        Response::Exec(vec![
            Response::Integer(1000),
            Response::Integer(0),
            Response::Bulk(Some(owned(b"1-a-2"))),
        ])
    );
}

#[test]
fn string_dump_is_not_available() {
    assert_eq!(
        eval_body(b"return type(string.dump)", ScriptLimits::default()),
        Response::Bulk(Some(owned(b"nil")))
    );
}

#[test]
fn eval_enforces_instruction_and_call_quotas() {
    let tight = ScriptLimits {
        max_instructions: 1_000,
        max_calls: 2,
        ..ScriptLimits::default()
    };

    assert_eq!(
        eval_body(b"while true do end", tight),
        Response::Error(b"ERR Lua instruction quota exceeded")
    );
    assert_eq!(
        eval_body(
            b"ruvio.call('PING'); ruvio.call('PING'); return ruvio.call('PING')",
            tight
        ),
        Response::Error(b"ERR Lua ruvio.call quota exceeded")
    );
}

#[test]
fn function_load_fcall_list_and_delete_stay_out_of_the_wal() {
    // FUNCTION LOAD, FUNCTION LIST, FUNCTION DELETE, and FCALL stay in memory.
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let mut sink = NoopMutationSink;
    let limits = ScriptLimits::default();
    let loaded = execute_script_command(
        &mut shard,
        &mut sink,
        &mut (),
        &mut cache,
        limits,
        Command::FunctionLoad {
            name: owned(b"put"),
            body: owned(b"return ruvio.call('SET', KEYS[1], ARGV[1])"),
        },
        0,
    )
    .unwrap();

    assert_eq!(
        loaded,
        Response::Bulk(Some(std::borrow::Cow::Owned(b"put".to_vec())))
    );
    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut sink,
            &mut (),
            &mut cache,
            limits,
            Command::FCall {
                name: owned(b"put"),
                keys: vec![owned(b"{t}k")],
                args: vec![owned(b"v")],
            },
            0
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(shard.get(b"{t}k"), Some(&b"v"[..]));
    let listed = execute_script_command(
        &mut shard,
        &mut sink,
        &mut (),
        &mut cache,
        limits,
        Command::FunctionList,
        0,
    )
    .unwrap();

    assert_eq!(
        listed,
        Response::Exec(vec![Response::Bulk(Some(std::borrow::Cow::Owned(
            b"put".to_vec()
        )))])
    );
    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut sink,
            &mut (),
            &mut cache,
            limits,
            Command::FunctionDelete(owned(b"put")),
            0
        )
        .unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        execute_script_command(
            &mut shard,
            &mut sink,
            &mut (),
            &mut cache,
            limits,
            Command::FCall {
                name: owned(b"put"),
                keys: vec![owned(b"{t}k")],
                args: vec![owned(b"v")],
            },
            0
        )
        .unwrap(),
        Response::Error(b"ERR function not found")
    );
}

fn eval_with<S: MutationSink>(
    shard: &mut KvShard,
    sink: &mut S,
    body: &[u8],
    keys: &[&[u8]],
) -> Result<Response<'static>, S::Error> {
    let keys: Vec<_> = keys.iter().map(|key| owned(key)).collect();

    execute_eval(
        shard,
        sink,
        &mut (),
        &mut ScriptCache::default(),
        ScriptLimits::default(),
        body,
        &keys,
        &[],
        0,
    )
}

fn big_sorted_set(shard: &mut KvShard) {
    let members: Vec<_> = (0..2_000u32)
        .map(|index| (f64::from(index), format!("m{index}").into_bytes()))
        .collect();

    shard.sorted_set_add_pairs(b"{t}z", &members).unwrap();
}

#[test]
fn a_reading_script_copies_no_key() {
    let mut shard = KvShard::new();

    big_sorted_set(&mut shard);
    crate::engine::UNDO_COPIES.set(0);
    let reply = eval_with(
        &mut shard,
        &mut NoopMutationSink,
        b"return ruvio.call('ZSCORE', KEYS[1], 'm7')",
        &[b"{t}z"],
    )
    .unwrap();

    assert_eq!(reply, Response::Bulk(Some(Cow::Borrowed(b"7"))));
    assert_eq!(crate::engine::UNDO_COPIES.get(), 0);
}

#[test]
fn a_failing_script_restores_only_what_it_wrote() {
    let mut shard = KvShard::new();

    big_sorted_set(&mut shard);
    shard.set_bytes(b"{t}a", b"old");
    let before = shard.snapshot_entries();
    crate::engine::UNDO_COPIES.set(0);

    let reply = eval_with(
        &mut shard,
        &mut NoopMutationSink,
        b"ruvio.call('ZCARD', KEYS[1]); ruvio.call('GET', KEYS[2]); \
              ruvio.call('SET', KEYS[2], 'new'); ruvio.call('APPEND', KEYS[2], 'x'); \
              ruvio.call('SET', KEYS[3], 'created'); error('abort')",
        &[b"{t}z", b"{t}a", b"{t}created"],
    )
    .unwrap();

    assert!(matches!(
        reply,
        Response::Error(_) | Response::OwnedError(_)
    ));
    assert_eq!(
        crate::engine::UNDO_COPIES.get(),
        2,
        "the read zset is not copied"
    );
    assert_eq!(shard.snapshot_entries(), before);
}

#[test]
fn a_refused_script_batch_restores_a_key_written_after_reads() {
    let mut shard = KvShard::new();

    big_sorted_set(&mut shard);
    let before = shard.snapshot_entries();

    assert_eq!(
        eval_with(
            &mut shard,
            &mut RefusingSink,
            b"ruvio.call('ZSCORE', KEYS[1], 'm1'); ruvio.call('ZADD', KEYS[1], 5, 'new'); \
                  return ruvio.call('ZREM', KEYS[1], 'm2')",
            &[b"{t}z"],
        ),
        Err(())
    );
    assert_eq!(shard.snapshot_entries(), before);
}

#[test]
fn a_script_refused_for_memory_leaves_its_keys_in_a_running_snapshot() {
    let mut shard = KvShard::new();

    shard.set_bytes(b"{t}a", b"old");
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 16));
    shard.begin_snapshot(4_096).unwrap();

    // The refused SET changes nothing, but the failed script still restores the key.
    let reply = eval_with(
        &mut shard,
        &mut NoopMutationSink,
        b"return ruvio.call('SET', KEYS[1], string.rep('x', 4096))",
        &[b"{t}a"],
    )
    .unwrap();

    assert!(matches!(
        reply,
        Response::Error(_) | Response::OwnedError(_)
    ));
    assert_eq!(shard.get(b"{t}a"), Some(&b"old"[..]));
    let snapshot = shard.finish_snapshot();

    assert!(
        snapshot.iter().any(|entry| entry.key == b"{t}a"
            && entry.value == crate::store::SnapshotValue::Bytes(b"old".to_vec())),
        "the snapshot lost a key the script did not change: {snapshot:?}"
    );
}

#[test]
fn a_rolled_back_timed_hash_still_expires_its_fields() {
    use crate::time::TestClock;

    let clock = TestClock::new(1_000);
    let mut shard = KvShard::with_clock(clock.clone());
    let mut cache = ScriptCache::default();
    let mut eval = |shard: &mut KvShard<TestClock>, body: &[u8]| {
        execute_eval(
            shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[owned(b"{t}h")],
            &[],
            0,
        )
        .unwrap()
    };

    eval(
        &mut shard,
        b"ruvio.call('HSET', KEYS[1], 'short', 'a', 'long', 'b'); \
              return ruvio.call('HPEXPIRE', KEYS[1], 100, 'FIELDS', 1, 'short')",
    );
    shard.set_memory_limit(Some(shard.memory_usage().charged_bytes() + 16));

    // Refused for memory: the script fails and puts the hash back.
    let refused = eval(
        &mut shard,
        b"return ruvio.call('HSET', KEYS[1], 'big', string.rep('x', 4096))",
    );

    assert!(matches!(
        refused,
        Response::Error(_) | Response::OwnedError(_)
    ));
    clock.set(2_000);
    assert_eq!(
        eval(&mut shard, b"return ruvio.call('HKEYS', KEYS[1])[1]"),
        Response::Bulk(Some(owned(b"long")))
    );
    assert_eq!(
        eval(&mut shard, b"return ruvio.call('HLEN', KEYS[1])"),
        Response::Integer(1)
    );
}

#[test]
fn status_and_error_replies_stay_on_one_line() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();

    for (body, error) in [
        (&br"return {err='bad\r\n+FORGED'}"[..], true),
        (&br"return {ok='fine\r\n+FORGED'}"[..], false),
        (&br"return ruvio.error_reply('bad\r\n:1')"[..], true),
        (&br"return {1, {err='bad\n:1'}}"[..], true),
    ] {
        let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            body,
            &[],
            &[],
            0,
        )
        .unwrap();
        let text = match &response {
            Response::OwnedError(text) | Response::OwnedSimple(text) if error || !error => {
                text.clone()
            }
            Response::Exec(items) => match &items[1] {
                Response::OwnedError(text) => text.clone(),
                other => panic!("unexpected {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        };

        assert!(!text.contains(&b'\r') && !text.contains(&b'\n'));
    }
}

#[test]
fn compiled_bytecode_is_refused() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let response = execute_eval(
        &mut shard,
        &mut NoopMutationSink,
        &mut (),
        &mut cache,
        ScriptLimits::default(),
        b"\x1bLua",
        &[],
        &[],
        0,
    )
    .unwrap();

    assert_eq!(
        response,
        Response::Error(b"ERR Lua bytecode is not allowed")
    );
}

#[test]
fn a_reply_repeating_one_large_string_is_refused() {
    let mut shard = KvShard::new();
    let mut cache = ScriptCache::default();
    let response = execute_eval(
            &mut shard,
            &mut NoopMutationSink,
            &mut (),
            &mut cache,
            ScriptLimits::default(),
            b"local s = string.rep('x', 1024 * 1024) local t = {} for i = 1, 100 do t[i] = s end return t",
            &[],
            &[],
            0,
        )
        .unwrap();

    assert_eq!(response, Response::Error(b"ERR Lua reply is too large"));
}

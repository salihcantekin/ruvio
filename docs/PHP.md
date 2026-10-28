# PHP

There is no first-party PHP client. The two common Redis clients work against Ruvio:
[phpredis](https://github.com/phpredis/phpredis) (the `redis` extension) and
[Predis](https://github.com/predis/predis).

Checked against Ruvio with phpredis (PHP 8.3) and Predis 3.7: `hMSet`/`hmset`, `hGetAll`,
`set` with `NX`/`EX`, `setex`, `get`, `expireAt` and `ping` behave as with Redis.

## Connecting

```php
// phpredis
$redis = new Redis();
$redis->connect('127.0.0.1', 6379, 2.0); // 2 s connect timeout
$redis->auth('password');                // when the server has one

// Predis
$client = new Predis\Client('tcp://127.0.0.1:6379?password=password');
```

Neither client sends `CLIENT SETINFO` or `HELLO` when it connects. If your code sends
`CLIENT SETINFO` itself, Ruvio answers with an error and the connection stays open.

With `shards` above 1 any one port serves every key: a command for a key another shard owns
is forwarded there, so a plain client works. A cluster-aware client (`RedisCluster` in
phpredis, `'cluster' => 'redis'` in Predis) connects to each shard and saves that hop; keep
the keys of one multi-key command in one slot with a hash tag (`{user:1}:a`, `{user:1}:b`).

## Frameworks

Framework cache and session handlers that use these clients need nothing special:

- **CodeIgniter 4**: the Redis cache handler writes entries with `HMSET`, which Ruvio
  supports (it replies `OK`, like Redis). Set `$redis` in `app/Config/Cache.php` to the
  Ruvio host, port and password, and use `RedisHandler` for sessions the same way.
- **Laravel**: set `REDIS_CLIENT=phpredis` (or `predis`), `REDIS_HOST`, `REDIS_PORT` and
  `REDIS_PASSWORD`; `CACHE_STORE=redis` and `SESSION_DRIVER=redis` then use Ruvio.

Commands Ruvio does not support reply with an error; the full list of what is supported
is in [COMPATIBILITY.md](COMPATIBILITY.md).

# Limits

Ruvio runs a shard's commands on one thread, so a command that walks a lot of data holds up every
client of that shard. These bounds keep one call short. They are fixed in the server unless a
setting is named.

| Command | Bound | What happens past it |
| --- | --- | --- |
| `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | `COUNT` at most 10000 | A larger `COUNT` is lowered to 10000. A scan call may return fewer keys than `COUNT` (or none) and still not be finished; keep calling with the returned cursor until it is `0`. |
| `KEYS` | 100000 keys in the database | The command answers an error; use `SCAN`. |
| `ZPOPBYSCORE` | 10000 members per call | At most 10000 members are popped; call again for the rest. |

## Reading big collections

`SMEMBERS`, `HGETALL`, `LRANGE` and similar commands read the whole answer on the shard thread
before the reply is sent. For a collection with many thousands of members use `SSCAN`, `HSCAN`,
`ZSCAN` or a ranged `LRANGE` instead. The reply buffer of a connection is limited as well, and a
client that does not read its replies is disconnected when the buffer is full; see
`docs/CONFIGURATION.md`.

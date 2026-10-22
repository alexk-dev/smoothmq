# Restricted Redis credentials

Use a patched Redis release (7.4.11 or newer supported/security-patched branch).
Keep Redis private and dedicate it to the queue. The broker needs queue data
operations, Lua evaluation, INFO persistence and CONFIG GET for readiness; it does
not need administrative mutation, module loading, migration or restore commands.

Example ACL for a deployment whose --redis-prefix is queue. Replace the password
placeholder outside source control; keep a separate protected operator identity
for backup and maintenance. Redis 7.2+ also supports CLIENT SETINFO.

```
user default off
user broker on >REPLACE_WITH_RANDOM_SECRET ~queue:* -@all +ping +hello +auth +select +client|setname +info +config|get +eval +evalsha +script|load +script|exists +get +set +del +unlink +incr +hget +hset +hdel +hlen +hgetall +sadd +scard +smembers +srem +sscan +zadd +zcard +zcount +zrange +zrangebyscore +zrem +zrevrange +time +client|setinfo
```

Connect with redis://broker:PASSWORD@HOST:6379/0 (or rediss for TLS), preferably
through a mounted URL file. Key patterns must match the configured prefix. The
acceptance suite uses a restricted test identity with this command set while its
isolated fixture keeps a separate administrator for failure simulation.

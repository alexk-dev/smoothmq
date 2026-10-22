# Restricted Redis credentials

Use a patched Redis release (7.4.11 or newer supported/security-patched branch).
Keep Redis private and dedicate it to the queue. The broker needs queue data
operations, Lua evaluation, INFO persistence and CONFIG GET for readiness; it does
not need administrative mutation, module loading, migration or restore commands.

Example ACL for a deployment whose --redis-prefix is queue. Replace the password
placeholder outside source control; keep a separate protected operator identity
for backup and maintenance. Redis 7.2+ also supports CLIENT SETINFO.

```
user default off resetpass ~queue:* +@all
user broker on >REPLACE_WITH_RANDOM_SECRET ~queue:* -@all +ping +hello +auth +select +client|setname +info +config|get +eval +evalsha +script|load +script|exists +get +set +del +unlink +incr +hget +hset +hdel +hlen +hgetall +sadd +scard +smembers +srem +sscan +zadd +zcard +zcount +zrange +zrangebyscore +zrem +zrevrange +time +client|setinfo
```

Connect with redis://broker:PASSWORD@HOST:6379/0 (or rediss for TLS), preferably
through a mounted URL file. Key patterns must match the configured prefix. The
acceptance suite uses a restricted test identity with this command set while its
isolated fixture keeps a separately authenticated administrator for failure
simulation. Anonymous access and authentication as `default` remain disabled.

The disabled `default` user's permissions are required for Redis 7.4.11 AOF
replay. Its internal AOF client uses that identity, and `EXEC` checks its command
and key permissions when replaying Lua write transactions. A bare `user default
off` can therefore reject persisted writes during startup even though the broker
was allowed to write them. Keep `off resetpass`: do not add `on`, `nopass`, or a
password to this identity. The key pattern must cover every persisted queue
prefix in this dedicated Redis database. `+@all` grants internal replay
permissions; it does not enable network authentication.

This behavior is visible in Redis 7.4.11's
[AOF client creation](https://github.com/redis/redis/blob/7.4.11/src/aof.c#L1250),
[default client identity](https://github.com/redis/redis/blob/7.4.11/src/networking.c#L86),
and [transaction ACL checks](https://github.com/redis/redis/blob/7.4.11/src/multi.c#L167).
Validate crash recovery with the actual ACL configuration, and check startup
logs for AOF-loading errors as well as readiness. If a replay fails, preserve the
original AOF before changing configuration; do not clear or rewrite it to make
the server start.

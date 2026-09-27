"""Cache and distributed-lock adapters: in-memory (tests/local) and Redis."""

from __future__ import annotations

import time
import uuid

from redis.asyncio import Redis

# Release only if we still own the lock (the TTL may have expired and another
# replica may hold it now).
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


class InMemoryCache:
    def __init__(self) -> None:
        self._data: dict[str, tuple[str, float]] = {}

    async def get(self, key: str) -> str | None:
        item = self._data.get(key)
        if item is None:
            return None
        value, expires = item
        if expires < time.monotonic():
            self._data.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: str, ttl_seconds: float) -> None:
        self._data[key] = (value, time.monotonic() + ttl_seconds)

    async def delete(self, *keys: str) -> None:
        for k in keys:
            self._data.pop(k, None)


class InMemoryLock:
    def __init__(self) -> None:
        self._held: dict[str, tuple[str, float]] = {}

    async def acquire(self, name: str, ttl_seconds: float) -> str | None:
        now = time.monotonic()
        held = self._held.get(name)
        if held is not None and held[1] > now:
            return None
        token = uuid.uuid4().hex
        self._held[name] = (token, now + ttl_seconds)
        return token

    async def release(self, name: str, token: str) -> bool:
        held = self._held.get(name)
        if held is None or held[0] != token:
            return False
        del self._held[name]
        return True


class RedisCache:
    def __init__(self, redis: Redis, prefix: str = "bls:") -> None:
        self.redis = redis
        self.prefix = prefix

    async def get(self, key: str) -> str | None:
        value = await self.redis.get(self.prefix + key)
        return value.decode() if isinstance(value, bytes) else value

    async def set(self, key: str, value: str, ttl_seconds: float) -> None:
        await self.redis.set(self.prefix + key, value, px=int(ttl_seconds * 1000))

    async def delete(self, *keys: str) -> None:
        if keys:
            await self.redis.delete(*(self.prefix + k for k in keys))


class RedisLock:
    """SET NX PX lock with owner token. Good enough for "one sweeper at a time";
    not a fencing-token lock, so it must not guard money movements (those rely on
    database row locks)."""

    def __init__(self, redis: Redis, prefix: str = "bls:lock:") -> None:
        self.redis = redis
        self.prefix = prefix

    async def acquire(self, name: str, ttl_seconds: float) -> str | None:
        token = uuid.uuid4().hex
        ok = await self.redis.set(self.prefix + name, token, nx=True, px=int(ttl_seconds * 1000))
        return token if ok else None

    async def release(self, name: str, token: str) -> bool:
        res = await self.redis.eval(_RELEASE_LUA, 1, self.prefix + name, token)  # type: ignore[misc]
        return bool(res)

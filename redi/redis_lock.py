# -*- coding: utf-8 -*-
import uuid

import redis

from redi.redis_client import redis_client


def acquire_lock(lock_key: str, ttl: int) -> str | None:
    try:
        lock_value = str(uuid.uuid4())
        ok = redis_client.set(
            name=lock_key,
            value=lock_value,
            nx=True,
            ex=ttl
        )
        return lock_value if ok else None

    except redis.exceptions.AuthenticationError as e:
        # 认证错误，必须直接抛出
        raise Exception(f"Redis auth failed: {e}")

    except Exception as e:
        raise Exception(f"Redis lock error: {e}")


RELEASE_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def release_lock(lock_key: str, lock_value: str):
    try:
        redis_client.eval(
            RELEASE_LUA,
            1,
            lock_key,
            lock_value
        )
    except redis.exceptions.AuthenticationError as e:
        raise Exception(f"Redis auth failed: {e}")
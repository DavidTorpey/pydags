"""
This module contains the base classes and implementations for caches to be used
by both the pipeline internals during DAG execution, as well as for users to
potentially subclass. Any cache must implement as least 'read', 'write', and
'delete' methods.

The two caches implemented here are a in-memory cache powered by Redis, and a
local disk cache.

The Redis cache is used by the pipeline to read/write data pertaining to the
execution of the pipeline and its constituent stages (e.g. stages that are in
progress, completed, etc.).

The disk cache is not used by the pipeline, but it, along with the Redis cache,
can be subclassed by a user to inherent the associated caching functionality.
The most common use case for doing so is to pass data throughout your DAG.

References:
    https://pypi.org/project/diskcache/
    https://redis.io/
"""

from abc import ABC, abstractmethod

import diskcache
import redis


class Cache(ABC):
    """
    Abstract base class for all cache implementations. ALl subclasses must
    implement a read, write, and delete method.
    """

    @abstractmethod
    def read(self, *args, **kwargs):
        ...

    @abstractmethod
    def write(self, *args, **kwargs):
        ...

    @abstractmethod
    def delete(self, *args, **kwargs):
        ...


class InvalidCacheTypeException(Exception):
    pass


class InvalidKeyTypeException(Exception):
    pass


class InvalidValueTypeException(Exception):
    pass


class RedisCache(Cache):
    """
    Implementation for an in-memory cache to be used by the Pipeline
    implementation itself, as well as potentially by users who wish to inherit
    this functionality. The in-memory caching technology used is the key-value
    store Redis.

    The use of the cache assumes redis has been installed and is running.
    """

    def __init__(self, redis_instance: redis.Redis):
        if not isinstance(redis_instance, redis.Redis):
            raise InvalidCacheTypeException('Please ensure redis_instance is of type redis.Redis')

        self.redis_instance = redis_instance

    def read(self, k: str) -> bytes:
        """Read a value from Redis given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        return self.redis_instance.get(k)

    def write(self, k: str, v: bytes) -> None:
        """Write a value to Redis given a key-value pair."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        if not isinstance(v, (str, bytes)):
            raise InvalidValueTypeException('Please ensure value is of type string or bytes')

        self.redis_instance.set(k, v)

    def delete(self, k: str) -> None:
        """Delete a value from Redis given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        self.redis_instance.delete(k)


class DiskCache(Cache):
    """
    Implementation for a disk cache to be used by users who wish to inherit
    this functionality. We use the diskcache Python package from pypi to
    implement this caching feature.
    """

    def __init__(self, disk_cache: diskcache.Cache):
        if not isinstance(disk_cache, diskcache.Cache):
            raise InvalidCacheTypeException('Please ensure disk_cache is of type diskcache.Cache')

        self.disk_cache = disk_cache

    def read(self, k: str) -> bytes:
        """Read a value from the disk cache given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        return self.disk_cache[k]

    def write(self, k: str, v: bytes) -> None:
        """Write a value to the disk cache given a key-value pair."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        if not isinstance(v, (str, bytes)):
            raise InvalidValueTypeException('Please ensure value is of type string or bytes')

        self.disk_cache[k] = v

    def delete(self, k: str) -> None:
        """Delete a value from disk cache given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        self.disk_cache.delete(k)

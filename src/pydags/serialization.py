"""
This module contains the bases classes and implementations for the different
serializers in use in pydags. Serialization is important to enable reading from
and writing to the various key-value caches implemented in pydags (e.g.
pydags.cache.RedisCache and pydags.cache.DiskCache). Users may implement their
own serializers, however, any implementation must contain as least 'serialize'
and 'deserialize' methods.

Various pickle-based serialization algorithms are implemented: 1) vanilla
pickle, 2) dill, and 3) cloudpickle. There are limitations of each, however,
the default is pickle unless more comprehensive serialization is required
(such as with the Pipeline implementation, where cloudpickle is used).

Some of the noted limitations include:
1. pickle
    - Cannot serialize decorated functions

2. dill
    - Cannot serialize subclasses of abstract base classes
"""

import pickle
from abc import ABC, abstractmethod

import dill
import cloudpickle


class Serializer(ABC):
    @abstractmethod
    def serialize(self, *args, **kwargs):
        ...

    @abstractmethod
    def deserialize(self, *args, **kwargs):
        ...


class SerializationException(Exception):
    pass


class InvalidTypeForDeserializationException(SerializationException):
    pass


class PickleSerializer(Serializer):  # Cannot serialize decorated functions
    def serialize(self, obj: object) -> bytes:
        return pickle.dumps(obj)

    def deserialize(self, serialized_obj: bytes) -> object:
        if not isinstance(serialized_obj, bytes):
            raise InvalidTypeForDeserializationException('Please ensure the serialized object is of type bytes.')
        return pickle.loads(serialized_obj)


class DillSerializer(Serializer):  # Cannot serialize (subclasses of) abstract base classes
    def serialize(self, obj: object) -> bytes:
        return dill.dumps(obj)

    def deserialize(self, serialized_obj: bytes) -> object:
        if not isinstance(serialized_obj, bytes):
            raise InvalidTypeForDeserializationException('Please ensure the serialized object is of type bytes.')
        return dill.loads(serialized_obj)


class CloudPickleSerializer(Serializer):
    def serialize(self, obj: object) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize(self, serialized_obj: bytes) -> object:
        if not isinstance(serialized_obj, bytes):
            raise InvalidTypeForDeserializationException('Please ensure the serialized object is of type bytes.')
        return cloudpickle.loads(serialized_obj)

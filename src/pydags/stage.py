"""
This module contains the base classes of the Stage functionality for pydags,
as well as various implementations of Stages to be used in different contexts.
Most implementations can be subclassed and extended by the user. All Stages
must contain 1) a 'name' property and 2) 'run' method.

There are two primary ways for users to define Stages in their own pipelines.
The first is by decorating a function with the pydags.stage.stage decorator.
The other is by subclassing Stage, RedisStage, DiskCacheStage.
"""

from abc import ABC, abstractmethod

import diskcache
import redis

from .serialization import PickleSerializer
from .cache import RedisCache, DiskCache


class Stage(ABC):
    """
    Base abstract class from which all stages must inherit. All subclasses must
    implement at least `name` and `run` methods.

    preceding_stages: List of preceding stages for the stage
    name: Name of the stage
    """
    def __init__(self):
        self.preceding_stages = list()

    def after(self, pipeline_stage):
        """Method to add stages as dependencies for the current stage."""
        self.preceding_stages.append(pipeline_stage)
        return self

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def run(self, *args, **kwargs):
        ...


class DecoratorStage(Stage):
    """
    Class to wrap any user-defined function decorated with the stage decorator.

    stage_function: The callable defined by the user-defined function pipeline
                    stage.
    args: The arguments to the user-defined function pipeline stage.
    kwargs: The keyword arguments to the user-defined function pipeline stage.
    """

    def __init__(self, stage_function: callable, *args, **kwargs):
        super().__init__()

        self.stage_function = stage_function
        self.args = args
        self.kwargs = kwargs

    @property
    def name(self) -> str:
        """Name is given by the name of the user-defined decorated function."""
        return self.stage_function.__name__

    def run(self) -> None:
        """
        Stage is run by calling the wrapped user function with its arguments.
        """
        self.stage_function(*self.args, **self.kwargs)


def stage(stage_function: callable):
    """
    Decorator used to specify user-defined functions as pipeline stages (i.e.
    DAG nodes). The decorated wraps the decorated function in the
    DecoratorStage class, as this follows the expected format for a pipeline
    stage.
    """
    def wrapper(*args, **kwargs) -> DecoratorStage:
        return DecoratorStage(stage_function, *args, **kwargs)

    return wrapper


class StageExecutor(PickleSerializer, RedisCache):
    """
    Context manager for the execution of a stage, or group of stages, of a
    pipeline.

    The setup phase (__enter__) persists relevant metadata such as the stages
    currently in progress to a Redis server.

    The teardown phase (__exit__) deletes relevant metadata from the Redis
    server.

    redis_instance: A connection to Redis.
    stages: The stages that are currently in progress.
    """
    def __init__(self, redis_instance: redis.Redis, stages):
        RedisCache.__init__(self, redis_instance)

        self.pipeline = self.read('pipeline')
        self.stages = stages

    def __enter__(self):
        self.write('in_progress', self.serialize(self.stages))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        completed = self.deserialize(self.read('in_progress'))
        current_done = self.read('done')
        if current_done is None:
            current_done = []
        else:
            current_done = self.deserialize(current_done)
        current_done += completed
        self.write('done', self.serialize(current_done))
        self.delete('in_progress')

    @staticmethod
    def execute(fn: callable, *args, **kwargs) -> None:
        """Execute the stage/group of stages."""
        fn(*args, **kwargs)


class RedisStage(Stage, PickleSerializer, RedisCache):
    """
    Stage type to use if an in-memory cache (i.e. Redis) is required. Redis can
    be used to pass data between stages, or cache values to be used elsewhere
    downstream. It's completely up to the implementer/user, as this interface
    to Redis is generic, and enables the reading/writing of generic Python
    objects from/to Redis through pickle-based serialization.

    The underlying DAG of the Pipeline object requires serialisation itself as
    part of the inner workings of pydags. As such, the __getstate__ and
    __setstate__ dunder methods are overridden in order to temporarily stop
    Redis upon pickling, and to restart it when unpickling.
    """

    def __init__(self, redis_instance):
        RedisCache.__init__(self, redis_instance=redis_instance)
        Stage.__init__(self)

    def __getstate__(self):
        self.redis_metadata = self.redis_instance.connection_pool.connection_kwargs
        self.redis_instance.close()
        return self

    def __setstate__(self, state):
        self.redis_instance = redis.Redis(**self.redis_metadata)

    def read(self, k: str) -> object:
        return self.deserialize(RedisCache.read(self, k))

    def write(self, k: str, v: object) -> None:
        RedisCache.write(self, k, self.serialize(v))

    @property
    def name(self) -> str:
        """
        The name is the final subclass (i.e. the name class defined by the user
        when subclassing this class.
        """
        return self.__class__.__name__


class DiskCacheStage(Stage, PickleSerializer, DiskCache):
    """
    Stage type to use if a disk-based cache is required. The disk cache can be
    used to pass data between stages, or cache values to be used elsewhere
    downstream. It's completely up to the implementer/user, as this interface
    to the disk cache is generic, and enables the reading/writing of generic
    Python objects from/to the disk cache through pickle-based serialization.
    """

    def __init__(self, cache: diskcache.Cache):
        DiskCache.__init__(self, disk_cache=cache)
        Stage.__init__(self)

    def read(self, k: str) -> object:
        return self.deserialize(DiskCache.read(self, k))

    def write(self, k: str, v: object) -> None:
        DiskCache.write(self, k, self.serialize(v))

    @property
    def name(self) -> str:
        """
        The name is the final subclass (i.e. the name class defined by the user
        when subclassing this class.
        """
        return self.__class__.__name__

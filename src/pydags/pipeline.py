"""
This module contains the implementation of the 'Pipeline': the entity
synonymous with a DAG, which contains one or more 'Stages'. The pipeline
models the associated DAG as a networkx.DiGraph object, and ensure the DAG
property remains satisfied as nodes/stages are added.

The execution of the pipeline can be run serial mode or in parallel mode. In
serial mode, the stages are executed in the correct order (as defined by the
interdependencies between stages), but are run in sequence. In this mode,
stages that can be run in parallel are not. In parallel mode, stages that can
be run in parallel are, and are split across the number of cores specific upon
pipeline execution.

A bundle of stages in a pipeline that can be run in parallel are known as a
'Group'. In serial mode, Groups contain only the single stage currently being
executed. In parallel mode, Groups contains all the nodes that can be run in
parallel at that time. In both cases, groups are run within a bespoke context
manager (pydags.stage.StageExecutor). It is during startup and teardown of this
context manager that pertinent information relating to DAG execution (such as
stages in progress, completed stages, etc.) is written to Redis. This
information can then be read from Redis and used downstream.
"""

from io import BytesIO
from multiprocessing.pool import ThreadPool
import typing
import logging

import networkx as nx
import redis
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from .serialization import CloudPickleSerializer
from .cache import RedisCache
from .stage import StageExecutor, Stage


class StageException(Exception):
    pass


class InvalidStageTypeException(StageException):
    pass


class DAGException(Exception):
    pass


class DAGVerificationException(DAGException):
    pass


class Pipeline(CloudPickleSerializer, RedisCache):
    """
    Implementation of the Pipeline functionality of pydags. The goal of a
    Pipeline is to orchestrate the execution of the DAG's constituent stages,
    and write associated Stage metadata during execution. This metadata, at
    present, consists of the nodes in progress, and the completed nodes.

    pipeline: The instance of networkx.DiGraph to house the underlying DAG of
              the pipeline. The reason this library is used is because various
              important things are already implemented, including topological
              sorting, graph visualization, numerous graph algorithms, etc.
    """

    pipeline = nx.DiGraph()

    def __init__(self, redis_cache=redis.Redis(host='localhost', port=6379, db=1)):
        """
        We use database number 1 from Redis for the Pipeline's underlying Redis
        cache.
        """

        RedisCache.__init__(self, redis_cache)

    def topological_sort_grouped(self) -> typing.Generator:
        """
        Method to perform a topological sort on the DAG/pipeline. However,
        this sort differs from nx.topological_sort in that it is grouped. This
        means that stages that can be run in parallel at each level of the DAG
        are grouped together. This is done in the case the user wants to
        distribute the execution of the pipeline across cores. In that case,
        the stages in each group will run in parallel. The default behaviour,
        however, is to run the entire pipeline serially, include stages within
        the same paralellizable group.

        Returns:
             Generator where each element is a list of nodes in the same group.
        """

        logging.info('Computing a grouped topological sort of the pipeline DAG')
        indegree_map = {v: d for v, d in self.pipeline.in_degree() if d > 0}
        zero_indegree = [v for v, d in self.pipeline.in_degree() if d == 0]
        while zero_indegree:
            yield zero_indegree
            new_zero_indegree = []
            for v in zero_indegree:
                for _, child in self.pipeline.edges(v):
                    indegree_map[child] -= 1
                    if not indegree_map[child]:
                        new_zero_indegree.append(child)
            zero_indegree = new_zero_indegree

    def add_stage(self, stage: Stage) -> None:
        """
        Method to add a stage to the pipeline. Stages are not added if they
        already exist in the DAG (although networkx accommodates for this).
        A stage is defined according to its name (usually the user-defined
        class or function name), and an attribute called 'stage_wrapper',
        which is the actual instance of a subclass of BaseStage. This object
        is used to run the associated DAG stage.

        Additionally, we add edges in the DAG between a stage and its preceding
        stages (as defined by the user).

        Lastly, a check is done to ensure that after adding the stage, the DAG
        is still indeed a DAG.
        """

        if not isinstance(stage, Stage):
            raise InvalidStageTypeException('Please ensure your stage is a subclass of pydags.stage.Stage')

        self.pipeline.add_node(stage.name, stage_wrapper=stage)

        for preceding_stage in stage.preceding_stages:
            self.pipeline.add_edges_from([(preceding_stage.name, stage.name)])

        if not nx.is_directed_acyclic_graph(self.pipeline):
            raise DAGVerificationException('Pipeline is no longer a DAG!')

    def add_stages(self, stages: typing.List[Stage]) -> None:
        """
        Method to add a list of stages to the pipeline. Stages are not added if
        they already exist in the DAG (although networkx accommodates for
        this). A stage is defined according to its name (usually the
        user-defined class or function name), and an attribute called
        'stage_wrapper', which is the actual instance of a subclass of
        BaseStage. This object is used to run the associated DAG stage.

        Additionally, we add edges in the DAG between a stage and its preceding
        stages (as defined by the user).

        Lastly, a check is done to ensure that after adding the stage, the DAG
        is still indeed a DAG.
        """

        for stage in stages:
            self.add_stage(stage)

    def run_stage(self, stage_name: str) -> None:
        """
        Method to run a particular stage of the pipeline/DAG. The
        associated instance of BaseStage is obtained used the stage name and
        executed using the required 'run' method.

        Args:
            stage_name <str>: Name of the stage in the pipeline.
        """
        self.pipeline.nodes[stage_name]['stage_wrapper'].run()

    def start(self, num_cores: int = None) -> None:
        """
        Method to execute the pipeline (and all its constituent stages). The
        order of execution is defined by the grouped topological sort. The
        stages within a group will be executed in parallel (across cores) if
        num_cores is a positive integer. If num_cores remains None (as per
        default), then the entire pipeline (including stages within the same
        group) will run serially.

        Args:
            num_cores [<int>, <None>]: Number of cores to distribute across.
        """

        logging.info('Serializing pipeline and writing to Redis')
        self.write('pipeline', self.serialize(self.pipeline))

        sorted_grouped_stages = self.topological_sort_grouped()
        for group in sorted_grouped_stages:
            logging.info('Processing group: %s', group)
            if num_cores:
                pool = ThreadPool(num_cores)
                with StageExecutor(self.redis_instance, group) as stage_executor:
                    stage_executor.execute(pool.map, self.run_stage, group)
            else:
                for stage in group:
                    with StageExecutor(self.redis_instance, [stage]) as stage_executor:
                        stage_executor.execute(self.run_stage, stage)
        self.delete('done')

    def visualize(self):
        """
        Method to visualize the pipeline/DAG by rendering a matplotlib figure.

        References:
            https://stackoverflow.com/questions/10379448/plotting-directed-graphs-in-python-in-a-way-that-show-all-edges-separately
        """

        drawing = nx.drawing.nx_pydot.to_pydot(self.pipeline)

        png_str = drawing.create_png()
        sio = BytesIO()
        sio.write(png_str)
        sio.seek(0)

        img = mpimg.imread(sio)
        plt.imshow(img)

        plt.show()

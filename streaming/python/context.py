from abc import ABC, abstractmethod

import ray.streaming.function as function
from ray.streaming.datastream import StreamSource
from ray.streaming.runtime.gateway_client import GatewayClient


class StreamingContext:
    """Wrapper of java org.ray.streaming.api.context.StreamingContext"""

    class Builder:
        def __init__(self):
            self._options = {}

        def option(self, key=None, value=None, conf=None):
            if conf is None:
                self._options[key] = str(value)
            else:
                for k, v in conf.items():
                    self._options[k] = v
            return self

        def build(self):
            ctx = StreamingContext()
            ctx._gateway_client.with_config(self._options)
            return ctx

    def __init__(self):
        self._python_gateway_actor = self.__create_python_gateway_actor()
        self.__gateway_client = GatewayClient(self._python_gateway_actor)
        self._j_ctx = self._gateway_client.create_streaming_context()

    def source(self, source_func):
        return StreamSource.build_source(self, source_func)

    def from_values(self, *values):
        return self.from_collection(values)

    def from_collection(self, values):
        func = function.ListSourceFunction(values)
        return self.source(func)

    def reset_text_file(self, filename):
        """Reads the given file line-by-line and creates a data stream that
         contains a string with the contents of each such line."""
        func = function.LocalFileSourceFunction(filename)
        return self.source(func)

    def execute(self):
        """Construct job DAG, and execute the job.
        """
        self._gateway_client.execute()

    def _do_init(self):
        pass

    def __create_python_gateway_actor(self):
        pass

    @property
    def _gateway_client(self):
        return self.__gateway_client


class RuntimeContext(ABC):
    @abstractmethod
    def get_task_id(self):
        pass

    @abstractmethod
    def get_task_index(self):
        pass

    @abstractmethod
    def get_parallelism(self):
        pass


class RuntimeContextImpl(RuntimeContext):
    def __init__(self, task_id, task_index, parallelism):
        self.task_id = task_id
        self.task_index = task_index
        self.parallelism = parallelism

    def get_task_id(self):
        return self.task_id

    def get_task_index(self):
        return self.task_index

    def get_parallelism(self):
        return self.parallelism

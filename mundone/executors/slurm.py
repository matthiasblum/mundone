import os
import re
import sys
from datetime import datetime
from subprocess import Popen, DEVNULL, PIPE

from mundone import runner, states


class SlurmExecutor:
    def __init__(self, **params):
        self.name = params.get("name")
        self.queue = params.get("queue")
        self.project = params.get("project")
        self.num_cpus = params.get("cpu")
        self.memory = params.get("mem")
        self.temp = params.get("tmp")
        self.scratch = params.get("scratch")
        self.out_file = None
        self.id = None

    def submit(self, src: str, dst: str, out: str, err: str) -> int | None:
        pass  # TODO

    def poll(self) -> int:
        pass  # TODO

    def ready_to_collect(self) -> bool:
        pass  # TODO

    def get_times(self) -> tuple[datetime, datetime]:
        pass  # TODO

    @staticmethod
    def get_times_from_string(stdout: str) -> tuple[datetime, datetime]:
        pass  # TODO

    @staticmethod
    def get_max_memory(stdout: str) -> int | None:
        pass  # TODO

    @staticmethod
    def get_cpu_time(stdout: str) -> int | None:
        pass  # TODO

    def kill(self, force: bool = False):
        pass  # TODO

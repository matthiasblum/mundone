import os
import sys
from datetime import datetime
from subprocess import Popen

from mundone import runner, states


class LocalExecutor:
    def __init__(self, **params):
        self.name = params.get("name")
        self.queue = params.get("queue")
        self.project = params.get("project")
        self.num_cpus = params.get("cpu")
        self.num_gpus = params.get("gpu")
        self.memory = params.get("mem")
        self.temp = params.get("tmp")
        self.scratch = params.get("scratch")
        self.id = None
        self.out = self.err = self.proc = None
        self.start_time = self.end_time = None

    def submit(self, src: str, dst: str, out: str, err: str) -> int:
        cmd = [
            sys.executable,
            os.path.realpath(runner.__file__),
            src,
            dst
        ]

        self.out = open(out, "wt")
        self.err = open(err, "wt")
        self.proc = Popen(cmd, stdout=self.out, stderr=self.err)
        self.start_time = datetime.now()
        return self.proc.pid

    def poll(self) -> int:
        returncode = self.proc.poll()
        if returncode is None:
            return states.RUNNING

        self.out.close()
        self.err.close()
        self.end_time = datetime.now()
        return states.SUCCESS if returncode == 0 else states.ERROR

    def ready_to_collect(self) -> bool:
        return True

    def get_times(self, *args) -> tuple[datetime, datetime]:
        return self.start_time, self.end_time

    @staticmethod
    def is_oom(*args) -> bool:
        # Cannot know? Assume false.
        return False

    @staticmethod
    def get_max_memory(*args):
        return None

    @staticmethod
    def get_cpu_time(*args):
        return None

    def kill(self, force: bool = False):
        if self.proc is not None:
            self.proc.kill()

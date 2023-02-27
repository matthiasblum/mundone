import math
import os
import re
import sys
from datetime import datetime, timedelta
from subprocess import Popen, DEVNULL, PIPE

from mundone import runner, states


STATES = {
    "BOOT_FAIL": states.ERROR,
    "CANCELLED": states.ERROR,
    "COMPLETED": states.SUCCESS,
    "DEADLINE": states.ERROR,
    "FAILED": states.ERROR,
    "NODE_FAIL": states.ERROR,
    "OUT_OF_MEMORY": states.ERROR,
    "PENDING": states.PENDING,
    "PREEMPTED": states.ERROR,
    "RUNNING": states.RUNNING,
    "REQUEUED": states.RUNNING,
    "RESIZING": states.RUNNING,
    "REVOKED": states.ERROR,
    "SUSPENDED": states.RUNNING,
    "TIMEOUT": states.ERROR,
}
JOB_SIGNATURE = "__mundone_slurm__"


class SlurmExecutor:
    def __init__(self, **params):
        self.name = params.get("name")
        self.queue = params.get("queue")
        self.project = params.get("project")
        self.num_cpus = params.get("cpu")
        self.memory = params.get("mem")
        self.temp = params.get("tmp")
        self.scratch = params.get("scratch")
        self.limit = timedelta(hours=params.get("hours", 24))
        self.out_file = None
        self.id = None

    def submit(self, src: str, dst: str, out: str, err: str) -> int | None:
        self.out_file = out
        self.id = None
        cmd = ["sbatch", "--parsable"]
        if self.name and isinstance(self.name, str):
            cmd += ["-J", self.name]

        if self.queue and isinstance(self.queue, str):
            cmd += ["-p", self.queue]

        cmd += [
            "-N", "1",  # --nodes=1
            "-n", "1",  # --ntasks=1
        ]
        if isinstance(self.num_cpus, int) and self.num_cpus > 1:
            cmd += ["-c", str(self.num_cpus)]

        if isinstance(self.memory, (float, int)):
            cmd.append(f"--mem={self.memory}M")

        if isinstance(self.temp, (float, int)):
            cmd.append(f"--tmp={self.temp}M")

        time_limit = self.limit.total_seconds()
        hours, remainder = divmod(time_limit, 3600)
        minutes, seconds = divmod(remainder, 60)
        cmd += ["-t", f"{hours:.0f}:{minutes:02.0f}:{seconds:02.0f}"]
        cmd += ["-o", out, "-e", err]

        runner_file = os.path.realpath(runner.__file__)
        cmd += ["--wrap", f'"{sys.executable} {runner_file} {src} {dst}; '
                          f'echo ${JOB_SIGNATURE}"']
        outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        outs = outs.strip().decode()
        self.id = int(outs)
        return self.id

    def poll(self) -> int:
        info = self.run_sacct(self.id)
        state = info.get("State")
        return STATES.get(state, states.NOT_FOUND)

    def ready_to_collect(self) -> bool:
        try:
            with open(self.out_file, "rt") as fh:
                return JOB_SIGNATURE in fh.read()
        except FileNotFoundError:
            return False

    def get_times(self) -> tuple[datetime, datetime]:
        info = self.run_sacct(self.id)
        fmt = "%Y-%m-%dT%H:%M:%S"
        return (
            datetime.strptime(info["Start"], fmt),
            datetime.strptime(info["End"], fmt)
        )

    @staticmethod
    def run_sacct(jobid: int) -> dict[str, str]:
        columns = ["Submit", "Start", "End", "State", "TotalCPU", "MaxRSS"]
        cmd = [
            "sacct",
            "-P",
            "-o", ",".join(columns),
            "-X",
            "-j", str(jobid)
        ]
        outs, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        lines = outs.decode().splitlines()
        keys = lines[0].split("|")
        try:
            values = lines[1].split("|")
        except IndexError:
            values = [None] * len(keys)

        return dict(zip(keys, values))

    def get_max_memory(self, *args) -> int | None:
        info = self.run_sacct(self.id)
        # todo
        if not info["MaxRSS"]:
            return None

    def get_cpu_time(self, *args) -> int:
        info = self.run_sacct(self.id)
        pattern = r"(?:(?:(\d+)-)?(\d+):)?(\d+):(\d+)"  # [DD-[HH:]]MM:SS
        d, h, m, s = re.fullmatch(pattern, info["TotalCPU"]).groups()
        seconds = timedelta(
            days=int(d) if d else 0,
            hours=int(h) if h else 0,
            minutes=int(m),
            seconds=int(s)
        ).total_seconds()
        return math.floor(seconds)

    def kill(self, force: bool = False):
        if self.id is None:
            return

        cmd = ["sancel", str(self.id)]
        Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()

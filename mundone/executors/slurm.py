import math
import os
import re
import sys
from datetime import datetime, timedelta
from subprocess import Popen, DEVNULL, PIPE
from typing import Any

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


class SlurmExecutor:
    def __init__(self, **params):
        self.name = params.get("name")
        self.queue = params.get("queue")
        self.project = params.get("project")
        self.num_cpus = params.get("cpu")
        self.num_gpus = params.get("gpu")
        self.memory = params.get("mem")
        self.temp = params.get("tmp")
        self.scratch = params.get("scratch")
        self.limit = timedelta(hours=params.get("hours", 24))
        self.out_file = None
        self.id = None
        self._jobinfo = None
        self._seen_running = False
        self._cancelled = False

    def submit(self, src: str, dst: str, out: str, err: str) -> int | None:
        self.out_file = out
        self.id = None
        self._jobinfo = None
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
            # --cpus-per-task
            cmd += ["-c", str(self.num_cpus)]

        if isinstance(self.num_gpus, int) and self.num_gpus >= 1:
            cmd += ["--gpus-per-task", str(self.num_gpus)]
        elif isinstance(self.num_gpus, str):
            # format: [type:]<number>
            # https://slurm.schedmd.com/sbatch.html#OPT_gpus-per-task
            cmd += ["--gpus-per-task", self.num_gpus]

        if isinstance(self.memory, (float, int)):
            cmd.append(f"--mem={self.memory:.0f}M")

        if isinstance(self.temp, (float, int)):
            cmd.append(f"--tmp={self.temp:.0f}M")

        time_limit = self.limit.total_seconds()
        hours, remainder = divmod(time_limit, 3600)
        minutes, seconds = divmod(remainder, 60)
        cmd += ["-t", f"{hours:.0f}:{minutes:02.0f}:{seconds:02.0f}"]
        cmd += ["-o", out, "-e", err]

        runner_file = os.path.realpath(runner.__file__)
        cmd += ["--wrap", f"{sys.executable} {runner_file} {src} {dst}"]
        outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        outs = outs.strip().decode()
        try:
            job_id = int(outs)
        except ValueError as exc:
            sys.stderr.write(f"ValueError/start: {exc}: "
                             f"{outs.rstrip()} - {errs.rstrip()}\n{cmd}\n")
        else:
            self.id = job_id
            return job_id

        return None

    def poll(self) -> int:
        info = self.get_jobinfo(force=True)
        state = info.get("State")
        status = STATES.get(state, states.PENDING)

        if status == states.RUNNING:
            self._seen_running = True
        elif status == states.CANCELLED:
            self._cancelled = True

        return status

    def ready_to_collect(self) -> bool:
        return os.path.isfile(self.out_file) or self._cancelled

    def get_times(self, *args) -> tuple[datetime | None, datetime | None]:
        info = self.get_jobinfo()
        return info["Start"], info["End"]

    def get_jobinfo(self, force: bool = False) -> dict[str, Any]:
        if force or self._jobinfo is None:
            self._jobinfo = self.run_sacct(self.id)

        return self._jobinfo

    @staticmethod
    def parse_date(s: str) -> datetime | None:
        try:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
        else:
            return dt

    @staticmethod
    def parse_time(s: str) -> int:
        # [DD-[HH:]]MM:SS[.MS]
        d, h, m, s = re.fullmatch(
            r"(?:(?:(\d+)-)?(\d+):)?(\d+):(\d+(?:\.\d+)?)",
            s
        ).groups()
        seconds = timedelta(
            days=int(d) if d else 0,
            hours=int(h) if h else 0,
            minutes=int(m),
            seconds=float(s)
        ).total_seconds()
        return math.floor(seconds)

    @staticmethod
    def parse_memory(s: str) -> float | None:
        if not s or s == "16?":
            return None

        suffixes = {"K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
        factor = suffixes.get(s[-1], 0)
        if factor != 0:
            s = s[:-1]

        # In MB
        return float(s) * pow(1024, factor) / pow(1024, 2)

    @staticmethod
    def parse_state(s: str) -> str:
        return s.split()[0]

    @staticmethod
    def run_sacct(jobid: int) -> dict[str, Any]:
        fields = [
            ("JobID", str, None),
            ("Submit", SlurmExecutor.parse_date, min),
            ("Start", SlurmExecutor.parse_date, min),
            ("End", SlurmExecutor.parse_date, max),
            ("State", SlurmExecutor.parse_state, None),
            ("TotalCPU", SlurmExecutor.parse_time, max),
            ("ReqMem", SlurmExecutor.parse_memory, None),
            ("MaxRSS", SlurmExecutor.parse_memory, max),
        ]
        sep = chr(30)
        cmd = [
            "sacct",
            "--parsable2",
            "--delimiter", sep,
            "--noheader",
            "--format", ",".join([field[0] for field in fields]),
            "-j", str(jobid)
        ]
        outs, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

        data = {field[0]: None for field in fields}
        for line in outs.splitlines():
            values = line.decode("utf-8").split(sep)

            if values[0].endswith(".extern"):
                continue

            for (key, parser, comparator), value in zip(fields, values):
                if value == "":
                    continue

                value = parser(value)
                if value is None:
                    continue
                elif data[key] is None:
                    data[key] = value
                elif comparator:
                    data[key] = comparator(data[key], value)

        return data

    def get_max_memory(self, *args) -> int | None:
        info = self.get_jobinfo()
        return info["MaxRSS"]

    def is_oom(self, *args) -> bool:
        info = self.get_jobinfo()
        return info.get("State") == "OUT_OF_MEMORY"

    def get_cpu_time(self, *args) -> int | None:
        info = self.get_jobinfo()
        if info["TotalCPU"] is not None:
            return math.floor(info["TotalCPU"])
        return None

    def kill(self, force: bool = False):
        if self.id is None:
            return

        cmd = ["scancel", str(self.id)]
        Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()

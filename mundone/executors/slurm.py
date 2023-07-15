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
        self.jobinfo = None

    def submit(self, src: str, dst: str, out: str, err: str) -> int | None:
        self.out_file = out
        self.id = None
        self.jobinfo = None
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
            cmd.append(f"--mem={self.memory}M")

        if isinstance(self.temp, (float, int)):
            cmd.append(f"--tmp={self.temp}M")

        time_limit = self.limit.total_seconds()
        hours, remainder = divmod(time_limit, 3600)
        minutes, seconds = divmod(remainder, 60)
        cmd += ["-t", f"{hours:.0f}:{minutes:02.0f}:{seconds:02.0f}"]
        cmd += ["-o", out, "-e", err]

        runner_file = os.path.realpath(runner.__file__)
        cmd += ["--wrap", f"{sys.executable} {runner_file} {src} {dst}"]
        outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        outs = outs.strip().decode()
        self.id = int(outs)
        return self.id

    def poll(self) -> int:
        info = self.run_sacct(self.id)
        state = info.get("State")
        return STATES.get(state, states.NOT_FOUND)

    def ready_to_collect(self) -> bool:
        # TODO: check that output (binary) file exists if job completed?
        return os.path.isfile(self.out_file)

    def get_times(self) -> tuple[datetime, datetime]:
        info = self.get_jobinfo()
        fmt = "%Y-%m-%dT%H:%M:%S"
        return (
            datetime.strptime(info["Start"], fmt),
            datetime.strptime(info["End"], fmt)
        )

    def get_jobinfo(self, force: bool = False) -> dict[str, Any]:
        if force or self.jobinfo is None:
            self.jobinfo = self.run_sacct(self.id)

        return self.jobinfo

    @staticmethod
    def parse_date(s: str) -> datetime | None:
        try:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
        else:
            return dt

    @staticmethod
    def parse_time(s: str) -> int | None:
        # [days-]hours:minutes:seconds[.microseconds]
        m = re.fullmatch(r"(\d+-)?(\d+):(\d+):(\d+)(\.\d+)?", s)
        if m:
            d, h, m, s, ms = m.groups()
            d = int(d) if d else 0
            ms = float(ms) if ms else 0
            td = timedelta(days=d,
                           hours=int(h),
                           minutes=int(m),
                           seconds=int(s),
                           microseconds=ms)
            return int(td.total_seconds())

        return None

    @staticmethod
    def parse_memory(s: str) -> float | None:
        m = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGTP])?", s)
        if m:
            value, suffix = m.groups()
            i = [None, "K", "M", "G", "T", "P"].index(suffix)
            return float(value) / pow(1024, i)

        return None

    @staticmethod
    def run_sacct(jobid: int) -> dict[str, Any]:
        fields = ["JobID", "Submit", "Start",
                  "End", "State", "TotalCPU", "ReqMem", "MaxRSS"]
        sep = chr(30)
        cmd = [
            "sacct",
            "--parsable2",
            "--delimiter", sep,
            "--noheader",
            "--format", ",".join(fields),
            "-j", str(jobid)
        ]
        outs, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

        data = {key: None for key in fields}

        for line in outs.decode().splitlines():
            try:
                values = line[0].split(sep)
            except IndexError:
                raise ValueError(f"cannot parse sacct output: {line}")

            if values[0].endswith(".extern"):
                continue

            for field, value in zip(fields[1:], values[1:]):
                if field in ("Submit", "Start", "State"):
                    if field != "State":
                        value = SlurmExecutor.parse_date(value)

                    if value and data[field] is None:
                        data[field] = value
                else:
                    if field == "End":
                        value = SlurmExecutor.parse_date(value)
                    elif field == "TotalCPU":
                        value = SlurmExecutor.parse_time(value)
                    else:
                        value = SlurmExecutor.parse_memory(value)

                    if data[field] is None or value > data[field]:
                        data[field] = value

        return data

    def get_max_memory(self, *args) -> int | None:
        info = self.get_jobinfo()
        return info["MaxRSS"]

    def get_cpu_time(self, *args) -> int:
        info = self.get_jobinfo()
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

        cmd = ["scancel", str(self.id)]
        Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()

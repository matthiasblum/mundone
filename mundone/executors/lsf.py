import os
import re
import sys
from datetime import datetime
from subprocess import Popen, PIPE
from typing import Optional

from mundone import runner, states


STATES = {
    "PEND": states.PENDING,
    "DONE": states.SUCCESS,
    "EXIT": states.ERROR,
    "RUN": states.RUNNING,
    "UNKWN": states.UNKNOWN,
    "ZOMBI": states.ZOMBIE,
}


class LsfExecutor:
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

    def submit(self, src: str, dst: str, out: str, err: str) -> Optional[int]:
        self.out_file = out
        self.id = None
        cmd = ["bsub"]
        if self.name and isinstance(self.name, str):
            cmd += ["-J", self.name]

        if self.queue and isinstance(self.queue, str):
            cmd += ["-q", self.queue]

        if self.project and isinstance(self.project, str):
            cmd += ["-P", self.project]

        if isinstance(self.num_cpus, int) and self.num_cpus > 1:
            cmd += ["-n", str(self.num_cpus), "-R", "span[hosts=1]"]

        if isinstance(self.memory, (float, int)):
            cmd += [
                "-M", f"{self.memory:.0f}M",
                "-R", f"select[mem>={self.memory:.0f}M]",
                "-R", f"rusage[mem={self.memory:.0f}M]"
            ]

        for key, val in [("tmp", self.temp), ("scratch", self.scratch)]:
            if isinstance(val, (float, int)):
                cmd += [
                    "-R", f"select[{key}>={val:.0f}M]",
                    "-R", f"rusage[{key}={val:.0f}M]"
                ]

        cmd += ["-o", out, "-e", err]
        cmd += [sys.executable, os.path.realpath(runner.__file__), src, dst]

        outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        outs = outs.strip().decode()

        # Expected: Job <job_id> is submitted to [default ]queue <queue>.
        try:
            job_id = int(outs.split('<')[1].split('>')[0])
        except IndexError as exc:
            sys.stderr.write(f"IndexError/start: {exc}: "
                             f"{outs.rstrip()} - {errs.rstrip()}\n")
        else:
            self.id = job_id
            return job_id

        return None

    def poll(self) -> Optional[int]:
        cmd = ["bjobs", "-w", str(self.id)]

        try:
            out, err = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        except OSError:
            return None

        out = out.strip().decode()
        err = err.strip().decode()

        if out:
            try:
                lsf_status = out.splitlines()[1].split()[2]
            except IndexError:
                # Assume pending so checked again later
                return states.PENDING

            return STATES.get(lsf_status, states.PENDING)

        # Job likely not found
        return None

    def ready_to_collect(self) -> bool:
        try:
            with open(self.out_file, "rt") as fh:
                return "Resource usage summary:" in fh.read()
        except FileNotFoundError:
            return False

    def get_start_end_times(self) -> tuple[datetime, datetime]:
        start_time = end_time = None

        with open(self.out_file, "rt") as fh:
            stdout = fh.read()

        fmt = "%a %b %d %H:%M:%S %Y"
        match = re.search(r"^Started at (.+)$", stdout, re.M)
        try:
            start_time = datetime.strptime(match.group(1), fmt)
        except (AttributeError, ValueError):
            pass

        match = re.search(r"^Terminated at (.+)$", stdout, re.M)
        try:
            end_time = datetime.strptime(match.group(1), fmt)
        except (AttributeError, ValueError):
            pass

        return start_time, end_time


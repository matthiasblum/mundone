import os
import re
import sys
from datetime import datetime
from subprocess import Popen, DEVNULL, PIPE

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
        self.num_gpus = params.get("gpu")
        self.memory = params.get("mem")
        self.temp = params.get("tmp")
        self.scratch = params.get("scratch")
        self.limit = None  # we don't support runtime limit (-W) for LSF
        self.out_file = None
        self.id = None
        self._seen_running = False

    def submit(self, src: str, dst: str, out: str, err: str) -> int | None:
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

        if isinstance(self.num_gpus, int) and self.num_gpus >= 1:
            cmd += ["-gpu", f"num={self.num_gpus}"]
        elif isinstance(self.num_gpus, str):
            # https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=o-gpu
            cmd += ["-gpu", self.num_gpus]

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
                             f"{outs.rstrip()} - {errs.rstrip()}\n{cmd}\n")
        except ValueError as exc:
            sys.stderr.write(f"ValueError/start: {exc}: "
                             f"{outs.rstrip()} - {errs.rstrip()}\n{cmd}\n")
        else:
            self.id = job_id
            return job_id

        return None

    def poll(self) -> int:
        cmd = ["bjobs", "-w", str(self.id)]

        try:
            out, err = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
        except OSError:
            return states.PENDING

        out = out.strip().decode()
        err = err.strip().decode()

        if out:
            try:
                lsf_status = out.splitlines()[1].split()[2]
            except IndexError:
                # Assume pending so checked again later
                return states.PENDING

            status = STATES.get(lsf_status, states.PENDING)
            if status == states.RUNNING:
                self._seen_running = True

            return status

        return states.NOT_FOUND

    def ready_to_collect(self) -> bool:
        if self._seen_running:
            try:
                with open(self.out_file, "rt") as fh:
                    return "Resource usage summary:" in fh.read()
            except FileNotFoundError:
                return False
        else:
            return True

    @staticmethod
    def get_times(stdout: str) -> tuple[datetime | None, datetime | None]:
        return LsfExecutor.get_times_from_string(stdout)

    @staticmethod
    def get_times_from_string(stdout: str) -> tuple[datetime | None,
                                                    datetime | None]:
        fmt = "%a %b %d %H:%M:%S %Y"
        start_time = end_time = None
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

    @staticmethod
    def get_max_memory(stdout: str) -> int | None:
        match = re.search(r"^\s*Max Memory :\s+(\d+\sMB|-)$", stdout, re.M)
        try:
            group = match.group(1)
            return 0 if group == "-" else int(group.split()[0])
        except (AttributeError, ValueError):
            return None

    def is_oom(self, stdout: str) -> bool:
        maxmem = self.get_max_memory(stdout)
        return (maxmem is not None and
                self.memory is not None and
                maxmem >= self.memory)

    @staticmethod
    def get_cpu_time(stdout: str) -> int | None:
        match = re.search(r"^\s*CPU time :\s+(\d+)\.\d+ sec.$", stdout, re.M)
        try:
            return int(match.group(1))
        except (AttributeError, ValueError):
            return None

    def kill(self, force: bool = False):
        if self.id is None:
            return
        elif force:
            cmd = ["bkill", "-r", str(self.id)]
        else:
            cmd = ["bkill", str(self.id)]

        Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()

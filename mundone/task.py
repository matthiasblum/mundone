# -*- coding: utf-8 -*-

import inspect
import os
import pickle
import struct
import tempfile
import sys
import time
from datetime import datetime
from subprocess import Popen, PIPE, DEVNULL
from typing import Callable, Collection, Optional, Set

from . import runner

STATUSES = {
    "pending": None,
    "running": 1,
    "success": 0,
    "error": 2
}


def mktemp(prefix: Optional[str]=None, suffix: Optional[str]=None,
           dir: Optional[str]=None, isdir: bool=False) -> str:

    if isdir:
        path = tempfile.mkdtemp(suffix, prefix, dir)
    else:
        fd, path = tempfile.mkstemp(suffix, prefix, dir)
        os.close(fd)

    return path


class Task(object):
    def __init__(self, fn: Callable, args: Collection=[], kwargs: dict=dict(),
                 **_kwargs):

        if not callable(fn):
            raise TypeError("'{}' is not callable".format(fn))
        elif not isinstance(args, (list, tuple)):
            raise TypeError("Task() arg 2 must be a list or a tuple")
        elif not isinstance(kwargs, dict):
            raise TypeError("Task() arg 3 must be a dict")

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

        self.name = _kwargs.get("name", fn.__name__)
        self.status = STATUSES["pending"]
        self.proc = None
        self.job_id = None
        self.input_f = None
        self.output_f = None
        self.stdout_f = None
        self.stderr_f = None
        self.log_files = None  # (stdout, stderr) file handlers (local job)

        self.stdout = None
        self.stderr = None
        self._output = None

        self._submit_time = None
        self._start_time = None
        self._end_time = None

        self._trust_scheduler = True
        self._returncode = None

        if _kwargs.get("scheduler"):
            if isinstance(_kwargs["scheduler"], dict):
                self.scheduler = _kwargs["scheduler"]
            else:
                self.scheduler = {}
        else:
            self.scheduler = None

        if isinstance(_kwargs.get("requires"), (tuple, list)):
            requires = self.inputs
            for dep in set(_kwargs["requires"]):
                if isinstance(dep, Task):
                    requires.add(dep.name)
                else:
                    requires.add(dep)
            self.requires = requires
        else:
            self.requires = self.inputs

    def __repr__(self) -> str:
        return self.name

    @property
    def inputs(self) -> Set[str]:
        names = set()
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                names.add(arg.task)

        for key, arg in self.kwargs.items():
            if isinstance(arg, TaskOutput):
                names.add(arg.task)

        return names

    @property
    def output(self):
        return TaskOutput(self)

    @property
    def pid(self) -> Optional[int]:
        if self.proc is not None:
            return self.proc.pid
        elif self.job_id is not None:
            return -self.job_id
        else:
            return None

    @property
    def start_time(self) -> Optional[str]:
        if self._start_time:
            return self._start_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    @property
    def end_time(self) -> Optional[str]:
        if self._end_time:
            return self._end_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    @property
    def submit_time(self) -> Optional[str]:
        if self._submit_time:
            return self._submit_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    @property
    def state(self) -> str:
        for name, value in STATUSES.items():
            if self.status == value:
                return name

    def pack(self, workdir: str) -> str:
        os.makedirs(workdir, exist_ok=True)

        self.input_f = mktemp(prefix=self.name, suffix=".in.p", dir=workdir)
        self.output_f = self.input_f[:-5] + ".out.p"

        with open(self.input_f, "wb") as fh:
            module = inspect.getmodule(self.fn)
            module_path = module.__file__
            module_name = module.__name__

            for _ in range(len(module_name.split('.'))):
                module_path = os.path.dirname(module_path)

            p = pickle.dumps((self.fn, self.args, self.kwargs))

            if module_name == "__main__":
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                p = p.replace(b"c__main__", b"c" + module_name.encode())

            fh.write(struct.pack(
                "<2I{}s{}s".format(len(module_path), len(module_name)),
                len(module_path), len(module_name), module_path.encode(), module_name.encode()
            ))

            fh.write(p)

        return self.input_f[:-5]

    def ready(self) -> bool:
        args = []
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                if arg.ready():
                    args.append(arg.read())
                else:
                    return False
            else:
                args.append(arg)

        kwargs = {}
        for key, arg in self.kwargs.items():
            if isinstance(arg, TaskOutput):
                if arg.ready():
                    kwargs[key] = arg.read()
                else:
                    return False
            else:
                kwargs[key] = arg

        self.args = args
        self.kwargs = kwargs
        return True

    def run(self, workdir: str=os.getcwd(), trust_scheduler: bool=True):
        if not self.ready():
            return

        self._trust_scheduler = trust_scheduler

        self._output = None
        self.stdout = None
        self.stderr = None
        self._end_time = None

        basepath = self.pack(workdir)
        self.stdout_f = basepath + ".out"
        self.stderr_f = basepath + ".err"
        self._submit_time = datetime.now()

        if isinstance(self.scheduler, dict):
            cmd = ["bsub", "-J", self.name]

            if isinstance(self.scheduler.get("queue"), str):
                cmd += ["-q", self.scheduler["queue"]]

            num_cpus = self.scheduler.get("cpu")
            if isinstance(num_cpus, int) and num_cpus > 1:
                cmd += ["-n", str(num_cpus), "-R", "span[hosts=1]"]

            mem = self.scheduler.get("mem")
            if isinstance(mem, int):
                cmd += [
                    "-M", str(mem),
                    "-R", "select[mem>{}]".format(mem),
                    "-R", "rusage[mem={}]".format(mem)
                ]

            tmp = self.scheduler.get("tmp")
            if isinstance(tmp, int):
                cmd += [
                    "-R", "select[tmp>{}]".format(tmp),
                    "-R", "rusage[tmp={}]".format(tmp)
                ]

            tmp = self.scheduler.get("scratch")
            if isinstance(tmp, int):
                cmd += [
                    "-R", "select[scratch>{}]".format(tmp),
                    "-R", "rusage[scratch={}]".format(tmp)
                ]

            cmd += ["-o", self.stdout_f, "-e", self.stderr_f]
            cmd += [
                sys.executable,
                os.path.realpath(runner.__file__),
                self.input_f,
                self.output_f
            ]

            outs, errs = Popen(cmd, stdout=PIPE).communicate()
            outs = outs.strip().decode()
            try:
                # Expected format: Job <job_id> is submitted to queue <queue>.
                job_id = int(outs.split('<')[1].split('>')[0])
            except (IndexError, ValueError):
                self.status = STATUSES["error"]
            else:
                self.job_id = job_id

                # not running, actually just submitted
                self.status = STATUSES["running"]
                self._start_time = None
        else:
            cmd = [
                sys.executable,
                os.path.realpath(runner.__file__),
                self.input_f,
                self.output_f
            ]

            outs = open(self.stdout_f, "wt")
            errs = open(self.stderr_f, "wt")
            self.proc = Popen(cmd, stdout=outs, stderr=errs)
            self.status = STATUSES["running"]
            self.log_files = (outs, errs)
            self._start_time = datetime.now()  # immediately started

    def collect(self):
        if self.log_files:
            outs, errs = self.log_files
            outs.close()
            errs.close()
            self.log_files = None

        with open(self.stdout_f, "rt") as fh:
            self.stdout = fh.read()

        with open(self.stderr_f, "rt") as fh:
            self.stderr = fh.read()

        try:
            fh = open(self.output_f, "rb")
        except FileNotFoundError:
            # Process/job: killed the output file might not exist
            self._output = None
            self.status = STATUSES["error"]
            self._end_time = datetime.now()
        else:
            res = pickle.load(fh)
            fh.close()

            self._output = res[0]
            self._returncode = res[1]
            self._start_time = res[2]
            self._end_time = res[3]

        self.clean()

    def clean(self):
        for f in (self.stdout_f, self.stderr_f, self.input_f, self.output_f):
            try:
                os.remove(f)
            except FileNotFoundError:
                pass

    def ping(self) -> int:
        if self.proc is not None:
            returncode = self.proc.poll()

            if returncode is None:
                self.status = STATUSES["running"]
            else:
                self.collect()
                self.proc = None
                if returncode == 0:
                    self.status = STATUSES["success"]
                else:
                    self.status = STATUSES["error"]
        elif self.job_id is not None:
            cmd = ["bjobs", str(self.job_id)]
            outs, errs = Popen(cmd, stdout=PIPE, stderr=DEVNULL).communicate()
            outs = outs.strip().decode()

            status = None
            try:
                status = outs.splitlines()[1].split()[2]
            except IndexError:
                pass
            finally:
                if status in ("DONE", "EXIT"):
                    self.collect()
                    self.job_id = None
                    if self._trust_scheduler:
                        if status == "DONE":
                            self.status = STATUSES["success"]
                        else:
                            self.status = STATUSES["error"]
                    elif self._returncode == 0:
                        self.status = STATUSES["success"]
                    else:
                        self.status = STATUSES["error"]
                else:
                    # PEND, RUN, UNKNOWN, etc.
                    self.status = STATUSES["running"]

                    if status == "RUN" and self._start_time is None:
                        """
                        First time we see the status as running
                        Keep this time for now,
                        it will be updated from the output file
                        """
                        self._start_time = datetime.now()

        return self.status

    def running(self) -> bool:
        return self.ping() == STATUSES["running"]

    def successful(self) -> bool:
        return self.ping() == STATUSES["success"]

    def done(self) -> bool:
        return self.ping() in (STATUSES["success"], STATUSES["error"])

    def cancel(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc = None
        elif self.job_id is not None:
            cmd = ["bkill", str(self.job_id)]
            Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()
            self.job_id = None

            """
            Wait until the stdout file exists and is not empty
            as LSF can take some time to flush the job report to the disk
            """
            while True:
                n = 0
                try:
                    fh = open(self.stdout_f, "rt")
                except FileNotFoundError:
                    pass
                else:
                    n = len(fh.read())
                finally:
                    if n:
                        break
                    else:
                        time.sleep(1)
        else:
            return

        self.collect()
        self.status = STATUSES["error"]

    def update(self, **kwargs):
        try:
            self.status = kwargs["status"]
        except KeyError:
            pass

        try:
            self._output = kwargs["output"]
        except KeyError:
            pass

        try:
            self.stdout = kwargs["stdout"]
        except KeyError:
            pass

        try:
            self.stderr = kwargs["stderr"]
        except KeyError:
            pass

        try:
            self.input_f = kwargs["input_file"]
        except KeyError:
            pass

        try:
            self.output_f = kwargs["output_file"]
        except KeyError:
            pass

        try:
            self._start_time = kwargs["start_time"]
        except KeyError:
            pass

        try:
            self._end_time = kwargs["end_time"]
        except KeyError:
            pass

        try:
            self.job_id = kwargs["job_id"]
        except KeyError:
            self.job_id = None
        finally:
            self.proc = None

    @staticmethod
    def collect_alt(output_file: str) -> tuple:
        basepath = output_file[:-6]
        input_file = basepath + ".in.p"
        stderr_file = basepath + ".err"
        stdout_file = basepath + ".out"

        with open(stderr_file, "rt") as fh:
            stderr = fh.read()

        with open(stdout_file, "rt") as fh:
            stdout = fh.read()

        with open(output_file, "rb") as fh:
            output, status, start_time, end_time = pickle.load(fh)

        for f in (input_file, output_file, stderr_file, stdout_file):
            os.remove(f)

        return status, output, stdout, stderr, start_time, end_time


class TaskOutput(object):
    def __init__(self, task: Task):
        self._task = task

    def ready(self) -> bool:
        return self._task.done()

    def read(self):
        return self._task._output

    @property
    def task(self) -> str:
        return self._task.name

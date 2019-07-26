# -*- coding: utf-8 -*-

import inspect
import os
import pickle
import sys
import time
from datetime import datetime
from random import choices
from string import ascii_lowercase, digits
from subprocess import Popen, PIPE, DEVNULL
from tempfile import mkstemp
from typing import Callable, Optional, Set, Union

from . import runner

STATUS_PENDING = None
STATUS_RUNNING = 1
STATUS_SUCCESS = 0
STATUS_ERROR = 2
STATUS_CANCELLED = 3

SUFFIX_INPUT = ".in.p"
SUFFIX_RESULT = ".out.p"
SUFFIX_STDOUT = ".out"
SUFFIX_STDERR = ".err"


def gen_random_string(k: int):
    return ''.join(choices(ascii_lowercase + digits, k=k))


class Task(object):
    def __init__(self, fn: Callable, args: Union[list, tuple]=list(),
                 kwargs: dict=dict(), **_kwargs):
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
        self.status = STATUS_PENDING
        self.basepath = None

        # Local process
        self.proc = None
        self.file_handlers = None # tuple of file handlers (stdout, stderr)

        # LSF job
        self.jobid = None

        self.stdout = None
        self.stderr = None
        self.result = None

        self._create_time = None
        self._submit_time = None
        self._start_time = None
        self._end_time = None

        self.returncode = None
        self.trust_scheduler = True

        if _kwargs.get("scheduler"):
            if isinstance(_kwargs["scheduler"], dict):
                self.scheduler = _kwargs["scheduler"]
            else:
                self.scheduler = {}
        else:
            self.scheduler = None

        requires = _kwargs.get("requires", [])
        if not isinstance(requires, (dict, list, set, tuple)):
            raise TypeError("'requires' must of one of these types: "
                            "dict, list, set, tuple")

        self.requires = self.inputs
        for dep in set(requires):
            if isinstance(dep, Task):
                self.requires.add(dep.name)
            elif isinstance(dep, str):
                self.requires.add(dep)
            else:
                raise TypeError("'requires' must be a sequence "
                                "of strings or Tasks")

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
    def id(self) -> Optional[int]:
        if self.proc is not None:
            return self.proc.pid
        elif self.jobid is not None:
            return -self.jobid
        else:
            return None

    @property
    def submit_time(self) -> Optional[str]:
        return self._submit_time

    @submit_time.setter
    def submit_time(self, value):
        if isinstance(value, str):
            self._submit_time = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        else:
            self._submit_time = value

    @property
    def submit_time_str(self) -> Optional[str]:
        try:
            return self.submit_time.strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            return None

    @property
    def start_time(self) -> Optional[str]:
        return self._start_time

    @start_time.setter
    def start_time(self, value):
        if isinstance(value, str):
            self._start_time = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        else:
            self._start_time = value

    @property
    def start_time_str(self) -> Optional[str]:
        try:
            return self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            return None

    @property
    def end_time(self) -> Optional[str]:
        return self._end_time

    @end_time.setter
    def end_time(self, value):
        if isinstance(value, str):
            self._end_time = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        else:
            self._end_time = value

    @property
    def end_time_str(self) -> Optional[str]:
        try:
            return self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            return None

    @property
    def state(self) -> str:
        if self.status == STATUS_PENDING:
            return "pending"
        elif self.status == STATUS_RUNNING:
            return "running"
        elif self.status == STATUS_ERROR:
            return "failed"
        elif self.status == STATUS_CANCELLED:
            return "cancelled"
        elif self.status == STATUS_SUCCESS:
            return "done"

    def _pack(self, workdir: str):
        os.makedirs(workdir, exist_ok=True)

        fd, self.basepath = mkstemp(prefix=self.name, dir=workdir)
        os.close(fd)
        os.remove(self.basepath)

        input_file = self.basepath + SUFFIX_INPUT
        with open(input_file, "wb") as fh:
            module = inspect.getmodule(self.fn)
            module_path = module.__file__
            module_name = module.__name__

            for _ in range(len(module_name.split('.'))):
                module_path = os.path.dirname(module_path)

            p = pickle.dumps((self.fn, self.args, self.kwargs))

            if module_name == "__main__":
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                p = p.replace(b"c__main__", b"c" + module_name.encode())

            pickle.dump(module_path.encode(), fh)
            pickle.dump(module_name.encode(), fh)
            pickle.dump(p, fh)

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

    def start(self, workdir: str=os.getcwd(), trust_scheduler: bool=True):
        if self.running(update=False) or not self.ready():
            return

        self._pack(workdir)
        stdout_file = self.basepath + SUFFIX_STDOUT
        stderr_file = self.basepath + SUFFIX_STDERR
        input_file = self.basepath + SUFFIX_INPUT
        result_file = self.basepath + SUFFIX_RESULT
        self.trust_scheduler = trust_scheduler
        self.proc = self.jobid = self.file_handlers = None

        if isinstance(self.scheduler, dict):
            cmd = ["bsub", "-J", self.name]

            if isinstance(self.scheduler.get("queue"), str):
                cmd += ["-q", self.scheduler["queue"]]

            if isinstance(self.scheduler.get("project"), str):
                cmd += ["-P", self.scheduler["project"]]

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

            cmd += ["-o", stdout_file, "-e", stderr_file]
            cmd += [
                sys.executable,
                os.path.realpath(runner.__file__),
                input_file,
                result_file
            ]

            outs, errs = Popen(cmd, stdout=PIPE).communicate()
            outs = outs.strip().decode()

            # Expected format: Job <job_id> is submitted to queue <queue>.
            self.jobid = int(outs.split('<')[1].split('>')[0])
        else:
            cmd = [
                sys.executable,
                os.path.realpath(runner.__file__),
                input_file,
                result_file
            ]

            fh_out = open(stdout_file, "wt")
            fh_err = open(stderr_file, "wt")
            self.proc = Popen(cmd, stdout=fh_out, stderr=fh_err)
            self.file_handlers = (fh_out, fh_err)

        self.status = STATUS_RUNNING  # actually not running: submitted
        self.submit_time = datetime.now()
        self.start_time = self.end_time = None

    def wait(self, seconds: int=60):
        while not self.done():
            time.sleep(seconds)

    def _collect(self) -> Optional[int]:
        if self.file_handlers:
            fh_out, fh_err = self.file_handlers
            fh_out.close()
            fh_err.close()
            self.file_handlers = None

        try:
            fh = open(self.basepath + SUFFIX_STDOUT, "rt")
        except (FileNotFoundError, TypeError):
            pass
        else:
            self.stdout = fh.read()
            fh.close()
            os.remove(self.basepath + SUFFIX_STDOUT)

        try:
            fh = open(self.basepath + SUFFIX_STDERR, "rt")
        except (FileNotFoundError, TypeError):
            pass
        else:
            self.stderr = fh.read()
            fh.close()
            os.remove(self.basepath + SUFFIX_STDERR)

        try:
            os.remove(self.basepath + SUFFIX_INPUT)
        except (FileNotFoundError, TypeError):
            pass

        returncode = None
        try:
            fh = open(self.basepath + SUFFIX_RESULT, "rb")
        except (FileNotFoundError, TypeError):
            # Process/job: killed the output file might not exist
            self.result = None
            self.status = STATUS_ERROR
            self.end_time = datetime.now()
        else:
            res = pickle.load(fh)
            fh.close()
            os.remove(self.basepath + SUFFIX_RESULT)

            self.result = res[0]
            returncode = res[1]
            self.start_time = res[2]
            self.end_time = res[3]

        return returncode

    def _update_status(self):
        if self.status != STATUS_RUNNING:
            return

        if self.proc is not None:
            returncode = self.proc.poll()
            if returncode is not None:
                self._collect()
                self.proc = None
                if returncode == 0:
                    self.status = STATUS_SUCCESS
                else:
                    self.status = STATUS_ERROR
        elif self.jobid is not None:
            cmd = ["bjobs", str(self.jobid)]
            outs, errs = Popen(cmd, stdout=PIPE, stderr=DEVNULL).communicate()
            outs = outs.strip().decode()
            status = None

            try:
                status = outs.splitlines()[1].split()[2]
            except IndexError:
                pass
            finally:
                if status in ("DONE", "EXIT"):
                    returncode = self._collect()
                    self.jobid = None
                    if self.trust_scheduler:
                        if status == "DONE":
                            self.status = STATUS_SUCCESS
                        else:
                            self.status = STATUS_ERROR
                    elif returncode == 0:
                        self.status = STATUS_SUCCESS
                    else:
                        self.status = STATUS_ERROR
        elif self.basepath and os.path.isfile(self.basepath + SUFFIX_RESULT):
            returncode = self._collect()
            self.status = STATUS_SUCCESS if returncode == 0 else STATUS_ERROR

    def running(self, update: bool=True) -> bool:
        if update:
            self._update_status()
        return self.status == STATUS_RUNNING

    def successful(self, update: bool=True) -> bool:
        if update:
            self._update_status()
        return self.status == STATUS_SUCCESS

    def done(self, update: bool=True) -> bool:
        if update:
            self._update_status()
        return self.status in (STATUS_SUCCESS, STATUS_ERROR, STATUS_CANCELLED)

    def terminate(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc = None
        elif self.jobid is not None:
            cmd = ["bkill", str(self.jobid)]
            Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()
            self.jobid = None

            """
            Wait until the stdout file exists and is not empty
            as LSF can take some time to flush the job report to the disk
            """
            while True:
                try:
                    fh = open(self.basepath + SUFFIX_STDOUT, "rt")
                except FileNotFoundError:
                    n = 0
                else:
                    n = len(fh.read())
                    fh.close()
                finally:
                    time.sleep(1)
                    if n:
                        break

        self._collect()
        self.status = STATUS_CANCELLED

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
        return self._task.result

    @property
    def task(self) -> str:
        return self._task.name

import inspect
import os
import pickle
import shutil
import sys
import time
from datetime import datetime
from subprocess import Popen, PIPE, DEVNULL
from tempfile import mkdtemp
from typing import Callable, Optional, Sequence

from . import runner

STATUS_PENDING = None
STATUS_RUNNING = 1
STATUS_SUCCESS = 0
STATUS_ERROR = 2
STATUS_CANCELLED = 3

INPUT_FILE = "input.pickle"
OUTPUT_FILE = "output.log"
ERROR_FILE = "error.log"
RESULT_FILE = "output.pickle"


class Task:
    def __init__(self, fn: Callable, args: Optional[Sequence] = None,
                 kwargs: Optional[dict] = None, **_kwargs):
        if not callable(fn):
            raise TypeError(f"'{fn}' is not callable")
        elif args is not None and not isinstance(args, (list, tuple)):
            raise TypeError("Task() arg 2 must be a list or a tuple")
        elif kwargs is not None and not isinstance(kwargs, dict):
            raise TypeError("Task() arg 3 must be a dict")

        self.fn = fn
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

        self.name = str(_kwargs.get("name", fn.__name__))
        self.status = STATUS_PENDING
        self.workdir = None

        # Local process
        self.proc = None
        self.file_handlers = None  # file handlers (stdout, stderr)

        # LSF job
        self.jobid = None
        self.unknown_start = None  # first time job status is UNKWN

        self.stdout = None
        self.stderr = None
        self.result = None

        self.create_time = datetime.now()
        self.submit_time = None
        self.start_time = None
        self.end_time = None

        self.returncode = None
        self.trust_scheduler = True

        if _kwargs.get("scheduler"):
            if isinstance(_kwargs["scheduler"], dict):
                self.scheduler = _kwargs["scheduler"]
            else:
                self.scheduler = {}
        else:
            self.scheduler = None

        self.requires = set()
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                self.requires.add(arg.task_name)

        for arg in self.kwargs.values():
            if isinstance(arg, TaskOutput):
                self.requires.add(arg.task_name)

        requires = _kwargs.get("requires", [])
        if not isinstance(requires, (dict, list, set, tuple)):
            raise TypeError("'requires' must of one of these types: "
                            "dict, list, set, tuple")

        for item in set(requires):
            if isinstance(item, Task):
                self.requires.add(item.name)
            elif isinstance(item, str):
                self.requires.add(item)
            else:
                raise TypeError("'requires' must be a sequence "
                                "of strings or Tasks")

        self.add_random_suffix = _kwargs.get("random_suffix", True)

    def __repr__(self) -> str:
        return self.name

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
        if self.workdir is None:
            if self.add_random_suffix:
                self.workdir = mkdtemp(prefix=self.name, dir=workdir)
            else:
                self.workdir = os.path.join(workdir, self.name)

        try:
            os.makedirs(self.workdir)
        except FileExistsError:
            pass

        for file in [OUTPUT_FILE, ERROR_FILE, INPUT_FILE, RESULT_FILE]:
            try:
                os.unlink(os.path.join(self.workdir, file))
            except FileNotFoundError:
                pass

        with open(os.path.join(self.workdir, INPUT_FILE), "wb") as fh:
            module = inspect.getmodule(self.fn)
            module_path = module.__file__
            module_name = module.__name__

            for _ in range(len(module_name.split('.'))):
                module_path = os.path.dirname(module_path)

            p = pickle.dumps((self.fn, self.args, self.kwargs), protocol=0)
            if module_name == "__main__":
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                p = p.replace(b"(c__main__", f"(c{module_name}".encode())

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

    def running(self) -> bool:
        return self.status == STATUS_RUNNING

    def completed(self) -> bool:
        return self.status == STATUS_SUCCESS

    def done(self) -> bool:
        return self.status in (STATUS_SUCCESS, STATUS_ERROR, STATUS_CANCELLED)

    def start(self, **kwargs) -> bool:
        workdir = kwargs.get("dir", os.getcwd())
        trust_scheduler = kwargs.get("trust_scheduler", True)

        if self.running() or not self.ready():
            return True

        self._pack(workdir)
        output_file = os.path.join(self.workdir, OUTPUT_FILE)
        error_file = os.path.join(self.workdir, ERROR_FILE)
        input_file = os.path.join(self.workdir, INPUT_FILE)
        result_file = os.path.join(self.workdir, RESULT_FILE)
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
            if isinstance(mem, (float, int)):
                cmd += [
                    "-M", f"{mem:.0f}M",
                    "-R", f"select[mem>={mem:.0f}M]",
                    "-R", f"rusage[mem={mem:.0f}M]"
                ]

            for key in ["tmp", "scratch"]:
                tmp = self.scheduler.get(key)
                if isinstance(tmp, (float, int)):
                    cmd += [
                        "-R", f"select[{key}>={tmp:.0f}M]",
                        "-R", f"rusage[{key}={tmp:.0f}M]"
                    ]

            cmd += ["-o", output_file, "-e", error_file]
            cmd += [
                sys.executable,
                os.path.realpath(runner.__file__),
                input_file,
                result_file
            ]

            outs, errs = Popen(cmd, stdout=PIPE).communicate()
            outs = outs.strip().decode()

            # Expected: Job <job_id> is submitted to [default ]queue <queue>.
            try:
                self.jobid = int(outs.split('<')[1].split('>')[0])
            except IndexError as exc:
                sys.stderr.write(f"IndexError/start: {exc}: "
                                 f"{outs.rstrip()} - {errs.rstrip()}\n")
                time.sleep(3)
                return False
        else:
            cmd = [
                sys.executable,
                os.path.realpath(runner.__file__),
                input_file,
                result_file
            ]

            fh_out = open(output_file, "wt")
            fh_err = open(error_file, "wt")
            self.proc = Popen(cmd, stdout=fh_out, stderr=fh_err)
            self.file_handlers = (fh_out, fh_err)

        self.status = STATUS_RUNNING  # actually not running: submitted
        self.submit_time = datetime.now()
        self.start_time = self.end_time = None
        return True

    def _clean(self, seconds: int = 30, max_attempts: int = 5):
        num_attempts = 0
        while True:
            num_attempts += 1
            try:
                shutil.rmtree(self.workdir)
            except OSError:
                if num_attempts == max_attempts:
                    raise
                else:
                    time.sleep(seconds)
            else:
                break

    def wait(self, seconds: int = 10):
        while not self.done():
            self.poll()
            time.sleep(seconds)

    def poll(self):
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
            cmd = ["bjobs", "-w", str(self.jobid)]
            outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
            outs = outs.strip().decode()
            errs = errs.strip().decode()

            if outs:
                try:
                    status = outs.splitlines()[1].split()[2]
                except IndexError:
                    return

                if status in ("DONE", "EXIT") and self._lsf_stdout_ready():
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
                elif status == "RUN":
                    # Reset unknown status timer
                    self.unknown_start = None
                elif status == "UNKWN":
                    now = datetime.now()
                    if self.unknown_start is None:
                        self.unknown_start = now
                    elif (now - self.unknown_start).total_seconds() >= 3600:
                        self.terminate(force=True)
                elif status == "ZOMBI":
                    self.terminate(force=True)
            elif errs == f"Job <{self.jobid}> is not found":
                if not self._try_collect():
                    # Job does not exist and results not found: error
                    self.status = STATUS_ERROR
        else:
            self._try_collect()

    def _try_collect(self) -> bool:
        if self.workdir and os.path.isfile(os.path.join(self.workdir,
                                                        RESULT_FILE)):
            returncode = self._collect()
            if returncode == 0:
                self.status = STATUS_SUCCESS
            else:
                self.status = STATUS_ERROR
            return True

        return False

    def _lsf_stdout_ready(self) -> bool:
        try:
            with open(os.path.join(self.workdir, OUTPUT_FILE), "rt") as fh:
                return "Resource usage summary:" in fh.read()
        except FileNotFoundError:
            return False

    def _collect(self) -> Optional[int]:
        if self.workdir is None:
            # Task cancelled before started
            self.result = None
            self.end_time = datetime.now()
            return None
        elif self.file_handlers:
            fh_out, fh_err = self.file_handlers
            fh_out.close()
            fh_err.close()
            self.file_handlers = None

        try:
            with open(os.path.join(self.workdir, OUTPUT_FILE), "rt") as fh:
                self.stdout = fh.read()
        except FileNotFoundError:
            self.stdout = ""

        try:
            with open(os.path.join(self.workdir, ERROR_FILE), "rt") as fh:
                self.stderr = fh.read()
        except FileNotFoundError:
            self.stderr = ""

        returncode = None
        try:
            with open(os.path.join(self.workdir, RESULT_FILE), "rb") as fh:
                res = pickle.load(fh)
        except (FileNotFoundError, EOFError):
            """
            The file may not exist if the task is killed, or may be empty
            if the task failed.
            """
            self.result = None
            self.status = STATUS_ERROR
            self.end_time = datetime.now()
        else:
            self.result = res[0]
            returncode = res[1]
            self.start_time = res[2]
            self.end_time = res[3]

        self._clean()
        return returncode

    def terminate(self, force: bool = False):
        if self.done():
            return
        elif self.proc is not None:
            self.proc.kill()
        elif self.jobid is not None:
            if force:
                cmd = ["bkill", "-r", str(self.jobid)]
            else:
                cmd = ["bkill", str(self.jobid)]

            Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()

            while not self._lsf_stdout_ready():
                time.sleep(1)

        self._collect()
        self.proc = self.jobid = None
        self.status = STATUS_CANCELLED


class TaskOutput:
    def __init__(self, task: Task):
        self._task = task

    def ready(self) -> bool:
        self._task.poll()
        return self._task.done()

    def read(self):
        return self._task.result

    @property
    def task_name(self) -> str:
        return self._task.name


def as_completed(tasks: Sequence[Task], seconds: int = 10):
    while tasks:
        _tasks = []

        for t in tasks:
            t.poll()
            if t.done():
                yield t
            else:
                _tasks.append(t)

        tasks = _tasks
        if tasks:
            time.sleep(seconds)

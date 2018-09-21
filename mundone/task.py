#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import os
import pickle
import struct
import tempfile
import sys
from datetime import datetime
from subprocess import Popen, PIPE, DEVNULL

from . import runner

STATUSES = {
    'pending': None,
    'running': 1,
    'success': 0,
    'error': 2
}


def mktemp(prefix=None, suffix=None, dir=None, isdir=False):
    if isdir:
        pathname = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    else:
        fd, pathname = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=dir)
        os.close(fd)

    return pathname


class Task(object):
    def __init__(self, fn, args=[], kwargs={}, **_kwargs):
        if not callable(fn):
            raise TypeError("'{}' is not callable".format(fn))
        elif not isinstance(args, (list, tuple)):
            raise TypeError("Task() arg 2 must be a list or a tuple")
        elif not isinstance(kwargs, dict):
            raise TypeError("Task() arg 3 must be a dict")

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

        self.name = _kwargs.get('name', fn.__name__)
        self.status = STATUSES['pending']
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

        if _kwargs.get("scheduler"):
            if isinstance(_kwargs["scheduler"], dict):
                self.scheduler = _kwargs["scheduler"]
            else:
                self.scheduler = {}
        else:
            self.scheduler = None

        if isinstance(_kwargs.get("requires"), (tuple, list)):
            requires = set()
            for dep in set(_kwargs["requires"]):
                if isinstance(dep, Task):
                    requires.add(dep.name)
                else:
                    requires.add(dep)
            self.requires = requires | set(self.inputs)
        else:
            self.requires = set(self.inputs)

    def __repr__(self):
        return self.name

    def pack(self, workdir=None):
        try:
            os.makedirs(workdir)
        except FileExistsError:
            pass
        except (AttributeError, PermissionError, TypeError):
            workdir = None

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

    def ready(self):
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

    def run(self, workdir=None):
        if not self.ready():
            return

        self._output = None
        self.stdout = None
        self.stderr = None
        self._start_time = None
        self._end_time = None

        self.pack(workdir)

        basepath = self.input_f[:-5]
        self.stdout_f = basepath + ".out"
        self.stderr_f = basepath + ".err"
        self._submit_time = datetime.now()

        if isinstance(self.scheduler, dict):
            cmd = ["bsub", "-J", self.name]

            if isinstance(self.scheduler.get("queue"), str):
                cmd += ["-q", self.scheduler["queue"]]

            if isinstance(self.scheduler.get("cpu"), int):
                cmd += ["-n", str(self.scheduler["cpu"])]

            if isinstance(self.scheduler.get("mem"), int):
                mem = self.scheduler["mem"]
            else:
                mem = 100

            cmd += [
                "-M", "{}M".format(mem),
                "-R", "rusage[mem={}M]".format(mem)
            ]

            if isinstance(self.scheduler.get("tmp"), int):
                cmd += [
                    "-R",
                    "rusage[tmp={}M]".format(self.scheduler["tmp"])
                ]

            cmd += ["-o", self.stdout_f, "-e", self.stderr_f]
            cmd += [
                sys.executable,
                os.path.realpath(runner.__file__),
                self.input_f,
                self.output_f
            ]

            output = Popen(cmd, stdout=PIPE).communicate()[0].strip().decode()
            try:
                # Expected format: Job <job_id> is submitted to queue <queue>.
                job_id = int(output.split('<')[1].split('>')[0])
            except (IndexError, ValueError):
                self.status = STATUSES['error']
            else:
                self.job_id = job_id
                self.status = STATUSES['running']
        else:
            cmd = [
                sys.executable,
                os.path.realpath(runner.__file__),
                self.input_f,
                self.output_f
            ]

            out = open(self.stdout_f, "wt")
            err = open(self.stderr_f, "wt")
            self.proc = Popen(cmd, stdout=out, stderr=err)
            self.status = STATUSES['running']
            self.log_files = (out, err)

    def kill(self):
        if self.proc is not None:
            self.proc.kill()
            self.status = STATUSES['error']
            self.proc = None
        elif self.job_id is not None:
            cmd = ["bkill", str(self.job_id)]
            Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()
            self.status = STATUSES['error']
            self.job_id = None

        self.clean()

    def is_running(self):
        return self.ping() == STATUSES['running']

    def is_success(self):
        return self.ping() == STATUSES['success']

    def is_terminated(self):
        return self.ping() in (STATUSES['success'], STATUSES['error'])

    def clean(self):
        for f in (self.stdout_f, self.stderr_f, self.input_f, self.output_f):
            try:
                os.unlink(f)
            except FileNotFoundError:
                pass

    def _collect(self):
        if self.log_files:
            out, err = self.log_files
            out.close()
            err.close()
            self.log_files = None

        with open(self.stdout_f, 'rt') as fh:
            self.stdout = fh.read()

        with open(self.stderr_f, 'rt') as fh:
            self.stderr = fh.read()

        with open(self.output_f, 'rb') as fh:
            # todo: use returncode to confirm status found by `ping()`
            (self._output, returncode,
             self._start_time, self._end_time) = pickle.load(fh)

        self.clean()

    @staticmethod
    def collect(output_file):
        basepath = output_file[:-6]
        input_file = basepath + ".in.p"
        stderr_file = basepath + ".err"
        stdout_file = basepath + ".out"

        with open(stderr_file, 'rt') as fh:
            stderr = fh.read()

        with open(stdout_file, 'rt') as fh:
            stdout = fh.read()

        with open(output_file, 'rb') as fh:
            output, returncode, start_time, end_time = pickle.load(fh)

        for f in (input_file, output_file, stderr_file, stdout_file):
            os.unlink(f)

        return returncode, output, stdout, stderr, start_time, end_time

    def ping(self):
        if self.proc is not None:
            returncode = self.proc.poll()

            if returncode is None:
                self.status = STATUSES['running']
            else:
                self._collect()
                self.proc = None
                self.status = STATUSES['success'] if returncode == 0 else STATUSES['error']
        elif self.job_id is not None:
            cmd = ["bjobs", str(self.job_id)]
            output = Popen(cmd, stdout=PIPE, stderr=DEVNULL).communicate()[0].strip().decode()
            status = None
            try:
                status = output.splitlines()[1].split()[2]
            except IndexError:
                pass
            finally:
                if status in ("PEND", "RUN"):
                    # PEND == pending on the cluster, but we submitted the task so we want it to run
                    self.status = STATUSES['running']
                else:
                    self._collect()
                    self.job_id = None
                    self.status = STATUSES['success'] if status == "DONE" else STATUSES['error']

        return self.status

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

    def get_status(self):
        for k, v in STATUSES.items():
            if self.status == v:
                return k

    @property
    def inputs(self):
        names = set()
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                names.add(arg.task)

        for key, arg in self.kwargs.items():
            if isinstance(arg, TaskOutput):
                names.add(arg.task)

        return list(names)

    @property
    def output(self):
        return TaskOutput(self)

    @property
    def pid(self):
        if self.proc is not None:
            return self.proc.pid
        elif self.job_id is not None:
            return -self.job_id
        else:
            return None

    @property
    def start_time(self):
        if self._start_time:
            return self._start_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    @property
    def end_time(self):
        if self._end_time:
            return self._end_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    @property
    def submit_time(self):
        if self._submit_time:
            return self._submit_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None


class TaskOutput(object):
    def __init__(self, task):
        self._task = task

    def ready(self):
        return self._task.is_terminated()

    def read(self):
        return self._task._output

    @property
    def task(self):
        return self._task.name

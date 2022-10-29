import os
import re
import sys
from subprocess import Popen, DEVNULL, PIPE
from typing import Optional

from . import runner, statuses


def ready_to_collect(file: str) -> bool:
    try:
        with open(file, "rt") as fh:
            return "Resource usage summary:" in fh.read()
    except FileNotFoundError:
        return False


def submit(name: str, in_file: str, out_file: str, stdout_file: str,
           stderr_file: str, **kwargs) -> Optional[int]:
    queue = kwargs.get("queue")
    project = kwargs.get("project")
    num_cpus = kwargs.get("cpu")
    memory = kwargs.get("mem")
    temp = kwargs.get("tmp")
    scratch = kwargs.get("scratch")

    cmd = ["bsub", "-J", name]

    if queue and isinstance(queue, str):
        cmd += ["-q", queue]

    if project and isinstance(project, str):
        cmd += ["-P", project]

    if isinstance(num_cpus, int) and num_cpus > 1:
        cmd += ["-n", str(num_cpus), "-R", "span[hosts=1]"]

    if isinstance(memory, (float, int)):
        cmd += [
            "-M", f"{memory:.0f}M",
            "-R", f"select[mem>={memory:.0f}M]",
            "-R", f"rusage[mem={memory:.0f}M]"
        ]

    for key, val in [("tmp", temp), ("scratch", scratch)]:
        if isinstance(val, (float, int)):
            cmd += [
                "-R", f"select[{key}>={val:.0f}M]",
                "-R", f"rusage[{key}={val:.0f}M]"
            ]

    cmd += ["-o", stdout_file, "-e", stderr_file]
    cmd += [
        sys.executable,
        os.path.realpath(runner.__file__),
        in_file,
        out_file
    ]

    outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
    outs = outs.strip().decode()

    # Expected: Job <job_id> is submitted to [default ]queue <queue>.
    try:
        return int(outs.split('<')[1].split('>')[0])
    except IndexError as exc:
        sys.stderr.write(f"IndexError/start: {exc}: "
                         f"{outs.rstrip()} - {errs.rstrip()}\n")

    return None


def kill(job_id: int, force: bool = True):
    if force:
        cmd = ["bkill", "-r", str(job_id)]
    else:
        cmd = ["bkill", str(job_id)]

    Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).communicate()


def check(job_id: int) -> tuple[bool, Optional[str]]:
    cmd = ["bjobs", "-w", str(job_id)]
    outs, errs = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
    outs = outs.strip().decode()
    errs = errs.strip().decode()

    found = True
    status = None

    if outs:
        try:
            lsf_status = outs.splitlines()[1].split()[2]
        except IndexError:
            return found, None

        if lsf_status == "DONE":
            status = statuses.STATUS_SUCCESS
        elif lsf_status == "EXIT":
            status = statuses.STATUS_ERROR
        elif lsf_status == "RUN":
            status = statuses.STATUS_RUNNING
        elif lsf_status == "UNKWN":
            status = statuses.STATUS_UNKNOWN
        elif lsf_status == "ZOMBI":
            status = statuses.STATUS_ZOMBIE
    elif errs == f"Job <{job_id}> is not found":
        found = False

    return found, status

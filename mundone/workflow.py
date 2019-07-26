# -*- coding: utf-8 -*-

import json
import logging
import os
import sqlite3
import time
import sys
from datetime import datetime
from email.message import EmailMessage
from smtplib import SMTP
from tempfile import mkstemp
from typing import Any, Collection, Dict, List, Optional, Set, Tuple

from . import __version__
from .task import *


logger = logging.getLogger("mundone")
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(
    logging.Formatter(
        fmt="%(asctime)s: %(message)s",
        datefmt="%y-%m-%d %H:%M:%S"
    )
)
logger.addHandler(_ch)
logger.propagate = False

DBNAME = "mundone.sqlite"


class Workflow(object):
    def __init__(self, tasks: Collection[Task], **kwargs):
        self.name = kwargs.get("name")
        self.id = kwargs.get("id", "1")
        if not isinstance(self.id, str):
            raise ValueError("'id' expects a string")

        self.dir = kwargs.get("dir", os.getcwd())
        os.makedirs(self.dir, exist_ok=True)

        database = kwargs.get("db")
        if database:
            self.database = database
            self.temporary = False
        else:
            fd, self.database = mkstemp()
            os.close(fd)
            os.remove(self.database)
            self.temporary = True

        if not os.path.isfile(self.database):
            try:
                open(self.database, "w").close()
            except (FileNotFoundError, PermissionError):
                # Cannot create file
                raise RuntimeError("Cannot create database "
                                   "'{}'".format(self.database))
            else:
                os.remove(self.database)
        elif not self.is_sqlite3(self.database):
            raise RuntimeError("'{}' is not "
                               "an SQLite database".format(self.database))

        if not isinstance(tasks, (list, tuple)):
            raise TypeError("Workflow() arg 1 must be a list or a tuple")
        elif not tasks:
            raise ValueError("Workflow() arg 1 cannot be empty")
        elif not all([isinstance(task, Task) for task in tasks]):
            raise TypeError("Workflow() arg 1 expects a sequence "
                            "of Task objects")
        elif len(tasks) != len(set([t.name for t in tasks])):
            raise RuntimeError("One or more tasks with the same name")

        email = kwargs.get("mail")
        if isinstance(email, dict):
            for k in ("host", "user"):
                try:
                    email[k]
                except KeyError:
                    raise KeyError(f"missing '{k}' key in 'mail'")
            self.email = email
        else:
            self.email = None

        self.daemon = kwargs.get("daemon", False)

        self.tasks = {t.name: t for t in tasks}
        for n, t in self.tasks.items():
            for nd in t.requires:
                if nd not in self.tasks:
                    raise ValueError(f"'{n}' requires an unknow task ('{nd}')")

        self._init_database()
        self._load_tasks(update=True)
        self._running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @staticmethod
    def is_sqlite3(database: str) -> bool:
        if not os.path.isfile(database):
            return False

        with open(database, "rb") as fh:
            return fh.read(16).decode() == "SQLite format 3\x00"

    def _init_database(self):
        with sqlite3.connect(self.database) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS task (
                  name TEXT NOT NULL,
                  pid INTEGER DEFAULT NULL,
                  wid TEXT NOT NULL,
                  active INTEGER NOT NULL DEFAULT 1,
                  locked INTEGER NOT NULL DEFAULT 0,
                  shared INTEGER NOT NULL DEFAULT 0,
                  status INTEGER DEFAULT NULL,
                  basepath TEXT DEFAULT NULL,
                  result TEXT NOT NULL,
                  stdout TEXT DEFAULT NULL,
                  stderr TEXT DEFAULT NULL,
                  create_time TEXT NOT NULL,
                  submit_time TEXT DEFAULT NULL,
                  start_time TEXT DEFAULT NULL,
                  end_time TEXT DEFAULT NULL
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS i_name ON task (name)")

    def _load_tasks(self, exclude: Collection[str]=list(), update: bool=False):
        with sqlite3.connect(self.database) as con:
            cur = con.execute(
                """
                SELECT name, pid, status, basepath, result, stdout, stderr,
                       submit_time, start_time, end_time
                FROM task
                WHERE wid = ? AND active = 1
                """, (self.id,)
            )

            for row in cur:
                name = row[0]
                try:
                    task = self.tasks[name]
                except KeyError:
                    continue

                if name in exclude:
                    continue
                elif row[1] is not None and row[1] < 0:
                    task.jobid = abs(row[1])
                task.status = row[2]
                task.basepath = row[3]
                task.result = json.loads(row[4])
                task.stdout = row[5]
                task.stderr = row[6]
                task.submit_time = row[7]
                task.start_time = row[8]
                task.end_time = row[9]

            if update:
                changes = []
                for task in self.tasks.values():
                    if task.running(update=False) and task.done():
                        # Task flagged as running in DB, but now is done
                        changes.append((
                            task.status,
                            json.dumps(task.result),
                            task.stdour,
                            task.stderr,
                            task.submit_time_str,
                            task.start_time_str,
                            task.end_time_str,
                            task.name,
                            self.id
                        ))

                if changes:
                    cur.executemany(
                        """
                        UPDATE task
                        SET status = ?, result = ?, stdout = ?, stderr = ?,
                            submit_time = ?, start_time = ?, end_time = ?
                        WHERE name = ? AND id = ? AND active = 1
                        """, changes
                    )
                    con.commit()
            cur.close()

    def start(self, tasks: Optional[Collection[str]]=None, **kwargs) -> bool:
        # If 0: collect completed tasks, register runs and exit
        seconds = kwargs.get("seconds", 10)
        # If True: run dependencies if not explicitly called
        dependencies = kwargs.get("dependencies", True)
        # If True: skip completed tasks
        resume = kwargs.get("resume", False)
        # If True: show tasks about to be run and quit
        dry = kwargs.get("dry", False)
        # Max times a task is resubmitted if it fails (-1: unlimited)
        resubmit = kwargs.get("resubmit", 0)
        # If True: trust job scheduler's status
        trust = kwargs.get("trust", True)

        if tasks is None:
            tasks = set(self.tasks.keys())
        elif isinstance(tasks, (list, tuple)):
            for name in tasks:
                try:
                    t = self.tasks[name]
                except KeyError:
                    raise ValueError(f"invalid task name: {name}")
            tasks = set(tasks)
        else:
            raise TypeError("start() arg 1 must be a list, a tuple, or None")

        to_run = self._init_runs(tasks, dependencies, resume, dry)
        if not to_run:
            return True
        elif dry:
            sys.stderr.write("task(s) to run:\n")
            for name in to_run:
                sys.stderr.write(f"  * {name}\n")
            return True
        elif not seconds:
            sys.stderr.write("task(s) added:\n")
            for name in to_run:
                sys.stderr.write(f"  * {name}\n")
            return True

        self._running = True
        return self.runn(to_run, trust, seconds, resubmit)

    def runn(self, pending: List[str], trust: bool, seconds: int, resubmit: int) -> bool:
        start_time = datetime.now()
        parent2children = {}
        child2parents = {}
        attempts = {}
        for name in pending:
            parent2children[name] = set()
            child2parents[name] = set()
            attempts[name] = 0

        for name in pending:
            for depname in self.tasks[name].requires:
                if depname in pending:
                    parent2children[depname].add(name)
                    child2parents[name].add(depname)

        pending = set(pending)
        running = {}
        for name in pending:
            if all([parent not in pending for parent in child2parents[name]]):
                task = self.tasks[name]
                task.start(workdir=self.dir, trust_scheduler=trust)
                self._persit(task)
                running[name] = task
                attempts[name] += 1
                logger.info(f"{name:<20} running")

        pending -= set(running)
        completed = set()
        failed = set()
        while pending or running:
            time.sleep(seconds)

            for name, task in running.items():
                if not task.done():
                    continue
                elif task.successful():
                    logger.info(f"{name:<20} done")
                    completed.add(name)
                    self._persit(task)
                elif attempts[name] <= resubmit:
                    logger.error(f"{name:<20} failed: retry")
                    self._persit(task, new=True)
                    task.start(workdir=self.dir, trust_scheduler=trust)
                    self._persit(task)
                    attempts[name] += 1
                else:
                    logger.error(f"{name:<20} failed")
                    failed.add(name)
                    self._persit(task)

            _running = {}
            for name, task in running.items():
                if name not in completed and name not in failed:
                    _running[name] = task
            running = _running

            for name in pending:
                pending_parents = []
                for parent in child2parents[name]:
                    if parent in failed:
                        # Cancel child
                        task = self.tasks[name]
                        task.terminate()
                        self._persit(task)
                        failed.add(name)
                        logger.error(f"{name:<20} cancelled")
                        break
                    elif parent not in completed:
                        pending_parents.append(parent)
                else:
                    if not pending_parents:
                        task = self.tasks[name]
                        task.start(workdir=self.dir, trust_scheduler=trust)
                        self._persit(task)
                        running[name] = task
                        attempts[name] += 1
                        logger.info(f"{name:<20} running")

            pending -= set(running) | failed
            self._load_tasks(exclude=running, update=False)

        self._running = False
        if failed:
            failed = ", ".join(sorted(failed))
            logger.error("workflow could not complete "
                         "because one or more tasks failed: "
                         f"{failed}")
            success = False
        else:
            logger.info("workflow completed successfully")
            success = True

        print(self.email)
        if self.email:
            status = "success" if success else "error"
            if self.name:
                subject = f"[{self.name}] Workflow completion notification: {status}"
            else:
                subject = f"Workflow completion notification: {status}"

            table = [
                ("Task", "Status", "Submitted", "Started", "Completed")
            ]
            for name in attempts:
                task = self.tasks[name]
                times = [task.submit_time_str, task.start_time_str,
                         task.end_time_str]
                table.append((
                    name,
                    task.state,
                    *['' if t is None else t for t in times]
                ))

            content = format_table(table, has_header=True)
            content += "\n\n"

            table = [
                ("Launch time", start_time.strftime("%d %b %Y %H:%M:%S")),
                ("Ending time", datetime.now().strftime("%d %b %Y %H:%M:%S")),
                ("Working directory", self.dir),
                ("Job database", self.database),
                ("User", os.getlogin()),
                ("Host", os.uname().nodename),
                ("Mundone version", __version__)
            ]

            content += format_table(table)

            msg = EmailMessage()
            msg.set_content(content)

            msg["Subject"] = subject
            msg["From"] = self.email["user"]

            to_addrs = self.email.get("to")
            if to_addrs and isinstance(to_addrs, (list, tuple)):
                to_addrs = set(to_addrs)
                msg["To"] = ','.join(to_addrs)
            else:
                msg["To"] = [self.email["user"]]

            host = self.email["host"]
            port = self.email.get("port", 475)
            with SMTP(host, port=port) as s:
                s.send_message(msg)

        return success

    def _persit(self, task: Task, new: bool=False):
        with sqlite3.connect(self.database) as con:
            con.execute(
                """
                UPDATE task
                SET status = ?, result = ?, stdout = ?, stderr = ?,
                    submit_time = ?, start_time = ?, end_time = ?,
                    active = ?
                WHERE name = ? AND wid = ? AND active = 1
                """,
                (
                    task.status, json.dumps(task.result), task.stdout,
                    task.stderr, task.submit_time_str, task.start_time_str,
                    task.end_time_str, 0 if new else 1, task.name, self.id
                )
            )

            if new:
                con.execute(
                    """
                    INSERT INTO task (name, wid, result, create_time)
                    VALUES (?, ?, ?, strftime("%Y-%m-%d %H:%M:%S"))
                    """, (task.name, self.id, json.dumps(None))
                )

            con.commit()

    def _get_graph(self, parent2children: bool=True) -> Dict[str, Set[str]]:
        graph = {n: set() for n in self.tasks}

        for name, task in self.tasks.items():
            for depname in task.requires:
                if parent2children:
                    graph[depname].add(name)
                else:
                    graph[name].add(depname)

        return graph

    @staticmethod
    def _merge_nodes(graph: Dict[str, Set[str]], key: str) -> Set[str]:
        nodes = set(graph[key])
        for node in graph[key]:
            nodes |= Workflow._merge_nodes(graph, node)

        return nodes

    def _init_runs(self, tasks: Set[str], dependencies: bool, resume: bool,
                   dry: bool) -> List[str]:
        # Direct child -> parents
        child2parents = self._get_graph(parent2children=False)

        # Child -> all ancestors (direct parents, grandparents, etc.)
        child2parents = {
            name: self._merge_nodes(child2parents, name)
            for name in child2parents
        }

        # Direct parent -> children
        parent2children = self._get_graph(parent2children=True)

        # Parent -> all descendants (direct children, grandchildrent, etc.)
        parent2children = {
            name: self._merge_nodes(parent2children, name)
            for name in parent2children
        }

        # All tasks to run if `dependencies=True` and `resume=False`
        task_run = set()
        for name in tasks:
            task_run.add(name)
            task_run |= child2parents[name]

        running = set()

        # Remove tasks that we do not need
        for name in list(task_run):
            task = self.tasks[name]
            if task.running(update=False):
                # Already running: do not run this task and its ancestors
                running.add(name)
                running |= child2parents[name]
                continue

            task = self.tasks[name]
            if not task.requires and task.successful(update=False) and resume:
                # No dependencies: skip `task`
                # `task` never completed: we can skip `dep`
                try:
                    task_run.remove(name)
                except KeyError:
                    pass
                continue

            inputs = task.inputs
            for depname in task.requires:
                if dependencies:
                    # We want dependencies to run
                    dep = self.tasks[depname]
                    if dep.successful(update=False) and resume:
                        # `dep` completed and we want to skip completee tasks
                        if not task.successful(update=False):
                            # `task` never completed: we can skip `dep`
                            try:
                                task_run.remove(depname)
                            except KeyError:
                                pass
                        elif dep.end_time <= task.end_time:
                            # Run in order: skip `dep`
                            try:
                                task_run.remove(depname)
                            except KeyError:
                                pass
                        else:
                            """
                            `dep`'s run is MORE RECENT than `task`'s:
                            all descendants of `dep` MUST run, unless not
                              scheduled to run in the first place

                            e.g. A -> B -> C -> E
                                           | -> F (F requires C, and D)
                                           D

                            Let's assume:
                              - we want to run F, but skip completed dependencies.
                                Dependencies are: A, B, C, D
                              - all tasks completed in the past,
                                  but A was re-run more recently

                            Then we can skip A (and D), but B and C must run!
                            E is descendant of A, but should not run
                              as it was not scheduled to run.

                            Edge case: if C is running, then we skip B and C
                            """
                            # Skip dependency
                            try:
                                task_run.remove(depname)
                            except KeyError:
                                pass

                            # A -> {B, C, E, F}
                            descendants = parent2children[depname]

                            # {A, B, C, D} & {B, C, E, F} = {B, C}
                            force = task_run & descendants
                            # add in `tasks` so never removed
                            tasks |= force
                            task_run |= force

        task_run -= running
        if task_run and not dry:
            with sqlite3.connect(self.database) as con:
                cur = con.cursor()
                cur.executemany(
                    """
                    UPDATE task
                    SET active = 0
                    WHERE name = ? AND wid = ?
                    """,
                    ((name, self.id) for name in task_run)
                )

                cur.executemany(
                    """
                    INSERT INTO task (name, wid, result, create_time)
                    VALUES (?, ?, ?, strftime("%Y-%m-%d %H:%M:%S"))
                    """,
                    ((name, self.id, json.dumps(None)) for name in task_run)
                )

                con.commit()
                cur.close()

        tasks = set()
        graph = {}
        for name in self.tasks:
            tasks.add(name)
            graph[name] = set()

        lst = []
        while task_run:
            tmp = set()
            for name in sorted(task_run):
                for depname in self.tasks[name].requires:
                    if depname in task_run:
                        tmp.add(name)
                        break
                else:
                    lst.append(name)

            task_run = tmp

        return lst

    def run(self, names: Optional[Collection[str]]=None, **kwargs) -> bool:
        to_run = self.register_runs(names, dependencies, resume, dry, not secs)
        if not to_run:
            return True
        elif dry:
            sys.stderr.write("task(s) to run:\n")
            for name in to_run:
                sys.stderr.write(f"  * {name}\n")
            return True
        elif not secs:
            # We just register runs
            sys.stderr.write("task(s) added:\n")
            for name in to_run:
                sys.stderr.write(f"  * {name}\n")
            return True

        start_time = datetime.now()

        # Number of tries per task
        tries = {task_name: 0 for task_name in to_run}

        # Whether or not the status has been logged
        to_run = {task_name: False for task_name in to_run}

        self.active = True
        failures = set()
        while self.active:
            tasks_started = []
            tasks_done = []
            keep_running = False

            for task_name in to_run:
                run = self.acquire_run(task_name, lock=True)
                task = self.tasks[task_name]

                if run["status"] == STATUSES["running"]:
                    # task flagged as running in the DB, is it still the case?

                    if task.pid != run["pid"]:
                        # Run by another workflow instance

                        if run["shared"] and run["pid"] < 0:
                            # Shareable LSF job: adopt run
                            task.update(
                                status=run["status"],
                                output=run["output"],
                                stdout=run["stdout"],
                                stderr=run["stderr"],
                                job_id=-run["pid"]
                            )
                        else:
                            # Not shareable: wait for completion
                            keep_running = True
                            continue

                    if task.done():
                        # Completed
                        if task.successful():
                            logger.info("'{}' has been completed".format(task))
                            run["status"] = task.status
                            _resubmit = False
                            try:
                                failures.remove(task.name)
                            except KeyError:
                                pass
                        elif tries[task_name] <= resubmit:
                            # Failed but will resubmit task
                            logger.error("'{}' has failed".format(task))
                            run["status"] = STATUSES["pending"]
                            _resubmit = True
                        else:
                            # TODO: because of dependency or not?
                            logger.error("'{}' has failed".format(task))
                            run["status"] = task.status
                            _resubmit = False
                            failures.add(task.name)

                        tasks_done.append((task_name, _resubmit))
                        to_run[task_name] = True  # logged
                    else:
                        keep_running = True
                elif run["status"] == STATUSES["pending"]:
                    flag = 0

                    if dependencies:
                        deps = task.requires
                    else:
                        deps = task.inputs - task.requires

                    for dep_name in deps:
                        dep_run = self.acquire_run(dep_name, lock=False)

                        if dep_run["status"] == STATUSES["error"]:
                            flag |= 1
                        elif dep_run["status"] != STATUSES["success"]:
                            flag |= 2
                        elif dep_name in failures:
                            """
                            dependencies failed,
                            but then completed successfully
                            """
                            failures.remove(dep_name)

                    if flag & 2:
                        # One or more dependencies pending/running
                        keep_running = True
                    elif flag & 1:
                        # All dependencies done but one or more failed:
                        # Cannot start this task, hence flag it as failed
                        self.tasks[task_name].update(status=STATUSES["error"])
                        tasks_done.append((task_name, False))
                        failures.add(task_name)
                    else:
                        # Ready!
                        logger.info("'{}' is running".format(task))
                        task.run(workdir=self.workdir, trust_scheduler=trust)
                        tasks_started.append(task_name)
                        tries[task_name] += 1
                        keep_running = True
                        to_run[task_name] = False  # reset logging status
                elif not to_run[task_name]:
                    # Completed (either success or error) and not logged

                    # Update the task
                    task.update(status=run["status"], output=run["output"],
                                stdout=run["stdout"], stderr=run["stderr"])

                    if run["status"] == STATUSES["success"]:
                        logger.info("'{}' has been completed".format(task))
                    else:
                        logger.error("'{}' has failed".format(task))

                    to_run[task_name] = True

                self.release_run(task_name)

            self.update_runs(tasks_started, tasks_done)

            if secs:
                self.active = keep_running
                time.sleep(secs)
            else:
                break

    def terminate(self):
        return
        to_kill = []
        for task_name, task in self.tasks.items():
            if task.running():
                to_kill.append((task_name, task))

        if to_kill:
            logging.info("terminating running tasks")
            runs_terminated = []
            for task_name, task in to_kill:
                logging.info("\t- {}".format(task_name))
                task.terminate()
                runs_terminated.append((task_name, False))

            self.update_runs([], runs_terminated)
        self.active = False

    def close(self):
        if not self.daemon:
            self.terminate()

        if self.temporary:
            try:
                os.remove(self.database)
            except FileNotFoundError:
                pass


def format_table(table, has_header=False, margin=4):
    lengths = [0] * len(table[0])
    for row in table:
        for i, col in enumerate(row):
            if len(col) > lengths[i]:
                lengths[i] = len(col)

    lengths = [l + margin for l in lengths]

    content = ""
    for i, row in enumerate(table):
        line = ''.join(["{{:<{}}}".format(col) for col in lengths])
        line = line.format(*row)
        content += line + "\n"

        if not i and has_header:
            content += '-' * len(line) + "\n"

    return content

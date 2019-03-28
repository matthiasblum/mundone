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
from typing import Collection, Optional, Set

from mundone import __version__
from .task import STATUSES, Task

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
            raise ValueError("'id' expects a str")

        self.workdir = kwargs.get("dir", os.getcwd())
        os.makedirs(self.workdir, exist_ok=True)

        self.database = kwargs.get("db", os.path.join(self.workdir, DBNAME))
        if isinstance(self.database, str):
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
                    raise KeyError(
                        "'mail' excepts the 'host' and 'user' keys"
                    )
            self.email = email
        else:
            self.email = None

        self.daemon = kwargs.get("daemon", False)

        self.tasks = {t.name: t for t in tasks}
        self.init_database()
        self.active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.daemon:
            self.kill()

    def __del__(self):
        if not self.daemon:
            self.kill()

    @staticmethod
    def is_sqlite3(database: str) -> bool:
        if not os.path.isfile(database):
            return False

        with open(database, "rb") as fh:
            return fh.read(16).decode() == "SQLite format 3\x00"

    def init_database(self):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task (
                  pid INTEGER DEFAULT NULL,
                  name TEXT NOT NULL,
                  workflow_id TEXT NOT NULL,
                  active INTEGER NOT NULL DEFAULT 1,
                  locked INTEGER NOT NULL DEFAULT 0,
                  shared INTEGER NOT NULL DEFAULT 0,
                  status INTEGER DEFAULT NULL,
                  input_file TEXT DEFAULT NULL,
                  output_file TEXT DEFAULT NULL,
                  output TEXT DEFAULT NULL,
                  stdout TEXT DEFAULT NULL,
                  stderr TEXT DEFAULT NULL,
                  create_time TEXT NOT NULL,
                  submit_time TEXT DEFAULT NULL,
                  start_time TEXT DEFAULT NULL,
                  end_time TEXT DEFAULT NULL
                )
                """
            )

            cur.close()

    def run(self, tasks: Optional[Collection[str]]=None, **kwargs):
        # if 0: collect completed tasks, register runs and exit
        secs = kwargs.get("secs", 60)

        # whether or not run dependencies
        dependencies = kwargs.get("dependencies", True)

        # whether or not skip already completed tasks
        resume = kwargs.get("resume", False)

        # If True: show tasks about to be run and quit
        dry = kwargs.get("dry", False)

        # max times a task is resubmitted if it fails (-1: unlimited)
        resubmit = kwargs.get("resubmit", 0)

        if tasks is None:
            pass
        elif isinstance(tasks, (list, tuple)):
            for task_name in tasks:
                if task_name not in self.tasks:
                    raise RuntimeError("invalid task name: "
                                       "'{}'".format(task_name))
        else:
            raise TypeError("run() arg 1 must be a list, a tuple, or None")

        to_run = self.register_runs(tasks, dependencies, resume, dry, not secs)
        if not to_run:
            return
        elif dry:
            sys.stderr.write("tasks to run:\n")
            for task_name in to_run:
                sys.stderr.write("    * {}\n".format(task_name))
            return
        elif not secs:
            # We just register runs
            sys.stderr.write("tasks added:\n")
            for task_name in to_run:
                sys.stderr.write("    * {}\n".format(task_name))
            return

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
                        task.run(self.workdir)
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

        if failures:
            logger.error("workflow could not complete "
                         "because one or more tasks failed: "
                         "{}".format(", ".join(failures)))
            success = False
        else:
            logger.info("workflow completed successfully")
            success = True

        if self.email:
            if self.name:
                subject = "[{}] Workflow completion notification: {}".format(
                    self.name,
                    "success" if success else "error"
                )
            else:
                subject = "Workflow completion notification: {}".format(
                    "success" if success else "error"
                )

            table = [
                ("Task", "Status", "Submitted", "Started", "Completed")
            ]
            for task_name in to_run:
                task = self.tasks[task_name]
                times = [task.submit_time, task.start_time, task.end_time]
                table.append((
                    task_name,
                    task.state,
                    *['' if t is None else t for t in times]
                ))

            content = format_table(table, has_header=True)
            content += "\n\n"

            table = [
                ("Launch time", start_time.strftime("%d %b %Y %H:%M:%S")),
                ("Ending time", datetime.now().strftime("%d %b %Y %H:%M:%S")),
                ("Working directory", self.workdir),
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

    def update_runs(self, runs_started, runs_terminated):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            for task_name in runs_started:
                # for pid, task_name, input_file, output_file in runs_started:
                task = self.tasks[task_name]
                cur.execute(
                    """
                    UPDATE task
                    SET
                      pid = ?,
                      status = ?,
                      input_file = ?,
                      output_file = ?,
                      submit_time = ?
                    WHERE name = ? AND active = 1 AND workflow_id = ?
                    """,
                    (task.pid, STATUSES["running"],
                     task.input_f, task.output_f,
                     task.submit_time, task_name, self.id)
                )

            new_runs = []
            for task_name, resubmit in runs_terminated:
                task = self.tasks[task_name]
                cur.execute(
                    """
                    UPDATE task
                    SET
                      status = ?,
                      output = ?,
                      stdout = ?,
                      stderr = ?,
                      start_time = ?,
                      end_time = ?
                    WHERE name = ? AND active = 1 AND workflow_id = ?
                    """,
                    (task.status, json.dumps(task.output.read()),
                     task.stdout, task.stderr, task.start_time,
                     task.end_time, task_name, self.id)
                )

                if task.status == STATUSES["error"] and resubmit:
                    new_runs.append(task_name)

            if new_runs:
                cur.execute(
                    """
                    UPDATE task
                    SET active = 0
                    WHERE name in ({}) AND workflow_id = ?
                    """.format(','.join(['?' for _ in new_runs])),
                    tuple(new_runs) + (self.id,)
                )

                cur.executemany(
                    """
                    INSERT INTO task (name, workflow_id, create_time)
                    VALUES (?, ?, strftime("%Y-%m-%d %H:%M:%S"))
                    """,
                    ((task_name, self.id) for task_name in new_runs)
                )

            cur.close()

    def acquire_run(self, task_name, lock=False):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT
                  pid, shared, status, output, stdout, stderr,
                  input_file, output_file, start_time, end_time
                FROM task
                WHERE active = 1 AND workflow_id = ? AND name = ?
                """,
                (self.id, task_name)
            )

            run = dict(zip(
                ("pid", "shared", "status", "output", "stdout",
                 "stderr", "input_file", "output_file",
                 "start_time", "end_time"),
                cur.fetchone()
            ))

            if lock:
                cur.execute(
                    """
                    UPDATE task
                    SET locked = 1
                    WHERE active = 1 AND workflow_id = ? AND name = ?
                    """,
                    (self.id, task_name)
                )
            cur.close()
        return run

    def release_run(self, task_name):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE task
                SET locked = 0
                WHERE active = 1 AND workflow_id = ? AND name = ?
                """,
                (self.id, task_name)
            )
            cur.close()

    def register_runs(self, tasks: Optional[Collection[str]],
                      dependencies: bool=True, resume: bool=False,
                      dry: bool=False, share_runs: bool=False
                      ) -> Collection[str]:
        con = sqlite3.connect(self.database)
        cur = con.cursor()
        cur.execute(
            """
            SELECT name, status, locked, input_file, output_file
            FROM task
            WHERE active = 1 AND workflow_id = ?
            """,
            (self.id,)
        )

        tasks_success = set()
        tasks_running = set()
        tasks_pending = set()
        tasks_update = []
        for task_name, status, locked, input_file, output_file in cur:
            if task_name not in self.tasks:
                # Task in DB that is not in the workflow any more
                continue
            elif status == STATUSES["success"]:
                """
                Task completed successfully:
                    tasks depending on this one can run
                """
                tasks_success.add(task_name)
            elif status == STATUSES["running"]:
                # Flagged as running in the database
                if output_file is None:
                    # ... but actually pending (would not be None if running)
                    tasks_pending.add(task_name)
                elif os.path.isfile(output_file) and not locked:
                    # ... but the output file exists!
                    keys = ("status", "output", "stdout", "stderr",
                            "start_time", "end_time")
                    values = Task.collect_alt(output_file)
                    res = dict(zip(keys, values))

                    if res["status"] == STATUSES["success"]:
                        tasks_success.add(task_name)

                    self.tasks[task_name].update(res)
                    tasks_update.append(task_name)
                else:
                    # Assume the task is still running
                    tasks_running.add(task_name)
            elif status == STATUSES["pending"]:
                tasks_pending.add(task_name)

            # TODO: remove in/stdour/stderr files if they exist

        # Updated completed tasks that were still flagged as running
        for task_name in tasks_update:
            task = self.tasks[task_name]
            cur.execute(
                """
                UPDATE task
                SET
                  status = ?,
                  output = ?,
                  stdout = ?,
                  stderr = ?,
                  start_time = ?,
                  end_time = ?
                WHERE name = ? AND active = 1 AND workflow_id = ?
                """,
                (task.status, json.dumps(task.output.read()),
                 task.stdout, task.stderr, task.start_time,
                 task.end_time, task_name, self.id)
            )

        if tasks is None:
            # All tasks not already completed/running/pending
            to_run = set()
            for task_name in self.tasks:
                if task_name in tasks_success and resume:
                    continue
                elif task_name in tasks_running or task_name in tasks_pending:
                    continue
                else:
                    to_run.add(task_name)
        else:
            # Run passed tasks
            to_run = set(tasks)

        to_lookup = to_run

        # Add dependencies
        while True:
            run_dependencies = set()

            for task_name in to_lookup:
                task = self.tasks[task_name]
                inputs = task.inputs
                for dep_name in task.requires:
                    if dep_name not in self.tasks:
                        raise ValueError("'{}' requires an invalid task: "
                                         "'{}'".format(task, dep_name))
                    elif task_name == dep_name:
                        raise ValueError(
                            "'{}' cannot require itself".format(task)
                        )

                    if dep_name in tasks_success and resume:
                        # skip completed dependency
                        continue
                    elif not dependencies and dep_name not in inputs:
                        """
                        Skip because:
                            - dependencies skipping is enabled
                            - task does not take another task's output as input
                        """
                        continue
                    else:
                        run_dependencies.add(dep_name)

            if run_dependencies:
                # Found new dependencies to look up
                to_run |= run_dependencies
                to_lookup = run_dependencies
            else:
                # No more dependencies
                break

        if not dry:
            to_run_insert = to_run - tasks_pending - tasks_running

            if to_run_insert:
                # Update runs
                cur.execute(
                    """
                    UPDATE task
                    SET active = 0
                    WHERE name in ({}) AND workflow_id = ?
                    """.format(','.join(['?' for _ in to_run_insert])),
                    tuple(to_run_insert) + (self.id,)
                )

                # And insert new ones
                cur.executemany(
                    """
                    INSERT INTO task (name, workflow_id, shared, create_time)
                    VALUES (?, ?, ?, strftime("%Y-%m-%d %H:%M:%S"))
                    """,
                    ((task_name, self.id, 1 if share_runs else 0)
                     for task_name in to_run_insert)
                )

        con.commit()
        cur.close()
        con.close()

        return self.sort_task(to_run)

    def sort_task(self, tasks: Set[str]) -> Collection[str]:
        """
        Parse a tree of tasks where the roots are tasks without dependencies
        and the leaves are the tasks with dependencies:

                    A       B
                  C  D     E
                 F    G
                  \  /      B -> E
                   H        A -> C -> F
                              -> D -> G
                            (F + G) -> H

        We iterate over all tasks in the tree:
          - if a task does not have any dependency in the tree, it's taken out
          - if a task has a dependency that is still in the tree, it's kept

        e.g. A and B are removed at the 1st iteration,
             leaving C, D, and E without an in-tree dependency:
                -> they will be taken out at the next iteration

        We continue until the tree is empty
        """
        ordered = []
        while tasks:
            with_deps = set()
            without_deps = set()
            for task_name in tasks:
                for dep_name in self.tasks[task_name].requires:
                    if dep_name in tasks:
                        with_deps.add(task_name)
                        break
                else:
                    without_deps.add(task_name)

            tasks = with_deps
            ordered += sorted(list(without_deps))

        return ordered

    def kill(self):
        to_kill = []
        for task_name, task in self.tasks.items():
            if task.running():
                to_kill.append((task_name, task))

        if to_kill:
            logging.info("killing running tasks")
            runs_terminated = []
            for task_name, task in to_kill:
                logging.info("\t- {}".format(task_name))
                task.cancel()
                runs_terminated.append((task_name, False))

            self.update_runs([], runs_terminated)
        self.active = False


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

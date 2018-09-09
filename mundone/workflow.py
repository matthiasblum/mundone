#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import sqlite3
import time

from .task import STATUSES, Task


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter(
        fmt='%(asctime)s: %(message)s',
        datefmt='%y-%m-%d %H:%M:%S'
    )
)
logger.addHandler(ch)


class Workflow(object):
    def __init__(self, tasks, **kwargs):
        self.id = kwargs.get("id", "1")
        if not isinstance(self.id, str):
            raise ValueError("'id' expects a str")

        self.workdir = kwargs.get("dir", os.getcwd())
        try:
            os.makedirs(self.workdir)
        except FileExistsError:
            pass
        except (AttributeError, PermissionError, TypeError):
            raise RuntimeError("Cannot create directory '{}'".format(self.workdir))

        self.database = kwargs.get("database", os.path.join(self.workdir, "mundone.sqlite"))
        if isinstance(self.database, str):
            if os.path.isfile(self.database) and not self.is_sqlite3(self.database):
                raise RuntimeError("'{}' is not an SQLite database".format(self.database))
            elif not os.path.isfile(self.database):
                try:
                    open(self.database, "w").close()
                except (FileNotFoundError, PermissionError):
                    # Cannot create file
                    raise RuntimeError("Cannot create database '{}'".format(self.database))

        if not isinstance(tasks, (list, tuple)):
            raise TypeError("Workflow() arg 1 must be a list or a tuple")
        elif not tasks:
            raise ValueError("Workflow() arg 1 cannot be empty")
        elif not all([isinstance(task, Task) for task in tasks]):
            raise TypeError("Workflow() arg 1 expects a sequence of Task objects")

        self.tasks = self.init_database(tasks)
        self.active = True

    @staticmethod
    def is_sqlite3(database):
        with open(database, "rb") as fh:
            return fh.read(16).decode() == "SQLite format 3\x00"

    def init_database(self, tasks):
        if len(tasks) != len(set([t.name for t in tasks])):
            raise RuntimeError("One or more tasks with the same name")

        with sqlite3.connect(self.database) as con:
            cur = con.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task (
                  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL UNIQUE
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_run (
                  task_id INTEGER NOT NULL,
                  workflow_id TEXT NOT NULL DEFAULT '1',
                  active INTEGER NOT NULL DEFAULT 1,
                  status INTEGER DEFAULT NULL,
                  input_file TEXT DEFAULT NULL,
                  output_file TEXT DEFAULT NULL,
                  output TEXT DEFAULT NULL,
                  stdout TEXT DEFAULT NULL,
                  stderr TEXT DEFAULT NULL,
                  create_time TEXT NOT NULL,
                  start_time TEXT DEFAULT NULL,
                  end_time TEXT DEFAULT NULL,
                  FOREIGN KEY(task_id) REFERENCES task(id)
                )
                """
            )

            cur.execute("SELECT id, name FROM task")
            existing_tasks = {name: task_id for task_id, name in cur}
            _tasks = {}

            # Add new tasks
            for task in tasks:
                if task.name in existing_tasks:
                    task_id = existing_tasks[task.name]
                else:
                    cur.execute("INSERT INTO task (name) VALUES (?)", (task.name,))
                    task_id = cur.lastrowid

                _tasks[task_id] = task

            cur.close()

        return _tasks

    def run(self, **kwargs):
        tasks = kwargs.get("tasks", [])
        secs = kwargs.get("secs", 60)
        dependencies = kwargs.get("dependencies", True)
        resume = kwargs.get("resume", False)
        dry = kwargs.get("dry", False)
        resubmit = kwargs.get("resubmit", 0)

        if not isinstance(tasks, (list, tuple)):
            raise TypeError("run() arg 1 must be a list or a tuple")

        task_names = {task.name: task_id for task_id, task in self.tasks.items()}
        for task_name in tasks:
            if task_name not in task_names:
                raise RuntimeError("invalid task name: '{}'".format(task_name))

        to_run = self.register_runs(tasks, dependencies, resume, dry)
        if dry:
            logger.info('tasks to run:')
            for task_id in to_run:
                logger.info('    * {}'.format(self.tasks[task_id].name))
            return

        to_run = {task_id: resubmit for task_id in to_run}

        self.active = True
        while self.active:
            runs = self.get_runs()
            runs_started = []
            runs_terminated = []
            keep_running = False

            for task_id in self.sort_task(runs.keys()):
                run = runs[task_id]
                task = self.tasks[task_id]

                if run['status'] == STATUSES['running']:
                    if task.is_terminated():
                        if task.is_success():
                            logger.info("'{}' has been completed".format(task))
                            run['status'] = task.status
                        elif to_run.get(task_id, 0) > 0:
                            # Failed but will resubmit task
                            logger.info("'{}' has failed".format(task))
                            run['status'] = STATUSES['pending']
                            to_run[task_id] -= 1
                        else:
                            logger.info("'{}' has failed".format(task))
                            run['status'] = task.status

                        runs_terminated.append((
                            task_id, task.status,
                            task.output.read(), task.stdout, task.stderr,
                            resubmit
                        ))
                    else:
                        keep_running = True
                elif run['status'] == STATUSES['pending']:
                    keep_running = True
                    flag = 0

                    if dependencies:
                        deps = task.requires
                    else:
                        deps = task.inputs - task.requires

                    for dep_name in deps:
                        dep_id = task_names[dep_name]
                        dep_run = runs[dep_id]

                        if dep_run['status'] == STATUSES['error']:
                            flag |= 1
                        elif dep_run['status'] != STATUSES['success']:
                            flag |= 2

                    if flag & 1:
                        # Cannot submit task because at least one dependency failed
                        # Flag this task as failed
                        runs_terminated.append((
                            task_id, STATUSES['error'],
                            None, None, None, False
                        ))
                    elif flag & 2:
                        # One or more dependencies pending/running
                        continue
                    else:
                        # Ready!
                        logger.info("'{}' is running".format(task))
                        task.run(self.workdir)
                        runs_started.append((task_id, task.input_f, task.output_f))

            if runs_started or runs_terminated:
                self.update_runs(runs_started, runs_terminated)

            if secs:
                self.active = keep_running
                time.sleep(secs)
            else:
                break

    def update_runs(self, runs_started, runs_terminated):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            for task_id, input_file, output_file in runs_started:
                cur.execute(
                    """
                    UPDATE task_run
                    SET
                      status = ?,
                      input_file = ?,
                      output_file = ?,
                      start_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE task_id = ? AND active = 1
                    """,
                    (STATUSES['running'], input_file, output_file, task_id)
                )

            new_runs = []
            for run in runs_terminated:
                task_id, status, output, stdout, stderr, resubmit = run
                cur.execute(
                    """
                    UPDATE task_run
                    SET
                      status = ?,
                      output = ?,
                      stdout = ?,
                      stderr = ?,
                      end_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE task_id = ? AND active = 1
                    """,
                    (status, json.dumps(output), stdout, stderr, task_id)
                )

                if status == STATUSES['error'] and resubmit:
                    new_runs.append(task_id)

            if new_runs:
                cur.execute(
                    """
                    UPDATE task_run
                    SET active = 0
                    WHERE task_id in ({})
                    """.format(','.join(['?' for _ in new_runs])),
                    new_runs
                )

                cur.executemany(
                    """
                    INSERT INTO task_run (task_id, create_time)
                    VALUES (?, strftime('%Y-%m-%d %H:%M:%S'))
                    """,
                    ((task_id,) for task_id in new_runs)
                )

            cur.close()

    def get_runs(self):
        runs = {}

        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT task_id, status, output
                FROM task_run
                WHERE active = 1 AND workflow_id = ?
                """,
                (self.id,)
            )

            for task_id, status, output in cur:
                if output is not None:
                    output = json.loads(output)

                runs[task_id] = {
                    "status": status,
                    "output": output
                }

            cur.close()
        return runs

    def register_runs(self, tasks, dependencies=True, resume=False, dry=False):
        task_names = {task.name: task_id for task_id, task in self.tasks.items()}

        con = sqlite3.connect(self.database)
        cur = con.cursor()
        cur.execute(
            """
            SELECT task_id, status, input_file, output_file
            FROM task_run
            WHERE active = 1 AND workflow_id = ?
            """,
            (self.id,)
        )

        tasks_success = set()
        tasks_error = set()
        tasks_running = set()
        runs_done = []
        runs_pending = set()
        for task_id, status, input_file, output_file in cur:
            if status == STATUSES['success']:
                # Task completed successfully: tasks depending on this one can run
                tasks_success.add(task_id)
            elif status == STATUSES['running']:
                # Flagged as running in the database
                if os.path.isfile(output_file):
                    # But the output file exists!
                    returncode, output, stdout, stderr = Task.collect_run(output_file)
                    if returncode == STATUSES['success']:
                        tasks_success.add(task_id)
                    else:
                        tasks_error.add(task_id)
                    task = self.tasks[task_id]
                    task.update(returncode, output, stdout, stderr)
                    runs_done.append((task_id, returncode, output, stdout, stderr))
                else:
                    # Assume the task is still running
                    tasks_running.add(task_id)
            elif status == STATUSES['pending']:
                runs_pending.add(task_id)

        # Updated completed runs that were still flagged as running
        if runs_done:
            for task_id, returncode, output, stdout, stderr in runs_done:
                cur.execute(
                    """
                    UPDATE task_run
                    SET
                      status = ?,
                      output = ?,
                      stdout = ?,
                      stderr = ?,
                      end_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE task_id = ? AND active = 1 AND workflow_id = ?
                    """,
                    (returncode, json.dumps(output), stdout, stderr, task_id, self.id)
                )

            con.commit()

        if tasks:
            # Run only passed tasks
            to_run = set([task_names[name] for name in tasks])
            to_lookup = to_run

            # Add dependencies
            while True:
                run_dependencies = set()

                for task_id in to_lookup:
                    task = self.tasks[task_id]
                    inputs = task.inputs
                    for name in task.requires:
                        if name not in task_names:
                            raise ValueError("'{}' requires an invalid task: "
                                             "'{}'".format(task, name))

                        dep_id = task_names[name]
                        if dep_id == task_id:
                            raise ValueError(
                                "'{}' cannot require itself".format(task)
                            )
                        if dep_id in tasks_success and resume:
                            # skip completed dependency
                            continue
                        elif not dependencies and name not in inputs:
                            # skip dependency (only if the task does not require the dependency's output)
                            continue
                        else:
                            run_dependencies.add(dep_id)

                if run_dependencies:
                    to_run |= run_dependencies
                    to_lookup = run_dependencies
                else:
                    break

            # Remove already running tasks
            to_run -= set(tasks_running)
        else:
            # Run all talks...
            to_run = set()
            for task_id in self.tasks:
                if task_id in tasks_running:
                    # ... except those already running
                    continue
                elif task_id in tasks_success and resume:
                    # ... and those that completed
                    continue
                else:
                    to_run.add(task_id)

        if to_run and not dry:
            to_run_insert = to_run - runs_pending

            if to_run_insert:
                # Update runs
                cur.execute(
                    """
                    UPDATE task_run
                    SET active = 0
                    WHERE task_id in ({}) AND workflow_id = ?
                    """.format(','.join(['?' for _ in to_run_insert])),
                    tuple(to_run_insert) + (self.id,)
                )

                cur.executemany(
                    """
                    INSERT INTO task_run (task_id, workflow_id, create_time)
                    VALUES (?, ? strftime('%Y-%m-%d %H:%M:%S'))
                    """,
                    ((task_id, self.id) for task_id in to_run_insert)
                )

            con.commit()

        cur.close()
        con.close()

        return self.sort_task(to_run)

    def sort_task(self, tasks):
        task_names = {task.name: task_id for task_id, task in self.tasks.items()}
        tasks = set(tasks)
        ordered = []
        while tasks:
            with_deps = set()
            without_deps = set()
            for task_id in tasks:
                for name in self.tasks[task_id].requires:
                    dep_id = task_names[name]
                    if dep_id in tasks:
                        with_deps.add(task_id)
                        break
                else:
                    without_deps.add(task_id)

            tasks = with_deps
            ordered += sorted(list(without_deps), key=lambda task_id: self.tasks[task_id].name)

        return ordered

    def kill(self):
        runs_terminated = []
        for task_id, task in self.tasks.items():
            if task.is_running():
                task.kill()
                runs_terminated.append((
                    task_id, task.status, None, None, None, False
                ))

        self.update_runs([], runs_terminated)

    def get_task(self, name):
        return

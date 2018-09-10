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
        elif len(tasks) != len(set([t.name for t in tasks])):
            raise RuntimeError("One or more tasks with the same name")

        self.tasks = {t.name: t for t in tasks}
        self.init_database()
        self.active = True

    @staticmethod
    def is_sqlite3(database):
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
                  workflow_id TEXT NOT NULL DEFAULT '1',
                  active INTEGER NOT NULL DEFAULT 1,
                  locked INTEGER NOT NULL DEFAULT 0,
                  status INTEGER DEFAULT NULL,
                  input_file TEXT DEFAULT NULL,
                  output_file TEXT DEFAULT NULL,
                  output TEXT DEFAULT NULL,
                  stdout TEXT DEFAULT NULL,
                  stderr TEXT DEFAULT NULL,
                  create_time TEXT NOT NULL,
                  start_time TEXT DEFAULT NULL,
                  end_time TEXT DEFAULT NULL
                )
                """
            )

            cur.close()

    def run(self, tasks=[], **kwargs):
        secs = kwargs.get("secs", 60)
        dependencies = kwargs.get("dependencies", True)
        resume = kwargs.get("resume", False)
        dry = kwargs.get("dry", False)
        resubmit = kwargs.get("resubmit", 0)

        if not isinstance(tasks, (list, tuple)):
            raise TypeError("run() arg 1 must be a list or a tuple")

        for task_name in tasks:
            if task_name not in self.tasks:
                raise RuntimeError("invalid task name: '{}'".format(task_name))

        to_run = self.register_runs(tasks, dependencies, resume, dry)
        if dry:
            logger.info('tasks to run:')
            for task_name in to_run:
                logger.info('    * {}'.format(task_name))
            return

        # Number of tries per task
        tries = {task_name: 0 for task_name in to_run}

        # Whether or not the status has been logged
        to_run = {task_name: False for task_name in to_run}

        self.active = True
        failures = []
        while self.active:
            tasks_started = []
            tasks_done = []
            keep_running = False

            for task_name in to_run:
                run = self.acquire_run(task_name, lock=True)
                task = self.tasks[task_name]

                if run['status'] == STATUSES['running']:
                    # task flagged as running in the DB, is it still the case?

                    if task.pid != run["pid"]:
                        # Not the same job ID: task being run by another workflow instance
                        keep_running = True
                    elif task.is_terminated():
                        # Completed
                        if task.is_success():
                            logger.info("'{}' has been completed".format(task))
                            run['status'] = task.status
                            _resubmit = False
                        elif tries[task_name] < resubmit:
                            # Failed but will resubmit task
                            logger.error("'{}' has failed".format(task))
                            run['status'] = STATUSES['pending']
                            _resubmit = True
                        else:
                            logger.error("'{}' has failed".format(task))
                            run['status'] = task.status
                            _resubmit = False
                            failures.append(task.name)

                        tasks_done.append((
                            task_name, task.status,
                            task.output.read(), task.stdout, task.stderr,
                            _resubmit
                        ))
                        to_run[task_name] = True  # logged
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
                        dep_run = self.acquire_run(dep_name, lock=False)

                        if dep_run['status'] == STATUSES['error']:
                            flag |= 1
                        elif dep_run['status'] != STATUSES['success']:
                            flag |= 2
                        elif dep_name in failures:
                            # dependencies failed, but then completed successfully
                            failures.remove(dep_name)

                    if flag & 2:
                        # One or more dependencies pending/running
                        continue
                    elif flag & 1:
                        # All dependencies done but one or more failed:
                        # Cannot start this task, hence flag it as failed
                        tasks_done.append((
                            task_name, STATUSES['error'],
                            None, None, None, False
                        ))
                    else:
                        # Ready!
                        logger.info("'{}' is running".format(task))
                        task.run(self.workdir)
                        tasks_started.append(
                            (task.pid, task_name, task.input_f, task.output_f)
                        )
                        tries[task_name] += 1
                elif not to_run[task_name]:
                    # Completed (either success or error) and not logged

                    # Update the task
                    task.update(status=run['status'], output=run['output'],
                                stdout=run['stdout'], stderr=run['stderr'])

                    if run['status'] == STATUSES['success']:
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
                         "{}".format(', '.join(failures)))
        else:
            logger.info("workflow completed successfully")

    def update_runs(self, runs_started, runs_terminated):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            for pid, task_name, input_file, output_file in runs_started:
                cur.execute(
                    """
                    UPDATE task
                    SET
                      pid = ?,
                      status = ?,
                      input_file = ?,
                      output_file = ?,
                      start_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE name = ? AND active = 1 AND workflow_id = ?
                    """,
                    (pid, STATUSES['running'],
                     input_file, output_file, task_name, self.id)
                )

            new_runs = []
            for run in runs_terminated:
                task_name, status, output, stdout, stderr, resubmit = run
                cur.execute(
                    """
                    UPDATE task
                    SET
                      status = ?,
                      output = ?,
                      stdout = ?,
                      stderr = ?,
                      end_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE name = ? AND active = 1 AND workflow_id = ?
                    """,
                    (status, json.dumps(output), stdout, stderr, task_name, self.id)
                )

                if status == STATUSES['error'] and resubmit:
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
                    VALUES (?, ?, strftime('%Y-%m-%d %H:%M:%S'))
                    """,
                    ((task_name, self.id) for task_name in new_runs)
                )

            cur.close()

    def acquire_run(self, task_name, lock=False):
        with sqlite3.connect(self.database) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT pid, status, output, stdout, stderr, input_file, output_file
                FROM task
                WHERE active = 1 AND workflow_id = ? AND name = ?
                """,
                (self.id, task_name)
            )

            run = dict(zip(
                ("pid", "status", "output", "stdout",
                 "stderr", "input_file", "output_file"),
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

    def register_runs(self, tasks, dependencies=True, resume=False, dry=False):
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
                continue  # todo: error message
            elif status == STATUSES['success']:
                # Task completed successfully: tasks depending on this one can run
                tasks_success.add(task_name)
            elif status == STATUSES['running']:
                # Flagged as running in the database
                if os.path.isfile(output_file) and not locked:
                    # But the output file exists!
                    status, output, stdout, stderr = Task.collect(output_file)
                    if status == STATUSES['success']:
                        tasks_success.add(task_name)
                    task = self.tasks[task_name]
                    task.update(status=status, output=output,
                                stdout=stdout, stderr=stderr)
                    tasks_update.append((task_name, status, output, stdout, stderr))
                else:
                    # Assume the task is still running
                    tasks_running.add(task_name)
            elif status == STATUSES['pending']:
                tasks_pending.add(task_name)

        # Updated completed tasks that were still flagged as running
        if tasks_update:
            for task_name, returncode, output, stdout, stderr in tasks_update:
                cur.execute(
                    """
                    UPDATE task
                    SET
                      status = ?,
                      output = ?,
                      stdout = ?,
                      stderr = ?,
                      end_time = strftime('%Y-%m-%d %H:%M:%S')
                    WHERE name = ? AND active = 1 AND workflow_id = ?
                    """,
                    (returncode, json.dumps(output),
                     stdout, stderr, task_name, self.id)
                )

            con.commit()

        if tasks:
            # Run only passed tasks
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
                            # skip dependency (only if the task does not require the dependency's output)
                            continue
                        else:
                            run_dependencies.add(dep_name)

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
            for task_name in self.tasks:
                if task_name in tasks_running:
                    # ... except those already running
                    continue
                elif task_name in tasks_success and resume:
                    # ... and those that completed
                    continue
                else:
                    to_run.add(task_name)

        if to_run and not dry:
            to_run_insert = to_run - tasks_pending

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

                cur.executemany(
                    """
                    INSERT INTO task (name, workflow_id, create_time)
                    VALUES (?, ?, strftime('%Y-%m-%d %H:%M:%S'))
                    """,
                    ((task_name, self.id) for task_name in to_run_insert)
                )

            con.commit()

        cur.close()
        con.close()

        return self.sort_task(to_run)

    def sort_task(self, tasks):
        tasks = set(tasks)
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
        runs_terminated = []
        for task_name, task in self.tasks.items():
            if task.is_running():
                task.kill()
                runs_terminated.append((
                    task_name, task.status, None, None, None, False
                ))

        self.update_runs([], runs_terminated)
        self.active = False

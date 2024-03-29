import argparse
import json
import logging
import os
import sqlite3
import time
import sys
from datetime import datetime

from .executors.lsf import LsfExecutor
from . import states
from .task import Task


logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logger.setLevel(logging.INFO)
    _ch = logging.StreamHandler()
    _ch.setFormatter(
        logging.Formatter(fmt="%(asctime)s: %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(_ch)

DBNAME = "mundone.sqlite"


class Workflow:
    def __init__(self, tasks: list[Task], **kwargs):
        self.name = kwargs.get("name")
        self.id = kwargs.get("id", "1")
        if not isinstance(self.id, str):
            raise ValueError("'id' expects a string")

        self.dir = kwargs.get("dir", os.getcwd())
        try:
            os.makedirs(self.dir)
        except FileExistsError:
            pass

        database = kwargs.get("database")
        if database:
            self.database = database
        elif self.name:
            self.database = os.path.join(self.dir, self.name + ".db")
        else:
            self.database = os.path.join(self.dir, DBNAME)

        if not os.path.isfile(self.database):
            try:
                os.makedirs(os.path.dirname(self.database))
            except FileExistsError:
                pass

            try:
                open(self.database, "w").close()
            except (FileNotFoundError, PermissionError):
                # Cannot create file
                raise RuntimeError(f"Cannot create database '{self.database}'")
            else:
                os.remove(self.database)
        elif not self.is_sqlite3(self.database):
            raise RuntimeError(f"'{self.database}' is not an SQLite database")

        if not isinstance(tasks, (list, tuple)):
            raise TypeError("Workflow() arg 1 must be a list or a tuple")
        elif not tasks:
            raise ValueError("Workflow() arg 1 cannot be empty")
        elif not all([isinstance(task, Task) for task in tasks]):
            raise TypeError("Workflow() arg 1 expects a sequence "
                            "of Task objects")
        elif len(tasks) != len(set([t.name for t in tasks])):
            raise RuntimeError("One or more tasks with the same name")

        self.tasks = {t.name: t for t in tasks}
        for name, task in self.tasks.items():
            for _name in task.requires:
                if _name not in self.tasks:
                    raise ValueError(f"'{name}' requires an unknown task "
                                     f"('{_name}')")

        self.create_database()
        self.get_tasks(update=True)
        self.running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    def __del__(self):
        self.terminate()

    def create_database(self):
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
                  workdir TEXT DEFAULT NULL,
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

    def get_tasks(self, exclude: list[str] | None = None,
                  update: bool = False):
        if exclude is None:
            exclude = []

        con = sqlite3.connect(self.database)

        cur = con.execute(
            """
            SELECT name, pid, status, workdir, result, stdout, stderr,
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

            task.status = row[2]
            task.workdir = row[3]
            task.result = json.loads(row[4])
            task.stdout = row[5]
            task.stderr = row[6]
            task.submit_time = self.strptime(row[7])
            task.start_time = self.strptime(row[8])
            task.end_time = self.strptime(row[9])

        if update:
            # Check running tasks
            changes = []
            for task in self.tasks.values():
                if task.is_running():
                    # Task is flagged as running the database, check this
                    task.poll()

                    if task.is_done():
                        # Task is now done: update database
                        changes.append((
                            task.status,
                            json.dumps(task.result),
                            task.stdout,
                            task.stderr,
                            self.strftime(task.submit_time),
                            self.strftime(task.start_time),
                            self.strftime(task.end_time),
                            task.name,
                            self.id
                        ))

            if changes:
                cur.executemany(
                    """
                    UPDATE task
                    SET status = ?, result = ?, stdout = ?, stderr = ?,
                        submit_time = ?, start_time = ?, end_time = ?
                    WHERE name = ? AND wid = ? AND active = 1
                    """, changes
                )
                con.commit()
        cur.close()
        con.close()

    def run(self, tasks: list[str] | None = None,
            dry_run: bool = False, max_retries: int = 0,
            monitor: bool = True) -> bool:
        """
        Args:
            tasks:
                Optional sequence of task names to run. If None, then all tasks
                are run and none is skipped (even if it completed in the past).
            dry_run:
                If True: show tasks about to be run , then exit.
            max_retries:
                Max times a task is resubmitted if it fails (-1: unlimited).
            monitor:
                If False, then only insert tasks to run in the task table,
                and exit.

        Returns:
            False if an error occurred (e.g. one or more tasks failed),
            True otherwise

        """

        if tasks:
            for name in tasks:
                if name not in self.tasks:
                    raise ValueError(f"invalid task name: {name}")

            tasks = set(tasks)
        else:
            tasks = set(self.tasks.keys())

        tasks = self.init_tasks(tasks, dry_run)
        if not tasks:
            return True
        elif dry_run:
            sys.stderr.write("task(s) to run:\n")
            for name in tasks:
                sys.stderr.write(f"  * {name}\n")
            return True
        elif not monitor:
            sys.stderr.write("task(s) added:\n")
            for name in tasks:
                sys.stderr.write(f"  * {name}\n")
            return True

        self.running = True
        return self.run_tasks(tasks, max_retries)

    def get_remaining_tasks(self) -> list[str]:
        # Find tasks without descendants
        leaves = set(self.tasks.keys())
        for task in self.tasks.values():
            for parent_name in task.requires:
                try:
                    leaves.remove(parent_name)
                except KeyError:
                    continue

        # Remove completed leaves
        tasks = []
        for name in leaves:
            if not self.tasks[name].is_successful():
                tasks.append(name)

        return tasks

    def init_tasks(self, tasks: list[str], dry_run: bool) -> list[str]:
        """

        Args:
            tasks:
                Sequence of tasks to run.
            dry_run:
                If True, only show tasks about to be run, and exit.

        Returns:
            A list of all tasks to run, ordered by dependency.

        """
        tasks_to_run = set()
        for name in tasks:
            Workflow.eval_task(self.tasks, set(tasks), name, tasks_to_run)

        # Sort tasks (ancestors first, descendants last)
        tasks = []
        while tasks_to_run:
            tmp = set()
            for name in sorted(tasks_to_run):
                for parent_name in self.tasks[name].requires:
                    if parent_name in tasks_to_run:
                        # This task has at least one parent in `tasks_to_run`:
                        # We need to insert all parents in `tasks` before
                        tmp.add(name)
                        break
                else:
                    tasks.append(name)

            tasks_to_run = tmp

        if tasks and not dry_run:
            con = sqlite3.connect(self.database)
            cur = con.cursor()
            cur.executemany(
                """
                UPDATE task
                SET active = 0
                WHERE name = ? AND wid = ?
                """,
                ((name, self.id) for name in tasks)
            )

            cur.executemany(
                """
                INSERT INTO task (name, wid, result, create_time)
                VALUES (?, ?, ?, strftime("%Y-%m-%d %H:%M:%S"))
                """,
                ((name, self.id, json.dumps(None)) for name in tasks)
            )

            con.commit()
            cur.close()
            con.commit()

        return tasks

    def run_tasks(self, pending: list[str], max_retries: int,
                  seconds: int = 1) -> bool:
        child2parents = {}
        attempts = {}
        for name in pending:
            child2parents[name] = set()
            attempts[name] = 0

        for name in pending:
            for _name in self.tasks[name].requires:
                if _name in pending:
                    child2parents[name].add(_name)

        pending = set(pending)
        running = {}
        for name in pending:
            if all([parent not in pending for parent in child2parents[name]]):
                task = self.tasks[name]
                if task.start(dir=self.dir):
                    self.update_task(task)
                    running[name] = task
                    attempts[name] += 1
                    logger.info(f"{name:<40} running")

        pending -= set(running)
        completed = set()
        failed = []
        while pending or running:
            time.sleep(seconds)

            for name, task in running.items():
                task.poll()
                if not task.is_done():
                    continue
                elif task.is_successful():
                    logger.info(f"{name:<40} done")
                    completed.add(name)
                    self.update_task(task)
                elif attempts[name] <= max_retries:
                    self.update_task(task, active=True)

                    if task.start(dir=self.dir):
                        self.update_task(task, active=False)
                        self.insert_task(task)
                        logger.error(f"{name:<40} failed: retry")
                        attempts[name] += 1
                    else:
                        # Failed to start: back to pending
                        pending.add(name)
                else:
                    logger.error(f"{name:<40} failed")
                    failed.append(name)
                    self.update_task(task, active=True)

            _running = {}
            for name, task in running.items():
                if (name not in pending and name not in completed
                        and name not in failed):
                    _running[name] = task
            running = _running

            for name in pending:
                running_parents = failed_parents = 0
                for parent in child2parents[name]:
                    if parent in failed:
                        failed_parents += 1
                    elif parent not in completed:
                        # pending or running
                        running_parents += 1

                if running_parents == 0:
                    # No more pending/running parents

                    task = self.tasks[name]
                    if failed_parents == 0:
                        task.start(dir=self.dir)
                        self.update_task(task, active=True)
                        running[name] = task
                        attempts[name] += 1
                        logger.info(f"{name:<40} running")
                    else:
                        # At least one failed parent: cancel child task
                        task.terminate()
                        self.update_task(task, active=True)
                        failed.append(name)
                        logger.error(f"{name:<40} cancelled")

            pending -= set(running) | set(failed)
            self.get_tasks(exclude=running, update=False)

            """
            Add tasks that failed/completed but were submitted by another
            Workflow instance (e.g. with the --detach option on)
            """
            for name, task in self.tasks.items():
                if not task.is_running() and not task.is_done():
                    if name in completed:
                        completed.remove(name)
                        pending.add(name)

                    if name in failed:
                        failed.remove(name)
                        pending.add(name)

        self.running = False
        if failed:
            logger.error(f"one or more tasks did not complete: "
                         f"{', '.join(failed)}")
            success = False
        else:
            logger.info("all tasks completed successfully")
            success = True

        return success

    def insert_task(self, task: Task):
        con = sqlite3.connect(self.database)
        con.execute(
            """
            INSERT INTO task (name, wid, result, create_time)
            VALUES (?, ?, ?, strftime("%Y-%m-%d %H:%M:%S"))
            """, (task.name, self.id, json.dumps(None))
        )
        con.commit()
        con.close()

    def update_task(self, task: Task, active: bool = True):
        con = sqlite3.connect(self.database)
        con.execute(
            """
            UPDATE task
            SET status = ?, workdir = ?, result = ?, stdout = ?, stderr = ?,
                submit_time = ?, start_time = ?, end_time = ?,
                active = ?
            WHERE name = ? AND wid = ? AND active = 1
            """,
            (
                task.status, task.workdir, json.dumps(task.result),
                task.stdout, task.stderr, self.strftime(task.submit_time),
                self.strftime(task.start_time), self.strftime(task.end_time),
                1 if active else 0, task.name, self.id
            )
        )
        con.commit()
        con.close()

    def terminate(self):
        if not self.running:
            return

        to_kill = []
        for task_name, task in self.tasks.items():
            task.poll()
            if task.is_running():
                to_kill.append((task_name, task))

        if to_kill:
            logging.info("terminating running tasks")
            for task_name, task in to_kill:
                logging.info(f"\t- {task_name}")
                task.terminate()
                self.update_task(task)

        self.running = False

    @staticmethod
    def is_sqlite3(database: str) -> bool:
        if not os.path.isfile(database):
            return False

        with open(database, "rb") as fh:
            return fh.read(16).decode() == "SQLite format 3\x00"

    @staticmethod
    def eval_task(tasks: dict[str, Task], leaves: set[str], name: str,
                  result: set) -> bool:
        """
        Evaluate if a task need be run or skipped

        Args:
            tasks:
                Dictionary of all tasks in the workflow.
            leaves:
                Sequence of the name of "leaf" or final tasks,
                i.e. tasks ending the workflow, without other tasks
                to run after them (either because there is none,
                  or because the following tasks are ignored)
            name:
                Name of the task to evaluate.
            result:
                Set of tasks that need to be run

        Returns:
            True if the task's children need to be run, False otherwise.
        """
        run_children = False
        task = tasks[name]
        if task.is_running():
            # This task is running: we don't want to run it right now.
            return False
        elif name in leaves:
            result.add(name)
            run_children = True
        elif not task.is_successful():
            # Task never completed: need to run it.
            result.add(name)

        for parent_name in task.requires:
            parent_task = tasks[parent_name]

            if Workflow.eval_task(tasks, leaves, parent_name, result):
                # One of the task's ancestors completed more recently
                # than its direct child: need to run the descendants.
                run_children = True
                result.add(name)
            elif (task.is_successful() and parent_task.is_successful()
                  and parent_task.end_time > task.end_time):
                # Parent task completed more recently: run this task
                # and its descendants.
                run_children = True
                result.add(name)

        return run_children

    @staticmethod
    def strptime(date_string: str | None) -> datetime | None:
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        except TypeError:
            return None
        else:
            return dt

    @staticmethod
    def strftime(dt: datetime | None) -> str | None:
        try:
            date_string = dt.strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            return None
        else:
            return date_string


def query_db():
    parser = argparse.ArgumentParser(description="Mundone SQLite database "
                                                 "utility")
    parser.add_argument("db", metavar="mundone.sqlite", help="SQLite database")
    parser.add_argument("-n", "--name", help="task name")
    parser.add_argument("-t", "--time", help="task submit time "
                                             "(format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--all", action="store_true",
                        help="list all tasks, not only 'active' ones")
    parser.add_argument("--done", action="store_true",
                        help="list successful tasks only")
    parser.add_argument("--lsf-memory", action="store_true",
                        help="show the memory used by tasks run on LSF")
    args = parser.parse_args()

    if not os.path.isfile(args.db):
        parser.error(f"no such file: {args.db}")

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    try:
        if args.name:
            cond = "WHERE name = ? AND active = 1"
            params = [args.name]

            if args.time:
                try:
                    datetime.strptime(args.time, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    parser.error(f"-t, --time: invalid format")
                else:
                    cond = "WHERE name = ? AND submit_time = ?"
                    params = [args.name, args.time]

            cur.execute(f"SELECT stdout, stderr FROM task {cond}", params)
            row = cur.fetchone()
            if row:
                out, err = row
                sys.stdout.write(f"{out}")
                sys.stderr.write(f"{err}")
            else:
                sys.stderr.write(f"no records for task '{args.name}'\n")
                sys.exit(1)
        else:
            cur.execute(
                """
                SELECT name, submit_time, end_time, status, active, stdout
                FROM task 
                WHERE submit_time is not NULL 
                ORDER BY submit_time, start_time, end_time
                """
            )

            for name, start, end, status, active, stdout in cur:
                if not active and not args.all:
                    continue
                elif status == states.PENDING:
                    status = 'pending'
                elif status == states.SUCCESS:
                    status = 'done'
                elif status == states.RUNNING:
                    status = 'running'
                elif status == states.ERROR:
                    status = 'failed'
                else:
                    status = 'cancelled'

                if status != 'done' and args.done:
                    continue

                if args.lsf_memory:
                    max_mem = LsfExecutor.get_max_memory(stdout)
                    mem = f"{'?':>10}" if max_mem is None else f"{max_mem:>10}"
                else:
                    mem = ""

                print(f"{name:<50}    {start or '':<20}    "
                      f"{end or '':<20}    {status}{mem}")
    finally:
        cur.close()
        con.close()

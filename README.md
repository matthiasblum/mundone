# Mundone

Mundone is a small Python workflow library for running Python callables as tasks,
tracking their state, and orchestrating dependencies locally or via LSF/Slurm.

## Requirements

- Python `>=3.9`
- Linux/Unix environment (LSF/Slurm support requires the corresponding CLIs)

## Installation

Mundone is not published on PyPI. Install it from source:

```sh
pip install .
```

For development:

```sh
pip install -e .
```

## What Mundone Exports

```python
from mundone import (
    Pool,
    Task,
    Workflow,
    as_completed,
    get_descendants,
    get_terminals,
    states,
)
```

## Quickstart (local workflow)

```python
from mundone import Task, Workflow


def add(a, b):
    return a + b


def multiply(x, factor):
    return x * factor


t1 = Task(add, args=(2, 3), name="add")
t2 = Task(multiply, args=(t1.output, 10), name="multiply")  # depends on t1

with Workflow([t1, t2], dir="workflows") as wf:
    ok = wf.run()

print(ok)          # True/False
print(t2.result)   # 50
```

## Core Concepts

### Execution model

Each `Task` runs in a separate Python process through `mundone.runner`.

Practical implications:

- The callable, its arguments, and its return value must be picklable.
- The callable should be importable from a module (avoid lambdas/closures).
- `stdout`/`stderr` are captured and exposed on the `Task`.
- When tasks are managed by `Workflow`, `task.result` is also stored in SQLite as
  JSON, so return values should be JSON-serializable (or `None`).

### `Task`

`Task` is the basic execution unit.

Constructor:

```python
Task(fn, args=None, kwargs=None, **options)
```

Required arguments:

- `fn` (`Callable`): function to execute.
- `args` (`list | tuple | None`): positional arguments.
- `kwargs` (`dict | None`): keyword arguments.

Common `options`:

- `name` (`str`, default: `fn.__name__`): unique task name (must be unique in a
  `Workflow`).
- `scheduler` (`dict | None`): run via scheduler (`lsf`/`slurm`) instead of
  local execution.
- `requires` (iterable of `str | Task`): explicit dependencies.
- `random_suffix` (`bool`, default: `True`): if `False`, use a stable task
  workdir name (`<workflow_dir>/<task_name>`).
- `keep` (`bool`, default: `False`): keep temporary task files/workdir after
  collection.

#### Declaring dependencies

Mundone supports two dependency styles (often mixed):

1. Explicit dependency names/tasks via `requires=...`
2. Data dependency via `task.output`

Example:

```python
t1 = Task(step1, name="step1")
t2 = Task(step2, args=(t1.output,), name="step2")  # auto-dependency on step1
t3 = Task(step3, name="step3", requires=["step2"])
```

When a task starts, any `TaskOutput` arguments are replaced with the upstream
task result (only if the upstream task completed successfully).

#### Scheduler configuration (`scheduler=...`)

If `scheduler` is omitted, Mundone uses local execution.

`scheduler` is a dict with:

- `type` (required): `"lsf"` or `"slurm"` (case-insensitive)
- `queue`: LSF queue / Slurm partition
- `project`: LSF project (`-P`) (stored on Slurm executor but not currently used)
- `cpu`: CPUs requested
- `gpu`: GPU request (`int` or scheduler-specific string)
- `mem`: memory in MB
- `tmp`: temporary disk in MB
- `scratch`: scratch space in MB (LSF only)
- `hours`: wall time limit in hours (Slurm only; default `24`)

Typical scheduler-backed task declaration:

```python
Task(
    fn=export_data,
    args=(src_uri, out_file),
    name="export-data",
    scheduler=dict(type="slurm", queue="production", cpu=16, mem=32000, hours=48),
)
```

#### Task lifecycle methods

- `start(dir=...) -> bool`: submit/start the task (local or scheduler-backed).
- `poll()`: refresh status.
- `wait(seconds=10)`: block until completion.
- `terminate(force=False)`: stop the task.
- `collect()`: collect logs/result and finalize state.
- `clean(seconds=30, max_attempts=5)`: remove task temp workdir (usually called
  by `collect()` unless `keep=True`).

Status helpers:

- `is_ready()`
- `is_running()`
- `is_done()`
- `is_successful()`
- `is_oom()`

#### Task properties

- `name`
- `id` (PID / scheduler job id)
- `status` (numeric constant from `mundone.states`)
- `state` (`"pending" | "running" | "failed" | "cancelled" | "done"`)
- `workdir`
- `stdout`, `stderr`
- `result`
- `create_time`, `submit_time`, `start_time`, `end_time`
- `cputime` (scheduler-dependent)
- `maxmem` (MB, scheduler-dependent)
- `output` (dependency placeholder used by downstream tasks)

### `Workflow`

`Workflow` orchestrates a DAG of tasks and persists task state in a SQLite
database.

Constructor:

```python
Workflow(tasks, name=None, id="1", dir=os.getcwd(), database=None)
```

Behavior:

- Validates that task names are unique.
- Validates that all `requires` dependencies refer to known tasks.
- Creates/uses a SQLite database to track executions.
- Restores task state from the database on startup.

Database path resolution:

- If `database` is provided, it is used directly.
- Else if `name` is provided: `<dir>/<name>.db`
- Else: `<dir>/mundone.sqlite`

`id` is a workflow-instance identifier stored in the database and must be a
string.

#### Running a workflow

```python
ok = workflow.run(tasks=None, dry_run=False, max_retries=0, monitor=True)
```

Parameters:

- `tasks` (`list[str] | None`):
  - `None`: run all tasks (do not skip previously completed tasks)
  - list of task names: run only those targets and their required ancestors;
    Mundone may skip already-up-to-date tasks based on prior state/timestamps
- `dry_run` (`bool`): print task names that would run and exit
- `max_retries` (`int`): retries for failed tasks (`-1` = unlimited)
- `monitor` (`bool`):
  - `True`: submit and monitor until completion
  - `False`: insert task rows in the workflow DB and return immediately,
    without submitting/running them yet (used by "detach" CLIs)

Return value:

- `True` if all required tasks completed successfully
- `False` if one or more tasks failed/cancelled

#### Workflow helper methods

- `get_remaining_tasks() -> list[str]`: return terminal tasks that are not yet
  successful (useful for "run all except completed" behavior)
- `terminate()`: terminate running tasks tracked by this workflow instance

`Workflow` is a context manager; exiting the context calls `terminate()`.

### Workflow graph helpers

#### `get_terminals(tasks, targets=None) -> list[Task]`

Returns terminal tasks (tasks that are not dependencies of other tasks).

Common pattern: create a barrier/group task that depends on all terminal tasks
in a phase.

```python
def wait():
    pass

export_tasks = [...]
tasks = export_tasks + [
    Task(fn=wait, name="export", requires=get_terminals(export_tasks))
]
```

With `targets`, only terminal tasks among the selected names are returned:

```python
Task(fn=wait, name="xrefs", requires=get_terminals(tasks, [t.name for t in xref_tasks]))
```

#### `get_descendants(tasks, root) -> list[Task]`

Returns all downstream tasks that depend (directly or indirectly) on `root`.

#### `as_completed(tasks, seconds=10)`

Poll a list of already-started tasks and yield them as they complete.

```python
tasks = [Task(job, args=(i,), name=f"job-{i}") for i in range(10)]
for t in tasks:
    t.start()

for t in as_completed(tasks, seconds=1):
    print(t.name, t.state, t.result)
```

### `Pool`

`Pool` is a lightweight task runner for many independent tasks with a cap on the
number of concurrently running tasks.

Constructor:

```python
Pool(path, max_running, kill_on_exit=True, threads=4)
```

Parameters:

- `path` (`str`): directory used when starting tasks (`Task.start(dir=path)`)
- `max_running` (`int`): max concurrent running tasks
- `kill_on_exit` (`bool`, default: `True`): terminate queued/running tasks when
  the pool is closed
- `threads` (`int`, default: `4`): internal polling/manager threads

Methods:

- `submit(task)`
- `as_completed(wait=False)`
  - `wait=False`: yield tasks that are already complete, then return
  - `wait=True`: block until all queued/running tasks are done
- `terminate()`

Example:

```python
from mundone import Pool, Task


def work(i):
    return i * i


with Pool(path="tmp", max_running=8) as pool:
    for i in range(100):
        pool.submit(Task(work, args=(i,), name=f"work-{i}"))

    for task in pool.as_completed(wait=True):
        print(task.name, task.result)
```

## Inspecting Workflow Databases (`mundone-query`)

Mundone installs a small CLI utility:

```sh
mundone-query path/to/workflow.sqlite
```

Useful options:

- `-n/--name TASK`: print stored `stdout`/`stderr` for a task
- `-t/--time "YYYY-MM-DD HH:MM:SS"`: disambiguate by submit time
- `--all`: include inactive (superseded/retried) task rows
- `--done`: show successful tasks only
- `--lsf-memory`: show parsed LSF max memory usage

Examples:

```sh
mundone-query myflow.sqlite
mundone-query myflow.sqlite --done
mundone-query myflow.sqlite -n export-data
```

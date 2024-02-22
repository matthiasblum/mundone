# Mundone

Mundone is a Python library to get mundane tasks done by building complex workflows.

## Requirements

* Python 3.9+

## Installation

```sh
pip install mundone
```

## Usage

### Task

A `Task` is a basic processing unit to execute functions.

#### Parameters

* _fn_ (`Callable`): function to be executed.
* _args_ (`list` | `tuple` | None): list or tuple of arguments to be passed to _fn_.
* _kwargs_ (`dict` | None): dictionary of keyword arguments to be passed to _fn_.
* _name_ (`str` | None): name of the task. Used to name the job when executing 
                         the task using the LSF or Slurm job scheduler (defaults to the name of _fn_).
* _scheduler_ (`dict` | None): dictionary specifying the job scheduler to use and job requirements:
  * _type_ (`str`): must be `lsf` or `slurm`.
  * _queue_ (`str` | None): LSF queue or Slurm partition.
  * _cpu_ (`int` | None): specifies to number of processors required by the job.
  * _gpu_ (`int` | `str` | None): specifies properties of GPU resources required by the job.
  * _mem_ (`int` | `float` | None): specifies the memory (in MB) required by the job.
  * _tmp_ (`int` | `float` | None): specifies the amount temporary disk space required by the job.
* _requires_ (`list`): list of the names of tasks the task directly depends on.
* _random_suffix_ (`bool`): if True (default), temporary files are created using the name of the task and a random suffix.
* _keep_ (`bool`): if True (default), temporary files are deleted once the task has completed.

#### Methods

> is_ready()

Returns True if all 

> is_running()
> is_done()
> is_successful()
> start(dir: str)
> terminate(force: bool = False)
> clean(seconds: int = 30, max_attempts: int = 5)
> wait(seconds: int = 10)
> poll()
> collect()

#### Properties

* _state_ (`str`): represents the current state of the task (pending, running, failed, cancelled, done).
* _cputime_ (`int` | None): CPU time, for tasks that completed using a job scheduler.
* _maxmem_ (`int` | None): highest memory used, for tasks that completed using a job scheduler.
* _stdout_ (`str`): standard output of the task.
* _stderr_ (`str`): standard error of the task.
* _result_: whatever is returned by the task's function, or None if the task has not successfully completed.
* _submit_time_ (`datetime.datetime` | None): date/time at which the task started.
* _start_time_ (`datetime.datetime` | None): date/time at which the task actually started (when running task using job scheduler).
* _end_time_ (`datetime.datetime` | None): date/time at which the task finished.

#### Example

The following code defines a function which uses [hmmsearch](http://hmmer.org/) 
to search [Swiss-Prot protein sequences](https://www.uniprot.org/uniprotkb?facets=reviewed%3Atrue&query=%2A) 
using [Pfam profile hidden Markov models](https://www.ebi.ac.uk/interpro/entry/pfam/), 
and a Task to execute the function using the LSF job scheduler.

```py
import subprocess as sp
from mundone import Task


def hmmsearch(hmmfile: str, seqdb: str, output: str, **kwargs):
    num_threads = kwargs.get("threads")
    
    cmd = ["hmmsearch", "-o", output]
    
    if isinstance(num_threads, int) and num_threads >= 0:
        cmd += ["--cpu", str(num_threads)]
        
    cmd += [hmmfile, seqdb]
    sp.run(cmd, check=True)


hmmfile = "Pfam-A.hmm"
seqdb = "uniprot_sprot.fasta"
output = "Pfam-A.hits.out"
task = Task(hmmsearch, [hmmfile, seqdb, output], {"threads": 8},
            name="run-hmmsearch",
            scheduler={
                "type": "lsf",
                "queue": "standard",
                "cpu": 8,
                "memory": 1000
            })

task.start()
task.wait()
if task.is_successful():
    print("ok")
else:
    print(f"error: {task.stdout} {task.stderr}")
```

### Pool

A `Pool` controls a pool of worker 
A task pool object which controls a pool of worker processes to which jobs 
can be submitted. It supports asynchronous results with timeouts and callbacks and has a parallel map implementation.

#### Parameters

* _path_ (`str`):
* _max_running_ (`int`):
* _kill_on_exit_ (`bool`):
* _threads_ (`int`):

#### Methods

> submit(task: Task)

dfg

> as_completed(wait: bool = False)

> terminate()

#### Example

### Workflow

#### Parameters

* _tasks_
* _name_
* _id_
* _dir_
* _database_

#### Methods

run(tasks: list[str] | None = None, dry_run: bool = False, max_retries: int = 0, monitor: bool = True)

terminate()

#### Example

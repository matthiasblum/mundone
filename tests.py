# -*- coding: utf-8 -*-

import os
import random
import time

from mundone import Task, Workflow, as_completed


def hello():
    print("Hello!")


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def divide(a, b, p=0):
    if random.random() < p:
        return 1 / 0
    return a / b


def stringify(a):
    return '---> {}'.format(a)


def goodbye():
    print('Good bye!')


def report():
    print('So far, so good...')


def retry():
    if os.path.isfile('mundone_retry'):
        os.unlink('mundone_retry')
        print("Great!")
    else:
        open('mundone_retry', 'wt').close()
        raise FileNotFoundError("Shame...")


def sleep():
    secs = random.random() * 10
    time.sleep(secs)
    return secs


def workflow():
    t1 = Task(hello)
    t2 = Task(add, (1, 2), requires=('hello',))
    t3 = Task(multiply, (t2.output, 5))
    t4 = Task(report, requires=('multiply',))
    t5 = Task(divide, (t3.output, 2))
    t6 = Task(stringify, (t5.output,))
    t7 = Task(retry)
    t8 = Task(goodbye, requires=(t6, "retry"))

    with Workflow([t1, t2, t3, t4, t5, t6, t7, t8]) as w:
        w.start(max_retries=0)


def batch():
    tasks = []
    for i in range(1, 10):
        t = Task(sleep)
        t.start()
        tasks.append(t)

    for t in as_completed(tasks, seconds=0):
        print(t.basepath, t.result)

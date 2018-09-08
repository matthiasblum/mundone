#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mundone import Task, Workflow


def hello():
    print("hello")


def status():
    print("halfway")


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def stringify(a):
    return '---> {}'.format(a)


def goodbye():
    print('good bye!')


def test():
    t0 = Task(hello)
    t1 = Task(add, (1, 2), requires=('hello',))
    t2 = Task(multiply, (t1.output, 5))
    t5 = Task(status, requires=(t2,))
    t3 = Task(stringify, (t2.output,))
    t4 = Task(goodbye, requires=(t3,))

    return Workflow([t0, t1, t2, t3, t4, t5])

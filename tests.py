#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random

from mundone import Task, Workflow


def hello():
    print("Hello!")


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def divide_error(a, b):
    if random.random() < 0.75:
        return 1 / 0
    return a / b


def stringify(a):
    return '---> {}'.format(a)


def goodbye():
    print('Good bye!')


def independant():
    print('I am rather independant...')


def test():
    t1 = Task(hello)
    t2 = Task(add, (1, 2), requires=('hello',))
    t3 = Task(multiply, (t2.output, 5))
    t4 = Task(divide_error, (t3.output, 2))
    t5 = Task(stringify, (t4.output,))
    t6 = Task(goodbye, requires=(t5,))
    t7 = Task(independant, requires=('multiply',))

    return Workflow([t1, t2, t3, t4, t5, t6, t7])

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import pickle
import struct
import sys


def main():
    src = sys.argv[1]
    dst = sys.argv[2]

    with open(src, "rb") as fh:
        k, l, = struct.unpack("<2I", fh.read(8))
        dirname = fh.read(k).decode()
        module_name = fh.read(l).decode()

        sys.path.append(dirname)
        importlib.import_module(module_name)

        fn, args, kwargs = pickle.loads(fh.read())

    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        sys.stderr.write("{}, line {}: {}\n".format(exc_type, exc_tb.tb_lineno, e))
        result = None
        returncode = 2  # error status code (see `mundone.task`)
    else:
        returncode = 0

    with open(dst, "wb") as fh:
        pickle.dump((result, returncode), fh)

    exit(returncode)


if __name__ == "__main__":
    main()

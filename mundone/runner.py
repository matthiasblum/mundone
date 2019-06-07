#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import pickle
import struct
import sys
from datetime import datetime


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

    start_time = datetime.now()

    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()

        # line_no = exc_tb.tb_lineno
        sys.stderr.write("{}: {}\n".format(exc_type, e))
        result = None
        returncode = 2  # error status code (see `mundone.task`)
    else:
        returncode = 0

    with open(dst, "wb") as fh:
        pickle.dump((result, returncode, start_time, datetime.now()), fh)

    return returncode


if __name__ == "__main__":
    sys.exit(main())
    

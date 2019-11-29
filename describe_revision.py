#!/usr/bin/env python
# Helper for setuptools-scm.

import datetime as dt
import subprocess

ref = subprocess.check_output("git describe --dirty --all --long --first-parent --match master".split()).strip().decode("utf-8")
last_commit = subprocess.check_output('git show -s --format=format:%ct master'.split()).strip().decode("ascii")

version_string = dt.datetime.utcfromtimestamp(int(last_commit)).strftime("%Y%m%d%H%M%S")
print(ref.replace("heads/master", version_string))

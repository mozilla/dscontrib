# dscontrib

A Python library for Mozilla Data Science code snippets.

## Installation of the bleeding edge on databricks clusters
Databricks-wide installations can't be updated without restarting the entire cluster. If you're using newish code then you'll want to install `dscontrib` locally to your notebook:
```lang=py
dbutils.library.installPyPI("dscontrib")
...
import dscontrib
```

Each commit to `master` triggers the release of a new version on PyPI.

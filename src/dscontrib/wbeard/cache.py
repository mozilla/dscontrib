from functools import wraps
from pathlib import Path
import shutil

from os import path
import os

import joblib
import pandas as pd


def cached_fn_called(f, *a, **k):
    """
    For joblib cached func `f`, return whether
    or not it has been called with `*a, **k`
    """
    base, dir = f._get_output_identifiers(*a, **k)
    cached_dir = path.join(f.store_backend.location, base, dir)
    return path.exists(cached_dir)


def parquet_cache(mem: joblib.Memory, engine="auto"):
    """
    mem = joblib.Memory(cachedir="cache", verbose=0)

    @parquet_cache(mem)
    def basic_df(a):
        time.sleep(1)
        return DataFrame(dict(a=[a] * 10))

    basic_df.clear() to empty
    """

    def mk_deco(fn):
        job_lib_func = mem.cache(fn)
        path = Path(
            job_lib_func.store_backend.location
        ) / joblib.memory._build_func_identifier(fn)

        def clear():
            shutil.rmtree(path)

        @wraps(fn)
        def deco(*a, **k):
            arg_hash = job_lib_func._get_argument_hash(*a, **k)
            loc = path / f"{arg_hash}.pq"
            if loc.exists():
                return pd.read_parquet(loc, engine=engine)
            df = fn(*a, **k)
            df.to_parquet(loc, engine=engine)
            return df

        deco.path = path
        deco.clear = clear
        return deco

    return mk_deco


class Df_cache:
    def __init__(self, loc):
        self.loc = Path(loc)

    def __call__(self, f):
        return self.mk_deco(f)

    def mk_deco(self, f):
        @wraps(f)
        def wrapper(*a, **k):
            if self.loc.exists():
                print(f"cached at {self.loc}")
                return pd.read_parquet(self.loc)

            df = f(*a, **k)
            assert isinstance(df, pd.DataFrame), f"{f} must return a DataFrame"
            df.to_parquet(self.loc)
            return df

        return wrapper

    def clear(self):
        os.remove(str(self.loc))

from functools import wraps
from pathlib import Path
import shutil

import joblib
import pandas as pd


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

from pandas import DataFrame
from pytest import raises

from dscontrib.wbeard.pandas_utils import requires_cols

__doc__ = """
$ pwd
~/repos/dscontrib-moz/src/dscontrib/wbeard
$ py.test test_utils.py
"""


@requires_cols(["a", "b"])
def add_a_b(df):
    return df.a + df.b


def test_requires_cols():
    df_a_b = DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    res = add_a_b(df_a_b)
    should_be = [5, 7, 9]
    assert (res == should_be).all()
    print(res)

    df_b_c = df_a_b.rename(columns={"a": "c"})
    raises(KeyError, add_a_b, df_b_c)

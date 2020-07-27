from copy import deepcopy
from pandas import DataFrame  # type: ignore
from functools import wraps
from textwrap import dedent


def requires_cols(
    cols, keep_extra=True, assert_new=None, verbose=False
):
    """
    Decorator to document and ensure that a function
    requires the DataFrame (which is the first argument)
    to have columns `cols`.
    To ensure
    The following function

    @requires_cols(['a', 'b'])
    def add_a_b(df):
        return df.a + df.b

    will raise a KeyError on the following input:
    >>> add_a_b(DataFrame(dict(a=[1, 2], c=[10, 10])))
    but not on the following
    >>> add_a_b(DataFrame(dict(a=[1, 2], b=[10, 10])))

    This mainly helps document the required columns that
    a DataFrame will need, and enforces the documentation.

    If `assert_new` is a sequence of columns, the decorator
    ensures that the function creates these new columns exactly.
    """
    assert_new = set(assert_new or [])

    def deco(f):
        @wraps(f)
        def wrapper(df, *a, **kw):
            if not isinstance(df, DataFrame):
                raise TypeError("First arg must be a DataFrame")
            orig_cols = set(df)
            _df = df
            df = df[cols].copy()
            tmp_missing_cols = set(df) - orig_cols

            res_df = f(df, *a, **kw)

            if assert_new:
                assert isinstance(
                    res_df, DataFrame
                ), f"result is a {type(res_df)}, not a DataFrame"
                new_cols = set(res_df) - set(cols)
                assert (
                    new_cols == assert_new
                ), f"{new_cols} != {assert_new}"

            if verbose:
                s2 = set(res_df)
                print(f"+ {sorted(s2 - set(cols))}")
                print(f"- {sorted(set(cols) - s2)}")
            if keep_extra:
                for c in tmp_missing_cols:
                    res_df[c] = _df[c]
            return res_df

        return wrapper

    return deco


def requires_cols_kw(**kw2cols):
    """
    Decorator to document and ensure that a function
    requires the DataFrame arguments (with names specified
    in kw2cols) to have columns specified as the keys.
    The following function

    @pfu.requires_cols_kw(df1=['a'], df2=['b'])
    def add_a_b(df1, df2):
        return df1.a + df2.b

    will raise a KeyError on the following input:
    >>> add_a_b(df1=DataFrame(dict(a=[1, 2])),
    df2=DataFrame(dict(a=[10, 10])))
    but not on the following
    >>> add_a_b(df1=DataFrame(dict(a=[1, 2])),
    df2=DataFrame(dict(b=[10, 10])))

    This mainly helps document the required columns that
    a DataFrame will need, and enforces the documentation.

    The wrapped function must take the `kw2cols` as keyword
    arguments.
    """

    def mod_kwds(dct):
        dct = deepcopy(dct)
        for argname, cols in kw2cols.items():
            if not isinstance(cols, list):
                raise TypeError(
                    f"keys for {argname} must be a "
                    "list in `requires_cols_kw`"
                )
            if argname not in dct:
                raise ValueError(
                    f"Arg `{argname}` required to be passed as a keyword"
                )
            df = dct[argname]
            if not isinstance(df, DataFrame):
                raise TypeError(f"Arg {argname} must be a DataFrame")
            if set(cols) - set(df):
                raise ValueError(
                    f"{argname} missing {', '.join(set(cols) - set(df))}"
                )
            dct[argname] = df[cols].copy()
        return dct

    def deco(f):
        @wraps(f)
        def wrapper(*a, **kw):
            kw = mod_kwds(kw)
            res = f(*a, **kw)
            return res

        new_docs = "\n".join(
            f"@{kw}: DataFrame{cols}" for kw, cols in kw2cols.items()
        )
        wrapper = mod_fn_docstring(f, wrapper, new_docs)
        wrapper.__name__ = f"{f.__name__}_requires_cols_kw"
        return wrapper

    return deco


def mod_fn_docstring(f_old, f_new, docs_prefix):
    existing_docs = getattr(f_old, "__doc__", "")
    new_docs = f"{docs_prefix}\n======\n{existing_docs}"
    f_new.__doc__ = dedent(new_docs)
    return f_new

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import numpy as np

import mozanalysis.stats as mzas

from dscontrib.flawrence.abtest_stats import (
    summarize_one_sample_set, compare_two_sample_sets
)


def bootstrap_one(sc, data, num_samples=10000, seed_start=0):
    """Bootstrap on the means of one variation on its own.

    Generates `num_samples` sampled means, then returns summary
    statistics for their distribution.

    Args:
        sc: The spark context
        data: The data as a list, 1D numpy array, or pandas Series
        num_samples: The number of bootstrap iterations to perform
        seed_start: An int with which to seed numpy's RNG. It must
            be unique to this set of calculations.
    """
    samples = _resample_parallel(sc, data, num_samples, seed_start)
    return summarize_one_sample_set(samples)


def bootstrap_two(
    sc,
    df=None, col_label=None, focus_label=None, control_label='control',
    focus=None, reference=None,
    num_samples=10000, filter_outliers=None
):
    """Jointly sample bootstrapped means from two distributions then compare them.

    Calculates various quantiles on the uplift of the focus branch's
    mean value with respect to the reference branch's mean value.

    Either supply df, col_label focus_label and optionally control_label,
    or supply focus and reference.

    Args:
        sc: The spark context
        df: a pandas DataFrame of experiment data. Each row represents
            data about an individual test subject. One column is named
            'branch' and contains the test subject's branch. The other
            columns contain the test subject's values for each metric.
        col_label: When df is supplied, the name of the column
            containing the metric of interest.
        control_label: When df is supplied, the name of the control
            branch.
        focus: When df is not supplied, the data for the focal branch
            as a list, 1D numpy array, or pandas Series.
        reference: When df is not supplied, the data for the reference
            (typically control) branch.
        num_samples: The number of bootstrap iterations to perform
        filter_outliers: An optional threshold quantile, above which to
            discard outliers.

    Returns a dictionary:
        'comparative': pandas.Series of summary statistics for the possible
            uplifts - see docs for `compare_two_sample_sets`
        'individual': list of summary stats for (focus, reference) means.
            Each set of summary stats is a pandas.Series
    """
    if df is None:
        assert focus is not None and reference is not None
        assert col_label is None and focus_label is None
    else:
        assert focus is None and reference is None
        assert focus_label is not None
        focus = df[col_label][df.branch == focus_label]
        reference = df[col_label][df.branch == control_label]

    # FIXME: should we be filtering or truncating outliers?
    if filter_outliers:
        assert filter_outliers < 1
        focus = focus[focus <= np.quantile(focus, filter_outliers)]
        reference = reference[reference <= np.quantile(reference, filter_outliers)]

    focus_samples = _resample_parallel(sc, focus, num_samples)
    reference_samples = _resample_parallel(sc, reference, num_samples)

    return {
        'comparative':
            compare_two_sample_sets(focus_samples, reference_samples),
        'individual': [
                summarize_one_sample_set(focus_samples),
                summarize_one_sample_set(reference_samples)
            ],
    }


def _resample_parallel(sc, data, num_samples, seed_start=None):
    """Return bootstrapped samples for the mean of `data`.

    Do the resampling in parallel over the cluster.

    Args:
        sc: The spark context
        data: The data as a list, numpy array, or pandas series
        num_samples: The number of samples to return
        seed_start: A seed for the random number generator; this
            function will use seeds in the range
                [seed_start, seed_start + num_samples)
            and these particular seeds must not be used elsewhere
            in this calculation. By default, use a random seed.

    Returns a numpy array of sampled means
    """
    if not type(data) == np.ndarray:
        data = np.array(data)

    if seed_start is None:
        seed_start = np.random.randint(np.iinfo(np.uint32).max)

    # Deterministic "randomness" requires careful state handling :(
    # Need to ensure every iteration has a unique, deterministic seed.
    # N.B. `num_iterations` in mozanalysis is called `num_samples` here
    seed_range = range(seed_start, seed_start + num_samples)

    try:
        broadcast_data = sc.broadcast(data)

        summary_stat_samples = sc.parallelize(seed_range).map(
            lambda seed: mzas._resample(
                iteration=seed % np.iinfo(np.uint32).max,
                stat_fn=np.mean,
                broadcast_data=broadcast_data,
            )
        ).collect()

        return np.array(summary_stat_samples)

    finally:
        broadcast_data.unpersist()


def _resample_local(data, num_samples):
    """Equivalent to `_resample_parallel` but doesn't require Spark.

    The main purpose of this function is to document what's being done
    in `_resample_parallel` :D
    """
    return np.array([
        np.mean(np.random.choice(data, size=len(data)))
        for _ in range(num_samples)
    ])

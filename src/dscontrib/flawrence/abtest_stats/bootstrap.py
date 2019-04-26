# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import numpy as np

from dscontrib.flawrence.abtest_stats import (
    summarize_one_sample_set, compare_two_sample_sets
)


def compare(
    sc, df, col_label, ref_branch_label='control', num_samples=10000,
    filter_outliers=None
):
    """Jointly sample bootstrapped means then compare them.

    Args:
        df: a pandas DataFrame of queried experiment data in the
            standard format.
        col_label: Label for the df column contaning the metric to be
            analyzed.
        ref_branch_label: String in `df['branch']` that identifies the
            the branch with respect to which we want to calculate
            uplifts - usually the control branch.
        num_samples: The number of bootstrap iterations to perform
        filter_outliers: An optional threshold quantile, above which to
            discard outliers.

    Returns a dictionary:
        'comparative': dictionary mapping branch names to a pandas
            Series of summary statistics for the possible uplifts of the
            bootstrapped means relative to the reference branch - see
            docs for `compare_two_sample_sets`.
        'individual': dictionary mapping branch names to a pandas
            Series of summary stats for the bootstrapped means.
    """
    branch_list = df.branch.unique()

    assert ref_branch_label in branch_list

    if filter_outliers:
        assert filter_outliers < 1
        for b in branch_list:
            threshold = df[df.branch == b][col_label].quantile(filter_outliers)
            df = df[(df.branch != b) | (df[col_label] < threshold)]

    samples = {
        b: _resample_parallel(sc, df[col_label][df.branch == b], num_samples)
        for b in branch_list
    }

    # TODO: should 'comparative' and 'individual' be dfs?
    return {
        'comparative': {
                b: compare_two_sample_sets(
                    samples[b], samples[ref_branch_label]
                ) for b in set(branch_list) - {ref_branch_label}
            },
        'individual': {
                b: summarize_one_sample_set(samples[b])
                for b in branch_list
            },
    }


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
    # Need to ensure every call has a unique, deterministic seed.
    seed_range = range(seed_start, seed_start + num_samples)

    try:
        broadcast_data = sc.broadcast(data)

        summary_stat_samples = sc.parallelize(seed_range).map(
            lambda seed: _mzas_resample(
                unique_seed=seed % np.iinfo(np.uint32).max,
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


def _mzas_resample(unique_seed, stat_fn, broadcast_data):
    np.random.seed(unique_seed)
    n = len(broadcast_data.value)
    randints = np.random.randint(0, n, n)
    return stat_fn(broadcast_data.value[randints])

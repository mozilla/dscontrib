# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import scipy.stats as st
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from dscontrib.flawrence.abtest_stats.beta import compare_two_from_summary


def compare_cdfs(df, col_label, control_label='control'):
    """Compute and plot CDFs and uplifts on CDFs for an A/B test metric

    Args:
        df: a pandas DataFrame of experiment data. Each row represents
            data about an individual test subject. One column is named
            'branch' and contains the test subject's branch. The other
            columns contain the test subject's values for each metric.
        col_label: the name of the column containing the metric of interest
        control_label: the name of the control branch
    """
    df = threshold_and_summarize(df, col_label)

    fig = plt.figure(figsize=(6, 10))
    ax = plt.subplot(211)
    plot_cdf(df, ax, col_label)

    ax = plt.subplot(212)
    plot_relative_differences(df, ax, control_label, col_label)
    return fig


def threshold_and_summarize(df, col_label, thresholds=None):
    """Return values on the CDF for df[col_label] for each branch"""
    if not thresholds:
        thresholds = get_thresholds(df[col_label])

    res = pd.DataFrame(
        index=['num_enrollments'] + thresholds,
        columns=sorted(df.branch.unique())
    )

    for branch in res.columns:
        bdat = df[col_label].loc[df['branch'] == branch]
        res.loc['num_enrollments', branch] = len(bdat)

        for t in thresholds:
            res.loc[t, branch] = (bdat > t).sum()

    return res.astype(np.int64)


def get_thresholds(col, max_num_thresholds=101):
    """Return a set of interesting thresholds for the dataset `col`

    Assumes that the values are non-negative, with zero as a special case.

    Args:
        col: a Series of individual data for a metric
        max_num_thresholds (int): Return at most this many threshold values.

    Returns:
        A list of thresholds. By default these are de-duped percentiles
        of the nonzero data.
    """
    # When taking quantiles, treat "0" as a special case so that we
    # still have resolution if 99% of users are 0.
    nonzero_quantiles = col[col > 0].quantile(
        np.linspace(0, 1, max_num_thresholds)
    )
    return sorted(
        [np.float64(0)] + list(nonzero_quantiles.unique())
    )[:-1]  # The thresholds get used as `>` not `>=`, so exclude the max value


def plot_cdf(pt, ax, xlabel):
    for label, c in pt.items():
        betas = st.beta(
            c.drop('num_enrollments') + 1,
            c.loc['num_enrollments'] - c.drop('num_enrollments') + 1,
        )
        line = ax.plot(c.drop('num_enrollments').index, betas.mean(), label=label)[0]
        col = line.get_color()
        ax.fill_between(
            c.drop('num_enrollments').index.astype('float'),
            betas.ppf(0.05),
            betas.ppf(0.95),
            color=col,
            alpha=0.1
        )
    ax.set_ylim(0, 1)
    ax.legend()
    ax.set_xlabel(xlabel if xlabel is not None else pt.index.name)
    ax.set_ylabel('Fraction of users')
    ax.set_title('1 - CDF: Fraction of users with {} > x'.format(
        'val' if xlabel is not None else pt.index.name)
    )


def plot_relative_differences(pt, ax, control_label, xlabel):
    for c in pt.columns.drop(control_label):
        res = pd.DataFrame([
            compare_two_from_summary(
                pt[[c, control_label]].T,
                control_label=control_label,
                num_conversions_label=x
            )
            for x in pt.index.drop('num_enrollments')
        ], index=pt.index.drop('num_enrollments'))
        line = ax.plot(res.index.astype('float'), res['rel_uplift_exp'], label=c)[0]
        col = line.get_color()
        ax.fill_between(
            res.index.astype('float'),
            res['rel_uplift_0.005'],
            res['rel_uplift_0.995'],
            color=col,
            alpha=0.05
        )
        ax.fill_between(
            res.index.astype('float'),
            res['rel_uplift_0.05'],
            res['rel_uplift_0.95'],
            color=col,
            alpha=0.05
        )
    ax.plot(
        [0, 1, pt.index.drop('num_enrollments').max()],
        [0, 0, 0],
        'k--', label='zero'
    )
    ax.set_xlabel(xlabel if xlabel is not None else pt.index.name)
    ax.set_ylabel('Relative uplift')
    ax.set_title('Uplift in # users >x, relative to {}'.format(control_label))

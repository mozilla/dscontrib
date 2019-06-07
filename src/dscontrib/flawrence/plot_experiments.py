import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import dscontrib.flawrence.bayesian_stats.binary as flbsbin
import dscontrib.flawrence.bayesian_stats.bayesian_bootstrap as flbsbb
import dscontrib.flawrence.bayesian_stats.survival_func as flbssf


def plot_ts(t_df, col_label, stats_model, ref_branch_label='control', sc=None):
    """Plot time series data for a metric.

    Args:
        t_df (dict): A dictionary keyed by time (e.g. an int representing
            the number of days since enrollment), where the values are
            standard format dfs of queried experiment data for that time.
    """
    # Check the number of enrollments is equal for all parts of time series
    assert len({len(v) for v in t_df.values()})

    data = crunch_nums_ts(t_df, col_label, stats_model, ref_branch_label, sc=sc)

    fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True, figsize=(6, 10))

    # Hackily guess whether to interpolate the data points
    if len(data['individual']) >= 10:
        plot_means_line(ax1, data['individual'], ref_branch_label)
        plot_uplifts_line(ax2, data['comparative'], ref_branch_label)
    else:
        plot_means_scatter(ax1, data['individual'])
        plot_uplifts_scatter(ax2, data['comparative'])

    ax1.set_title("{} per user over time".format(col_label))
    ax1.legend()
    ax1.set_ylabel(col_label)

    ax2.set_xlabel('Time since enrollment')
    ax2.set_ylabel('Uplift relative to {}'.format(ref_branch_label))
    fig.tight_layout()
    return fig


def plot_survival(df, col_label, ref_branch_label='control', thresholds=None):
    data = flbssf.compare_branches(df, col_label, ref_branch_label, thresholds)

    fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True, figsize=(6, 10))

    for ax in (ax1, ax2):
        ax.set_prop_cycle('color', plt.cm.tab20b(np.linspace(0, 1, 5)))

    plot_means_line(ax1, data['individual'], ref_branch_label)
    plot_uplifts_line(ax2, data['comparative'])

    ax1.set_title(
        'Survival fn: Fraction of users with {} > x'.format(col_label)
    )
    ax1.set_ylim((0, 1))
    ax1.set_ylabel('Fraction of users')
    ax1.legend()
    ax2.set_xlabel(col_label)
    ax2.set_ylabel('Uplift relative to {}'.format(ref_branch_label))

    fig.tight_layout()
    return fig


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


def plot_means_line(ax, branch_x_stats, ref_branch_label='control'):
    for branch_label in sort_branch_list(branch_x_stats.keys(), ref_branch_label):
        _plot_means_line(ax, branch_x_stats[branch_label], branch_label)


def plot_means_scatter(ax, branch_x_stats, ref_branch_label='control'):
    for branch_label in sort_branch_list(branch_x_stats.keys(), ref_branch_label):
        _plot_means_scatter(ax, branch_x_stats[branch_label], branch_label)


def _plot_means_line(ax, df, branch_label):
    line = ax.plot(df.index, df['mean'], label=branch_label)[0]
    col = line.get_color()
    ax.fill_between(
        df.index,
        df['0.005'],
        df['0.995'],
        color=col,
        alpha=0.05
    )
    ax.fill_between(
        df.index,
        df['0.05'],
        df['0.95'],
        color=col,
        alpha=0.05
    )


def _plot_means_scatter(ax, df, branch_label):
    # TODO: add an x offset between branches, and caps for matplotlib 3
    yerr_inner = (df[['0.05', '0.95']].T - df['mean']).abs().values
    yerr_outer = (df[['0.005', '0.995']].T - df['mean']).abs().values
    line = ax.errorbar(
        df.index, df['mean'], yerr=yerr_inner,
        fmt='.', elinewidth=2, capsize=0,
        label=branch_label
    )[0]
    col = line.get_color()
    ax.errorbar(
        df.index, df['mean'], yerr=yerr_outer,
        fmt='.', color=col, ecolor=col, label=None
    )


def plot_uplifts_line(ax, branch_x_stats):
    for branch_label in sorted(branch_x_stats.keys()):
        _plot_uplifts_line(ax, branch_x_stats[branch_label])

    xmin, xmax = ax.get_xlim()
    ax.plot(
        [0, xmin, xmax],
        [0, 0, 0],
        'k--', label='zero'
    )


def _plot_uplifts_line(ax, df):
    line = ax.plot(df.index, df.rel_uplift_exp)[0]
    col = line.get_color()
    ax.fill_between(
        df.index,
        df['rel_uplift_0.005'],
        df['rel_uplift_0.995'],
        color=col,
        alpha=0.05
    )
    ax.fill_between(
        df.index,
        df['rel_uplift_0.05'],
        df['rel_uplift_0.95'],
        color=col,
        alpha=0.05
    )


def plot_uplifts_scatter(ax, branch_x_stats):
    for branch_label in sorted(branch_x_stats.keys()):
        _plot_uplifts_scatter(ax, branch_x_stats[branch_label])

    xmin, xmax = ax.get_xlim()
    ax.plot(
        [0, xmin - 1, xmax + 1],
        [0, 0, 0],
        'k--', label='zero'
    )


def _plot_uplifts_scatter(ax, x_df):
    df = pd.DataFrame(x_df, columns=sorted(x_df.keys())).T
    yerr_inner = (
        df[['rel_uplift_0.05', 'rel_uplift_0.95']].T - df['rel_uplift_exp']
    ).abs().values
    yerr_outer = (
        df[['rel_uplift_0.005', 'rel_uplift_0.995']].T - df['rel_uplift_exp']
    ).abs().values
    line = ax.errorbar(
        df.index, df['rel_uplift_exp'], yerr=yerr_inner,
        fmt='.', elinewidth=2, capsize=0,
    )[0]
    col = line.get_color()
    ax.errorbar(
        df.index, df['rel_uplift_exp'], yerr=yerr_outer,
        fmt='.', color=col, ecolor=col, label=None
    )
    # # matplotlib 1 :(
    # ax.set_xticks(df.index)
    # ax.set_xlim((min(df.index) - 1, max(df.index) + 1))


def crunch_nums_ts(ts, col_label, stats_model, ref_branch_label='control', sc=None):
    assert all_eq((len(v) for v in ts.values()))
    assert all_eq((set(tuple(v.branch.unique()) for v in ts.values())))

    branch_list = next(iter(ts.values())).branch.unique()
    # # Maybe defaultdicts are offensive because they hide the schema?
    # res = collections.defaultdict(lambda: collections.defaultdict(dict))
    res = {
        'comparative': {
            b: {
                t: None for t in ts.keys()
            } for b in branch_list if b != ref_branch_label
        },
        'individual': {
            b: {
                t: None for t in ts.keys()
            } for b in branch_list
        },
    }

    # TODO: this really smells like a map then a zip?
    for k, v in ts.items():
        if stats_model == 'beta':
            bla = flbsbin.compare_branches(
                v, col_label, ref_branch_label=ref_branch_label
            )
        elif stats_model == 'bootstrap':
            assert sc is not None
            bla = flbsbb.compare_branches(
                sc, v, col_label,
                ref_branch_label=ref_branch_label, filter_outliers=0.9999
            )
        else:
            raise NotImplementedError

        for branch, data in bla['comparative'].items():
            res['comparative'][branch][k] = data
        for branch, data in bla['individual'].items():
            res['individual'][branch][k] = data

    return res


def sort_branch_list(branch_labels, ref_branch_label='control'):
    """Order branch labels for plotting, consistently.

    The control branch goes last because some plots include a series
    for it, and some plots don't, and we want the colors to be
    consistent between these plots.
    """
    assert ref_branch_label in branch_labels

    return sorted(set(branch_labels) - {ref_branch_label}) + [ref_branch_label]


def all_eq(iterable):
    return len(set(iterable)) <= 1

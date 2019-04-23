import matplotlib.pyplot as plt
import pandas as pd

import dscontrib.flawrence.abtest_stats.beta as flasb
import dscontrib.flawrence.abtest_stats.bootstrap as flasboot


def plot_ts(ts, col_label, stats_model, control_label='control', sc=None):
    # Check the number of enrollments is equal for all parts of time series
    assert len({len(v) for v in ts.values()})

    data = crunch_nums(ts, col_label, stats_model, control_label, sc=sc)

    fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True, figsize=(6, 10))

    # Hackily guess whether to interpolate the data points
    if len(data['individual']) >= 10:
        plot_means_line(data['individual'], ax1, control_label)
        plot_uplifts_line(data['comparative'], ax2, control_label)
    else:
        plot_means_scatter(data['individual'], ax1, control_label)
        plot_uplifts_scatter(data['comparative'], ax2, control_label)

    fig.tight_layout()
    return fig


def plot_means_line(dft, ax, control_label='control'):
    # TODO: make control always go last
    for branch, v in dft.items():
        df = pd.DataFrame(v, columns=sorted(v)).T
        line = ax.plot(df.index, df['mean'], label=branch)[0]
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
    ax.legend()


def plot_means_scatter(dft, ax, control_label='control'):
    # TODO: make control always go last
    for branch, v in dft.items():
        df = pd.DataFrame(v, columns=sorted(v)).T
        yerr_inner = (df[['0.05', '0.95']].T - df['mean']).abs().values
        yerr_outer = (df[['0.005', '0.995']].T - df['mean']).abs().values
        line = ax.errorbar(
            df.index, df['mean'], yerr=yerr_inner,
            fmt='.', elinewidth=2, capsize=0,
            label=branch
        )[0]
        col = line.get_color()
        ax.errorbar(
            df.index, df['mean'], yerr=yerr_outer,
            fmt=None, ecolor=col, label=None
        )

    ax.legend()


def plot_uplifts_line(dft, ax, control_label='control'):
    # TODO: choose color cycle based on what's being plotted
    # (time series vs survival func)
    df = pd.DataFrame(dft, columns=sorted(dft.keys())).T
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
    ax.plot(
        [0, min(df.index), max(df.index)],
        [0, 0, 0],
        'k--', label='zero'
    )
    ax.set_ylabel('Relative uplift')


def plot_uplifts_scatter(dft, ax, control_label='control'):
    # TODO: choose color cycle based on what's being plotted
    # (time series vs survival func)
    df = pd.DataFrame(dft, columns=sorted(dft.keys())).T
    yerr_inner = (df[['rel_uplift_0.05', 'rel_uplift_0.95']].T - df['rel_uplift_exp']).abs().values
    yerr_outer = (df[['rel_uplift_0.005', 'rel_uplift_0.995']].T - df['rel_uplift_exp']).abs().values
    line = ax.errorbar(
        df.index, df['rel_uplift_exp'], yerr=yerr_inner,
        fmt='.', elinewidth=2, capsize=0,
    )[0]
    col = line.get_color()
    ax.errorbar(
        df.index, df['rel_uplift_exp'], yerr=yerr_outer,
        fmt=None, ecolor=col, label=None
    )
    ax.plot(
        [0, min(df.index) - 1, max(df.index) + 1],
        [0, 0, 0],
        'k--', label='zero'
    )
    ax.set_ylabel('Relative uplift')

    # matplotlib 1 :(
    ax.set_xticks(df.index)
    ax.set_xlim((min(df.index) - 1, max(df.index) + 1))


def crunch_nums(ts, col_label, stats_model, control_label='control', sc=None):
    # assert all_eq((len(v) for v in ts.values()))
    assert all_eq((set(tuple(v.branch.unique()) for v in ts.values())))

    branch_list = next(iter(ts.values())).branch.unique()
    # # Maybe defaultdicts are offensive because they hide the schema?
    # res = collections.defaultdict(lambda: collections.defaultdict(dict))
    res = {
        'comparative': {k: None for k in ts.keys()},
        'individual': {
            b: {
                k: None for k in ts.keys()
            } for b in branch_list
        },
    }

    # TODO: this really smells like a map then a zip?
    for k, v in ts.items():
        if stats_model == 'beta':
            bla = flasb.compare_two(v, col_label, control_label=control_label)
        elif stats_model == 'bootstrap':
            # FIXME: huge hack re: 'variant'
            assert sc is not None
            bla = flasboot.bootstrap_two(sc, v, col_label, 'variant', control_label=control_label, filter_outliers=0.9999)
        else:
            raise NotImplementedError

        res['comparative'][k] = bla['comparative']
        for branch, data in bla['individual'].items():
            res['individual'][branch][k] = data

    return res


def all_eq(iterable):
    return len(set(iterable)) <= 1

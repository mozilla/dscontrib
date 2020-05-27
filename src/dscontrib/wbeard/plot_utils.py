import itertools as it

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np


########
# Plot #
########
def plot_tree_aggs(y, levels, no_legend=True, widths=[1, 3], ylabel=None):
    """
    @levels: e.g.: [2, 3, 6, 7, 8] will plot mid point (like median or mean)
    at 6, a thin horizontal bar from 2-8, and a thicker bar from 3-7.
    `y` indicates the height at which the horizontal bar is plotted.
    """
    label = "_nolegend_" if no_legend else None
    v_lo, lo, mid, hi, v_hi = levels
    plt.plot(mid, y, ".k", markersize=10, label=label)

    lines = [(v_lo, v_hi), (lo, hi)]
    for i, (x_lo, x_hi), width in zip(it.count(), lines[::-1], widths[::-1]):
        plt.hlines(y, x_lo, x_hi, linewidth=width, label=label)
    return y, ylabel


def plot_trees_aggs(rel_up, cs=None):
    cs = list(rel_up) if cs is None else cs
    yts = []
    for i, col in ((-i, col) for i, col in zip(it.count(), cs)):
        qs = rel_up[col].mul(100).values[:-1]
        plot_tree_aggs(i, qs, widths=[1, 3], no_legend=i)
        yts.append((i, col))
    plt.yticks(*zip(*yts))
    plt.legend(["mean", "95% CI", "99% CI"])
    return yts, i


def plot_trees_aggs_rel_uplift(rel_up, cs=None):
    yts, i = plot_trees_aggs(rel_up, cs=cs)
    plt.vlines(0, i, [0], "k", linestyle="-.")
    plt.xlabel("% uplift")


def decorate(**options):
    """
    Stolen from Allen Downey
    https://colab.research.google.com/github/AllenDowney/
    SurvivalAnalysisPython/blob/master/light_bulb.ipynb

    Decorate the current axes.
    Call decorate with keyword arguments like
    decorate(title='Title',
             xlabel='x',
             ylabel='y')
    The keyword arguments can be any of the axis properties
    https://matplotlib.org/api/axes_api.html
    """
    plt.gca().set(**options)
    plt.tight_layout()


def mk_sublots(nrows=1, ncols=2, figsize=(16, 5), **kw):
    """
    -> ("axes", "iter.n")
    """
    import matplotlib.pyplot as plt

    _, axs_ = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize, **kw)

    mk_sublots.axs_ = axs_
    axs = iter(axs_)
    axs = np.nditer(axs_, flags=["refs_ok"])

    def sca_ax_iter():
        for ax in axs:
            ax = ax[()]
            plt.sca(ax)
            yield ax

    class Axs(object):
        @property
        def n(self):
            return next(sis)

        def __next__(self):
            return next(sis)

        def __iter__(self):
            for a in sca_ax_iter():
                yield a

    sis = sca_ax_iter()
    return axs, Axs()


def log_scale(xaxis=True):
    fmtr = FuncFormatter(lambda x, p: format(int(x), ","))

    if xaxis:
        plt.xscale("log")
        ax = plt.gca().get_xaxis()
    else:
        plt.yscale("log")
        ax = plt.gca().get_yaxis()
    ax.set_major_formatter(fmtr)

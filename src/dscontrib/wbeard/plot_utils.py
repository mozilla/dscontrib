import itertools as it
import matplotlib.pyplot as plt


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

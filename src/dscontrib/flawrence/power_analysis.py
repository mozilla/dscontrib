import mozanalysis.bayesian_stats.binary as mabsbin
import pandas as pd
import scipy.stats as st
import numpy as np


def run_simmo(fake_results, control_rate, true_rel_change):
    lower = []
    upper = []

    for _ in range(1000):
        fake_results['num_conversions'] = st.binom(
            fake_results['num_enrollments'],
            np.array([control_rate, control_rate * (1 + true_rel_change)])
        ).rvs()

        stats = mabsbin.compare_branches_from_agg(fake_results)

        lower.append(stats['comparative']['test']['rel_uplift']['0.975'])
        upper.append(stats['comparative']['test']['rel_uplift']['0.025'])

    return pd.DataFrame({'lower': lower, 'upper': upper}).quantile([0.025, 0.975])


def set_up_fake_results_df(total_num_enrollments, test_branch_prop):
    t_pop = st.binom(total_num_enrollments, test_branch_prop).rvs()

    return pd.DataFrame(
        {'num_enrollments': [total_num_enrollments - t_pop, t_pop]},
        index=['control', 'test']
    )


def vary_rel_change(
    total_num_enrollments, test_branch_prop, control_rate, rel_change_list, ax,
    metric_name
):
    """Plot how various relative changes are likely to be reported.

    Can usually get away with only two `rel_change_list`; linear
    interpolation is great.

    Example usage in databricks:

        plt.close()
        plt.clf()
        ax = plt.figure().gca()

        vary_rel_change(100000, 0.5, 0.1, [0, 0.2], ax, 'fake metric')

        ax.figure.tight_layout()
        ax.figure.show()
        display()
    """
    fake_results = set_up_fake_results_df(total_num_enrollments, test_branch_prop)

    res = {rc: run_simmo(fake_results, control_rate, rc) for rc in rel_change_list}

    ax.fill_between(
        rel_change_list,
        [res[rc].loc[0.025, 'lower'] for rc in rel_change_list],
        [res[rc].loc[0.975, 'lower'] for rc in rel_change_list],
        alpha=0.1,
        color='red',
        label="2.5% worst case"
    )
    ax.fill_between(
        rel_change_list,
        [res[rc].loc[0.025, 'upper'] for rc in rel_change_list],
        [res[rc].loc[0.975, 'upper'] for rc in rel_change_list],
        alpha=0.1,
        color='green',
        label="97.5% best case"
    )

    for rc in rel_change_list:
        ax.plot([rc, rc], res[rc]['lower'].values, 'red')
        ax.plot([rc, rc], res[rc]['upper'].values, 'green')

    ax.plot([min(rel_change_list), max(rel_change_list)], [0, 0], 'k--', label='zero')
    ax.legend()
    ax.set_xlabel("'true' relative change w.r.t. control")
    ax.set_ylabel('reported relative change (95% CI)')
    ax.set_title(
        'Expected range of best/worst cases:\n'
        + metric_name + '\n'
        + '{e} enrollments, {t}% test branch, {r} baseline rate'.format(
            e=total_num_enrollments, t=test_branch_prop*100, r=control_rate
        )
    )


def vary_population_size(
    total_num_enrollments_list, test_branch_prop, control_rate, rel_change, ax,
    metric_name
):
    """Plot how various population sizes are likely to be reported.

    E.g. Find how large a population is required to reliably observe an
    effect as being positive.

    Linear interpolation is poor; supply several population sizes in
    `total_num_enrollments_list`.

    Example usage in databricks:

        plt.close()
        plt.clf()
        ax = plt.figure().gca()

        vary_population_size(
            [10000, 50000, 100000], 0.5, 0.1, 0.05, ax, 'fake metric'
        )

        ax.figure.tight_layout()
        ax.figure.show()
        display()
    """

    res = {
        tne: run_simmo(
            set_up_fake_results_df(tne, test_branch_prop),
            control_rate,
            rel_change
        )
        for tne in total_num_enrollments_list
    }

    ax.fill_between(
        total_num_enrollments_list,
        [res[rc].loc[0.025, 'lower'] for rc in total_num_enrollments_list],
        [res[rc].loc[0.975, 'lower'] for rc in total_num_enrollments_list],
        alpha=0.1,
        color='red',
        label="2.5% worst case"
    )
    ax.fill_between(
        total_num_enrollments_list,
        [res[rc].loc[0.025, 'upper'] for rc in total_num_enrollments_list],
        [res[rc].loc[0.975, 'upper'] for rc in total_num_enrollments_list],
        alpha=0.1,
        color='green',
        label="97.5% best case"
    )

    for rc in total_num_enrollments_list:
        ax.plot([rc, rc], res[rc]['lower'].values, 'red')
        ax.plot([rc, rc], res[rc]['upper'].values, 'green')

    ax.plot(
        [min(total_num_enrollments_list), max(total_num_enrollments_list)],
        [0, 0], 'k--', label='zero'
    )
    ax.legend()
    ax.set_xlabel("total population size")
    ax.set_ylabel('reported relative change (95% CI)')
    ax.set_title(
        'Expected range of best/worst cases:\n'
        + metric_name + '\n'
        + '{t}% test branch, {r} baseline rate, {rc} true rel change'.format(
            t=test_branch_prop*100, r=control_rate, rc=rel_change
        )
    )

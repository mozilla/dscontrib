import numpy as np
from sklearn.preprocessing.data import StandardScaler


def ss(x):
    return StandardScaler().fit_transform(x.values[:, None]).ravel()


def reshape_date_cols(
    df, gbcol="ctry", date_col="date", ycol="n", min_grp_val=1, agg="sum"
):
    """
    Given a dataframe with groups and a time series column,
    take log of and standardize y.
    Return DF[row: gb] with a column for each time step.
    """
    date_col_df = (
        df.groupby([gbcol, date_col])[ycol]
        .agg(agg)
        .reset_index(drop=0)
        .assign(minn=lambda x: x.groupby([gbcol])[ycol].transform("min"))
        .query(f"minn > {min_grp_val}")
        .drop("minn", axis=1)
        .set_index([gbcol, date_col])
        .unstack()
        .fillna(0)
        .add(1)
        .pipe(np.log10)
        .rename(columns=str)[ycol]
        .T.apply(ss)
        .T
    )
    return date_col_df


def reshape_clusters(date_col_df, gbcol, cls):
    """
    Given df where each column is a time step (and therefore feature),
    cluster each row. Return stacked tidy df where each row has
    a date, value, group and cluster.

    cls: clustering sklearn model.
    -> Df[`gbcol`, `date`, 'val', 'k']
    """
    ks = cls.fit_predict(date_col_df)
    # gbk = "ctry"
    gb2k = dict(zip(date_col_df.index, ks))
    pdf = (
        date_col_df.stack()
        .reset_index(drop=0)
        .rename(columns={0: "val"})
        .assign(k=lambda x: x[gbcol].map(gb2k))
    )
    return pdf


def mk_cluster_plot(A, pdf, gbcol="ctry", date_col="date", y="val"):

    h = (
        A.Chart(pdf)
        .mark_line()
        .encode(
            x=A.X(date_col, title=date_col),
            y=A.Y(y, title=y),
            color=gbcol,
            tooltip=[gbcol, date_col, y],
        )
    )

    return (h + h.mark_point()).facet(row="k", columns=3).interactive()


def main(A, df, KMeans):
    np.random.seed(0)
    df_gb = reshape_date_cols(
        df[["date", "ctry", "ncid"]],
        gbcol="ctry",
        date_col="date",
        ycol="ncid",
        min_grp_val=30,
    )
    pdf = reshape_clusters(df_gb, gbcol="ctry", cls=KMeans())
    return mk_cluster_plot(A, pdf, gbcol="ctry", date_col="date", y="val")

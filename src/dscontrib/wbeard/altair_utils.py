import altair.vegalite.v3 as A3
import altair.vegalite.v4 as A4


def set_json(prefix="data/altair-data", A=A3):
    A.data_transformers.enable("json", prefix=prefix)
    A.Chart.pipe = pipe


def set_ds(A):
    """
    https://github.com/altair-viz/altair/issues/1867#issuecomment-572879619
    pip install altair_data_server

    Another alternative here
    https://github.com/altair-viz/altair/issues/1867#issuecomment-564643088
    involving modifying register('json', to_json_for_lab)
    with altair.utils.data.to_json
    """
    A.data_transformers.enable("data_server")
    A.Chart.pipe = pipe


def pipe(h, f):
    return f(h)


def add_point(h):
    return h + h.mark_point()


def add_line(h):
    return h + h.mark_line()


def aconf(ch, l=22, m=18, sm=16):  # noqa
    return (
        ch.configure_axis(titleFontSize=m, labelFontSize=sm)
        .configure_title(fontSize=l)
        .configure_legend(titleFontSize=m, labelFontSize=sm)
    )


def pat(
    df,
    x="time",
    y="y",
    st="o",
    c=None,
    by=None,
    ncols=3,
    e1=None,
    e2=None,
    yind=True,
    nz=True,
    tt_extra=[],
    A=A4,
):
    "Plot altair"
    tooltip = [x, y] + tt_extra
    if c is not None:
        tooltip += [c]
    zscale = A.Scale(zero=not nz)

    h = A.Chart(df).encode(
        x=A.X(x, title=x),
        y=A.Y(y, title=y, scale=zscale),
        tooltip=tooltip,
    )
    if c is not None:
        h = h.encode(color=c)

    # Mark
    if st == "o":
        h = h.mark_point()
    elif st == "-":
        h = h.mark_line()
    elif st == "o-":
        h = h.mark_point() + h.mark_line()
    else:
        raise NotImplementedError(f"What is this? {st}")

    if e1 is not None:
        h = h + h.mark_errorband().encode(
            y=A.Y(e1, title=y, scale=zscale), y2=A.Y2(e2, title=y)
        )

    if by is not None:
        h = h.encode(facet=A.Facet(by, columns=ncols))
        if yind:
            h = h.resolve_scale(y="independent")
    return h.interactive()

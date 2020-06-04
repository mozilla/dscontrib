import altair.vegalite.v3 as A3


def set_json(prefix="data/altair-data", A=A3):
    A.data_transformers.enable("json", prefix=prefix)
    A.Chart.pipe = pipe


def set_ds(A):
    """
    https://github.com/altair-viz/altair/issues/1867#issuecomment-572879619
    pip install altair_data_server
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

import altair.vegalite.v3 as A

Chart = A.Chart


def set_json(prefix="data/altair-data"):
    A.data_transformers.enable("json", prefix=prefix)


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


A.Chart.pipe = pipe
nz = A.Scale(zero=False)
lgs = A.Scale(type="log", zero=False)

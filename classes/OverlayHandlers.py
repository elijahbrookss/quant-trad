import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from .OverlayRegistry import register_overlay_handler

@register_overlay_handler("addplot")
def handle_addplot(df, price_ax, specs):
    return specs

@register_overlay_handler("rect")
def handle_rectangle(df, price_ax, specs):
    last_num = mdates.date2num(df.index[-1])
    for r in specs:
        start_num = mdates.date2num(r["start"])
        rect = Rectangle(
            (start_num,  r["val"]),
            width      = last_num - start_num,
            height     = r["vah"] - r["val"],
            facecolor  = r.get("color", "gray"),
            alpha      = r.get("alpha", 0.2),
            edgecolor  = None,
            zorder = 1
        )
        price_ax.add_patch(rect)
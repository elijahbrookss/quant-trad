import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import matplotlib.transforms as mtransforms
from .OverlayRegistry import register_overlay_handler
from classes.Logger import logger
import pandas as pd
import mplfinance as mpf
import numpy as np

@register_overlay_handler("addplot")
def handle_addplot(df, price_ax, specs):
    logger.debug("Handling addplot overlays: count=%d", len(specs))
    return specs

@register_overlay_handler("rect")
def handle_rectangle(df, price_ax, specs):
    logger.info("Handling rectangle overlays: count=%d", len(specs))
    logger.debug("Axis limits before overlays: xlim=%s, ylim=%s", price_ax.get_xlim(), price_ax.get_ylim())
    
    x_vals = np.arange(len(df.index))  # Positional X values (0, 1, 2, ..., N)
    
    for idx, r in enumerate(specs):
        start = r.get("start", df.index[0])
        end = r.get("end", df.index[-1])
        val = r["val"]
        vah = r["vah"]
        color = r.get("color", "gray")
        alpha = r.get("alpha", 0.2)
        
        mask = (df.index >= start) & (df.index <= end)
        x_range = x_vals[mask]
        val_vals = np.full_like(x_range, val, dtype=float)
        vah_vals = np.full_like(x_range, vah, dtype=float)

        price_ax.fill_between(
            x=x_range,
            y1=val_vals,
            y2=vah_vals,
            color="black",
            alpha=alpha,
            linewidth=2,
            edgecolor=color,
            zorder=2
        )

        logger.debug(
            "Rectangle %d: fill_between points=%d, VAL=%.2f, VAH=%.2f, color=%s, alpha=%.2f",
            idx, len(x_range), val, vah, color, alpha
        )

    logger.debug("Axis limits after overlays: xlim=%s, ylim=%s", price_ax.get_xlim(), price_ax.get_ylim())



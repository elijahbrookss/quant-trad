from pathlib import Path
from classes.indicators.BaseIndicator import BaseIndicator

class VWAPIndicator(BaseIndicator):
    NAME = "vwap"

    def compute(self):
        tp = (self.df["High"] + self.df["Low"] + self.df["Close"]) / 3
        cum_vol = self.df["Volume"].cumsum()
        self.df["VWAP"] = (tp * self.df["Volume"]).cumsum() / cum_vol
        self.result = self.df["VWAP"]
        # Score stub – price above / below vwap?
        self.score = float((self.df.iloc[-1]["Close"] > self.result.iloc[-1]))
        return self.result

    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        fig, ax = self._init_price_ax(self.df, "VWAP vs Price")
        ax.plot(self.df.index, self.result, label="VWAP", color="yellow")
        ax.legend(facecolor="black", edgecolor="white", fontsize=8)
        return self._save_fig(fig, "vwap.png")

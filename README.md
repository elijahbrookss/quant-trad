## quant-trad

Modular Python framework for building and analyzing quantitative trading strategies.

---

### Features

- Multi-lookback pivot detection
- Ranked trendline analysis using linear regression
- Trendline scoring based on R², angle, proximity, and violation ratio
- Density visualization for pivot clustering
- Clean, modular architecture (`ChartPlotter`, `TrendlineAnalyzer`, etc.)
- Easy to extend with future plots (levels, channels, breakouts)

---

### Project Structure

```
quant-trad/
├── artifacts/               # Saved charts and outputs
├── classes/                # Core reusable components
│   ├── ChartPlotter.py
│   ├── PivotDetector.py
│   ├── StockData.py
│   ├── Trendline.py
│   ├── TrendlineAnalyzer.py
│   └── Logger.py
├── quant-env/              # Virtual environment (excluded from git)
├── strategy.py             # Legacy version (to be refactored)
├── main.py                 # Main execution pipeline
├── start.sh                # Custom startup script
├── README.md
└── .gitignore
```

---

### Quickstart

#### 1. Clone the repo

```bash
git clone https://github.com/elijahbrookss/quant-trad.git
cd quant-trad
```

#### 2. Set up environment manually

```bash
python3 -m venv quant-env
source quant-env/bin/activate
pip install -r python_imports
```

#### 3. Run analysis

```bash
python main.py
```

Outputs will be saved under `artifacts/trendlines/`.

---

### Quickstart (with `start.sh`)

Use the included shell script to simplify setup:

```bash
chmod +x start.sh
./start.sh
```

This script:
- Sets up command aliases
- Prompts you to create and activate a virtual environment
- Prompts to install requirements
- Adds these aliases to `~/.bash_aliases`:
  - `quant-env`: Activate the virtual environment
  - `quant-install`: Install dependencies
  - `quant-run`: Run the strategy script
  - `quant-deactivate`: Exit the environment
  - `quant-help`: View command summary

---

### Example Output
![image](https://github.com/user-attachments/assets/408f75f3-cb4c-4a89-93da-4418583ab046)

---

### Requirements

- pandas
- numpy
- matplotlib
- scipy
- yfinance

Install all dependencies with:

```bash
pip install -r python_imports
```

---

### Roadmap

- Support/resistance level detection
- Trendline/channel breakout detection
- Horizontal pivot value clustering
- Backtesting and performance tracking

---

### Author

[Elijah Brooks](https://github.com/elijahbrookss)

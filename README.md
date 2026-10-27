# Parallel-market USD/IQD daily series

**[العربية](README.ar.md)** | English

An open dataset and dashboard tracking the **parallel-market** US-dollar exchange rate in Iraq. This project automatically monitors public channels to provide the daily street rates that the Central Bank of Iraq no longer publishes.

بيانات مفتوحة لأسعار صرف الدولار مقابل الدينار العراقي في السوق الموازية، تشمل بغداد والبصرة وأربيل، مع بيانات تاريخية قابلة للتنزيل وتوثيق للمصادر.

- 📈 **Live dashboard: Iraq Parallel Exchange Rate Dataset** [Iraq Parallel Exchange Rate](https://HusseinKh90.github.io/Iraq-parallel-exchange-rate/)
- 💾 **Data Download:** [data/daily_fx.csv](data/daily_fx.csv) (A simple CSV containing the daily average exchange rates by city. Free to download and use.)

> **Disclaimer:** This is an aggregated parallel-market dataset derived from public channels. It is **not** an official Central Bank rate and **not** financial advice.

## ⚙️ How it Works

The GitHub Actions workflow `.github/workflows/update-data.yml` schedules daily updates at 18:00 Baghdad time (15:00 UTC). Scheduled runs may start late. The workflow also runs on relevant pushes to `main` and can be started manually from **Actions → Update exchange rates and publish dashboard → Run workflow**. It becomes active once the workflow is pushed to GitHub's default branch. The system follows three simple steps:

1. **Scrape:** We gather the latest exchange rate posts directly from three major public Telegram channels: `@dollariraqi`, `@iqborsa`, and `@dollar_price`. No API keys or logins are required.
2. **Parse:** The system reads the Arabic text in these posts and finds the opening and closing rates for major Iraqi cities (like Baghdad, Erbil, and Basra).
3. **Consensus:** Because street rates can vary, the system blends the numbers from all three channels. It automatically filters out obvious typos and calculates a reliable daily average for each city. The final results are saved directly to `data/daily_fx.csv`.

The workflow uses the existing extractor without changing its parsing or calculation rules. It verifies that all channel scrapes completed, validates the published CSV and checks that the dataset is no more than five days old before committing files under `data/` and `pipeline/`. The existing channel health check reports warnings for stale individual channels, low match rates or source disagreement; those warnings do not block otherwise valid updates. These checks do not guarantee every rate is accurate.

After updating the data, the workflow deploys the dashboard directly to GitHub Pages, including when there are no new data changes. The site contains `index.html`, the four published CSV files listed below, and two small files built on every run by `scripts/build_site_assets.py` from those CSVs: `data/dataset_stats.json` (the numbers in the page's "Dataset at a glance" line) and `social/card.png` (the preview image shown when the page is shared). Neither is committed to the repository, and the audit-trail folders are not part of the site. For this to work, **Settings → Pages → Build and deployment → Source** must be set to **GitHub Actions**. With *Deploy from a branch*, GitHub also starts its own `pages-build-deployment` on every push, which duplicates this workflow's deployment. No Telegram credentials or personal access token are required. Repository policies must permit the workflow to write commits to `main` and deploy to the `github-pages` environment. Review failed runs and health warnings in the Actions tab. GitHub may disable scheduled workflows in public repositories after 60 days without repository activity; check that the workflow remains enabled if updates stop.

## 📁 What's in `data/` and `pipeline/`

**Start with `data/daily_fx.csv`.** `data/` holds only the finished CSV files. Everything the pipeline needs to produce them lives in `pipeline/`, the audit trail behind the numbers.

**`data/` — the outputs**

| File | What it is |
|---|---|
| `daily_fx.csv` | **The dataset.** Daily open and close for Baghdad, Basra and Erbil from 2020-12-01. Columns: `date`, `<city>_open`, `<city>_close`, `baghdad_market` (which Baghdad wholesale sub-market that day's figure describes) and `channels_used` (the channels averaged that day). |
| `validate_fx.csv` | 2018-01-01 to 2020-11-30, closing rates only. The stretch used to check the method against the Central Bank of Iraq. |
| `consensus_daily.csv`, `consensus_city_day.csv` | The consensus before publishing: every market per day, with each channel's figure and the spread between channels. |

**`pipeline/` — the audit trail**

| Folder / file | What it is |
|---|---|
| `raw/` | Every post as scraped from Telegram, one JSON object per post. The scraper uses it as its cache and only fetches newer posts, so do not delete it. |
| `readings/` | Every accepted price reading, with a link to the Telegram post it came from. |
| `daily/` | Open/close tables per channel. |
| `unmatched/` | Posts the parser could not read anything from; used by `python iraq_fx_parser.py audit`. |
| `parse_report.json`, `scrape_report.json` | Counts of posts, readings and match rates from the last run. |

The four CSV files in `data/` are published on the dashboard site. `pipeline/` stays in this repository and is not served on the site.

## 🧭 Using the data for Iraq as a whole

The dataset publishes Baghdad, Basra and Erbil as separate series. It does not define a single national rate for Iraq. If your research needs one figure, there are two defensible choices. Pick one, say which in your methods, and consider reporting the other as a robustness check. The figures below use the days when all three cities were quoted (1,382 of 1,502 days, 92%), with data through 26 October 2026.

**Option 1: use Baghdad as the representative rate**

- Baghdad is the most complete series: 1,500 of 1,502 days, against 1,402 for Basra and 1,412 for Erbil.
- Its daily close differs from the three-city average by a median of 0.06%. The difference is within 0.28% on 95% of days and within 0.5% on 98.5% of days. The largest difference is about 1.3%.
- Day-to-day changes in Baghdad and in the three-city average move together (correlation 0.97).
- Caveat: Baghdad's figure comes from two sub-markets, the Kifah wholesale market and a general Baghdad quote, named in the `baghdad_market` column. The general quote has become more common (141 of the 213 days so far in 2026). On the 956 days when both exist they differ by a median of 0.19%, and by less than 0.54% on 95% of those days.

**Option 2: use the average of the three cities**

- Take the equal-weighted mean of `baghdad_close`, `basra_close` and `erbil_close`, and keep only days when all three exist so that the mix of cities does not change from day to day:

```python
import pandas as pd

df = pd.read_csv("data/daily_fx.csv", parse_dates=["date"])
city_cols = ["baghdad_close", "basra_close", "erbil_close"]
df = df[df[city_cols].notna().all(axis=1)].copy()
df["three_city_avg"] = df[city_cols].mean(axis=1)
```

- The weights are equal because no population or trade weights are available. Other governorates have too little data to include.
- This uses 1,382 of 1,502 days; days when Basra or Erbil was not quoted are dropped.

**Which to choose.** For levels and trends the two are practically interchangeable: on nearly every day they differ by well under 1%. Baghdad gives the longest unbroken daily record. The three-city average does not depend on a single city or on which Baghdad sub-market was quoted. Either way, these are quoted street rates, not an official national rate, and the choice should be stated when you cite the dataset.

## 🚀 How to Run it Locally

If you want to run the scraper manually instead of relying on the automated daily updates, you can easily do so on your own computer.

**Prerequisites:** You will need **Python 3.9 or newer** installed.

**1. Install Dependencies**
```bash
pip install -r requirements.txt
```

**2. Run the Parser**
```bash
python iraq_fx_parser.py run
```
*(This command will automatically scrape any missing recent data from Telegram, process it, and save the updated rates into the CSV file.)*

The finished CSVs are written to `data/`; the scrape cache and audit trail are kept in `pipeline/`. To use other folders, put `--data-dir` and `--work-dir` before the command, e.g. `python iraq_fx_parser.py --work-dir my_cache run`.

## 📊 Data, License & Attribution

- **Units:** Rates are tracked internally as IQD per 1 USD (e.g., 1507.8). Iraqi street prices are usually quoted per 100 USD (e.g., 150,780). You can multiply by 100 to match the street form.
- **License:** CC-BY 4.0 — free to use with attribution.
- **APA (7th Edition)**
  > Hussein, H. (2026). *Iraq Parallel Exchange Rate: High-Frequency Telegram Market Dataset* (Version 1.0.0) [Data set]. Zenodo. [https://doi.org/10.5281/zenodo.23209867](https://doi.org/10.5281/zenodo.23209867)

# Iraq Dinar Watch — parallel-market USD/IQD daily series

**[العربية](README.ar.md)** | English

An open dataset and dashboard tracking the **parallel-market** US-dollar exchange rate in Iraq. This project automatically monitors public channels to provide the daily street rates that the Central Bank of Iraq no longer publishes.

- 📈 **Live dashboard:** [Iraq Parallel Exchange Rate](https://HusseinKh90.github.io/Iraq-parallel-exchange-rate/)
- 💾 **Data Download:** [data/daily_fx.csv](data/daily_fx.csv) (A simple CSV containing the daily average exchange rates by city. Free to download and use.)

> **Disclaimer:** This is an aggregated parallel-market dataset derived from public channels. It is **not** an official Central Bank rate and **not** financial advice.

## ⚙️ How it Works

The GitHub Actions workflow `.github/workflows/update-data.yml` schedules daily updates at 18:00 Baghdad time (15:00 UTC). Scheduled runs may start late. The workflow also runs on relevant pushes to `main` and can be started manually from **Actions → Update exchange rates and publish dashboard → Run workflow**. It becomes active once the workflow is pushed to GitHub's default branch. The system follows three simple steps:

1. **Scrape:** We gather the latest exchange rate posts directly from three major public Telegram channels: `@dollariraqi`, `@iqborsa`, and `@dollar_price`. No API keys or logins are required.
2. **Parse:** The system reads the Arabic text in these posts and finds the opening and closing rates for major Iraqi cities (like Baghdad, Erbil, and Basra).
3. **Consensus:** Because street rates can vary, the system blends the numbers from all three channels. It automatically filters out obvious typos and calculates a reliable daily average for each city. The final results are saved directly to `data/daily_fx.csv`.

The workflow uses the existing extractor without changing its parsing or calculation rules. It verifies that all channel scrapes completed, validates the published CSV and checks that the dataset is no more than five days old before committing files under `data/`. The existing channel health check reports warnings for stale individual channels, low match rates or source disagreement; those warnings do not block otherwise valid updates. These checks do not guarantee every rate is accurate.

After updating the data, the workflow deploys the dashboard directly to GitHub Pages, including when there are no new data changes. This is necessary because commits made using `GITHUB_TOKEN` do not trigger the usual branch-based Pages build. No Telegram credentials or personal access token are required. Repository policies must permit the workflow to write commits to `main` and deploy to the `github-pages` environment. Review failed runs and health warnings in the Actions tab. GitHub may disable scheduled workflows in public repositories after 60 days without repository activity; check that the workflow remains enabled if updates stop.

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

## 📊 Data, License & Attribution

- **Units:** Rates are tracked internally as IQD per 1 USD (e.g., 1507.8). Iraqi street prices are usually quoted per 100 USD (e.g., 150,780). You can multiply by 100 to match the street form.
- **License:** CC-BY 4.0 — free to use with attribution.
- **Citation:**
  > Khalid, H. (2026) *Iraq parallel-market exchange rate: a daily USD/IQD series* [Dataset]. Available at: https://github.com/HusseinKh90/Iraq-parallel-exchange-rate

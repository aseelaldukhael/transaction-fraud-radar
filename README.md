# Transaction Fraud Radar

A data engineering project that detects suspicious card transactions on **simulated** Saudi data.
The pipeline runs on **Databricks** (Bronze / Silver / Gold on Delta Lake), and a bilingual website (English / Arabic) replays the results.

**Live demo:** https://aseelaldukhael.github.io/transaction-fraud-radar/

> All data is simulated. This is a portfolio project, not a banking product.

## What it does

1. **Generates** a week of card transactions for 2,000 customers across Saudi cities, with fraud patterns injected on purpose.
2. **Ingests** the transactions in small batches (micro-batches) with Auto Loader.
3. **Cleans** them and stores them in Delta tables.
4. **Detects** suspicious activity with four rules and writes alerts.
5. **Shows** the results in an interactive website that replays the transactions.

## Architecture

```
Transaction files (JSON)
        |
        v
  Bronze  -- Auto Loader, raw data as it arrives
        |
        v
  Silver  -- validation, de-duplication, MERGE into a Delta table
        |
        v
  Gold    -- fraud rules per card, alerts, daily summary
        |
        v
  data.json  -->  website (GitHub Pages)
```

- Ingestion: Auto Loader (`cloudFiles`) with Structured Streaming, `trigger(availableNow=True)`.
- Writes use Delta `MERGE`, so re-running a batch does not create duplicates.
- Checkpoints are stored in Unity Catalog volumes.
- The rules need history per card, so each micro-batch reads the last 60 transactions of each card from Silver.
- Checked: processing in micro-batches gives the same alerts as one pass over all data (278 alerts).

## Detection rules

| Rule | Shown as | Triggers when |
|---|---|---|
| High amount | High amount | Amount is more than 8x the card's usual amount and at least 3,000 SAR |
| Velocity | Rapid transactions | 5 or more transactions within 2 minutes |
| Impossible travel | Location change | Two transactions 300+ km apart at a speed above 900 km/h |
| Card testing | Card testing | 3 or more tiny transactions (10 SAR or less) within 5 minutes, or a large one right after |

## Results

Simulated week: 25,918 transactions, 220 of them fraud (0.85%), grouped in 51 fraud events.

| Measure | Result |
|---|---|
| Alerts raised | 278 |
| Fraud events caught | 50 of 51 |
| Fraudulent transactions caught (recall) | 64.1% (141 of 220) |
| Precision | 64.4% |
| False alarms | 78 (0.30% of normal transactions) |

Caught by rule: card testing 17/17, high amount 11/12, location change 11/11, rapid transactions 11/11.

Threshold tuning matters: a high-amount limit of 1,500 SAR gave 335 false alarms (30% precision), while 3,000 SAR gave 78 (64% precision).

## Limitations

- The fraud patterns were created by me, and the rules were tuned on the same data. Results on real data would be lower.
- Rules only: there is no machine learning model.
- Micro-batches on Databricks Free Edition, not continuous real-time streaming (the Free Edition has no always-on jobs).
- The website replays exported results. It does not run the pipeline in the browser.

## Next steps (planned)

- Kafka as the data source, and Airflow for scheduling.
- Scheduling the Databricks notebooks as a job.
- Comparing the rules with a simple ML model.

## Project structure

```
index.html          the website (GitHub Pages)
local/              run the idea on your own computer (Python + pandas)
  generate_transactions.py   create simulated transactions
  rules.py                   the fraud rules
  detect.py                  run the rules
  evaluate.py                measure precision and recall
databricks/         the pipeline, as Databricks notebooks (run in order)
  01_setup.py                schema and volumes
  02_producer.py             drops transaction files in batches
  03_pipeline.py             Bronze -> Silver -> Gold
  04_evaluate_export.py      metrics and data.json export
site-src/           website template and build script
```

## How to run

**On your computer**

```
cd local
python generate_transactions.py --customers 2000 --days 7 --seed 42
python detect.py
python evaluate.py
```

**On Databricks (Free Edition)**

1. Import the four notebooks from `databricks/`.
2. Run `01_setup.py`, then upload `transactions.csv` and `labels.csv` (from the local step) to the `raw` volume.
3. Run `02_producer.py` a few times (each run drops one batch of files), then run `03_pipeline.py` after each.
4. Run `04_evaluate_export.py` to see the metrics and create `data.json`.
5. Build the website: `python site-src/build_site.py data.json`.

## Tools

Python, Databricks, Apache Spark, Delta Lake, Auto Loader, pandas, HTML / JavaScript.

*Portfolio project by Aseel Aldukhael.*

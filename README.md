# Spielerstatistik — DFB Fallstudie (PySpark)

A PySpark data pipeline that builds a consolidated player statistics table for a national football team scenario, joining master data from an Oracle database with transactional data from CSV files, computing rolling and season-based performance metrics, and tracking premium/bonus payments.

This is a fictional case-study dataset (player names, ratings, and payments are all synthetic sample data), used to demonstrate a realistic data engineering workflow: multi-source ingestion, window-function analytics, and writing results into a Hive table.

## What it does

1. **Loads master data from Oracle** (via JDBC): player records, national team category, financial transactions.
2. **Loads transactional data from CSV**: match records and per-match player ratings.
3. **Joins and enriches**: player ratings are joined with match data to bring in season, match date, and whether it was an official international match (`istLaenderspiel`).
4. **Computes KPIs per player**:
   - Total matches played, and total official international matches
   - Average rating overall
   - Average rating over the last 5 matches (a window function, ranking each player's own matches by date)
   - Average rating for the current season
   - Total premium payments received during their national team career
   - Currently outstanding (unpaid) premium payments
5. **Writes the result** as a Parquet table (`spielerstatistik`) into Hive.

## Data

| File | Contents |
|---|---|
| `data/spieler.csv` | Player master data (name, birth date, club, etc.) |
| `data/nationalmannschaft.csv` | Which national team category each player belongs to |
| `data/spiel.csv` | Match records — date, season, opponent, result, attendance, sponsorship revenue |
| `data/spielerbewertung.csv` | Per-match player ratings (0-100%) with coach comments |
| `data/finanztransaktionen.csv` | Premium/bonus payments per player, with payment status |

In the actual pipeline, `spieler`, `nationalmannschaft`, and `finanztransaktionen` are loaded from Oracle (simulating master/reference data); `spiel` and `spielerbewertung` are loaded directly from CSV (simulating transactional/event data). The CSVs are included here so the source data is visible without needing the database running.

## Running it

This project uses Docker to run both an Oracle database and Spark.

1. Copy `.env.example` to `.env` and set your own `ORACLE_PASSWORD` value — this file is gitignored and never committed.
2. Start the containers:
   ```
   docker compose up -d
   ```
3. Load the CSV master-data tables (`spieler`, `nationalmannschaft`, `finanztransaktionen`) into the Oracle container.
4. Run the pipeline:
   ```
   docker exec spark-dfb spark-submit /app/spielerstatistik.py
   ```

**Note on credentials:** the database password is read from the `ORACLE_PASSWORD` environment variable (via `.env`) rather than being hardcoded in the script or `docker-compose.yml`.

## Tech stack

PySpark (DataFrame API, window functions, Hive integration) · Oracle (JDBC) · Docker Compose

## Presentation

`presentation/DFB_Spielerstatistik.pptx` — results and findings presentation.

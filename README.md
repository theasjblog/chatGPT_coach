# chatGPT_coach

A small data-staging repo for personal coaching data exported from Notion and Garmin (logs, health metrics, sleep, runs, schedules) and cleaned into CSVs.

## How to use
- Clone the repo
- Run `renv::restore()` once to install the pinned packages.
- Download your latest Notion export and drop the raw zip into `data/`. Do not unzip it.
- Drop Garmin activity JSON files (as exported by GoldenCheetah) into `data/`. They are auto-detected by Sport/SubSport:
  - Run (Sport=`Run`) — intervals in INTERVALS / samples in SAMPLES.
  - Bike indoor (Sport=`Bike`, SubSport=`turbo_trainer`).
  - Swim pool (Sport=`Swim`, SubSport=`lap swimming`) with rest/drill/freestyle interval typing.
- For bike_outdoor and swim_ow you can still provide the legacy split CSVs in `data/` (`bike_outdoor.csv`, `swim_ow.csv`).
- Download to the `data` folder the Health reports `Sleep.csv` and `Weight.csv` from Garmin.
- Run the file `R/main.R` to read the raw export and write cleaned CSVs into `exports/`.

## Expected `data/` contents
- Each file is a Notion export zip named like `<uuid>_ExportBlock-<uuid>.zip` that contains a single inner zip `ExportBlock-<id>-Part-1.zip`.
- Inside the inner zip is a `Logs` folder with the following files (IDs vary per export):
  - `Logs 29c9434858298062a3d3c77798048709.md` — the main Notion page content.
  - `Logs/Health metrics … .csv` and `Logs/Health metrics … _all.csv`.
  - `Logs/daily plan … .csv` and `Logs/daily plan … _all.csv`.
  - `Logs/weekly schedule … .csv` and `Logs/weekly schedule … _all.csv`.
  - `Logs/schedule … .csv` and `Logs/schedule … _all.csv`.

## Other directories
- `data_bck/`: archived raw exports plus ad-hoc CSV snapshots (Sleep.csv, run.csv, Weight.csv).
- `exports/`: cleaned/aggregated outputs named with the export date, including:
  - `sleep_trend`: full sleep history table from `Sleep.csv` (one row per day, all metrics).
  - `weight_trend`: full weight history from `Weight.csv` (no 2-week filter).
  - `run`, `bike_*`, `swim_*`: cleaned lap/interval tables (run/bike_indoor/swim_pool from JSON; bike_outdoor/swim_ow still accept CSV).
  - `health_daily`: health metrics from the latest Notion export.
  - `training_sessions_planned`: daily training plan for today.
  - `schedule`: schedule entries for today.
- `config.yml`: centralizes the data, backup, export, and temp directories.
- `renv/`, `renv.lock`: R environment bootstrap and lockfile.

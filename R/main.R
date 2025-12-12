library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(janitor)
library(tidyr)
library(config)

source(file.path(getwd(), "R", "functions.R"))
source(file.path(getwd(), "R", "activity_specs.R"))

config <- config::get()

ensure_dir(config$tmp_dir, clean = TRUE)
ensure_dir(config$exports_dir, clean = TRUE)

process_notion_exports(config)

# process activity csvs (run, bike, swim)
invisible(lapply(names(activity_specs), function(name) {
  spec <- activity_specs[[name]]
  process_activity(name, spec$pattern, spec$columns, config)
}))

# process sleep and weight csvs in data
process_sleep(file.path(config$data_dir, "Sleep.csv"), config$exports_dir, config$tz)

# backup data directory and reset to empty
backup_and_reset_data(config$data_dir, config$data_bck_dir)

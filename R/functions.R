log_info <- function(...) message("[notion_exports] ", paste(..., collapse = " "))
log_warn <- function(...) warning(paste0("[notion_exports] ", paste(..., collapse = " ")), call. = FALSE)

ensure_dir <- function(path, clean = FALSE) {
  if (dir.exists(path) && clean) {
    unlink(path, recursive = TRUE, force = TRUE)
  }
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
}

latest_file <- function(paths, tz) {
  if (length(paths) == 0) {
    return(NULL)
  }

  info <- file.info(paths)
  idx <- order(info$mtime, basename(paths), decreasing = TRUE)
  paths[idx[1]]
}

safe_unzip <- function(zip_path, exdir) {
  tryCatch(
    {
      utils::unzip(zip_path, exdir = exdir)
      TRUE
    },
    error = function(e) {
      log_warn("Failed to unzip", zip_path, ":", e$message)
      FALSE
    }
  )
}

read_csv_clean <- function(path) {
  tryCatch(
    readr::read_csv(path, show_col_types = FALSE) |>
      janitor::clean_names(),
    error = function(e) {
      log_warn("Failed to read", path, ":", e$message)
      NULL
    }
  )
}

select_expected_csvs <- function(csvs) {
  patterns <- c(
    health = "Health",
    sessions = "daily",
    blocks = "weekly"
  )
  lapply(patterns, function(pat) {
    match <- csvs[stringr::str_detect(csvs, pat)]
    if (length(match) == 0) return(NULL)
    match[[1]]
  })
}

filter_this_week <- function(df, tz) {
  df |>
    mutate(date = as.Date(date, format = "%d %B %Y")) |>
    filter(
      date >= floor_date(today(tzone = tz), "week", week_start = 1),
      date < floor_date(today(tzone = tz), "week", week_start = 1) + days(7)
    ) |>
    select(where(~ !all(is.na(.x))))
}

health_long_to_wide <- function(df) {
  if (!all(c("name", "value") %in% names(df))) {
    log_warn("Health metrics missing expected columns name/value.")
    return(NULL)
  }

  df |>
    mutate(row_id = 1) |>
    pivot_wider(
      names_from = name,
      values_from = value,
      values_fn = ~ dplyr::last(na.omit(.x))
    ) |>
    select(-row_id) |>
    janitor::clean_names()
}

write_export <- function(df, path) {
  tryCatch(
    {
      write_csv(df, path)
      TRUE
    },
    error = function(e) {
      log_warn("Failed to write", path, ":", e$message)
      FALSE
    }
  )
}

clean_old_zips <- function(data_dir, keep_path) {
  zips <- list.files(data_dir, pattern = "\\.zip$", full.names = TRUE)
  for (z in zips) {
    if (!identical(z, keep_path)) {
      file.remove(z)
    }
  }
}

parse_date_flex <- function(x, tz) {
  x_clean <- x |>
    stringr::str_replace_all("\\s*\\([^\\)]*\\)", "") |>
    stringr::str_squish()

  suppressWarnings(
    parse_date_time(
      x_clean,
      orders = c(
        "Y-m-d H:M", "Y-m-d",
        "d B Y H:M", "d b Y H:M", "d B Y", "d b Y",
        "m/d/Y H:M", "m/d/Y",
        "Y/m/d H:M", "Y/m/d",
        "d-m-Y H:M", "d-m-Y", "m-d-Y H:M", "m-d-Y",
        "Ymd HMS", "Ymd"
      ),
      tz = tz
    )
  )
}

load_weight_df <- function(path, tz) {
  if (!file.exists(path)) {
    log_warn("Weight.csv not found at", path)
    return(NULL)
  }

  weight_lines <- readr::read_lines(path) |>
    stringr::str_replace("^\ufeff", "") |>
    stringr::str_trim(side = "right")

  if (length(weight_lines) < 3) {
    log_warn("Weight.csv appears too short to parse.")
    return(NULL)
  }

  header <- weight_lines[1]
  rows <- weight_lines[-1]
  rows <- rows[rows != ""]

  date_rows <- rows[seq(1, length(rows), by = 2)]
  data_rows <- rows[seq(2, length(rows), by = 2)]

  if (length(date_rows) != length(data_rows)) {
    log_warn("Weight.csv appears malformed: date/time rows not paired.")
    return(NULL)
  }

  date_rows <- date_rows |>
    stringr::str_remove_all("\"") |>
    stringr::str_remove(",$") |>
    stringr::str_trim()

  weight_csv_text <- paste(c(header, data_rows), collapse = "\n")
  weight_df <- readr::read_csv(weight_csv_text, show_col_types = FALSE) |>
    janitor::clean_names()

  weight_df <- weight_df |>
    mutate(
      date_raw = date_rows,
      date = dmy(date_raw),
      timestamp = parse_date_time(paste(date_raw, time), orders = "d b Y H:M"),
      .after = time
    )

  weight_df
}

process_sleep <- function(path, exports_dir, tz) {
  if (!file.exists(path)) {
    log_warn("Sleep.csv not found at", path)
    return(invisible(FALSE))
  }

  lines <- readr::read_lines(path) |>
    stringr::str_replace("^\ufeff", "") |>
    stringr::str_trim()

  lines <- lines[lines != ""]
  if (length(lines) == 0) {
    log_warn("Sleep.csv is empty after cleaning.")
    return(invisible(FALSE))
  }

  sleep_df <- tibble(raw = lines) |>
    mutate(
      metric = str_split_fixed(raw, ",", 2)[, 1],
      value = str_split_fixed(raw, ",", 2)[, 2],
      metric = str_trim(metric),
      value = na_if(str_trim(value), "")
    ) |>
    select(metric, value) |>
    janitor::clean_names()

  sleep_wide <- sleep_df |>
    mutate(row_id = 1) |>
    pivot_wider(
      names_from = metric,
      values_from = value,
      values_fn = ~ dplyr::last(na.omit(.x))
    ) |>
    janitor::clean_names()

  write_export(
    sleep_wide,
    file.path(exports_dir, paste0(today(tzone = tz), "_sleep.csv"))
  )
  invisible(TRUE)
}

process_weight <- function(path, exports_dir, tz) {
  weight_df <- load_weight_df(path, tz)
  if (is.null(weight_df)) return(invisible(FALSE))

  weight_today <- weight_df |> filter(date == today(tzone = tz))

  if (nrow(weight_today) == 0) {
    log_warn("No weight entry for today found in Weight.csv.")
    return(invisible(FALSE))
  }

  write_export(
    weight_today,
    file.path(exports_dir, paste0(today(tzone = tz), "_weight.csv"))
  )
  invisible(TRUE)
}

process_activity <- function(name, pattern, columns, config) {
  files <- list.files(config$data_dir, pattern = "\\.csv$", full.names = TRUE)
  files <- files[stringr::str_detect(basename(files), pattern)]

  if (length(files) == 0) {
    log_warn("No", name, "CSV found in data.")
    return(invisible(FALSE))
  }
  if (length(files) > 1) {
    log_warn("Multiple", name, "CSVs found; using the most recent by modified time.")
  }

  path <- latest_file(files, config$tz)
  df <- read_csv_clean(path)
  if (is.null(df)) return(invisible(FALSE))

  df <- df |>
    select(any_of(columns))

  if ("laps" %in% names(df)) {
    df <- df |> filter(laps != "Summary")
  }
  if ("swim_stroke" %in% names(df)) {
    df <- df |> filter(swim_stroke != "--")
  }

  out_path <- file.path(
    config$exports_dir,
    paste0(today(tzone = config$tz), "_", name, ".csv")
  )
  write_export(df, out_path)
}

process_weight_week <- function(path, exports_dir, tz) {
  weight_df <- load_weight_df(path, tz)
  if (is.null(weight_df)) return(invisible(FALSE))

  week_start <- floor_date(today(tzone = tz), "week", week_start = 1)
  week_end <- week_start + days(6)

  weight_week <- weight_df |>
    filter(date >= week_start, date <= week_end)

  if (nrow(weight_week) == 0) {
    log_warn("No weight entries for current week found in Weight.csv.")
    return(invisible(FALSE))
  }

  write_export(
    weight_week,
    file.path(
      exports_dir,
      paste0(week_end, "_weight_weekly.csv")
    )
  )
}

backup_and_reset_data <- function(src_dir, backup_dir) {
  if (!dir.exists(src_dir)) {
    log_warn("Data directory not found; skipping backup/reset.")
    return(invisible(FALSE))
  }
  if (dir.exists(backup_dir)) {
    unlink(backup_dir, recursive = TRUE, force = TRUE)
  }
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)

  contents <- list.files(src_dir, full.names = TRUE, all.files = TRUE, no.. = TRUE)
  if (length(contents) > 0) {
    file.copy(contents, backup_dir, recursive = TRUE)
  }

  unlink(src_dir, recursive = TRUE, force = TRUE)
  dir.create(src_dir, recursive = TRUE, showWarnings = FALSE)
  log_info("Backed up data to", backup_dir, "and reset data directory.")
  invisible(TRUE)
}

process_notion_exports <- function(config) {
  data_zips <- list.files(config$data_dir, pattern = "\\.zip$", full.names = TRUE)
  data_newest_zip <- latest_file(data_zips, config$tz)

  if (is.null(data_newest_zip)) {
    log_info("No zip files found in data; skipping unzip and downstream processing.")
    return(invisible(NULL))
  }

  newest_zip_date <- as_date(file.info(data_newest_zip)$mtime, tz = config$tz)

  if (newest_zip_date != today(tzone = config$tz)) {
    log_info("Latest zip", basename(data_newest_zip), "is from", newest_zip_date, "- skipping processing.")
    return(invisible(NULL))
  }

  if (!safe_unzip(data_newest_zip, config$tmp_dir)) {
    return(invisible(NULL))
  }

  tmp_zips <- list.files(config$tmp_dir, pattern = "\\.zip$", full.names = TRUE)
  tmp_newest_zip <- latest_file(tmp_zips, config$tz)
  if (!is.null(tmp_newest_zip)) {
    safe_unzip(tmp_newest_zip, config$tmp_dir)
  }

  all_csvs <- list.files(config$tmp_dir, pattern = "\\_all.csv$", full.names = TRUE, recursive = TRUE)
  selected_csvs <- select_expected_csvs(all_csvs)

  if (any(vapply(selected_csvs, is.null, logical(1)))) {
    log_warn("Missing expected _all.csv files; skipping exports.")
    return(invisible(NULL))
  }

  health <- read_csv_clean(selected_csvs$health)
  sessions <- read_csv_clean(selected_csvs$sessions)
  blocks <- read_csv_clean(selected_csvs$blocks)

  if (any(vapply(list(health, sessions, blocks), is.null, logical(1)))) {
    log_warn("One or more data files failed to load; skipping exports.")
    return(invisible(NULL))
  }

  blocks <- blocks |> rename(date = start_date)

  sessions_week <- filter_this_week(sessions, config$tz)
  sessions_day <- sessions_week |> filter(date == today(tzone = config$tz))
  health_wide <- health_long_to_wide(health)

  write_export(
    sessions_day,
    file.path(
      config$exports_dir,
      paste0(today(tzone = config$tz), "_training_sessions_daily.csv")
    )
  )

  if (is.null(health_wide)) {
    log_warn("Skipping health export due to unexpected health data structure.")
  } else {
    write_export(
      health_wide,
      file.path(
        config$exports_dir,
        paste0(today(tzone = config$tz), "_health_daily.csv")
      )
    )
  }

  if (today(tzone = config$tz) == floor_date(today(tzone = config$tz), "week", week_start = 1) + days(6)) {
    process_weight_week(file.path(config$data_dir, "Weight.csv"), config$exports_dir, config$tz)
  }

  schedule_csv <- list.files(
    config$tmp_dir,
    pattern = "(?i)^schedule[[:space:]_-]?.*\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )

  if (length(schedule_csv) == 0) {
    log_info("No schedule.csv found in zip; skipping schedule export.")
  } else {
    if (length(schedule_csv) > 1) {
      log_warn("Multiple schedule.csv files found; using most recent by modified time.")
      schedule_csv <- latest_file(schedule_csv, config$tz)
    }
    schedule <- read_csv_clean(schedule_csv)
    if (is.null(schedule)) {
      log_warn("Failed to load schedule.csv; skipping.")
    } else {
      date_cols <- names(schedule)[stringr::str_detect(names(schedule), "date")]
      if (length(date_cols) == 0) {
        log_warn("schedule.csv has no date column; skipping.")
      } else {
        date_col <- date_cols[[1]]
        schedule <- schedule |>
          mutate(
            schedule_datetime = parse_date_flex(.data[[date_col]], config$tz),
            schedule_date = as_date(schedule_datetime, tz = config$tz)
          )

        schedule_today <- schedule |> filter(schedule_date == today(tzone = config$tz))

        write_export(
          schedule_today,
          file.path(
            config$exports_dir,
            paste0(today(tzone = config$tz), "_schedule.csv")
          )
        )
      }
    }
  }

  if (dir.exists(config$tmp_dir)) {
    unlink(config$tmp_dir, recursive = TRUE, force = TRUE)
  }
  ensure_dir(config$tmp_dir, clean = TRUE)
  if (config$cleanup_old_zips) {
    clean_old_zips(config$data_dir, data_newest_zip)
  }

  invisible(NULL)
}

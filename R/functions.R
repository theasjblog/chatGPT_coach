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

read_csv_clean <- function(path, col_types = NULL) {
  tryCatch(
    {
      read_args <- list(file = path, show_col_types = FALSE)
      if (!is.null(col_types)) {
        read_args$col_types <- col_types
      }
      do.call(readr::read_csv, read_args) |>
        janitor::clean_names()
    },
    error = function(e) {
      log_warn("Failed to read", path, ":", e$message)
      NULL
    }
  )
}

seconds_from_time_like_scalar <- function(x) {
  if (inherits(x, "difftime") || inherits(x, "hms")) {
    # interpret these via their printed form so hh:mm:ss values are treated as mm:ss
    return(seconds_from_time_like_scalar(as.character(x)))
  }
  if (is.numeric(x)) {
    return(as.numeric(x))
  }

  if (is.character(x)) {
    x <- stringr::str_trim(x)
    if (identical(x, "") || is.na(x)) return(NA_real_)

    parts <- stringr::str_split(x, ":", simplify = TRUE)
    parts <- parts[parts != ""]
    nums <- suppressWarnings(as.numeric(parts))
    if (any(is.na(nums))) return(NA_real_)

    if (length(nums) == 3) {
      # interpret as hh:mm:ss (common difftime formatting)
      return(nums[1] * 3600 + nums[2] * 60 + nums[3])
    }
    if (length(nums) == 2) {
      return(nums[1] * 60 + nums[2])
    }
    if (length(nums) == 1) {
      return(nums[1])
    }
  }

  NA_real_
}

format_mm_ss <- function(secs) {
  if (is.na(secs)) return(NA_character_)
  if (!is.finite(secs)) return(NA_character_)

  mins <- floor(secs / 60)
  rem <- secs - mins * 60

  # handle rounding edge where rem rounds to 60
  if (round(rem, 1) >= 60) {
    mins <- mins + 1
    rem <- 0
  }

  rem_int <- floor(rem)
  rem_frac <- round(rem - rem_int, 1)
  if (abs(rem_frac) < 1e-9) {
    sprintf("%d:%02d", mins, as.integer(rem_int))
  } else {
    sprintf("%d:%02d%s", mins, as.integer(rem_int), substring(sprintf("%.1f", rem_frac), 2))
  }
}

normalize_time_like <- function(x) {
  secs <- vapply(x, seconds_from_time_like_scalar, numeric(1))
  vapply(secs, format_mm_ss, character(1), USE.NAMES = FALSE)
}

drop_run_summary_rows <- function(df) {
  if (!"laps" %in% names(df)) return(df)

  df |>
    mutate(laps = as.character(laps)) |>
    filter(
      !is.na(laps),
      !laps %in% c("Summary", "Lap Summary"),
      stringr::str_detect(laps, "^\\d+(\\.\\d+)?$")
    )
}

load_run_activity <- function(path, columns) {
  raw <- read_csv_clean(
    path,
    col_types = readr::cols(.default = readr::col_character())
  )
  if (is.null(raw)) return(NULL)

  # handle legacy shape that already matches expected names
  if ("distance_km" %in% names(raw)) {
    time_cols <- intersect(c("time", "avg_pace_min_km", "avg_gap_min_km"), names(raw))
    legacy <- raw |>
      select(any_of(columns)) |>
      drop_run_summary_rows()

    time_cols_apply <- intersect(time_cols, names(legacy))
    legacy <- legacy |>
      mutate(across(all_of(time_cols_apply), normalize_time_like))

    if (nrow(legacy) == 0) {
      log_warn("Run CSV parsed but no lap rows detected.")
      return(NULL)
    }
    return(legacy)
  }

  if (!"step_type" %in% names(raw)) {
    log_warn("Run CSV missing step_type column; cannot parse.")
    return(NULL)
  }

  norm_power_col <- intersect(
    c("normalized_power_np", "normalized_power_r_np_r"),
    names(raw)
  )
  if (length(norm_power_col) == 0) {
    log_warn("Run CSV missing normalized power column; cannot parse.")
    return(NULL)
  }

  time_cols <- intersect(c("time", "avg_pace_min_km", "avg_gap_min_km"), names(raw))

  # drop empty placeholder column if present
  raw <- raw |>
    select(-any_of(c("x1", ""))) |>
    mutate(normalized_power = .data[[norm_power_col[[1]]]]) |>
    mutate(across(everything(), ~ na_if(as.character(.x), "")))

  run_aligned <- raw |>
    mutate(
      lap_num = dplyr::coalesce(
        if_else(stringr::str_detect(lap, "^\\d+$"), lap, NA_character_),
        if_else(stringr::str_detect(step_type, "^\\d+$"), step_type, NA_character_)
      ),
      shifted = stringr::str_detect(step_type, "^\\d+$") &
        !stringr::str_detect(lap, "^\\d+$")
    ) |>
    filter(!is.na(lap_num)) |>
    mutate(
      laps = lap_num,
      time = if_else(shifted, lap, time),
      distance_km = if_else(shifted, cumulative_time, distance),
      avg_pace_min_km = if_else(shifted, distance, avg_pace),
      avg_gap_min_km = if_else(shifted, avg_pace, avg_gap),
      avg_hr_bpm = if_else(shifted, avg_gap, avg_hr),
      max_hr_bpm = if_else(shifted, avg_hr, max_hr),
      total_ascent_m = if_else(shifted, max_hr, total_ascent),
      avg_power_w = if_else(shifted, normalized_power, avg_power),
      max_power_w = if_else(shifted, avg_w_kg, max_power),
      avg_run_cadence_spm = if_else(shifted, total_descent, avg_run_cadence),
      avg_ground_contact_time_ms = if_else(shifted, avg_run_cadence, avg_ground_contact_time),
      avg_stride_length_m = if_else(shifted, avg_ground_contact_time, avg_stride_length),
      avg_vertical_oscillation_cm = if_else(shifted, avg_stride_length, avg_vertical_oscillation),
      avg_vertical_ratio_percent = if_else(shifted, avg_vertical_oscillation, avg_vertical_ratio)
    ) |>
    drop_run_summary_rows() |>
    select(any_of(columns))

  time_cols_apply <- intersect(time_cols, names(run_aligned))
  run_aligned <- run_aligned |>
    mutate(across(all_of(time_cols_apply), normalize_time_like))

  if (nrow(run_aligned) == 0) {
    log_warn("Run CSV parsed but no lap rows detected.")
    return(NULL)
  }

  run_aligned
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

  sleep_df <- read_csv_clean(
    path,
    col_types = readr::cols(.default = readr::col_character())
  )

  if (is.null(sleep_df) || nrow(sleep_df) == 0) {
    log_warn("Sleep.csv is empty after cleaning.")
    return(invisible(FALSE))
  }

  sleep_df <- sleep_df |>
    mutate(across(everything(), ~ na_if(str_trim(.x), "--")))

  write_export(
    sleep_df,
    file.path(exports_dir, paste0(today(tzone = tz), "_sleep_trend.csv"))
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
  df <- if (identical(name, "run")) {
    load_run_activity(path, columns)
  } else {
    read_csv_clean(path)
  }
  if (is.null(df)) return(invisible(FALSE))

  if (!identical(name, "run")) {
    df <- df |> select(any_of(columns))
  }

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

  weight_week <- weight_df

  if (nrow(weight_week) == 0) {
    log_warn("No weight entries found in Weight.csv.")
    return(invisible(FALSE))
  }

  write_export(
    weight_week,
    file.path(
      exports_dir,
      paste0(today(tzone = tz), "_weight_trend.csv")
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

  blocks <- blocks |>
    select(-any_of("date")) |>
    rename(date = start_date)

  sessions_week <- filter_this_week(sessions, config$tz)
  sessions_day <- sessions_week |> filter(date == today(tzone = config$tz))
  health_wide <- health_long_to_wide(health)

  write_export(
    sessions_day,
    file.path(
      config$exports_dir,
      paste0(today(tzone = config$tz), "_training_sessions_planned.csv")
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

  process_weight_week(file.path(config$data_dir, "Weight.csv"), config$exports_dir, config$tz)

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
        schedule_today <- schedule_today |> select(-schedule_datetime, -schedule_date)

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

log_info <- function(...) message("[notion_exports] ", paste(..., collapse = " "))
log_warn <- function(...) warning(paste0("[notion_exports] ", paste(..., collapse = " ")), call. = FALSE)

`%||%` <- function(x, y) if (!is.null(x)) x else y

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

safe_mean <- function(x) {
  m <- mean(x, na.rm = TRUE)
  if (is.nan(m)) NA_real_ else m
}

safe_max <- function(x) {
  m <- suppressWarnings(max(x, na.rm = TRUE))
  if (!is.finite(m)) NA_real_ else m
}

safe_weighted_mean <- function(x, w) {
  if (length(x) == 0 || length(w) == 0) return(NA_real_)
  if (length(x) != length(w)) return(NA_real_)
  mask <- !is.na(x) & !is.na(w) & w > 0
  if (!any(mask)) return(NA_real_)
  sum(x[mask] * w[mask], na.rm = TRUE) / sum(w[mask], na.rm = TRUE)
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

  lap_pattern <- "^\\d+(?:\\.\\d+)?(?:\\s*-\\s*\\d+(?:\\.\\d+)?)?$"

  df |>
    mutate(laps = as.character(laps)) |>
    filter(
      !is.na(laps),
      !laps %in% c("Summary", "Lap Summary"),
      stringr::str_detect(laps, lap_pattern)
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
  lap_pattern <- "^\\d+(?:\\.\\d+)?(?:\\s*-\\s*\\d+(?:\\.\\d+)?)?$"

  # drop empty placeholder column if present
  raw <- raw |>
    select(-any_of(c("x1", ""))) |>
    mutate(normalized_power = .data[[norm_power_col[[1]]]]) |>
    mutate(across(everything(), ~ na_if(as.character(.x), "")))

  run_aligned <- raw |>
    mutate(
      lap_num = dplyr::coalesce(
        if_else(stringr::str_detect(lap, lap_pattern), lap, NA_character_),
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

summarise_run_interval <- function(samples, start_sec, stop_sec) {
  interval_samples <- samples |>
    filter(secs >= start_sec, secs <= stop_sec)

  if (nrow(interval_samples) < 2) return(NULL)

  interval_samples <- interval_samples |>
    arrange(secs) |>
    mutate(
      lead_secs = lead(secs),
      lead_km = lead(km),
      delta_secs = pmax(lead_secs - secs, 0),
      delta_km = lead_km - km
    )

  moving_rows <- interval_samples$delta_secs > 0 & interval_samples$delta_km > 0

  if (!any(moving_rows)) return(NULL)

  moving_weights <- ifelse(moving_rows, interval_samples$delta_secs, 0)

  moving_time <- sum(interval_samples$delta_secs[moving_rows], na.rm = TRUE)
  moving_distance <- sum(interval_samples$delta_km[moving_rows], na.rm = TRUE)
  if (!is.finite(moving_distance) || moving_distance <= 0) return(NULL)

  pace_sec_per_km <- moving_time / moving_distance

  cadence_spm <- interval_samples$rcad * 2
  speed_mps <- ifelse(interval_samples$delta_secs > 0,
    (interval_samples$delta_km * 1000) / interval_samples$delta_secs,
    NA_real_
  )
  stride_length <- ifelse(cadence_spm > 0, speed_mps / (cadence_spm / 60), NA_real_)
  vertical_ratio <- ifelse(stride_length > 0, (interval_samples$rvert / 100) / stride_length * 100, NA_real_)

  alt_diff <- diff(interval_samples$alt)
  moving_steps <- moving_rows[-length(moving_rows)]
  total_ascent <- sum(pmax(alt_diff[moving_steps], 0), na.rm = TRUE)

  tibble(
    time = format_mm_ss(moving_time),
    distance_km = round(moving_distance, 2),
    avg_pace_min_km = format_mm_ss(pace_sec_per_km),
    avg_gap_min_km = format_mm_ss(pace_sec_per_km),
    avg_hr_bpm = round(safe_weighted_mean(interval_samples$hr, moving_weights)),
    max_hr_bpm = safe_max(interval_samples$hr[moving_rows, drop = TRUE]),
    total_ascent_m = round(total_ascent, 1),
    avg_power_w = round(safe_weighted_mean(interval_samples$watts, moving_weights)),
    max_power_w = safe_max(interval_samples$watts[moving_rows, drop = TRUE]),
    avg_run_cadence_spm = round(safe_weighted_mean(cadence_spm, moving_weights), 1),
    avg_ground_contact_time_ms = round(safe_weighted_mean(interval_samples$rcon, moving_weights), 1),
    avg_stride_length_m = round(safe_weighted_mean(stride_length, moving_weights), 3),
    avg_vertical_oscillation_cm = round(safe_weighted_mean(interval_samples$rvert, moving_weights), 2),
    avg_vertical_ratio_percent = round(safe_weighted_mean(vertical_ratio, moving_weights), 2)
  )
}

compute_normalized_power <- function(power_vec, window = 30) {
  power_vec <- power_vec[is.finite(power_vec)]
  if (length(power_vec) < window) return(NA_real_)

  cums <- c(0, cumsum(power_vec))
  rolling_means <- (cums[(window + 1):length(cums)] - cums[seq_len(length(cums) - window)]) / window
  (mean(rolling_means^4))^(1 / 4)
}

compute_arc_length <- function(start, end) {
  if (length(start) == 0 || length(end) == 0) return(numeric(0))
  (end - start) %% 360
}

summarise_bike_interval <- function(samples, start_sec, stop_sec) {
  interval_samples <- samples |>
    filter(secs >= start_sec, secs <= stop_sec) |>
    arrange(secs) |>
    mutate(
      lead_secs = lead(secs),
      delta_secs = lead_secs - secs
    )

  interval_samples <- interval_samples |>
    mutate(delta_secs = ifelse(is.na(delta_secs) | delta_secs < 0, 0, delta_secs))

  if (nrow(interval_samples) < 2) return(NULL)

  moving_rows <- interval_samples$delta_secs > 0 &
    coalesce(interval_samples$watts, 0) > 0 &
    coalesce(interval_samples$kph, 0) > 0
  moving_rows[is.na(moving_rows)] <- FALSE

  if (!any(moving_rows, na.rm = TRUE)) return(NULL)

  moving_weights <- ifelse(moving_rows, interval_samples$delta_secs, 0)

  moving_time <- sum(interval_samples$delta_secs[moving_rows], na.rm = TRUE)
  if (!is.finite(moving_time) || moving_time <= 0) return(NULL)

  moving_powers <- interval_samples$watts[moving_rows]
  moving_times <- interval_samples$delta_secs[moving_rows]
  valid_np <- is.finite(moving_powers) & is.finite(moving_times) & moving_times > 0
  moving_power_rep <- if (any(valid_np)) {
    rep(
      moving_powers[valid_np],
      times = pmax(1L, as.integer(round(moving_times[valid_np])))
    )
  } else {
    numeric(0)
  }

  avg_power <- round(safe_weighted_mean(interval_samples$watts, moving_weights))
  max_power <- safe_max(interval_samples$watts[moving_rows, drop = TRUE])
  norm_power <- round(compute_normalized_power(moving_power_rep))

  avg_hr <- round(safe_weighted_mean(interval_samples$hr, moving_weights))
  max_hr <- safe_max(interval_samples$hr[moving_rows, drop = TRUE])
  avg_cadence <- round(safe_weighted_mean(interval_samples$cad, moving_weights), 1)

  left_balance <- round(safe_weighted_mean(interval_samples$lrbalance, moving_weights), 1)
  right_balance <- ifelse(is.na(left_balance), NA_real_, round(100 - left_balance, 1))

  pp_start_l <- safe_weighted_mean(interval_samples$lppb, moving_weights)
  pp_end_l <- safe_weighted_mean(interval_samples$lppe, moving_weights)
  pp_start_r <- safe_weighted_mean(interval_samples$rppb, moving_weights)
  pp_end_r <- safe_weighted_mean(interval_samples$rppe, moving_weights)
  pp_arc_l <- safe_weighted_mean(compute_arc_length(interval_samples$lppb, interval_samples$lppe), moving_weights)
  pp_arc_r <- safe_weighted_mean(compute_arc_length(interval_samples$rppb, interval_samples$rppe), moving_weights)

  peak_start_l <- safe_weighted_mean(interval_samples$lpppb, moving_weights)
  peak_end_l <- safe_weighted_mean(interval_samples$lpppe, moving_weights)
  peak_start_r <- safe_weighted_mean(interval_samples$rpppb, moving_weights)
  peak_end_r <- safe_weighted_mean(interval_samples$rpppe, moving_weights)
  peak_arc_l <- safe_weighted_mean(compute_arc_length(interval_samples$lpppb, interval_samples$lpppe), moving_weights)
  peak_arc_r <- safe_weighted_mean(compute_arc_length(interval_samples$rpppb, interval_samples$rpppe), moving_weights)

  tibble(
    time = format_mm_ss(moving_time),
    avg_hr = avg_hr,
    max_hr = max_hr,
    avg_bike_cadence = avg_cadence,
    normalized_power_r_np_r = norm_power,
    balance_left_percent = left_balance,
    balance_right_percent = right_balance,
    torque_effectiveness_left_percent = NA_real_,
    torque_effectiveness_right_percent = NA_real_,
    pedal_smoothness_left_percent = NA_real_,
    pedal_smoothness_right_percent = NA_real_,
    avg_power = avg_power,
    max_power = max_power,
    l_power_phase_start_angle = round(pp_start_l, 1),
    l_power_phase_end_angle = round(pp_end_l, 1),
    r_power_phase_start_angle = round(pp_start_r, 1),
    r_power_phase_end_angle = round(pp_end_r, 1),
    l_power_phase_arc_length = round(pp_arc_l, 1),
    r_power_phase_arc_length = round(pp_arc_r, 1),
    l_peak_power_phase_start_angle = round(peak_start_l, 1),
    l_peak_power_phase_end_angle = round(peak_end_l, 1),
    r_peak_power_phase_start_angle = round(peak_start_r, 1),
    r_peak_power_phase_end_angle = round(peak_end_r, 1),
    l_peak_power_phase_arc_length = round(peak_arc_l, 1),
    r_peak_power_phase_arc_length = round(peak_arc_r, 1)
  )
}

summarise_swim_pool_interval <- function(samples, start_sec, stop_sec, pool_length_m = 25) {
  interval_samples <- samples |>
    filter(secs >= start_sec, secs <= stop_sec) |>
    arrange(secs) |>
    mutate(
      lead_secs = lead(secs),
      lead_km = lead(km),
      delta_secs = lead_secs - secs,
      delta_km = lead_km - km
    )

  interval_samples <- interval_samples |>
    mutate(
      delta_secs = ifelse(is.na(delta_secs) | delta_secs < 0, 0, delta_secs),
      delta_km = ifelse(is.na(delta_km) | delta_km < 0, 0, delta_km)
    )

  if (nrow(interval_samples) < 2) return(NULL)

  interval_duration <- if (is.finite(start_sec) && is.finite(stop_sec)) {
    max(stop_sec - start_sec, 0)
  } else {
    interval_samples$secs[nrow(interval_samples)] - interval_samples$secs[1]
  }

  moving_rows <- interval_samples$delta_secs > 0 & interval_samples$delta_km > 0
  moving_rows[is.na(moving_rows)] <- FALSE

  if (!any(moving_rows)) {
    avg_hr_rest <- round(safe_weighted_mean(interval_samples$hr, rep(1, nrow(interval_samples))))
    max_hr_rest <- safe_max(interval_samples$hr)

    return(tibble(
      time = format_mm_ss(interval_duration),
      lengths = 0,
      distance = 0,
      avg_pace = NA_character_,
      best_pace = NA_character_,
      avg_swolf = NA_real_,
      avg_hr = avg_hr_rest,
      max_hr = max_hr_rest,
      total_strokes = 0,
      avg_strokes = 0,
      swim_stroke = NA_character_,
      interval_type = "rest"
    ))
  }

  moving_time <- sum(interval_samples$delta_secs[moving_rows], na.rm = TRUE)
  distance_m <- sum(interval_samples$delta_km[moving_rows], na.rm = TRUE) * 1000
  if (!is.finite(distance_m) || distance_m <= 0) {
    return(tibble(
      time = format_mm_ss(interval_duration),
      lengths = 0,
      distance = 0,
      avg_pace = NA_character_,
      best_pace = NA_character_,
      avg_swolf = NA_real_,
      avg_hr = round(safe_weighted_mean(interval_samples$hr, rep(1, nrow(interval_samples)))),
      max_hr = safe_max(interval_samples$hr),
      total_strokes = 0,
      avg_strokes = 0,
      swim_stroke = NA_character_,
      interval_type = "rest"
    ))
  }

  lengths <- distance_m / pool_length_m
  pace_sec_per_100m <- moving_time / distance_m * 100

  pace_per_100m_samples <- interval_samples$delta_secs[moving_rows] /
    (interval_samples$delta_km[moving_rows] * 1000) * 100

  best_pace <- min(pace_per_100m_samples[is.finite(pace_per_100m_samples)], na.rm = TRUE)
  if (!is.finite(best_pace)) best_pace <- NA_real_

  weights <- ifelse(moving_rows, interval_samples$delta_secs, 0)
  avg_hr <- round(safe_weighted_mean(interval_samples$hr, weights))
  max_hr <- safe_max(interval_samples$hr[moving_rows, drop = TRUE])

  stroke_rate <- interval_samples$cad
  total_strokes <- sum(stroke_rate[moving_rows] * interval_samples$delta_secs[moving_rows] / 60, na.rm = TRUE)
  avg_strokes_per_length <- ifelse(lengths > 0, total_strokes / lengths, NA_real_)
  avg_swolf <- ifelse(lengths > 0, moving_time / lengths + avg_strokes_per_length, NA_real_)

  interval_type <- if (distance_m > 0 && (!is.finite(total_strokes) || total_strokes <= 0)) {
    "drill"
  } else {
    "freestyle"
  }

  tibble(
    time = format_mm_ss(moving_time),
    lengths = round(lengths, 1),
    distance = round(distance_m, 1),
    avg_pace = format_mm_ss(pace_sec_per_100m),
    best_pace = format_mm_ss(best_pace),
    avg_swolf = round(avg_swolf, 1),
    avg_hr = avg_hr,
    max_hr = max_hr,
    total_strokes = round(total_strokes, 1),
    avg_strokes = round(avg_strokes_per_length, 1),
    swim_stroke = NA_character_,
    interval_type = interval_type
  )
}

load_run_activity_json <- function(paths, columns) {
  if (length(paths) == 0) return(NULL)

  ordered <- paths[order(file.info(paths)$mtime, decreasing = TRUE)]

  for (path in ordered) {
    json_obj <- tryCatch(
      jsonlite::fromJSON(readr::read_file(path)),
      error = function(e) {
        log_warn("Failed to parse run JSON", basename(path), ":", e$message)
        NULL
      }
    )
    if (is.null(json_obj)) next

    ride <- json_obj$RIDE %||% json_obj$ride
    if (is.null(ride)) next

    sport <- ride$TAGS$Sport %||% ride$tags$sport
    sport <- stringr::str_to_lower(stringr::str_trim(sport %||% ""))
    if (!identical(sport, "run")) next

    intervals_raw <- ride$INTERVALS %||% ride$intervals
    samples_raw <- ride$SAMPLES %||% ride$samples

    if (is.null(intervals_raw) || is.null(samples_raw)) {
      log_warn("Run JSON missing INTERVALS or SAMPLES; skipping", basename(path))
      next
    }

    intervals <- as_tibble(intervals_raw) |> janitor::clean_names()
    samples <- as_tibble(samples_raw) |> janitor::clean_names()

    if (!all(c("secs", "km") %in% names(samples))) {
      log_warn("Run JSON missing expected sample fields; skipping", basename(path))
      next
    }

    # ensure numeric where possible
    num_cols <- intersect(
      c("secs", "km", "watts", "kph", "hr", "alt", "rcad", "rvert", "rcon"),
      names(samples)
    )
    samples <- samples |> mutate(across(all_of(num_cols), as.numeric))

    summaries <- lapply(seq_len(nrow(intervals)), function(i) {
      int <- intervals[i, ]
      start_sec <- int$start
      stop_sec <- int$stop
      res <- summarise_run_interval(samples, start_sec, stop_sec)
      if (is.null(res)) return(NULL)
      res$laps <- as.character(i)
      res
    })

    summaries <- bind_rows(summaries)
    if (nrow(summaries) == 0) {
      log_warn("Run JSON parsed but no interval summaries created:", basename(path))
      next
    }

    # ensure column order and names align with expectations
    summaries <- summaries |> select(any_of(columns))

    return(summaries)
  }

  log_warn("No Run sport JSON files parsed successfully.")
  NULL
}

load_bike_indoor_activity_json <- function(paths, columns) {
  if (length(paths) == 0) return(NULL)

  ordered <- paths[order(file.info(paths)$mtime, decreasing = TRUE)]

  for (path in ordered) {
    json_obj <- tryCatch(
      jsonlite::fromJSON(readr::read_file(path)),
      error = function(e) {
        log_warn("Failed to parse bike JSON", basename(path), ":", e$message)
        NULL
      }
    )
    if (is.null(json_obj)) next

    ride <- json_obj$RIDE %||% json_obj$ride
    if (is.null(ride)) next

    tags <- ride$TAGS %||% ride$tags
    sport <- tags$Sport %||% tags$sport
    subsport <- tags$SubSport %||% tags$subSport %||% tags$subsport

    sport <- stringr::str_to_lower(stringr::str_trim(sport %||% ""))
    subsport <- stringr::str_to_lower(stringr::str_trim(subsport %||% ""))

    if (!(identical(sport, "bike") && identical(subsport, "turbo_trainer"))) next

    intervals_raw <- ride$INTERVALS %||% ride$intervals
    samples_raw <- ride$SAMPLES %||% ride$samples

    if (is.null(intervals_raw) || is.null(samples_raw)) {
      log_warn("Bike indoor JSON missing INTERVALS or SAMPLES; skipping", basename(path))
      next
    }

    intervals <- as_tibble(intervals_raw) |> janitor::clean_names()
    samples <- as_tibble(samples_raw) |> janitor::clean_names()

    if (!"secs" %in% names(samples) || !"watts" %in% names(samples)) {
      log_warn("Bike indoor JSON missing expected sample fields; skipping", basename(path))
      next
    }

    num_cols <- intersect(
      c(
        "secs", "km", "watts", "cad", "kph", "hr", "alt", "slope", "temp",
        "lrbalance", "lppb", "rppb", "lppe", "rppe",
        "lpppb", "rpppb", "lpppe", "rpppe"
      ),
      names(samples)
    )
    samples <- samples |> mutate(across(all_of(num_cols), as.numeric))

    summaries <- lapply(seq_len(nrow(intervals)), function(i) {
      int <- intervals[i, ]
      start_sec <- int$start
      stop_sec <- int$stop
      res <- summarise_bike_interval(samples, start_sec, stop_sec)
      if (is.null(res)) return(NULL)
      res$laps <- stringr::str_trim(int$name %||% as.character(i))
      res
    })

    summaries <- bind_rows(summaries)
    if (nrow(summaries) == 0) {
      log_warn("Bike indoor JSON parsed but no interval summaries created:", basename(path))
      next
    }

    summaries <- summaries |> select(any_of(columns))
    return(summaries)
  }

  log_warn("No Bike turbo_trainer JSON files parsed successfully.")
  NULL
}

load_swim_pool_activity_json <- function(paths, columns) {
  if (length(paths) == 0) return(NULL)

  ordered <- paths[order(file.info(paths)$mtime, decreasing = TRUE)]

  for (path in ordered) {
    json_obj <- tryCatch(
      jsonlite::fromJSON(readr::read_file(path)),
      error = function(e) {
        log_warn("Failed to parse swim JSON", basename(path), ":", e$message)
        NULL
      }
    )
    if (is.null(json_obj)) next

    ride <- json_obj$RIDE %||% json_obj$ride
    if (is.null(ride)) next

    tags <- ride$TAGS %||% ride$tags
    sport <- tags$Sport %||% tags$sport
    subsport <- tags$SubSport %||% tags$subSport %||% tags$subsport

    sport <- stringr::str_to_lower(stringr::str_trim(sport %||% ""))
    subsport <- stringr::str_to_lower(stringr::str_trim(subsport %||% ""))

    if (!(identical(sport, "swim") && identical(subsport, "lap swimming"))) next

    pool_len <- tags$`Pool Length` %||% tags$pool_length %||% tags$poolLength
    pool_len <- suppressWarnings(as.numeric(stringr::str_trim(pool_len %||% "25")))
    if (!is.finite(pool_len) || pool_len <= 0) pool_len <- 25

    intervals_raw <- ride$INTERVALS %||% ride$intervals
    samples_raw <- ride$SAMPLES %||% ride$samples

    if (is.null(intervals_raw) || is.null(samples_raw)) {
      log_warn("Swim pool JSON missing INTERVALS or SAMPLES; skipping", basename(path))
      next
    }

    intervals <- as_tibble(intervals_raw) |> janitor::clean_names()
    samples <- as_tibble(samples_raw) |> janitor::clean_names()

    if (!all(c("secs", "km") %in% names(samples))) {
      log_warn("Swim pool JSON missing expected sample fields; skipping", basename(path))
      next
    }

    num_cols <- intersect(c("secs", "km", "cad", "kph", "hr"), names(samples))
    samples <- samples |> mutate(across(all_of(num_cols), as.numeric))

    summaries <- lapply(seq_len(nrow(intervals)), function(i) {
      int <- intervals[i, ]
      res <- summarise_swim_pool_interval(samples, int$start, int$stop, pool_len)
      if (is.null(res)) return(NULL)
      res$intervals <- stringr::str_trim(int$name %||% as.character(i))
      res
    })

    summaries <- bind_rows(summaries)
    if (nrow(summaries) == 0) {
      log_warn("Swim pool JSON parsed but no interval summaries created:", basename(path))
      next
    }

    summaries <- summaries |> select(any_of(columns))
    return(summaries)
  }

  log_warn("No Swim lap swimming JSON files parsed successfully.")
  NULL
}

select_expected_csvs <- function(csvs) {
  patterns <- c(
    health = "(?i)health",
    sessions = "(?i)daily plan|daily",
    blocks = "(?i)schedule|weekly"
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
  df <- NULL

  if (identical(name, "run")) {
    json_paths <- list.files(config$data_dir, pattern = "\\.json$", full.names = TRUE)
    if (length(json_paths) == 0) {
      log_info("Run JSON not provided; skipping export.")
      return(invisible(FALSE))
    }
    df <- load_run_activity_json(json_paths, columns)
    if (is.null(df)) {
      log_warn("Run JSON found but parsing failed; skipping run export.")
      return(invisible(FALSE))
    }
  } else if (identical(name, "bike_indoor")) {
    json_paths <- list.files(config$data_dir, pattern = "\\.json$", full.names = TRUE)
    if (length(json_paths) == 0) {
      log_info("Bike indoor JSON not provided; skipping export.")
      return(invisible(FALSE))
    }
    df <- load_bike_indoor_activity_json(json_paths, columns)
    if (is.null(df)) {
      log_warn("Bike indoor JSON found but parsing failed; skipping bike_indoor export.")
      return(invisible(FALSE))
    }
  } else if (identical(name, "swim_pool")) {
    json_paths <- list.files(config$data_dir, pattern = "\\.json$", full.names = TRUE)
    if (length(json_paths) == 0) {
      log_info("Swim pool JSON not provided; skipping export.")
      return(invisible(FALSE))
    }
    df <- load_swim_pool_activity_json(json_paths, columns)
    if (is.null(df)) {
      log_warn("Swim pool JSON found but parsing failed; skipping swim_pool export.")
      return(invisible(FALSE))
    }
  } else {
    files <- list.files(config$data_dir, pattern = "\\.csv$", full.names = TRUE)
    files <- files[stringr::str_detect(basename(files), pattern)]

    if (length(files) == 0) {
      log_info("No", name, "CSV provided; skipping export.")
      return(invisible(FALSE))
    }
    if (length(files) > 1) {
      log_warn("Multiple", name, "CSVs found; using the most recent by modified time.")
    }

    path <- latest_file(files, config$tz)
    df <- read_csv_clean(path)
    if (is.null(df)) return(invisible(FALSE))
  }

  if (!identical(name, "run")) {
    df <- df |> select(any_of(columns))
  }

  if ("laps" %in% names(df)) {
    df <- df |> filter(laps != "Summary")
  }
  if ("swim_stroke" %in% names(df)) {
    df <- df |> filter(is.na(swim_stroke) | swim_stroke != "--")
  }
  df <- df |> select(where(~ !all(is.na(.x))))

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
    log_warn("Missing expected _all.csv files (health, daily plan, schedule); skipping exports.")
    return(invisible(NULL))
  }

  health <- read_csv_clean(selected_csvs$health)
  sessions <- read_csv_clean(selected_csvs$sessions)
  blocks <- read_csv_clean(selected_csvs$blocks)

  if (any(vapply(list(health, sessions, blocks), is.null, logical(1)))) {
    log_warn("One or more data files failed to load; skipping exports.")
    return(invisible(NULL))
  }

  if (!any(c("start_date", "date") %in% names(blocks))) {
    log_warn("Blocks (schedule) file missing a date column; skipping blocks parsing.")
    blocks <- tibble()
  } else {
    blocks <- blocks |>
      rename_with(~ "date", any_of(c("start_date", "date"))) |>
      mutate(date = parse_date_flex(date, config$tz)) |>
      filter(!is.na(date))
  }

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

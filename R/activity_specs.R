activity_specs <- list(
  run = list(
    pattern = "run",
    columns = c(
      "laps", "time", "distance_km", "avg_pace_min_km",
      "avg_gap_min_km", "avg_hr_bpm", "max_hr_bpm", "total_ascent_m",
      "avg_power_w", "max_power_w", "avg_run_cadence_spm",
      "avg_ground_contact_time_ms",
      "avg_stride_length_m", "avg_vertical_oscillation_cm",
      "avg_vertical_ratio_percent"
    )
  ),
  bike_indoor = list(
    pattern = "bike_indoor",
    columns = c(
      "laps", "time", "avg_hr",
      "max_hr", "avg_bike_cadence",
      "normalized_power_r_np_r",
      "balance_left_percent",
      "balance_right_percent",
      "torque_effectiveness_left_percent",
      "torque_effectiveness_right_percent",
      "pedal_smoothness_left_percent",
      "pedal_smoothness_right_percent",
      "avg_power", "max_power",
      "l_power_phase_start_angle",
      "l_power_phase_end_angle",
      "r_power_phase_start_angle",
      "r_power_phase_end_angle",
      "l_power_phase_arc_length",
      "r_power_phase_arc_length",
      "l_peak_power_phase_start_angle",
      "l_peak_power_phase_end_angle",
      "r_peak_power_phase_start_angle",
      "r_peak_power_phase_end_angle",
      "l_peak_power_phase_arc_length",
      "r_peak_power_phase_arc_length"
    )
  ),
  bike_outdoor = list(
    pattern = "bike_outdoor",
    columns = c(
      "laps", "time", "avg_hr",
      "max_hr", "avg_bike_cadence",
      "normalized_power_r_np_r",
      "balance_left_percent",
      "balance_right_percent",
      "torque_effectiveness_left_percent",
      "torque_effectiveness_right_percent",
      "pedal_smoothness_left_percent",
      "pedal_smoothness_right_percent",
      "avg_power", "max_power",
      "l_power_phase_start_angle",
      "l_power_phase_end_angle",
      "r_power_phase_start_angle",
      "r_power_phase_end_angle",
      "l_power_phase_arc_length",
      "r_power_phase_arc_length",
      "l_peak_power_phase_start_angle",
      "l_peak_power_phase_end_angle",
      "r_peak_power_phase_start_angle",
      "r_peak_power_phase_end_angle",
      "l_peak_power_phase_arc_length",
      "r_peak_power_phase_arc_length"
    )
  ),
  swim_pool = list(
    pattern = "swim_pool",
    columns = c(
      "intervals", "interval_type", "swim_stroke", "lengths",
      "distance", "time", "avg_pace",
      "best_pace",
      "avg_swolf", "avg_hr", "max_hr",
      "total_strokes", "avg_strokes"
    )
  ),
  swim_ow = list(
    pattern = "swim_ow",
    columns = c(
      "laps", "time", "distance_km", "avg_pace_min_100m", "max_pace_min_100m",
      "avg_hr_bpm", "swolf", "avg_swim_cadence_strokes_min"
    )
  )
)

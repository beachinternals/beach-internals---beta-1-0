"""
metric_functions.py
===================
Single source of truth for the metric execution namespace.

build_metric_namespace() is used by:
  - generate_player_metrics_json.py  (calculate_all_metrics)
  - calc_metric_correlations.py      (calc_metric_from_dict)

To add a new calculation function:
  1. Write it in server_functions.py
  2. Import it here
  3. Add it to the dict in build_metric_namespace()
  That's it — both modules pick it up automatically.
"""

import pandas as pd
import numpy as np

from server_functions import (
fbhe_obj,
calc_player_eso_obj,
calc_ev_obj,
calc_knock_out_obj,
count_oos_obj,
calc_trans_obj,
calc_error_density_obj,
find_ellipse_area,
count_good_passes_obj,
calc_att_height_metrics,
calc_dig_quality_obj,
)
from metric_calc_functions import (
consistency_sd_match,
consistency_sd_set2set,
calc_serve_pct_obj,
calc_angle_attacks_obj,
)


def build_metric_namespace(ppr_df, player_name):
  """
  Returns the execution namespace needed to run any metric
  function string from the metric dictionary via exec().

  Note: tri_df is intentionally excluded. The league PPR data
  is de-identified and tri_df / player_data are no longer used.

  Args:
      ppr_df      : Point-by-point DataFrame (already filtered if needed)
      player_name : Player UUID / display name string

  Returns:
      dict: namespace passed to exec() and eval()
  """
  return {
    'ppr_df'                 : ppr_df,
    'disp_player'            : player_name,
    'fbhe_obj'               : fbhe_obj,
    'calc_player_eso_obj'    : calc_player_eso_obj,
    'calc_ev_obj'            : calc_ev_obj,
    'calc_knock_out_obj'     : calc_knock_out_obj,
    'count_oos_obj'          : count_oos_obj,
    'calc_trans_obj'         : calc_trans_obj,
    'calc_error_density_obj' : calc_error_density_obj,
    'find_ellipse_area'      : find_ellipse_area,
    'consistency_sd_match'   : consistency_sd_match,
    'consistency_sd_set2set' : consistency_sd_set2set,
    'calc_serve_pct_obj'     : calc_serve_pct_obj,
    'calc_angle_attacks_obj' : calc_angle_attacks_obj,
    'count_good_passes_obj'  : count_good_passes_obj,
    'calc_att_height_metrics': calc_att_height_metrics,
    'calc_dig_quality_obj'   : calc_dig_quality_obj,
    'pd'                     : pd,
    'np'                     : np,
  }


def calc_metric_from_dict(metric_name, ppr_df, player_name, metric_dict_df):
  """
  Calculate a single metric for a single player using the metric dictionary.

  This is the universal single-source calculation used by the correlations
  module. It follows the exact same exec/eval path as calculate_all_metrics,
  so every metric is calculated identically in both places.

  Args:
      metric_name    : metric_id string to look up (e.g. 'goodpass', 'fbhe')
      ppr_df         : Full league PPR DataFrame (unfiltered)
      player_name    : Player UUID string
      metric_dict_df : metric dictionary as a pandas DataFrame

  Returns:
      float or None
  """
  row = metric_dict_df[metric_dict_df['metric_id'] == metric_name]
  if row.empty:
    print(f"  Metric not found in dictionary: {metric_name}")
    return None
  row = row.iloc[0]

  function_name = row['function_name']
  result_path   = row['result_path']
  data_filter   = row.get('data_filter', None)

  if pd.isna(function_name):
    print(f"  Metric {metric_name} has no function_name defined")
    return None

  try:
    # Apply data filter if present (same sanitization as calculate_all_metrics)
    if pd.notna(data_filter) and str(data_filter).strip():
      data_filter_clean = str(data_filter).replace('\u2018', "'").replace('\u2019', "'")
      data_filter_clean = data_filter_clean.replace('\u201c', '"').replace('\u201d', '"')
      filter_namespace  = {'ppr_df': ppr_df, 'disp_player': player_name}
      filtered_ppr      = eval(data_filter_clean, filter_namespace)
    else:
      filtered_ppr = ppr_df

    # Build namespace and execute the metric function string
    namespace = build_metric_namespace(filtered_ppr, player_name)
    exec(function_name, namespace)

    # Extract the value via result_path
    if pd.notna(result_path) and str(result_path).strip():
      value = eval(str(result_path), namespace)
    else:
      result_var_name = function_name.split('=')[0].strip()
      value = namespace[result_var_name]

    return float(value) if value is not None else None

  except Exception as e:
    print(f"  Error calculating {metric_name} for {player_name}: {e}")
    return None
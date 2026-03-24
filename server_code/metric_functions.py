# In metric_functions.py  
def build_metric_namespace(ppr_df, tri_df, player_name):
  """
  Returns the execution namespace needed to run any metric
  function string from the metric dictionary.
  Used by calculate_all_metrics AND calc_metric_correlations.
  """
  return {
    'ppr_df'                 : ppr_df,
    'tri_df'                 : tri_df,
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
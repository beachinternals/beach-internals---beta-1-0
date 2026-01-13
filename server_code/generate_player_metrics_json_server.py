"""
SERVER FUNCTION: generate_player_metrics_json()
===============================================
This function generates comprehensive metrics JSON for a player based on filters.
Add this to your Anvil server code module.
"""

import anvil.tables as tables
from anvil.tables import app_tables
import anvil.users
import anvil.server
import anvil.media
import pandas as pd
import numpy as np
import json
from datetime import datetime
import hashlib


@anvil.server.callable
def generate_player_metrics_json(league_value, team, **json_filters):
  """
    Generate comprehensive player metrics JSON file.
    
    Args:
        league_value (str): League|Gender|Year format (e.g., "AVP|W|2024")
        team (str): Team name
        **json_filters: Filter parameters including:
            - player (str): REQUIRED - Player name
            - player_shortname (str): Player short name for filename
            - comp_l1, comp_l2, comp_l3: Competition level filters
            - start_date, end_date: Date range filters
            - set, set_touch_type, pass_oos: Play-specific filters
            - att_ht_low, att_ht_high, att_speed_low, att_speed_high: Attack filters
            - set_ht_low, set_ht_high: Set filters
            - pass_ht_low, pass_ht_high: Pass filters
            - srv_fr, srv_to: Serve zone filters
    
    Returns:
        dict: {
            'media_obj': BlobMedia object for download,
            'filename': str,
            'summary': dict with metadata
        }
    """

  # Parse league value
  str_loc = league_value.index("|")
  league = league_value[: str_loc - 1].strip()
  league_value = league_value[str_loc + 1 :]
  str_loc = league_value.index("|")
  gender = league_value[: str_loc - 1].strip()
  year = int(league_value[str_loc + 1 :].strip())

  # Get player name (required)
  player_name = json_filters.get('player')
  if not player_name:
    raise ValueError("Player name is required")

  player_shortname = json_filters.get('player_shortname', 'player')

  # Load metric dictionary
  dict_rows = app_tables.metric_dictionary.search()
  metric_dict = pd.DataFrame([
    {col: row[col] for col in row.get_column_names()}
    for row in dict_rows
  ])

  # Get PPR data with filters
  ppr_df = get_filtered_ppr_data(league, gender, year, team, **json_filters)

  # Get triangle data (for set-to-set consistency)
  tri_df = get_filtered_triangle_data(league, gender, year, team, **json_filters)

  if len(ppr_df) == 0:
    raise ValueError("No data found for the specified filters")

    # Build metadata
  metadata = {
    'generated_at': datetime.now().isoformat(),
    'player_name': player_name,
    'player_shortname': player_shortname,
    'league': league,
    'gender': gender,
    'year': year,
    'team': team,
    'filters_applied': {k: str(v) for k, v in json_filters.items() if k not in ['player', 'player_shortname']},
    'total_points_analyzed': len(ppr_df),
    'total_sets_analyzed': len(tri_df) if len(tri_df) > 0 else 0,
    'dictionary_version': '1.0'
  }

  # Calculate all metrics
  metrics_result = calculate_all_metrics(metric_dict, ppr_df, tri_df, player_name)

  # Build final JSON structure
  json_output = {
    'metadata': metadata,
    'metrics': metrics_result['metrics'],
    'summary_stats': {
      'total_metrics_calculated': metrics_result['total_calculated'],
      'metrics_with_sufficient_data': metrics_result['successful'],
      'metrics_below_min_attempts': metrics_result['insufficient_data']
    }
  }

  # Generate filename
  filter_hash = generate_filter_hash(json_filters)
  timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
  filename = f"{league}_{gender}_{year}_{player_shortname}_{filter_hash}_{timestamp}.json"

  # Convert to JSON string
  json_string = json.dumps(json_output, indent=2)

  # Create media object
  media_obj = anvil.BlobMedia('application/json', json_string.encode('utf-8'), name=filename)

  return {
    'media_obj': media_obj,
    'filename': filename,
    'summary': metadata
  }


def get_filtered_ppr_data(league, gender, year, team, **filters):
  """
    Retrieve and filter point-by-point data.
    
    Returns:
        DataFrame: Filtered PPR data
    """
  # Get PPR data from your data source
  # This is a placeholder - adapt to your actual data retrieval method

  # Example: Load from ppr_csv_tables or similar
  ppr_rows = app_tables.ppr_csv_tables.search(
    league=league,
    gender=gender,
    year=year,
    team=team
  )

  # Convert to DataFrame (adapt based on your data structure)
  if len(list(ppr_rows)) == 0:
    return pd.DataFrame()

    # This assumes ppr_csv column contains the CSV data
    # Adapt based on your actual structure
  ppr_row = list(ppr_rows)[0]
  ppr_df = pd.read_csv(io.StringIO(ppr_row['ppr_csv']))

  # Apply filters
  if 'comp_l1' in filters:
    ppr_df = ppr_df[ppr_df['comp_l1'] == filters['comp_l1']]

  if 'comp_l2' in filters:
    ppr_df = ppr_df[ppr_df['comp_l2'] == filters['comp_l2']]

  if 'comp_l3' in filters:
    ppr_df = ppr_df[ppr_df['comp_l3'] == filters['comp_l3']]

  if 'start_date' in filters and 'end_date' in filters:
    ppr_df['date'] = pd.to_datetime(ppr_df['date'])
    ppr_df = ppr_df[
      (ppr_df['date'] >= filters['start_date']) &
      (ppr_df['date'] <= filters['end_date'])
      ]

  if 'set' in filters:
    ppr_df = ppr_df[ppr_df['set_num'] == filters['set']]

  if 'set_touch_type' in filters:
    ppr_df = ppr_df[ppr_df['set_touch_type'] == filters['set_touch_type']]

  if 'pass_oos' in filters:
    ppr_df = ppr_df[ppr_df['pass_oos'] == filters['pass_oos']]

    # Numeric filters
  if 'att_ht_low' in filters:
    ppr_df = ppr_df[ppr_df['att_height'] >= float(filters['att_ht_low'])]
  if 'att_ht_high' in filters:
    ppr_df = ppr_df[ppr_df['att_height'] <= float(filters['att_ht_high'])]

  if 'att_speed_low' in filters:
    ppr_df = ppr_df[ppr_df['att_speed'] >= float(filters['att_speed_low'])]
  if 'att_speed_high' in filters:
    ppr_df = ppr_df[ppr_df['att_speed'] <= float(filters['att_speed_high'])]

  if 'set_ht_low' in filters:
        ppr_df = ppr_df[ppr_df['set_height'] >= float(filters['set_ht_low'])]
    if 'set_ht_high' in filters:
        ppr_df = ppr_df[ppr_df['set_height'] <= float(filters['set_ht_high'])]
    
    if 'pass_ht_low' in filters:
        ppr_df = ppr_df[ppr_df['pass_height'] >= float(filters['pass_ht_low'])]
    if 'pass_ht_high' in filters:
        ppr_df = ppr_df[ppr_df['pass_height'] <= float(filters['pass_ht_high'])]
    
    # Zone filters
    if 'srv_fr' in filters:
        ppr_df = ppr_df[ppr_df['serve_src_zone_net'].isin(filters['srv_fr'])]
    
    if 'srv_to' in filters:
        ppr_df = ppr_df[ppr_df['serve_dest_zone'].isin(filters['srv_to'])]
    
    return ppr_df


def get_filtered_triangle_data(league, gender, year, team, **filters):
    """
    Retrieve and filter triangle (set-level) data.
    
    Returns:
        DataFrame: Filtered triangle data
    """
    # Similar to ppr_df but for triangle data
    # Placeholder - adapt to your actual data structure
    
    tri_rows = app_tables.triangle_data.search(
        league=league,
        gender=gender,
        year=year,
        team=team
    )
    
    if len(list(tri_rows)) == 0:
        return pd.DataFrame()
    
    # Convert to DataFrame
    tri_df = pd.DataFrame([
        {col: row[col] for col in row.get_column_names()}
        for row in tri_rows
    ])
    
    # Apply relevant filters
    if 'comp_l1' in filters:
        tri_df = tri_df[tri_df['comp_l1'] == filters['comp_l1']]
    
    if 'start_date' in filters and 'end_date' in filters:
        tri_df['date'] = pd.to_datetime(tri_df['date'])
        tri_df = tri_df[
            (tri_df['date'] >= filters['start_date']) &
            (tri_df['date'] <= filters['end_date'])
        ]
    
    return tri_df


def calculate_all_metrics(metric_dict, ppr_df, tri_df, player_name):
    """
    Calculate all metrics from the dictionary.
    
    Args:
        metric_dict (DataFrame): Metric dictionary
        ppr_df (DataFrame): Point-by-point data
        tri_df (DataFrame): Triangle (set-level) data
        player_name (str): Player to analyze
    
    Returns:
        dict: {
            'metrics': dict organized by category,
            'total_calculated': int,
            'successful': int,
            'insufficient_data': int
        }
    """
    
    metrics_output = {}
    total_calculated = 0
    successful = 0
    insufficient_data = 0
    
    # Loop through dictionary
    for idx, metric_row in metric_dict.iterrows():
        metric_id = metric_row['metric_id']
        category = metric_row['metric_category']
        function_name = metric_row['function_name']
        result_path = metric_row['result_path']
        data_filter = metric_row['data_filter']
        
        if pd.isna(function_name):
            continue
        
        total_calculated += 1
        
        try:
            # Apply data filter if specified
            if pd.notna(data_filter) and data_filter.strip():
                filtered_ppr = eval(data_filter)
            else:
                filtered_ppr = ppr_df
            
            # Set up local namespace for eval
            local_namespace = {
                'ppr_df': filtered_ppr,
                'tri_df': tri_df,
                'disp_player': player_name,
                # Import your metric functions here
                'fbhe_obj': fbhe_obj,
                'calc_player_eso_obj': calc_player_eso_obj,
                'calc_ev_obj': calc_ev_obj,
                'calc_knock_out_obj': calc_knock_out_obj,
                'count_oos_obj': count_oos_obj,
                'calc_trans_obj': calc_trans_obj,
                'calc_error_density_obj': calc_error_density_obj,
                'find_ellipse_area': find_ellipse_area,
                # New functions
                'consistency_sd_match': consistency_sd_match,
                'consistency_sd_set2set': consistency_sd_set2set,
                'calc_serve_pct_obj': calc_serve_pct_obj,
                'calc_angle_attacks_obj': calc_angle_attacks_obj
            }
            
            # Execute function
            exec(function_name, local_namespace)
            
            # Extract result using result_path
            result_var_name = function_name.split('=')[0].strip()
            func_result = local_namespace[result_var_name]
            
            # Extract the metric value
            if pd.notna(result_path) and result_path.strip():
                metric_value = eval(result_path, local_namespace)
            else:
                metric_value = func_result
            
            # Store in output organized by category
            if category not in metrics_output:
                metrics_output[category] = {}
            
            metrics_output[category][metric_id] = {
                'value': float(metric_value) if isinstance(metric_value, (int, float, np.number)) else metric_value,
                'metric_name': metric_row['metric_name']
            }
            
            successful += 1
            
        except Exception as e:
            # Metric calculation failed (likely insufficient data)
            insufficient_data += 1
            # Optionally log the error
            print(f"Error calculating {metric_id}: {str(e)}")
            continue
    
    return {
        'metrics': metrics_output,
        'total_calculated': total_calculated,
        'successful': successful,
        'insufficient_data': insufficient_data
    }


def generate_filter_hash(filters):
    """
    Generate a short hash of filters for filename.
    
    Args:
        filters (dict): Filter dictionary
    
    Returns:
        str: Short hash string
    """
    # Create a string representation of filters (excluding player name)
    filter_items = []
    for k, v in sorted(filters.items()):
        if k not in ['player', 'player_shortname']:
            filter_items.append(f"{k}:{v}")
    
    if not filter_items:
        return "all"
    
    filter_string = "_".join(filter_items)
    hash_obj = hashlib.md5(filter_string.encode())
    return hash_obj.hexdigest()[:8]
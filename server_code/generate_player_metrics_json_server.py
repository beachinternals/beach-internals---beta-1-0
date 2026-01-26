"""
SERVER FUNCTION: generate_player_metrics_json()
===============================================
Updated with logger_utils integration
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
import io

# Import your logging utilities
from logger_utils import log_info, log_error, log_debug, log_critical

from server_functions import *
from metric_calc_functions import *


@anvil.server.callable
def generate_player_metrics_json(league_value, team, **json_filters):
  """
    Generate comprehensive player metrics JSON file.
    
    Args:
        league_value (str): League|Gender|Year format (e.g., "AVP|W|2024")
        team (str): Team name
        **json_filters: Filter parameters
    
    Returns:
        dict: {'media_obj': BlobMedia, 'filename': str, 'summary': dict}
    """

  try:
    log_info("=== JSON Generation Started ===")
    log_info(f"Player: {json_filters.get('player')}")
    log_info(f"League: {league_value}")
    log_info(f"Team: {team}")

    # Parse league value
    str_loc = league_value.index("|")
    league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    gender = league_value[: str_loc - 1].strip()
    year = str(int(league_value[str_loc + 1 :].strip()))

    log_info(f"Parsed - League: {league}, Gender: {gender}, Year: {year}")

    # Get player name (required)
    player_name = json_filters.get('player')
    if not player_name:
      log_error("Player name not provided", with_traceback=False)
      raise ValueError("Player name is required")

    player_shortname = json_filters.get('player_shortname', 'player')

    # Load metric dictionary
    log_info("Loading metric dictionary from database...")
    dict_rows = list(app_tables.metric_dictionary.search())

    if len(dict_rows) == 0:
      log_error("No metrics found in metric_dictionary table", with_traceback=False)
      raise ValueError("Metric dictionary is empty")

    # Get column names from the table schema
    column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]

    # Convert to DataFrame
    metric_dict = pd.DataFrame([
      {col: row[col] for col in column_names}
      for row in dict_rows
    ])
    log_info(f"✓ Loaded {len(metric_dict)} metrics from dictionary")

    # Get PPR data with filters
    log_info(f"Retrieving and filtering PPR data... {league},{gender},{year},{team}")
    ppr_df = get_filtered_ppr_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(ppr_df)} points from PPR data")

    # Get triangle data (for set-to-set consistency)
    log_info("Retrieving triangle data...")
    tri_df = get_filtered_triangle_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(tri_df)} sets from triangle data")

    if len(ppr_df) == 0:
      log_error("No data found for the specified filters", with_traceback=False)
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
      'filters_applied': {k: str(v) for k, v in json_filters.items() 
                          if k not in ['player', 'player_shortname']},
      'total_points_analyzed': len(ppr_df),
      'total_sets_analyzed': len(tri_df) if len(tri_df) > 0 else 0,
      'dictionary_version': '1.0'
    }

    # Calculate all metrics
    log_info("Starting metric calculations...")
    metrics_result = calculate_all_metrics(metric_dict, ppr_df, tri_df, player_name)
    log_info(f"✓ Calculated {metrics_result['successful']} / {metrics_result['total_calculated']} metrics")
    log_info(f"  ({metrics_result['insufficient_data']} metrics had insufficient data)")

    # Build final JSON structure
    log_info("Building JSON output structure...")
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
    log_info(f"Generated filename: {filename}")

    # Convert to JSON string
    json_string = json.dumps(json_output, indent=2)
    log_info(f"JSON size: {len(json_string)} bytes")

    # Create media object
    media_obj = anvil.BlobMedia('application/json', json_string.encode('utf-8'), name=filename)

    log_info("=== JSON Generation Complete ===")

    return {
      'media_obj': media_obj,
      'filename': filename,
      'summary': metadata
    }

  except Exception as e:
    log_error(f"JSON generation failed: {str(e)}", with_traceback=True)
    raise

def get_filtered_ppr_data(league, gender, year, team, **filters):
  """
    Retrieve and filter point-by-point data.
    
    Uses logged-in user's team to determine which PPR data to access.
    
    Returns:
        DataFrame: Filtered PPR data
    """
  try:
    # Get the logged-in user's team from the users table
    current_user = anvil.users.get_user()
    if not current_user:
      log_error("No user logged in", with_traceback=False)
      return pd.DataFrame()

    user_team = current_user['team']
    log_info(f"User team from login: {user_team}")

    # Special handling for INTERNALS users
    if user_team == 'INTERNALS':
      search_team = 'League'
      log_info(f"INTERNALS user - using team='{search_team}'")
    else:
      search_team = user_team

    log_info(f"Querying PPR data for {league}/{gender}/{year}/team={search_team}...")

    # Get data from the user's team row
    ppr_rows = list(app_tables.ppr_csv_tables.search(
      league=league,
      gender=gender,
      year=year,
      team=search_team
    ))

    if len(ppr_rows) == 0:
      log_error(f"No PPR data found for {league}/{gender}/{year}/team={search_team}", with_traceback=False)
      return pd.DataFrame()

    log_info(f"Found {len(ppr_rows)} PPR data record(s)")

    # Get the first (or only) row
    ppr_row = ppr_rows[0]

    # Get column names from table schema
    column_names = [col['name'] for col in app_tables.ppr_csv_tables.list_columns()]

    # Check if ppr_csv column exists
    if 'ppr_csv' not in column_names:
      log_error("ppr_csv column not found in ppr_csv_tables", with_traceback=False)
      return pd.DataFrame()

    # Load the CSV data - handle Media object
    ppr_csv_data = ppr_row['ppr_csv']

    if hasattr(ppr_csv_data, 'get_bytes'):
      # It's a Media object - get the bytes and decode
      ppr_csv_string = ppr_csv_data.get_bytes().decode('utf-8')
      log_debug("Loaded ppr_csv from Media object")
    else:
      # It's already a string
      ppr_csv_string = ppr_csv_data
      log_debug("Loaded ppr_csv as string")

    ppr_df = pd.read_csv(io.StringIO(ppr_csv_string))

    log_info(f"Loaded {len(ppr_df)} raw points from PPR")

    # Use existing filter_ppr_df function to apply filters
    log_info("Applying filters using filter_ppr_df()...")
    ppr_df = filter_ppr_df(ppr_df, **filters)

    log_info(f"After filtering: {len(ppr_df)} points retained")

    return ppr_df

  except Exception as e:
    log_error(f"Error in get_filtered_ppr_data: {str(e)}", with_traceback=True)
    return pd.DataFrame()
    

def get_filtered_triangle_data(league, gender, year, team, **filters):
  """
    Retrieve and filter triangle (set-level) data.
    
    Returns:
        DataFrame: Filtered triangle data
    """
  try:
    log_info("Querying triangle data...")

    # Get date range from filters (optional)
    has_dates = 'start_date' in filters and 'end_date' in filters

    if has_dates:
      date_checked = True
      disp_start_date = filters['start_date']
      disp_end_date = filters['end_date']
      log_info(f"Using date range: {disp_start_date} to {disp_end_date}")
    else:
      # No date filtering - get all data
      date_checked = False
      disp_start_date = None
      disp_end_date = None
      log_info("No date range specified - retrieving all triangle data")

    # Get triangle data using existing server function
    tri_df, tri_found = get_tri_data(
      league, 
      gender, 
      year,
      date_checked,
      disp_start_date,
      disp_end_date
    )

    # Check if data was found
    if not tri_found or len(tri_df) == 0:
      log_info("No triangle data found")
      return pd.DataFrame()

    log_info(f"Loaded {len(tri_df)} raw sets from triangle data")

    # Filter by player (required for player-specific metrics)
    player_name = filters.get('player')
    if player_name and 'player' in tri_df.columns:
      tri_df = tri_df[tri_df['player'] == player_name]
      log_debug(f"After player filter: {len(tri_df)} sets")
    elif player_name:
      # If player column doesn't exist, check for player1/player2 columns
      if 'player1' in tri_df.columns and 'player2' in tri_df.columns:
        tri_df = tri_df[(tri_df['player1'] == player_name) | (tri_df['player2'] == player_name)]
        log_debug(f"After player filter (player1/player2): {len(tri_df)} sets")

    # Apply competition level filters (if not already applied by get_tri_data)
    if 'comp_l1' in filters and 'comp_l1' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l1'] == filters['comp_l1']]
      log_debug(f"After comp_l1 filter: {len(tri_df)} sets")

    if 'comp_l2' in filters and 'comp_l2' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l2'] == filters['comp_l2']]
      log_debug(f"After comp_l2 filter: {len(tri_df)} sets")

    if 'comp_l3' in filters and 'comp_l3' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l3'] == filters['comp_l3']]
      log_debug(f"After comp_l3 filter: {len(tri_df)} sets")

    log_info(f"After all filtering: {len(tri_df)} sets")
    return tri_df

  except Exception as e:
    log_error(f"Error in get_filtered_triangle_data: {str(e)}", with_traceback=True)
    return pd.DataFrame()
    

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

  log_info(f"Calculating {len(metric_dict)} metrics for {player_name}...")

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

    # Progress logging every 50 metrics
    if total_calculated % 50 == 0:
      log_info(f"Progress: {total_calculated}/{len(metric_dict)} metrics calculated...")

    try:
      # Apply data filter if specified
      if pd.notna(data_filter) and data_filter.strip():
        # Sanitize quotes - replace Unicode quotes with regular quotes
        data_filter_clean = data_filter.replace(''', "'").replace(''', "'")
        data_filter_clean = data_filter_clean.replace('"', '"').replace('"', '"')

        # Set up minimal namespace for eval
        filter_namespace = {'ppr_df': ppr_df}
        filtered_ppr = eval(data_filter_clean, filter_namespace)
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
        'calc_angle_attacks_obj': calc_angle_attacks_obj,
        # Add pandas/numpy for metric calculations
        'pd': pd,
        'np': np
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
      log_debug(f"Metric {metric_id} failed: {str(e)}")
      continue

  log_info(f"Calculation complete: {successful} successful, {insufficient_data} failed")

  return {
    'metrics': metrics_output,
    'total_calculated': total_calculated,
    'successful': successful,
    'insufficient_data': insufficient_data
  }



def generate_filter_hash(filters):
    """Generate a short hash of filters for filename."""
    filter_items = []
    for k, v in sorted(filters.items()):
        if k not in ['player', 'player_shortname']:
            filter_items.append(f"{k}:{v}")
    
    if not filter_items:
        return "all"
    
    filter_string = "_".join(filter_items)
    hash_obj = hashlib.md5(filter_string.encode())
    return hash_obj.hexdigest()[:8]

  


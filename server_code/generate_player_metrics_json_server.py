"""
SERVER FUNCTION: generate_player_metrics_json()
===============================================
Updated with logger_utils integration
Updated with attempts_path support for automatic attempt-gating
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

# ============================================================================
#  AUTH HELPERS
# ============================================================================

def _require_own_team(team):
  """
  Verify the caller is logged in AND requesting their own team's data.
  INTERNALS team can access any team's data.
  Returns user row or raises Exception.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS' and team != user['team']:
    raise Exception("Access denied: you can only access your own team's data.")
  return user


# ============================================================================
#
#  INPUT VALIDATION HELPER
#
#  _validate_league_params() — validates league, gender, team against live data
#  _validate_lgy_string()    — parses and validates "NCAA | W | 2025" strings
#
#  Leagues are read dynamically from app_tables.league_list, so adding a new
#  league to that table is all that's needed — no code change required here.
#
#  Team is validated against app_tables.subscriptions. INTERNALS is always
#  allowed regardless of subscription records.
#
# ============================================================================

VALID_GENDERS = {'M', 'W'}

def _get_valid_leagues():
  """Return the set of valid league names from the league_list table."""
  try:
    return {row['league'] for row in app_tables.league_list.search()}
  except Exception:
    # If table is unreachable, fail open rather than block all requests
    return None

def _get_valid_teams():
  """Return the set of valid team names from the subscriptions table."""
  try:
    return {row['team'] for row in app_tables.subscriptions.search()}
  except Exception:
    return None

def _validate_league_params(league=None, gender=None, team=None):
  """
  Validate user-supplied league, gender, and/or team parameters.
  Year is intentionally not validated here.
  Raises ValueError with a clear message if any value is invalid.

  Usage:
    _validate_league_params(league=league, gender=gender, team=team)
  """
  if league is not None:
    if not isinstance(league, str) or not league.strip():
      raise ValueError("League must be a non-empty string.")
    valid_leagues = _get_valid_leagues()
    if valid_leagues is not None and league.strip() not in valid_leagues:
      raise ValueError(f"Invalid league: {repr(league)}. Must be one of {sorted(valid_leagues)}.")

  if gender is not None:
    if not isinstance(gender, str) or gender.strip().upper() not in VALID_GENDERS:
      raise ValueError(f"Invalid gender: {repr(gender)}. Must be one of {sorted(VALID_GENDERS)}.")

  if team is not None:
    if not isinstance(team, str) or not team.strip():
      raise ValueError("Team must be a non-empty string.")
    if re.search(r'[<>]', team):
      raise ValueError(f"Invalid team name: {repr(team)}.")
    # INTERNALS can access any team — skip subscription check
    if team.strip() != 'INTERNALS':
      valid_teams = _get_valid_teams()
      if valid_teams is not None and team.strip() not in valid_teams:
        raise ValueError(f"Invalid team: {repr(team)}. Team is not registered in the system.")


def _validate_lgy_string(lgy):
  """
  Parse and validate an lgy string in format "NCAA | W | 2025" or "NCAA|W|2025".
  Validates league and gender. Year is not validated.
  Raises ValueError if format or values are invalid.
  Returns (league, gender, year_str) as cleaned values.
  """
  if not isinstance(lgy, str) or '|' not in lgy:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected 'LEAGUE | GENDER | YEAR'."
    )
  parts = [p.strip() for p in lgy.split('|')]
  if len(parts) != 3:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected exactly 3 parts separated by '|'."
    )
  league, gender, year_str = parts
  _validate_league_params(league=league, gender=gender)
  return league, gender, year_str



from server_functions import *
from metric_functions import build_metric_namespace


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
  _require_own_team(team)
  _validate_lgy_string(league_value)
  _validate_league_params(team=team)

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
    log_info(f"✓ Loaded {len(metric_dict)} metrics from dictionary - json_server.py")

    # Get PPR data with filters
    log_info(f"Retrieving and filtering PPR data... {league},{gender},{year},{team}")
    ppr_df = get_filtered_ppr_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(ppr_df)} points from PPR data - json_server.py")

    # Get triangle data (for set-to-set consistency)
    log_info("Retrieving triangle data...")
    tri_df = pd.DataFrame()  # tri_df sunset — sets counted from PPR directly
    num_sets = count_player_sets_from_ppr(ppr_df, player_name)
    log_info(f"✓ Counted {num_sets} sets for {player_name} (>=10 points each, from PPR)")

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
      'total_sets_analyzed': count_player_sets_from_ppr(ppr_df, player_name),
      'dictionary_version': '10.0'
    }

    # Calculate all metrics
    log_info("Starting metric calculations...")
    metrics_result = calculate_all_metrics(metric_dict, ppr_df, player_name)
    log_info(f"✓ Calculated {metrics_result['successful']} / {metrics_result['total_calculated']} metrics")
    log_info(f"  ({metrics_result['insufficient_data']} metrics suppressed - below min attempts)")

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
  Retrieve and filter PPR data, including Scout data.
  Delegates to get_ppr_data() in server_functions.py which already
  handles team + Scout appending correctly.
  """
  try:
    # Determine the team to search for
    # If user is logged in, use their team; otherwise use the team parameter
    try:
      current_user = anvil.users.get_user()
      if current_user and current_user['team']:
        search_team = current_user['team']
        if search_team == 'INTERNALS':
          search_team = 'League'
      else:
        search_team = team
    except:
      search_team = team  # Background task — no logged-in user

    log_info(f"Loading PPR data for {league}/{gender}/{year}/team={search_team} (scout=True)...")

    # Use the existing get_ppr_data which already handles Scout appending
    ppr_df = get_ppr_data(
      disp_league=league,
      disp_gender=gender,
      disp_year=str(year),
      disp_team=search_team,
      scout=True          # <-- this is what was missing!
    )

    if not isinstance(ppr_df, pd.DataFrame) or ppr_df.shape[0] == 0:
      log_error(f"No PPR data returned for {league}/{gender}/{year}/team={search_team}")
      return pd.DataFrame()

    log_info(f"Loaded {len(ppr_df)} raw points from PPR (team + scout)")

    # Apply filters
    log_info("Applying filters...")
    ppr_df = filter_ppr_df(ppr_df, **filters)
    log_info(f"After filtering: {len(ppr_df)} points retained")

    return ppr_df

  except Exception as e:
    log_error(f"Error in get_filtered_ppr_data: {str(e)}", with_traceback=True)
    return pd.DataFrame()


def get_filtered_triangle_data(league, gender, year, team, **filters):
  """
  DEPRECATED: Triangle data (tri_df) has been sunset.
  Set counting now happens directly from PPR data via count_player_sets_from_ppr().
  This stub returns an empty DataFrame so existing callers don't break.
  """
  log_info("get_filtered_triangle_data: tri_df is sunset — returning empty DataFrame")
  return pd.DataFrame()


def calculate_all_metrics(metric_dict, ppr_df, player_name):
  """
  Calculate all metrics from the dictionary.

  Uses attempts_path column (if present and populated) to:
    - Fetch the attempt count alongside the metric value
    - Suppress the metric value if attempts < min_attempts_for_ci
    - Always write the attempts count so AI can explain why a value is missing

  If attempts_path is blank/null for a metric, no gating is applied and
  the value is always written.

  Args:
      metric_dict (DataFrame): Metric dictionary (must include attempts_path column)
      ppr_df (DataFrame): Point-by-point data
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

  # Cache executed namespaces by "function_name||data_filter".
  # Many metrics share the same underlying function (e.g. fbhe_obj runs once
  # and produces fbhe_result, fbso_result etc. that multiple metric rows read).
  # Without the cache, each metric gets a fresh namespace and cross-references
  # like attempts_path = 'fbhe_result.points' fail with NameError.
  function_cache = {}

  log_info(f"Calculating {len(metric_dict)} metrics for {player_name}...")

  for idx, metric_row in metric_dict.iterrows():
    metric_id     = metric_row['metric_id']
    category      = metric_row['metric_category']
    function_name = metric_row['function_name']
    result_path   = metric_row['result_path']
    data_filter   = metric_row['data_filter']

    # attempts_path is the new column - handle gracefully if not yet in table
    attempts_path = metric_row.get('attempts_path', None)
    min_attempts  = metric_row.get('min_attempts_for_ci', 5)

    if pd.isna(function_name):
      continue

    total_calculated += 1

    #if total_calculated % 50 == 0:
    #  log_info(f"Progress: {total_calculated}/{len(metric_dict)} metrics calculated...")

    try:
      # ------------------------------------------------------------------
      # Apply data filter if specified
      # ------------------------------------------------------------------
      if pd.notna(data_filter) and data_filter.strip():
        # Sanitize smart quotes
        data_filter_clean = data_filter.replace('\u2018', "'").replace('\u2019', "'")
        data_filter_clean = data_filter_clean.replace('\u201c', '"').replace('\u201d', '"')
        filter_namespace = {'ppr_df': ppr_df, 'disp_player': player_name}
        filtered_ppr = eval(data_filter_clean, filter_namespace)
      else:
        filtered_ppr = ppr_df

      # ------------------------------------------------------------------
      # Execute the metric function — cached per function+filter combination.
      # Metrics sharing the same function+filter reuse the namespace so that
      # results created by one exec() (e.g. fbhe_result) are visible to
      # subsequent metrics that reference them in result_path or attempts_path.
      # ------------------------------------------------------------------
      cache_key = f"{function_name}||{data_filter}"
      if cache_key not in function_cache:
        local_namespace = build_metric_namespace(filtered_ppr, player_name)
        exec(function_name, local_namespace)
        function_cache[cache_key] = local_namespace
      else:
        local_namespace = function_cache[cache_key]

      # ------------------------------------------------------------------
      # Extract the metric value via result_path
      # ------------------------------------------------------------------
      if pd.notna(result_path) and result_path.strip():
        metric_value = eval(result_path, local_namespace)
      else:
        result_var_name = function_name.split('=')[0].strip()
        metric_value = local_namespace[result_var_name]

      # ------------------------------------------------------------------
      # Extract attempts count via attempts_path (new column)
      # attempts_path blank/null  => no gating, always write value
      # attempts_path populated   => fetch count, gate on min_attempts_for_ci
      # ------------------------------------------------------------------
      has_attempts_path = (
        attempts_path is not None
        and not (isinstance(attempts_path, float) and pd.isna(attempts_path))
        and str(attempts_path).strip() != ''
      )

      attempts_value = None
      if has_attempts_path:
        try:
          attempts_value = eval(str(attempts_path).strip(), local_namespace)
        except Exception as ae:
          log_debug(f"Metric {metric_id}: could not evaluate attempts_path '{attempts_path}': {ae}")

      # ------------------------------------------------------------------
      # Gate: suppress value if below minimum attempts
      # ------------------------------------------------------------------
      min_att = int(min_attempts) if pd.notna(min_attempts) else 5
      value_suppressed = False

      if has_attempts_path and attempts_value is not None:
        if attempts_value < min_att:
          value_suppressed = True
          insufficient_data += 1
          #log_debug(
          #  f"Metric {metric_id} suppressed: "
          #  f"{attempts_value} attempts < {min_att} minimum"
          #)

      # ------------------------------------------------------------------
      # Store result - always write attempts so AI knows why value is None
      # ------------------------------------------------------------------
      # Store in output organized by category
      if category not in metrics_output:
        metrics_output[category] = {}

      # --- Extract video links if video_path is set in dictionary ---
      video_links = ''
      video_path = metric_row.get('video_path', None)
      if pd.notna(video_path) and str(video_path).strip():
        try:
          raw_video = eval(str(video_path), local_namespace)
          # Only store if it looks like real links, not placeholder values
          if raw_video and str(raw_video).strip() not in ('N/A', 'No Data Available', ''):
            video_links = str(raw_video).strip()
        except Exception as ve:
          log_debug(f"video_path eval failed for {metric_id}: {ve}")

      metrics_output[category][metric_id] = {
        'value': float(metric_value) if isinstance(metric_value, (int, float, np.number)) else metric_value,
        'metric_name': metric_row['metric_name'],
        'parent_metric': metric_row['parent_metric'] if pd.notna(metric_row.get('parent_metric')) else None,
        'video_links': video_links,
        'attempts': attempts_value,
        'sufficient_data': not value_suppressed,
        'min_attempts': min_att
      }

      if not value_suppressed:
        successful += 1

    except Exception as e:
      insufficient_data += 1
      log_debug(f"Metric {metric_id} failed: {str(e)}")
      continue

  log_info(
    f"Calculation complete: {successful} successful, "
    f"{insufficient_data} suppressed/failed"
  )

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
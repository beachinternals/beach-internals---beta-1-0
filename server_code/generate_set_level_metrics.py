"""
Generate Set-Level Core Metrics for AI Export

This module calculates core metrics for individual sets (video_id + set number + player).
It reads the metric_dictionary to determine which metrics to calculate and uses
the existing calculation functions.

UPDATED: Now includes weather data fetched via weather_id from PPR
UPDATED: Added ai_optimized flag for token-efficient output format
UPDATED: Added comp_level (competitive ranking) for player, partner, and opponents

Author: Beach Volleyball Analytics
Created: 2026-02-05
Updated: 2026-02-07 - Added weather integration via weather_id
Updated: 2026-04-08 - Added ai_optimized dense format
Updated: 2026-05    - Added comp_level_rank / comp_level_score from master_player
"""

import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import pandas as pd
from datetime import datetime
import json

# ============================================================================
# LOGGING IMPORTS
# ============================================================================
from .logger_utils import log_debug, log_info, log_error, log_critical

# ============================================================================
# PERFORMANCE MONITORING IMPORTS
# ============================================================================
from server_functions import (
monitor_performance,
MONITORING_LEVEL_OFF,
MONITORING_LEVEL_CRITICAL,
MONITORING_LEVEL_IMPORTANT,
MONITORING_LEVEL_DETAILED,
MONITORING_LEVEL_VERBOSE
)

# Import ALL calculation functions (same approach as generate_player_metrics_json)
from server_functions import *
from metric_calc_functions import *


# ============================================================================
# CORE HELPER FUNCTIONS
# ============================================================================
def get_core_metrics_from_dictionary():
  """DEPRECATED shim — see get_set_level_metrics_from_dictionary."""
  return get_set_level_metrics_from_dictionary(half=False)

def flag_is_yes(raw):
  """
  Interpret a metric_dictionary flag column as a boolean.
  None / blank / 'No' -> False.  'Yes' (any case) / True / 1 -> True.
  Treating None as False is required: a newly-added Anvil column is
  None on every existing row until populated.
  """
  if raw is None:
    return False
  if isinstance(raw, bool):
    return raw
  s = str(raw).strip().lower()
  return s in ('yes', 'y', 'true', '1')

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_set_level_metrics_from_dictionary(half=False):
  """
  Query metric_dictionary for the metrics to calculate at set level,
  driven by the set_level_whole / set_level_half flag columns
  (NOT the old 'core' category + hard-coded extras).

  Args:
      half (bool): If False, select metrics flagged for WHOLE-set
                   calculation (set_level_whole = Yes).
                   If True, select metrics flagged for HALF-set
                   calculation (set_level_half = Yes).

  Returns:
      list: metric_dictionary rows whose relevant flag is Yes.
  """
  flag_col = 'set_level_half' if half else 'set_level_whole'
  log_info(f"Querying metric_dictionary for set-level metrics ({flag_col})...")

  try:
    # Fetch all rows, then filter in Python. We do NOT filter inside
    # .search() because the flag is a Yes/No/blank string maintained in
    # Numbers, and blank/None rows would behave inconsistently server-side.
    all_metrics = list(app_tables.metric_dictionary.search())
    selected = [m for m in all_metrics if flag_is_yes(m[flag_col])]

    log_info(f"Found {len(selected)} metrics flagged {flag_col}=Yes "
             f"(of {len(all_metrics)} in dictionary)")
    return selected

  except Exception as e:
    log_error(f"Error querying metric_dictionary for set-level metrics: {str(e)}")
    return []


# ============================================================================
# COMP LEVEL LOOKUP
# ============================================================================
def get_player_uuid(player_name, league_value):
  """
  Look up player_uuid from master_player for a name string
  ("TEAM NUMBER SHORTNAME"). Mirrors get_comp_level_for_player's
  parse + query. Returns the uuid string, or a safe placeholder
  (and logs an error) if not found — NEVER returns the real name,
  so de-identified files cannot leak even on a lookup miss.
  """
  placeholder = "PLYR-UNKNOWN"

  if not player_name or not isinstance(player_name, str):
    return placeholder

  parts = player_name.strip().split()
  if len(parts) < 3:
    log_error(f"get_player_uuid: cannot parse '{player_name}'")
    return placeholder

  # Anchor on jersey number — handles multi-word team names.
  num_idx = next((i for i, p in enumerate(parts) if p.isdigit()), None)
  if num_idx is None or num_idx == 0 or num_idx == len(parts) - 1:
    log_error(f"get_player_uuid: cannot parse '{player_name}' (no clear number token)")
    return placeholder

  team_part   = " ".join(parts[:num_idx])
  number_part = parts[num_idx]
  short_part  = " ".join(parts[num_idx+1:])

  try:
    lgy_parts = [p.strip() for p in league_value.split('|')]
    if len(lgy_parts) != 3:
      log_error(f"get_player_uuid: cannot parse league '{league_value}'")
      return placeholder
    league_str, gender_str, year_str = lgy_parts
  except Exception as e:
    log_error(f"get_player_uuid: league parse failed '{league_value}': {e}")
    return placeholder

  try:
    results = list(app_tables.master_player.search(
      league=league_str, gender=gender_str, year=year_str,
      team=team_part, number=number_part, shortname=short_part
    ))
    if not results:
      log_error(f"get_player_uuid: no master_player match for '{player_name}'")
      return placeholder
    uuid_val = results[0]['player_uuid']
    if not uuid_val:
      log_error(f"get_player_uuid: player '{player_name}' has no player_uuid")
      return placeholder
    return uuid_val.strip()
  except Exception as e:
    log_error(f"get_player_uuid: lookup failed for '{player_name}': {e}")
    return placeholder
    
def get_comp_level_for_player(player_name, league_value):
  """
  Look up comp_level_rank and comp_level_score from master_player for a
  given player name string (format "TEAM NUMBER SHORTNAME").

  Unique key in master_player: league + gender + year + team + number + shortname
  All fields are strings in Anvil (including number and year).

  Args:
      player_name (str): e.g. "FSU 35 Trusty"
      league_value (str): lgy string e.g. "NCAA | W | 2026"

  Returns:
      dict: {
          'comp_level_rank'  : int or None,
          'comp_level_score' : float or None
      }
      Always returns the dict (with None values if not found — never raises).
  """
  empty = {'comp_level_rank': None, 'comp_level_score': None}

  if not player_name or not isinstance(player_name, str):
    return empty

  # ── Parse "TEAM NUMBER SHORTNAME" ─────────────────────────────────────
  parts = player_name.strip().split()
  if len(parts) < 3:
    log_debug(f"Cannot parse player_name '{player_name}' for comp_level lookup")
    return empty

  # Anchor on the jersey number (first all-digit token): team is everything
  # before it, shortname everything after. Handles multi-word team names
  # ("SANTA CLARA", "SOUTHERN MISS") and multi-word shortnames.
  num_idx = next((i for i, p in enumerate(parts) if p.isdigit()), None)
  if num_idx is None or num_idx == 0 or num_idx == len(parts) - 1:
    log_debug(f"Cannot parse player_name '{player_name}' (no clear number token)")
    return empty

  team_part   = " ".join(parts[:num_idx])      # "SANTA CLARA"
  number_part = parts[num_idx]                 # "11"
  short_part  = " ".join(parts[num_idx+1:])    # "Avery"

  # ── Split lgy into league, gender, year ───────────────────────────────
  # lgy format: "NCAA | W | 2026"
  # master_player stores them as separate columns, all strings
  try:
    lgy_parts = [p.strip() for p in league_value.split('|')]
    if len(lgy_parts) != 3:
      log_debug(f"Cannot parse league_value '{league_value}' — expected 'league | gender | year'")
      return empty
    league_str = lgy_parts[0]   # "NCAA"
    gender_str = lgy_parts[1]   # "W"
    year_str   = lgy_parts[2]   # "2026" — keep as string
  except Exception as e:
    log_debug(f"Error parsing league_value '{league_value}': {e}")
    return empty

  # ── Query master_player ────────────────────────────────────────────────
  # All six fields needed for a unique match; all are strings in Anvil
  try:
    results = list(app_tables.master_player.search(
      league=league_str,
      gender=gender_str,
      year=year_str,
      team=team_part,
      number=number_part,
      shortname=short_part
    ))

    if not results:
      log_debug(f"No master_player match for {league_str}/{gender_str}/{year_str}/{team_part}/{number_part}/{short_part}")
      return empty

    if len(results) > 1:
      log_debug(f"Still multiple rows for {league_str}/{gender_str}/{year_str}/{team_part}/{number_part}/{short_part} — using first")

    row = results[0]

    rank_val  = row['comp_level_rank']
    score_val = row['comp_level_score']

    log_debug(f"comp_level for '{player_name}': rank={rank_val}, score={score_val}")

    return {
      'comp_level_rank':  rank_val,
      'comp_level_score': score_val
    }

  except Exception as e:
    log_debug(f"comp_level lookup failed for '{player_name}': {e}")
    return empty


# ============================================================================
# WEATHER HELPER
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_weather_from_weather_id(set_df):
  """
  Get weather data using weather_id from PPR dataframe.

  Flow: set_df → weather_id column → weather_data table → weather data

  Args:
      set_df: PPR dataframe filtered to a specific set

  Returns:
      dict with keys: temperature_f, wind_speed_mph, wind_gust_mph,
                      humidity_percent, uv_index, weather_id
      Returns None if no weather_id or not found in table.
  """
  try:
    if len(set_df) == 0:
      return None

    if 'weather_id' not in set_df.columns:
      log_debug("No weather_id column in PPR (backward compatible with old files)")
      return None

    first_row = set_df.iloc[0]
    weather_id = first_row.get('weather_id')

    if weather_id is None or pd.isna(weather_id):
      return None

    try:
      weather_row = app_tables.weather_data.get_by_id(weather_id)
      if not weather_row:
        log_error(f"weather_id {weather_id} not found in weather_data table")
        return None

      weather_data = {
        'weather_id':       weather_id,
        'temperature_f':    weather_row['temperature_f'],
        'wind_speed_mph':   weather_row['wind_speed_mph'],
        'wind_gust_mph':    weather_row['wind_gust_mph'],
        'humidity_percent': weather_row['humidity_percent'],
        'uv_index':         weather_row['uv_index']
      }

      log_debug(
        f"Weather (ID={weather_id}): "
        f"temp={weather_data['temperature_f']}°F, "
        f"wind={weather_data['wind_speed_mph']}mph"
      )
      return weather_data

    except Exception as e:
      log_error(f"Error fetching weather_id {weather_id}: {str(e)}")
      return None

  except Exception as e:
    log_error(f"Error in get_weather_from_weather_id: {str(e)}")
    return None


# ============================================================================
# SET METADATA  (now includes comp_level for all 4 players)
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_set_metadata(ppr_df, video_id, set_number, player_name, league_value):
  """
  Extract metadata for a specific set from ppr_df.

  Args:
      ppr_df:       Full PPR dataframe
      video_id:     Video identifier
      set_number:   Set number (1, 2, or 3)
      player_name:  Player name in format "TEAM NUMBER SHORTNAME"
      league_value: League string used to look up comp_level in master_player

  Returns:
      dict: Metadata including date, time, opponent, partner, venue_id,
            weather, and comp_level for player/partner/opponents.
  """
  log_debug(f"Getting metadata for video_id={video_id}, set={set_number}, player={player_name}")

  set_df = ppr_df[(ppr_df['video_id'] == video_id) & (ppr_df['set'] == set_number)]

  if len(set_df) == 0:
    log_error(f"No data found for video_id={video_id}, set={set_number}")
    return None

  first_row = set_df.iloc[0]

  # ── Determine which side the player is on ────────────────────────────
  player_on_team_a = player_name in [
    first_row.get('player_a1', ''),
    first_row.get('player_a2', '')
  ]

  if player_on_team_a:
    team         = first_row.get('teama', '')
    partner      = (first_row.get('player_a2', '')
                    if first_row.get('player_a1', '') == player_name
                    else first_row.get('player_a1', ''))
    opponent_team = first_row.get('teamb', '')
    opponent1    = first_row.get('player_b1', '')
    opponent2    = first_row.get('player_b2', '')
  else:
    team         = first_row.get('teamb', '')
    partner      = (first_row.get('player_b2', '')
                    if first_row.get('player_b1', '') == player_name
                    else first_row.get('player_b1', ''))
    opponent_team = first_row.get('teama', '')
    opponent1    = first_row.get('player_a1', '')
    opponent2    = first_row.get('player_a2', '')

  # ── Weather ───────────────────────────────────────────────────────────
  weather_data = get_weather_from_weather_id(set_df)

  # ── Competitive levels ────────────────────────────────────────────────
  # Fetch comp_level for the main player and all three other players.
  # Each returns {'comp_level_rank': N, 'comp_level_score': F} or Nones.
  comp_player   = get_comp_level_for_player(player_name, league_value)
  comp_partner  = get_comp_level_for_player(partner,     league_value)
  comp_opp1     = get_comp_level_for_player(opponent1,   league_value)
  comp_opp2     = get_comp_level_for_player(opponent2,   league_value)
  
  # ── Resolve player_uuids (de-identification chokepoint) ───────────────
  uuid_player = get_player_uuid(player_name, league_value)
  uuid_partner = get_player_uuid(partner,   league_value) if partner   else "PLYR-NONE"
  uuid_opp1   = get_player_uuid(opponent1,  league_value) if opponent1 else "PLYR-NONE"
  uuid_opp2   = get_player_uuid(opponent2,  league_value) if opponent2 else "PLYR-NONE"
  
  metadata = {
    'video_id':      video_id,
    'set':           set_number,
    'player':        player_name,
    'date':          first_row.get('game_date', ''),
    'match_time':    first_row.get('match_time', None),
    'venue_id':      first_row.get('venue_id', None),
    'venue_name':    first_row.get('venue_name', ''),
    'team':          team,
    'partner':       partner,
    'player_uuid':    uuid_player,
    'partner_uuid':   uuid_partner,
    'opponent1_uuid': uuid_opp1,
    'opponent2_uuid': uuid_opp2,
    'opponent_team': opponent_team,
    'opponent1':     opponent1,
    'opponent2':     opponent2,
    'total_points':  len(set_df),
    'filename':      first_row.get('filename', ''),
    'comp_l1':       first_row.get('comp_l1', ''),
    'comp_l2':       first_row.get('comp_l2', ''),
    'comp_l3':       first_row.get('comp_l3', ''),
    # Weather
    'weather':       weather_data,
    # Competitive levels — keyed by role so formatters can use them clearly
    'comp_level': {
      'player':   comp_player,
      'partner':  comp_partner,
      'opponent1': comp_opp1,
      'opponent2': comp_opp2,
    }
  }

  # ── Debug logging for missing optional fields ─────────────────────────
  if metadata['match_time'] is None:
    log_debug(f"match_time not found for video_id={video_id}")
  if metadata['venue_id'] is None:
    log_debug(f"venue_id not found for video_id={video_id}")
  if metadata['weather'] is None:
    log_debug(f"No weather for video_id={video_id} (backward compatible)")
  if comp_player['comp_level_rank'] is None:
    log_debug(f"No comp_level for player '{player_name}'")

  return metadata


# ============================================================================
# METRIC CALCULATION FOR A SINGLE SET
# ============================================================================
  
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def calculate_metric_for_set(metric_row, ppr_df_filtered, player_name):
  """
  Calculate a single metric for a filtered set of data.
  Uses the same dynamic execution approach as generate_player_metrics_json.

  Args:
      metric_row:       Row from metric_dictionary table
      ppr_df_filtered:  PPR dataframe already filtered to specific set
      player_name:      Player name

  Returns:
      dict: {'metric_id', 'metric_name', 'value', 'attempts'} or None
  """
  metric_id    = metric_row['metric_id']
  metric_name  = metric_row['metric_name']
  function_name = metric_row['function_name']
  data_filter  = metric_row['data_filter'] 

  if not function_name or function_name.strip() == '':
    log_debug(f"No function_name for metric {metric_id}, skipping")
    return None

  try:
    disp_player = player_name
    ppr_df      = ppr_df_filtered

    # ── Apply data_filter (matches the aggregate path) ────────────────────
    # The filter is PART OF THE METRIC DEFINITION. At set level, ppr_df is
    # already one set's rows; a phase/zone filter slices further. If the
    # filter yields no rows (e.g. a set==2 metric while processing set 1),
    # the metric does not apply to this set — skip it. Emptiness is the
    # signal; no filter-string parsing needed.
    if data_filter is not None and str(data_filter).strip():
      df_clean = str(data_filter).replace('\u2018', "'").replace('\u2019', "'")
      df_clean = df_clean.replace('\u201c', '"').replace('\u201d', '"')
      try:
        filtered = eval(df_clean, {'ppr_df': ppr_df, 'disp_player': disp_player})
      except Exception as fe:
        log_error(f"data_filter failed for {metric_id}: {fe}")
        return None
      if filtered is None or len(filtered) == 0:
        return None  # filter excludes this set — skip silently
      ppr_df = filtered

    exec_context = {
      'ppr_df':      ppr_df,
      'disp_player': disp_player,
      'player_name': player_name
    }
    exec_context.update(globals())
    exec(function_name, exec_context)

    return_type = metric_row['return_type']
    result_path = metric_row['result_path']

    value        = None
    attempts     = None
    distribution = None   # only populated for return_type == 'distribution'

    if return_type == 'object':
      parts = result_path.split('.')
      if len(parts) >= 2:
        obj_name  = parts[0]
        attr_name = parts[1]
        if obj_name in exec_context:
          result_obj = exec_context[obj_name]
          value      = getattr(result_obj, attr_name, None)
          attempts   = getattr(result_obj, 'attempts', None)

    elif return_type == 'dict':
      dict_name = result_path.split('[')[0]
      if dict_name in exec_context:
        result_dict = exec_context[dict_name]
        key_start   = result_path.find("['")
        key_end     = result_path.find("']")
        if key_start >= 0 and key_end > key_start:
          key      = result_path[key_start+2:key_end]
          value    = result_dict.get(key, None)
          attempts = (result_dict.get('attempts', None)
                      or result_dict.get('total_points', None))

    elif return_type == 'distribution':
      # result_path names the SrvDestResult-style object in exec_context,
      # e.g. "srv_dest_result". The object carries grid_pct (per-source
      # cells like '3_4d') plus n_inplay and srv_err_rate. We store the
      # rolled-up views in 'distribution'; the formatter decides what to print.
      obj_name = result_path.split('.')[0].strip()
      if obj_name in exec_context:
        dist_obj     = exec_context[obj_name]
        distribution = build_distribution_payload(dist_obj)
        # keep attempts populated with the in-play n so any scalar-only
        # consumer still sees a sane sample size
        attempts = getattr(dist_obj, 'n_inplay', None)

    return {
      'metric_id':    metric_id,
      'metric_name':  metric_name,
      'value':        value,
      'attempts':     attempts,
      'distribution': distribution   # None for normal metrics
    }

  except Exception as e:
    log_error(f"Error calculating metric {metric_id}: {str(e)}")
    return None


# ============================================================================
# MAIN GENERATION FUNCTION
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def generate_set_level_metrics_for_player(ppr_df, player_name, league_value, team):
  """
  Generate set-by-set metrics for a single player.

  Args:
      ppr_df:       Filtered PPR dataframe (already filtered for player/date/comp)
      player_name:  Player name in format "TEAM NUMBER SHORTNAME"
      league_value: League string (e.g., "NCAA | W | 2026")
      team:         Team name

  Returns:
      dict: {
          'player', 'league', 'team',
          'sets': [ {metadata + metrics + comp_level} ],
          'summary': { ... }
      }
  """
  log_info(f"Generating set-level metrics for {player_name}")

  core_metrics = get_set_level_metrics_from_dictionary(half=False)
  if len(core_metrics) == 0:
    log_error("No core metrics found in metric_dictionary")
    return None

  log_info(f"Will calculate {len(core_metrics)} core metrics per set")
  
  half_metrics = get_set_level_metrics_from_dictionary(half=True)
  log_info(f"Will also attempt {len(half_metrics)} half-set (phase) metrics per set")

  # ── Filter to rows where this player appears ──────────────────────────
  player_df = ppr_df[
    (ppr_df['player_a1'] == player_name) |
    (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) |
    (ppr_df['player_b2'] == player_name)
  ]

  if len(player_df) == 0:
    log_error(f"No data found for player {player_name}")
    return None

  log_info(f"Found {len(player_df)} total points for player")

  set_combinations = (
    player_df.groupby(['video_id', 'set'])
             .size()
             .reset_index(name='point_count')
  )
  log_info(f"Found {len(set_combinations)} unique sets for player")

  sets_data     = []
  sets_included = 0
  sets_excluded = 0

  for idx, row in set_combinations.iterrows():
    video_id    = row['video_id']
    set_num     = row['set']
    point_count = row['point_count']

    if point_count < 10:
      log_info(f"Excluding set {video_id}-{set_num}: {point_count} points (< 10 minimum)")
      sets_excluded += 1
      continue

    if point_count < 20:
      log_info(f"Warning: Set {video_id}-{set_num} has {point_count} points (< 20 typical)")
    if point_count > 50:
      log_info(f"Warning: Set {video_id}-{set_num} has {point_count} points (> 50 typical)")

    # ── Metadata now includes comp_level (pass league_value) ─────────
    metadata = get_set_metadata(ppr_df, video_id, set_num, player_name, league_value)
    if not metadata:
      log_error(f"Could not get metadata for set {video_id}-{set_num}")
      continue

    set_df = player_df[
      (player_df['video_id'] == video_id) &
      (player_df['set']      == set_num)
    ]

    # ── Calculate all core metrics ────────────────────────────────────
    set_metrics = {}
    for metric_row in core_metrics:
      metric_result = calculate_metric_for_set(metric_row, set_df, player_name)
      if metric_result:
        set_metrics[metric_result['metric_id']] = {
          'name':         metric_result['metric_name'],
          'value':        metric_result['value'],
          'attempts':     metric_result['attempts'],
          'distribution': metric_result.get('distribution')
        }

    # ── Half/phase metrics: each carries a data_filter (set==N & half).
    # Applied to this set_df, only the phases matching this set survive;
    # the rest self-skip (filter empties -> calculate_metric_for_set
    # returns None). So set 1 yields its 1a/1b, set 2 its 2a/2b, etc.
    half_set_metrics = {}
    for metric_row in half_metrics:
      metric_result = calculate_metric_for_set(metric_row, set_df, player_name)
      if metric_result:
        half_set_metrics[metric_result['metric_id']] = {
          'name':         metric_result['metric_name'],
          'value':        metric_result['value'],
          'attempts':     metric_result['attempts'],
          'distribution': metric_result.get('distribution')
        }
    set_data = {
      **metadata,
      'metrics':            set_metrics,
      'half_metrics':       half_set_metrics,
      'metrics_calculated': len(set_metrics)
    }

    sets_data.append(set_data)
    sets_included += 1
    log_debug(f"Processed set {video_id}-{set_num}: {point_count} pts, {len(set_metrics)} metrics")

  log_info(f"Set-level processing complete: {sets_included} included, {sets_excluded} excluded")

  summary = {
    'total_sets_analyzed': sets_included,
    'total_sets_excluded': sets_excluded,
    'total_points': (int(player_df['point_no'].nunique())
                     if 'point_no' in player_df.columns else len(player_df)),
    'date_range': {
      'start': str(player_df['game_date'].min()) if 'game_date' in player_df.columns else None,
      'end':   str(player_df['game_date'].max()) if 'game_date' in player_df.columns else None
    },
    'metrics_per_set': len(core_metrics)
  }

  return {
    'player':  player_name,
    'league':  league_value,
    'team':    team,
    'sets':    sets_data,
    'summary': summary
  }


# ============================================================================
# FORMAT HELPERS  (shared utilities)
# ============================================================================

def _fmt_val(value):
  """Format a metric value compactly."""
  if value is None:
    return "N/A"
  elif isinstance(value, float):
    return "0.000" if value == 0.0 else f"{value:.3f}"
  elif value == 0:
    return "0"
  else:
    return str(value)


def _fmt_comp_level(comp_dict):
  """
  Return a short string like 'rank:12 score:9.59' or 'N/A'.
  comp_dict has keys comp_level_rank and comp_level_score.
  """
  if not comp_dict:
    return "N/A"
  rank  = comp_dict.get('comp_level_rank')
  score = comp_dict.get('comp_level_score')
  if rank is None and score is None:
    return "N/A"
  parts = []
  if rank  is not None: parts.append(f"rank:{rank}")
  if score is not None: parts.append(f"score:{score:.2f}")
  return " ".join(parts)


def _fmt_weather_dense(weather):
  """
  Format weather as a single compact string.
  Example: "temp:72F wind:12mph gust:18mph hum:65%"
  """
  if not weather:
    return ""
  parts = []
  t = weather.get('temperature_f')
  if t is not None and not pd.isna(t):   parts.append(f"temp:{t:.0f}F")
  w = weather.get('wind_speed_mph')
  if w is not None and not pd.isna(w):   parts.append(f"wind:{w:.0f}mph")
  g = weather.get('wind_gust_mph')
  if g is not None and not pd.isna(g):   parts.append(f"gust:{g:.0f}mph")
  h = weather.get('humidity_percent')
  if h is not None and not pd.isna(h):   parts.append(f"hum:{h:.0f}%")
  uv = weather.get('uv_index')
  if uv is not None and not pd.isna(uv): parts.append(f"uv:{uv:.1f}")
  return " ".join(parts)


# ============================================================================
# FORMAT: HUMAN-READABLE  (ai_optimized=False)
# ============================================================================

def _format_set_level_human(set_level_data, display_name=None, display_team=None):
  """Human-readable markdown format."""
  log_info("Formatting set-level data as markdown (human)...")

  content_name = display_name if display_name else set_level_data['player']
  content_team = display_team if display_team else set_level_data['team']

  md = []

  # ── File header ───────────────────────────────────────────────────────
  md.append(f"# {content_name}")
  md.append(f"")
  md.append(f"**League:** {set_level_data['league']}")
  md.append(f"**Team:** {content_team}")
  md.append(f"")

  # ── Summary ───────────────────────────────────────────────────────────
  summary = set_level_data['summary']
  md.append(f"## Performance Summary")
  md.append(f"- Total Sets Analyzed: {summary['total_sets_analyzed']}")
  md.append(f"- Total Sets Excluded: {summary['total_sets_excluded']}")
  md.append(f"- Total Points: {summary['total_points']}")
  if summary['date_range']['start']:
    md.append(f"- Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
  md.append(f"- Core Metrics Per Set: {summary['metrics_per_set']}")
  md.append(f"")
  md.append(f"---")
  md.append(f"")

  # ── Individual sets ───────────────────────────────────────────────────
  md.append(f"## Individual Set Performance")
  md.append(f"")

  for set_data in set_level_data['sets']:
    opponent_display = f"vs {set_data['opponent_team']}"
    md.append(f"### {set_data['date']} | {opponent_display} | Set {set_data['set']}")
    md.append(f"")

    # Match Information
    md.append(f"**Match Information:**")
    if set_data.get('match_time'):
      md.append(f"- Time: {set_data['match_time']}")
    if set_data.get('venue_name'):
      md.append(f"- Venue: {set_data['venue_name']}")
    if set_data.get('venue_id'):
      md.append(f"- Venue ID: {set_data['venue_id']}")
    md.append(f"- Competition: {set_data.get('comp_l1','')} / {set_data.get('comp_l2','')} / {set_data.get('comp_l3','')}")
    md.append(f"- Total Points in Set: {set_data['total_points']}")
    md.append(f"- Video ID: {set_data['video_id']}")
    md.append(f"")

    # ── Competitive Levels ────────────────────────────────────────────
    comp = set_data.get('comp_level', {})
    player_cl  = comp.get('player',   {})
    partner_cl = comp.get('partner',  {})
    opp1_cl    = comp.get('opponent1',{})
    opp2_cl    = comp.get('opponent2',{})

    # Only render section if at least one player has a rating
    any_comp = any(
      d.get('comp_level_rank') is not None or d.get('comp_level_score') is not None
      for d in [player_cl, partner_cl, opp1_cl, opp2_cl]
    )
    if any_comp:
      md.append(f"**Competitive Levels:**")
      md.append(f"- {set_data['player']}: {_fmt_comp_level(player_cl)}")
      md.append(f"- Partner ({set_data['partner']}): {_fmt_comp_level(partner_cl)}")
      md.append(f"- Opponent 1 ({set_data['opponent1']}): {_fmt_comp_level(opp1_cl)}")
      md.append(f"- Opponent 2 ({set_data['opponent2']}): {_fmt_comp_level(opp2_cl)}")
      md.append(f"")

    # ── Weather ───────────────────────────────────────────────────────
    weather = set_data.get('weather')
    if weather and any(
      v is not None and not (isinstance(v, float) and pd.isna(v))
      for v in weather.values()
    ):
      md.append(f"**Weather Conditions:**")
      t = weather.get('temperature_f')
      if t is not None and not pd.isna(t):
        md.append(f"- Temperature: {t:.1f}°F")
      w = weather.get('wind_speed_mph')
      if w is not None and not pd.isna(w):
        gust = weather.get('wind_gust_mph')
        wind_str = f"{w:.1f} mph"
        if gust is not None and not pd.isna(gust):
          wind_str += f" (gusts to {gust:.1f} mph)"
        md.append(f"- Wind: {wind_str}")
      h = weather.get('humidity_percent')
      if h is not None and not pd.isna(h):
        md.append(f"- Humidity: {h:.0f}%")
      uv = weather.get('uv_index')
      if uv is not None and not pd.isna(uv):
        md.append(f"- UV Index: {uv:.1f}")
      md.append(f"")

    # ── Core Metrics ──────────────────────────────────────────────────
    md.append(f"**Core Metrics:**")
    md.append(f"")
    for metric_id, metric_info in sorted(set_data['metrics'].items()):
      value_str = _fmt_val(metric_info['value'])
      attempts  = metric_info['attempts']
      if attempts is not None:
        md.append(f"- **{metric_info['name']}**: {value_str} (n={attempts})")
      else:
        md.append(f"- **{metric_info['name']}**: {value_str}")

    md.append(f"")
    md.append(f"---")
    md.append(f"")

  return "\n".join(md)


# ============================================================================
# FORMAT: AI-OPTIMIZED DENSE  (ai_optimized=True)
# ============================================================================

def _format_set_level_dense(set_level_data, display_name=None, display_team=None):
  """
  Token-efficient dense format for AI consumption.

  SET header line:
    SET|date|vs TEAM|S#|pts:N|comp:l1/l2[|weather]
  Comp level line (omitted if all N/A):
    CL|player:rank#/score|partner:rank#/score|opp1:rank#/score|opp2:rank#/score
  Metrics line:
    metric_id:value(n=att) ...
  """
  log_info("Formatting set-level data as markdown (ai_optimized dense)...")

  content_name = display_name if display_name else set_level_data['player']
  content_team = display_team if display_team else set_level_data['team']

  summary    = set_level_data['summary']
  date_start = summary['date_range']['start'] or ''
  date_end   = summary['date_range']['end']   or ''
  date_str   = f"{date_start}..{date_end}" if date_start else "all"

  md = []

  # ── Compact file header ───────────────────────────────────────────────
  md.append(
    f"PLAYER|{content_name}|{set_level_data['league']}|{content_team}|"
    f"sets:{summary['total_sets_analyzed']}|pts:{summary['total_points']}|dates:{date_str}"
  )
  md.append(f"")

  # ── Legend ────────────────────────────────────────────────────────────
  md.append("# Set-Level Metrics")
  md.append("# Format: SET|date|opponent|set_no|pts:N|comp:l1/l2[|weather]")
  md.append("# CL line: CL|player:rank/score|partner:rank/score|opp1:rank/score|opp2:rank/score")
  md.append("# Metrics line: metric_id:value(n=attempts) ...")
  md.append("# Decode metric_id codes using the metric_dictionary file.")
  md.append("")

  # ── Individual sets ───────────────────────────────────────────────────
  for set_data in set_level_data['sets']:
    weather_str = _fmt_weather_dense(set_data.get('weather'))

    comp_parts = [set_data.get('comp_l1',''), set_data.get('comp_l2',''), set_data.get('comp_l3','')]
    comp_str   = "/".join(p for p in comp_parts if p)

    # SET line — opponent identified by the two opponent uuids (no team name)
    opp_uuids = f"{set_data.get('opponent1_uuid','PLYR-NONE')} {set_data.get('opponent2_uuid','PLYR-NONE')}"
    set_line = (
      f"SET|{set_data['date']}|vs {opp_uuids}|"
      f"S{set_data['set']}|pts:{set_data['total_points']}|comp:{comp_str}"
    )
    if weather_str:
      set_line += f"|{weather_str}"
    md.append(set_line)

    # ── Competitive Level line ─────────────────────────────────────────
    comp = set_data.get('comp_level', {})

    def _cl_dense(role_key, ident):
      """Format one player's comp_level as 'uuid:rank/score' or 'uuid:N/A'."""
      d = comp.get(role_key, {})
      rank  = d.get('comp_level_rank')
      score = d.get('comp_level_score')
      if rank is None and score is None:
        return f"{ident}:N/A"
      r_str = str(rank)  if rank  is not None else "?"
      s_str = f"{score:.2f}" if score is not None else "?"
      return f"{ident}:{r_str}/{s_str}"

    cl_player  = _cl_dense('player',   set_data.get('player_uuid',    'player'))
    cl_partner = _cl_dense('partner',  set_data.get('partner_uuid',   'partner'))
    cl_opp1    = _cl_dense('opponent1',set_data.get('opponent1_uuid', 'opp1'))
    cl_opp2    = _cl_dense('opponent2',set_data.get('opponent2_uuid', 'opp2'))

    # Only emit CL line if at least one player has real data
    any_comp = any(
      comp.get(k, {}).get('comp_level_rank') is not None or
      comp.get(k, {}).get('comp_level_score') is not None
      for k in ('player','partner','opponent1','opponent2')
    )
    if any_comp:
      md.append(f"CL|{cl_player}|{cl_partner}|{cl_opp1}|{cl_opp2}")

    # Metrics line
    metric_parts = []
    for metric_id, metric_info in sorted(set_data['metrics'].items()):
      value_str = _fmt_val(metric_info['value'])
      attempts  = metric_info['attempts']
      if attempts is not None:
        metric_parts.append(f"{metric_id}:{value_str}(n={attempts})")
      else:
        metric_parts.append(f"{metric_id}:{value_str}")

    if metric_parts:
      md.append(" ".join(metric_parts))

    # ── Serve destination distribution (Option D: all non-zero source_dest cells) ──
    for metric_id, metric_info in sorted(set_data['metrics'].items()):
      dist = metric_info.get('distribution')
      if not dist:
        continue
      cells = [(k, v) for k, v in dist.get('cells_full', {}).items() if v >= 0.005]
      cells.sort(key=lambda it: (it[0].split('_')[0], -it[1]))  # by source, then desc pct
      if cells:
        cell_str = " ".join(f"{k}:{v:.2f}" for k, v in cells)
        n        = dist.get('n')
        err      = dist.get('err_rate')
        head     = f"SRVDEST|n={n}"
        if err is not None:
          head += f"|err:{err:.3f}"
        md.append(f"{head}|{cell_str}")
head     = f"SRVDEST|n={n}"
        if err is not None:
          head += f"|err:{err:.3f}"
        md.append(f"{head}|{cell_str}")

    # ── Half/phase metrics (1a/1b for set 1, 2a/2b for set 2, etc.) ──────
    # These were computed per-set; their data_filter already restricted each
    # to its phase, so only the phases matching this set are present.
    half_parts = []
    for metric_id, metric_info in sorted(set_data.get('half_metrics', {}).items()):
      value_str = _fmt_val(metric_info['value'])
      attempts  = metric_info['attempts']
      if attempts is not None:
        half_parts.append(f"{metric_id}:{value_str}(n={attempts})")
      else:
        half_parts.append(f"{metric_id}:{value_str}")
    if half_parts:
      md.append("HALF|" + " ".join(half_parts))

    md.append("")  # blank line between sets

    md.append("")  # blank line between sets

  return "\n".join(md)


# ============================================================================
# PUBLIC FORMAT ENTRY POINT
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_set_level_data_as_markdown(
  set_level_data,
  display_name=None,
  display_team=None,
  ai_optimized=False
):
  """
  Format set-level data as markdown.

  Args:
      set_level_data: Output from generate_set_level_metrics_for_player
      display_name  : Optional override for player name in headers
      display_team  : Optional override for team name in headers
      ai_optimized  : If True, use dense token-efficient format

  Returns:
      str: Markdown formatted text
  """
  if not set_level_data:
    return ""

  if ai_optimized:
    return _format_set_level_dense(set_level_data, display_name, display_team)
  else:
    return _format_set_level_human(set_level_data, display_name, display_team)


# ============================================================================
# JSON FORMAT
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_set_level_data_as_json(set_level_data):
  """
  Format set-level data as JSON.

  Returns:
      str: JSON formatted text
  """
  if not set_level_data:
    return "{}"

  log_info("Formatting set-level data as JSON...")

  json_data = {
    'player':  set_level_data['player'],
    'league':  set_level_data['league'],
    'team':    set_level_data['team'],
    'summary': set_level_data['summary'],
    'sets':    set_level_data['sets']
    # comp_level is already embedded inside each set dict
  }

  return json.dumps(json_data, indent=2, default=str)
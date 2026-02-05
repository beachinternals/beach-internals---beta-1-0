"""
Generate Set-Level Core Metrics for AI Export

This module calculates core metrics for individual sets (video_id + set number + player).
It reads the metric_dictionary to determine which metrics to calculate and uses
the existing calculation functions.

Author: Beach Volleyball Analytics
Created: 2026-02-05
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

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_core_metrics_from_dictionary():
  """
    Query metric_dictionary table to get all core metrics.
    
    Returns:
        list: List of metric rows with metric_category='core'
    """
  log_info("Querying metric_dictionary for core metrics...")

  try:
    # Get all metrics where metric_category = 'core'
    core_metrics = list(app_tables.metric_dictionary.search(
      metric_category='core'
    ))

    log_info(f"Found {len(core_metrics)} core metrics in dictionary")
    return core_metrics

  except Exception as e:
    log_error(f"Error querying metric_dictionary: {str(e)}")
    return []


@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_set_metadata(ppr_df, video_id, set_number, player_name):
  """
    Extract metadata for a specific set from ppr_df.
    
    Args:
        ppr_df: Full PPR dataframe
        video_id: Video identifier
        set_number: Set number (1, 2, or 3)
        player_name: Player name in format "TEAM NUMBER SHORTNAME"
        
    Returns:
        dict: Metadata including date, time, opponent, partner, venue_id
    """
  log_debug(f"Getting metadata for video_id={video_id}, set={set_number}, player={player_name}")

  # Filter to this specific set
  set_df = ppr_df[(ppr_df['video_id'] == video_id) & (ppr_df['set'] == set_number)]

  if len(set_df) == 0:
    log_error(f"No data found for video_id={video_id}, set={set_number}")
    return None

    # Get first row for metadata (all rows in set should have same metadata)
  first_row = set_df.iloc[0]

  # Determine which team the player is on (a or b)
  player_on_team_a = player_name in [first_row.get('player_a1', ''), first_row.get('player_a2', '')]

  if player_on_team_a:
    team = first_row.get('teama', '')
    partner = first_row.get('player_a2', '') if first_row.get('player_a1', '') == player_name else first_row.get('player_a1', '')
    opponent_team = first_row.get('teamb', '')
    opponent1 = first_row.get('player_b1', '')
    opponent2 = first_row.get('player_b2', '')
  else:
    team = first_row.get('teamb', '')
    partner = first_row.get('player_b2', '') if first_row.get('player_b1', '') == player_name else first_row.get('player_b1', '')
    opponent_team = first_row.get('teama', '')
    opponent1 = first_row.get('player_a1', '')
    opponent2 = first_row.get('player_a2', '')

  metadata = {
    'video_id': video_id,
    'set': set_number,
    'player': player_name,
    'date': first_row.get('game_date', ''),
    'match_time': first_row.get('match_time', None),  # May not exist yet
    'venue_id': first_row.get('venue_id', None),  # May not exist yet
    'venue_name': first_row.get('venue_name', ''),
    'team': team,
    'partner': partner,
    'opponent_team': opponent_team,
    'opponent1': opponent1,
    'opponent2': opponent2,
    'total_points': len(set_df),
    'filename': first_row.get('filename', ''),
    'comp_l1': first_row.get('comp_l1', ''),
    'comp_l2': first_row.get('comp_l2', ''),
    'comp_l3': first_row.get('comp_l3', '')
  }

  # Log if match_time or venue_id are missing
  if metadata['match_time'] is None:
    log_debug(f"match_time not found in ppr_df for video_id={video_id}")
  if metadata['venue_id'] is None:
    log_debug(f"venue_id not found in ppr_df for video_id={video_id}")

  return metadata


@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def calculate_metric_for_set(metric_row, ppr_df_filtered, player_name):
  """
    Calculate a single metric for a filtered set of data.
    Uses the same dynamic execution approach as generate_player_metrics_json.
    
    Args:
        metric_row: Row from metric_dictionary table
        ppr_df_filtered: PPR dataframe already filtered to specific set
        player_name: Player name
        
    Returns:
        dict: {'metric_id': value, 'attempts': attempts} or None if calculation fails
    """
  metric_id = metric_row['metric_id']
  metric_name = metric_row['metric_name']
  function_name = metric_row['function_name']  # This is the actual Python code to execute!

  log_debug(f"Calculating metric: {metric_id} ({metric_name})")

  # Skip if no function_name (shouldn't happen for core metrics)
  if not function_name or function_name.strip() == '':
    log_debug(f"No function_name for metric {metric_id}, skipping")
    return None

  try:
    # Set up the execution context (same as generate_player_metrics_json)
    disp_player = player_name
    ppr_df = ppr_df_filtered

    # Execute the function_name string dynamically
    # This evaluates expressions like "fbhe_result = fbhe_obj(ppr_df, disp_player, 'both', False)"
    exec_context = {
      'ppr_df': ppr_df,
      'disp_player': disp_player,
      'player_name': player_name
    }

    # Add all imported functions to the execution context
    exec_context.update(globals())

    # Execute the function_name (NOT calculation_formula!)
    exec(function_name, exec_context)

    # Extract the result based on result_path
    return_type = metric_row['return_type']
    result_path = metric_row['result_path']

    value = None
    attempts = None

    # Parse the result_path to extract the value
    if return_type == 'object':
      # Object notation like fbhe_result.fbhe
      parts = result_path.split('.')
      if len(parts) >= 2:
        obj_name = parts[0]
        attr_name = parts[1]
        if obj_name in exec_context:
          result_obj = exec_context[obj_name]
          value = getattr(result_obj, attr_name, None)
          attempts = getattr(result_obj, 'attempts', None)

    elif return_type == 'dict':
      # Dictionary notation like eso_result['eso']
      # Extract dict name and key
      dict_name = result_path.split('[')[0]
      if dict_name in exec_context:
        result_dict = exec_context[dict_name]
        # Extract key from result_path
        key_start = result_path.find("['")
        key_end = result_path.find("']")
        if key_start >= 0 and key_end > key_start:
          key = result_path[key_start+2:key_end]
          value = result_dict.get(key, None)
          attempts = result_dict.get('attempts', None) or result_dict.get('total_points', None)
        
        return {
            'metric_id': metric_id,
            'metric_name': metric_name,
            'value': value,
            'attempts': attempts
        }
        
  except Exception as e:
        log_error(f"Error calculating metric {metric_id}: {str(e)}")
        return None


# ============================================================================
# MAIN GENERATION FUNCTIONS
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_set_level_metrics_for_player(ppr_df, player_name, league_value, team):
    """
    Generate set-level core metrics for a single player across all their sets.
    
    Args:
        ppr_df: Full PPR dataframe (already filtered by date range, etc.)
        player_name: Player name in format "TEAM NUMBER SHORTNAME"
        league_value: League value string (e.g., "NCAA | W | 2026")
        team: Team name
        
    Returns:
        dict: {
            'player': player_name,
            'league': league_value,
            'team': team,
            'sets': [list of set data],
            'summary': summary statistics
        }
    """
    log_info(f"Generating set-level metrics for player: {player_name}")
    
    # Get core metrics from dictionary
    core_metrics = get_core_metrics_from_dictionary()
    if not core_metrics:
        log_error("No core metrics found in metric_dictionary")
        return None
    
    log_info(f"Will calculate {len(core_metrics)} core metrics per set")
    
    # Filter ppr_df to this player's points only
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
    
    # Get unique video_id + set combinations
    set_combinations = player_df.groupby(['video_id', 'set']).size().reset_index(name='point_count')
    log_info(f"Found {len(set_combinations)} unique sets for player")
    
    # Process each set
    sets_data = []
    sets_included = 0
    sets_excluded = 0
    
    for idx, row in set_combinations.iterrows():
        video_id = row['video_id']
        set_num = row['set']
        point_count = row['point_count']
        
        # Check point count thresholds
        if point_count < 10:
            log_info(f"Excluding set {video_id}-{set_num}: only {point_count} points (< 10 minimum)")
            sets_excluded += 1
            continue
        
        if point_count < 20:
            log_info(f"Warning: Set {video_id}-{set_num} has {point_count} points (< 20 typical minimum)")
        
        if point_count > 50:
            log_info(f"Warning: Set {video_id}-{set_num} has {point_count} points (> 50 typical maximum)")
        
        # Get metadata for this set
        metadata = get_set_metadata(ppr_df, video_id, set_num, player_name)
        if not metadata:
            log_error(f"Could not get metadata for set {video_id}-{set_num}")
            continue
        
        # Filter ppr_df to this specific set
        set_df = player_df[
            (player_df['video_id'] == video_id) & 
            (player_df['set'] == set_num)
        ]
        
        # Calculate all core metrics for this set
        set_metrics = {}
        for metric_row in core_metrics:
            metric_result = calculate_metric_for_set(metric_row, set_df, player_name)
            if metric_result:
                set_metrics[metric_result['metric_id']] = {
                    'name': metric_result['metric_name'],
                    'value': metric_result['value'],
                    'attempts': metric_result['attempts']
                }
        
        # Combine metadata and metrics
        set_data = {
            **metadata,
            'metrics': set_metrics,
            'metrics_calculated': len(set_metrics)
        }
        
        sets_data.append(set_data)
        sets_included += 1
        
        log_debug(f"Processed set {video_id}-{set_num}: {point_count} points, {len(set_metrics)} metrics")
    
    log_info(f"Set-level processing complete: {sets_included} sets included, {sets_excluded} excluded")
    
    # Create summary
    summary = {
        'total_sets_analyzed': sets_included,
        'total_sets_excluded': sets_excluded,
        'total_points': int(player_df['point_no'].nunique()) if 'point_no' in player_df.columns else len(player_df),
        'date_range': {
            'start': str(player_df['game_date'].min()) if 'game_date' in player_df.columns else None,
            'end': str(player_df['game_date'].max()) if 'game_date' in player_df.columns else None
        },
        'metrics_per_set': len(core_metrics)
    }
    
    return {
        'player': player_name,
        'league': league_value,
        'team': team,
        'sets': sets_data,
        'summary': summary
    }


@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_set_level_data_as_markdown(set_level_data):
    """
    Format set-level data as markdown.
    
    Args:
        set_level_data: Output from generate_set_level_metrics_for_player
        
    Returns:
        str: Markdown formatted text
    """
    if not set_level_data:
        return ""
    
    log_info("Formatting set-level data as markdown...")
    
    md = []
    
    # Header - Player is H1
    md.append(f"# {set_level_data['player']}")
    md.append(f"")
    md.append(f"**League:** {set_level_data['league']}")
    md.append(f"**Team:** {set_level_data['team']}")
    md.append(f"")
    
    # Summary - H2
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
    
    # Individual sets
    md.append(f"## Individual Set Performance")
    md.append(f"")
    
    for idx, set_data in enumerate(set_level_data['sets'], 1):
        # Better header format: Date | vs Opponent | Set N
        opponent_display = f"vs {set_data['opponent_team']}"
        set_header = f"## {set_data['date']} | {opponent_display} | Set {set_data['set']}"
        
        md.append(set_header)
        md.append(f"")
        md.append(f"**Match Information:**")
        if set_data.get('match_time'):
            md.append(f"- Time: {set_data['match_time']}")
        if set_data.get('venue_name'):
            md.append(f"- Venue: {set_data['venue_name']}")
        if set_data.get('venue_id'):
            md.append(f"- Venue ID: {set_data['venue_id']}")
        md.append(f"- Competition: {set_data.get('comp_l1', '')} / {set_data.get('comp_l2', '')} / {set_data.get('comp_l3', '')}")
        md.append(f"- Partner: {set_data['partner']}")
        md.append(f"- Opponents: {set_data['opponent1']}, {set_data['opponent2']}")
        md.append(f"- Total Points in Set: {set_data['total_points']}")
        md.append(f"- Video ID: {set_data['video_id']}")
        md.append(f"")
        
        md.append(f"**Core Metrics:**")
        md.append(f"")
        
        # Sort metrics by metric_id for consistent ordering
        sorted_metrics = sorted(set_data['metrics'].items())
        
        for metric_id, metric_info in sorted_metrics:
            value = metric_info['value']
            attempts = metric_info['attempts']
            
            # Format value with better N/A handling
            if value is None:
                value_str = "No Data"  # Better than "N/A"
            elif isinstance(value, float):
                if value == 0.0:
                    value_str = "0.000"  # Explicit zero
                else:
                    value_str = f"{value:.3f}"
            elif value == 0:
                value_str = "0"  # Integer zero
            else:
                value_str = str(value)
            
            # Format attempts
            if attempts is not None:
                md.append(f"- **{metric_info['name']}**: {value_str} (n={attempts})")
            else:
                md.append(f"- **{metric_info['name']}**: {value_str}")
        
        md.append(f"")
        md.append(f"---")
        md.append(f"")
    
    return "\n".join(md)


@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_set_level_data_as_json(set_level_data):
    """
    Format set-level data as JSON.
    
    Args:
        set_level_data: Output from generate_set_level_metrics_for_player
        
    Returns:
        str: JSON formatted text
    """
    if not set_level_data:
        return "{}"
    
    log_info("Formatting set-level data as JSON...")
    
    # Convert to JSON-serializable format
    json_data = {
        'player': set_level_data['player'],
        'league': set_level_data['league'],
        'team': set_level_data['team'],
        'summary': set_level_data['summary'],
        'sets': set_level_data['sets']
    }
    
    return json.dumps(json_data, indent=2, default=str)
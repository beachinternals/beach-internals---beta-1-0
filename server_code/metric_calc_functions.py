"""
NEW FUNCTIONS FOR BEACH VOLLEYBALL METRICS
===========================================
These 4 functions complete the metric dictionary implementation.
Copy each function into your Anvil server code.
"""

import pandas as pd
import numpy as np
from datetime import datetime


# ==============================================================================
# FUNCTION 1: consistency_sd_match()
# ==============================================================================
def consistency_sd_match(ppr_df, player_name, metric_name):
  """
    Calculate the standard deviation of a metric within a single match.
    
    This measures how consistent a player's performance is during a match.
    Lower std dev = more consistent performance.
    
    Args:
        ppr_df (DataFrame): Point-by-point dataframe for the match
        player_name (str): Player to analyze
        metric_name (str): Which metric to calculate consistency for.
                          Options: 'fbhe', 'error_density', 'knockout', 
                                   'pass_oos', 'points', 'transition'
    
    Returns:
        dict: {
            'std_dev': float (standard deviation),
            'metric_name': str,
            'window_size': int (size of calculation window),
            'num_windows': int (number of windows analyzed),
            'mean_value': float (average metric value)
        }
    """

  # Filter for player
  player_df = ppr_df[
    (ppr_df['player_a1'] == player_name) | 
    (ppr_df['player_a2'] == player_name)
    ].copy()

  if len(player_df) == 0:
    return {
      'std_dev': None,
      'metric_name': metric_name,
      'window_size': 0,
      'num_windows': 0,
      'mean_value': None,
      'error': 'No data for player'
    }

    # Calculate metric values for rolling windows
  window_size = 5  # Calculate metric every 5 points
  window_values = []

  total_points = len(player_df)

  # If not enough points, use smaller window
  if total_points < window_size:
    window_size = max(3, total_points // 2)

    # Calculate metric for each window
  for i in range(0, total_points - window_size + 1, window_size):
    window_df = player_df.iloc[i:i + window_size]

    # Calculate the specified metric
    metric_value = None

    if metric_name == 'fbhe':
      # First ball hitting efficiency
      attacks = window_df[window_df['skill'] == 'attack']
      if len(attacks) > 0:
        kills = len(attacks[attacks['eval_code'].isin(['#', 'winning_attack'])])
        errors = len(attacks[attacks['eval_code'].isin(['=', 'attack_error'])])
        attempts = len(attacks)
        metric_value = (kills - errors) / attempts if attempts > 0 else None

    elif metric_name == 'error_density':
      # Total errors / total points
      errors = len(window_df[window_df['eval_code'].str.contains('error', case=False, na=False)])
      points = len(window_df)
      metric_value = errors / points if points > 0 else None

    elif metric_name == 'knockout':
      # (Aces + opponent OOS passes) / serves
      serves = window_df[window_df['skill'] == 'serve']
      if len(serves) > 0:
        aces = len(serves[serves['eval_code'] == '#'])
        # Note: OOS passes would need to be tracked in opponent data
        # For now, just use aces
        metric_value = aces / len(serves)

    elif metric_name == 'pass_oos':
      # Out of system passes / total passes
      passes = window_df[window_df['skill'] == 'pass']
      if len(passes) > 0:
        oos = len(passes[passes['eval_code'].isin(['!', '/', '-'])])
        metric_value = oos / len(passes)

    elif metric_name == 'points':
      # Points per window (simple count)
      metric_value = len(window_df)

    elif metric_name == 'transition':
      # Simplified transition - would need proper transition identification
      # For now, use rally length as proxy
      metric_value = len(window_df)

    if metric_value is not None:
      window_values.append(metric_value)

    # Calculate standard deviation
  if len(window_values) >= 2:
    std_dev = float(np.std(window_values, ddof=1))
    mean_value = float(np.mean(window_values))
  else:
    std_dev = None
    mean_value = None

  return {
    'std_dev': std_dev,
    'metric_name': metric_name,
    'window_size': window_size,
    'num_windows': len(window_values),
    'mean_value': mean_value
  }


# ==============================================================================
# FUNCTION 2: consistency_sd_set2set()
# ==============================================================================
def consistency_sd_set2set(tri_df, player_name, metric_name):
  """
    Calculate the consistency of a metric from set to set.
    
    This measures whether a player performs similarly across different sets.
    Lower std dev = more consistent across sets.
    
    Args:
        tri_df (DataFrame): Triangle dataframe (one row per set)
        player_name (str): Player to analyze
        metric_name (str): Which metric to calculate consistency for.
                          Options: 'fbhe', 'error_density', 'knockout',
                                   'pass_oos', 'points', 'transition'
    
    Returns:
        dict: {
            'std_dev': float (std dev across sets),
            'metric_name': str,
            'num_sets': int,
            'mean_value': float,
            'set_breakdown': list of dicts with per-set values
        }
    """

  # Filter for player's sets
  player_df = tri_df[
    (tri_df['player_a1'] == player_name) | 
    (tri_df['player_a2'] == player_name)
    ].copy()

  if len(player_df) == 0:
    return {
      'std_dev': None,
      'metric_name': metric_name,
      'num_sets': 0,
      'mean_value': None,
      'set_breakdown': [],
      'error': 'No data for player'
    }

    # Sort by set number
  if 'set_num' in player_df.columns:
    player_df = player_df.sort_values('set_num')

    # Calculate metric for each set
  set_values = []
  set_breakdown = []

  for idx, row in player_df.iterrows():
    metric_value = None
    attempts = 0

    if metric_name == 'fbhe':
      # Get from triangle data columns
      if 'fbhe' in row:
        metric_value = row['fbhe']
        attempts = row.get('att_attempts', 0)
      elif 'att_kills' in row and 'att_errors' in row and 'att_attempts' in row:
        attempts = row['att_attempts']
        if attempts > 0:
          metric_value = (row['att_kills'] - row['att_errors']) / attempts

    elif metric_name == 'error_density':
      if 'total_errors' in row and 'total_points' in row:
        total_points = row['total_points']
        if total_points > 0:
          metric_value = row['total_errors'] / total_points
          attempts = total_points

    elif metric_name == 'knockout':
      if 'srv_aces' in row and 'srv_attempts' in row:
        attempts = row['srv_attempts']
        if attempts > 0:
          metric_value = row['srv_aces'] / attempts

    elif metric_name == 'pass_oos':
      if 'pass_oos' in row and 'pass_attempts' in row:
        attempts = row['pass_attempts']
        if attempts > 0:
          metric_value = row['pass_oos'] / attempts

    elif metric_name == 'points':
      if 'total_points' in row:
        metric_value = row['total_points']
        attempts = 1

    elif metric_name == 'transition':
      if 'transition_kills' in row and 'transition_attempts' in row:
        attempts = row['transition_attempts']
        if attempts > 0:
          metric_value = row['transition_kills'] / attempts

    if metric_value is not None:
      set_values.append(metric_value)
      set_breakdown.append({
        'set_num': row.get('set_num', idx + 1),
        'metric_value': float(metric_value),
        'attempts': int(attempts)
      })

    # Calculate standard deviation across sets
  if len(set_values) >= 2:
    std_dev = float(np.std(set_values, ddof=1))
    mean_value = float(np.mean(set_values))
  else:
    std_dev = None
    mean_value = None

  return {
    'std_dev': std_dev,
    'metric_name': metric_name,
    'num_sets': len(set_values),
    'mean_value': mean_value,
    'set_breakdown': set_breakdown
  }


# ==============================================================================
# FUNCTION 3: calc_serve_pct_obj()
# ==============================================================================
def calc_serve_pct_obj(ppr_df, player_name):
  """
    Calculate serve outcome percentages.
    
    Args:
        ppr_df (DataFrame): Point-by-point dataframe (can be pre-filtered by zone)
        player_name (str): Player to analyze
    
    Returns:
        Object (SimpleNamespace) with attributes:
            .ace_pct (float): aces / total_serves
            .error_pct (float): serve_errors / total_serves
            .attempts (int): total number of serves
            .aces (int): number of aces
            .errors (int): number of serve errors
    """
  from types import SimpleNamespace

  # Filter for player's serves
  serves = ppr_df[
    ((ppr_df['player_a1'] == player_name) | (ppr_df['player_a2'] == player_name)) &
    (ppr_df['skill'] == 'serve')
    ].copy()

  total_serves = len(serves)

  if total_serves == 0:
    return SimpleNamespace(
      ace_pct=0.0,
      error_pct=0.0,
      attempts=0,
      aces=0,
      errors=0
    )

    # Count aces (direct point from serve)
  aces = len(serves[serves['eval_code'] == '#'])

  # Count errors (serve out, net, foot fault)
  errors = len(serves[serves['eval_code'].isin(['=', 'serve_error', '=!', '=/'])])

  # Calculate percentages
  ace_pct = aces / total_serves
  error_pct = errors / total_serves

  return SimpleNamespace(
    ace_pct=float(ace_pct),
    error_pct=float(error_pct),
    attempts=int(total_serves),
    aces=int(aces),
    errors=int(errors)
  )


# ==============================================================================
# FUNCTION 4: calc_angle_attacks_obj()
# ==============================================================================
def calc_angle_attacks_obj(ppr_df, player_name):
  """
    Calculate attack distribution and efficiency by angle.
    
    This function analyzes ALL attacks from a specific zone (zone filter 
    applied before calling this function) and breaks down:
    1. What percentage of attacks went to each angle?
    2. What was the FBHE for attacks to each angle?
    
    Args:
        ppr_df (DataFrame): Point-by-point dataframe, PRE-FILTERED by attack zone
                           Example: ppr_df[ppr_df['att_src_zone_net'] == 1]
        player_name (str): Player to analyze
    
    Returns:
        Object (SimpleNamespace) with attributes for each angle:
            .angle_right_pct, .angle_right_fbhe, .angle_right_attempts
            .angle_left_pct, .angle_left_fbhe, .angle_left_attempts
            .cut_right_pct, .cut_right_fbhe, .cut_right_attempts
            .cut_left_pct, .cut_left_fbhe, .cut_left_attempts
            .straight_pct, .straight_fbhe, .straight_attempts
            .total_attempts
    """
  from types import SimpleNamespace

  # Filter for player's attacks
  attacks = ppr_df[
    ((ppr_df['player_a1'] == player_name) | (ppr_df['player_a2'] == player_name)) &
    (ppr_df['skill'] == 'attack')
    ].copy()

  total_attacks = len(attacks)

  if total_attacks == 0:
    return SimpleNamespace(
      angle_right_pct=0.0, angle_right_fbhe=0.0, angle_right_attempts=0,
      angle_left_pct=0.0, angle_left_fbhe=0.0, angle_left_attempts=0,
      cut_right_pct=0.0, cut_right_fbhe=0.0, cut_right_attempts=0,
      cut_left_pct=0.0, cut_left_fbhe=0.0, cut_left_attempts=0,
      straight_pct=0.0, straight_fbhe=0.0, straight_attempts=0,
      total_attempts=0
    )

    # Define angle mappings
    # att_angular_zone column values:
    # A1 = cut left, A2 = angle left, A3 = straight, A4 = angle right, A5 = cut right

  def calc_angle_stats(df, angle_code):
    """Calculate percentage and FBHE for a specific angle"""
    angle_attacks = df[df['att_angular_zone'] == angle_code]
    attempts = len(angle_attacks)

    if attempts == 0:
      return 0.0, 0.0, 0

      # Calculate FBHE
    kills = len(angle_attacks[angle_attacks['eval_code'].isin(['#', 'winning_attack'])])
    errors = len(angle_attacks[angle_attacks['eval_code'].isin(['=', 'attack_error'])])
    fbhe = (kills - errors) / attempts

    # Calculate percentage
    pct = attempts / total_attacks

    return float(pct), float(fbhe), int(attempts)

    # Calculate for each angle
  cut_left_pct, cut_left_fbhe, cut_left_attempts = calc_angle_stats(attacks, 'A1')
  angle_left_pct, angle_left_fbhe, angle_left_attempts = calc_angle_stats(attacks, 'A2')
  straight_pct, straight_fbhe, straight_attempts = calc_angle_stats(attacks, 'A3')
  angle_right_pct, angle_right_fbhe, angle_right_attempts = calc_angle_stats(attacks, 'A4')
  cut_right_pct, cut_right_fbhe, cut_right_attempts = calc_angle_stats(attacks, 'A5')

  return SimpleNamespace(
    angle_right_pct=angle_right_pct,
    angle_right_fbhe=angle_right_fbhe,
    angle_right_attempts=angle_right_attempts,

    angle_left_pct=angle_left_pct,
    angle_left_fbhe=angle_left_fbhe,
    angle_left_attempts=angle_left_attempts,

    cut_right_pct=cut_right_pct,
    cut_right_fbhe=cut_right_fbhe,
    cut_right_attempts=cut_right_attempts,

    cut_left_pct=cut_left_pct,
    cut_left_fbhe=cut_left_fbhe,
    cut_left_attempts=cut_left_attempts,

    straight_pct=straight_pct,
    straight_fbhe=straight_fbhe,
    straight_attempts=straight_attempts,

    total_attempts=int(total_attacks)
  )


# ==============================================================================
# USAGE EXAMPLES
# ==============================================================================

"""
# Example 1: Calculate within-match consistency
result = consistency_sd_match(ppr_df, 'Smith', 'fbhe')
print(f"FBHE consistency: {result['std_dev']:.3f}")
print(f"Average FBHE: {result['mean_value']:.3f}")

# Example 2: Calculate set-to-set consistency
result = consistency_sd_set2set(tri_df, 'Smith', 'fbhe')
print(f"Set-to-set std dev: {result['std_dev']:.3f}")
for set_data in result['set_breakdown']:
    print(f"  Set {set_data['set_num']}: {set_data['metric_value']:.3f}")

# Example 3: Calculate serve percentages (overall)
srv_result = calc_serve_pct_obj(ppr_df, 'Smith')
print(f"Ace %: {srv_result.ace_pct:.1%}")
print(f"Error %: {srv_result.error_pct:.1%}")

# Example 4: Calculate serve percentages from zone 1
ppr_zone1 = ppr_df[ppr_df['serve_src_zone_net'] == 1]
srv_result = calc_serve_pct_obj(ppr_zone1, 'Smith')
print(f"Zone 1 Ace %: {srv_result.ace_pct:.1%}")

# Example 5: Calculate angle attacks from zone 2
ppr_zone2 = ppr_df[ppr_df['att_src_zone_net'] == 2]
angle_result = calc_angle_attacks_obj(ppr_zone2, 'Smith')
print(f"From Zone 2:")
print(f"  Angle right: {angle_result.angle_right_pct:.1%} (FBHE: {angle_result.angle_right_fbhe:.3f})")
print(f"  Angle left: {angle_result.angle_left_pct:.1%} (FBHE: {angle_result.angle_left_fbhe:.3f})")
print(f"  Straight: {angle_result.straight_pct:.1%} (FBHE: {angle_result.straight_fbhe:.3f})")
"""

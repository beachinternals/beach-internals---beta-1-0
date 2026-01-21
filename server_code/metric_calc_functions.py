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
    Calculate the standard deviation of a metric across set periods.
    
    Splits sets into early/late game:
    - Set 1: Early (≤21 points), Late (>21 points)
    - Set 2: Early (≤21 points), Late (>21 points)  
    - Set 3: Early (≤15 points), Late (>15 points)
    
    This measures how consistent a player's performance is across different game periods.
    Lower std dev = more consistent performance.
    
    Args:
        ppr_df (DataFrame): Point-by-point dataframe
        player_name (str): Player to analyze
        metric_name (str): Which metric to calculate consistency for.
                          Options: 'fbhe', 'error_density', 'knockout', 
                                   'pass_oos', 'points', 'transition'
    
    Returns:
        dict: {
            'std_dev': float (standard deviation across periods),
            'metric_name': str,
            'num_periods': int (number of periods analyzed),
            'mean_value': float (average metric value across periods),
            'period_values': list (metric value for each period)
        }
    """

  # Filter for player
  player_df = ppr_df[
    (ppr_df['player_a1'] == player_name) | 
    (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) | 
    (ppr_df['player_b2'] == player_name)
    ].copy()

  if len(player_df) == 0:
    return {
      'std_dev': None,
      'metric_name': metric_name,
      'num_periods': 0,
      'mean_value': None,
      'period_values': [],
      'error': 'No data for player'
    }

  # Calculate total score for each point
  player_df['total_score'] = player_df['a_score'] + player_df['b_score']

  # Assign period group based on set and score
  def assign_period(row):
    set_num = row['set']
    total_score = row['total_score']

    if set_num == 1:
      return 1 if total_score <= 21 else 2
    elif set_num == 2:
      return 3 if total_score <= 21 else 4
    elif set_num == 3:
      return 5 if total_score <= 15 else 6
    else:
      return None  # Invalid set number

  player_df['period'] = player_df.apply(assign_period, axis=1)

  # Remove any rows with invalid periods
  player_df = player_df[player_df['period'].notna()]

  # Group by video_id and period to get unique periods
  period_values = []

  for (video_id, period), period_df in player_df.groupby(['video_id', 'period']):

    # Calculate the specified metric for this period
    metric_value = None

    if metric_name == 'fbhe':
      # First ball hitting efficiency
      attacks = period_df[period_df['att_player'] == player_name]
      if len(attacks) > 0:
        kills = len(attacks[attacks['point_outcome'] == 'FBK'])
        errors = len(attacks[attacks['point_outcome'] == 'FBE'])
        attempts = len(attacks)
        metric_value = (kills - errors) / attempts if attempts > 0 else None

    elif metric_name == 'error_density':
      # Total errors / total points
      att_errors = len(period_df[(period_df['point_outcome'] == 'FBE') & (period_df['att_player'] == player_name)])
      tran_errors = len(period_df[(period_df['point_outcome'] == 'TE') & (period_df['point_outcome_team'].str.contains(player_name, na=False))]) / 2
      serve_errors = len(period_df[(period_df['point_outcome'] == 'TSE') & (period_df['serve_player'] == player_name)])
      errors = att_errors + tran_errors + serve_errors
      points = len(period_df)
      metric_value = errors / points if points > 0 else None

    elif metric_name == 'knockout':
      # (Aces + opponent OOS passes) / serves
      serves = period_df[period_df['serve_player'] == player_name]
      if len(serves) > 0:
        aces = len(serves[serves['point_outcome'] == 'TSA'])
        oos = len(serves[serves['pass_oos'] != 0])
        metric_value = (aces + oos) / len(serves)

    elif metric_name == 'pass_oos':
      # Out of system passes / total passes
      passes = period_df[period_df['pass_player'] == player_name]
      if len(passes) > 0:
        oos = len(passes[passes['pass_oos'] != 0])
        metric_value = oos / len(passes)

    elif metric_name == 'points':
      # Points won / total points in period
      points_earned = len(period_df[
        ((period_df['point_outcome'] == 'FBK') | 
         (period_df['point_outcome'] == 'TK') | 
         (period_df['point_outcome'] == 'TSA')) &
        (period_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      metric_value = points_earned / len(period_df) if len(period_df) > 0 else None

    elif metric_name == 'transition':
      # Transition points won / total transition points
      tran_pts_won = len(period_df[
        (period_df['point_outcome'] == 'TK') & 
        (period_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      tran_pts_opp_err = len(period_df[
        (period_df['point_outcome'] == 'TE') & 
        (~period_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      tran_pts = tran_pts_won + tran_pts_opp_err

      total_tran = len(period_df[
        (period_df['point_outcome'] == 'TK') | 
        (period_df['point_outcome'] == 'TE')
        ])

      metric_value = tran_pts / total_tran if total_tran > 0 else None

    if metric_value is not None:
      period_values.append(metric_value)

  # Calculate standard deviation across periods
  if len(period_values) >= 2:
    std_dev = float(np.std(period_values, ddof=1))
    mean_value = float(np.mean(period_values))
  else:
    std_dev = None
    mean_value = None

  return {
    'std_dev': std_dev,
    'metric_name': metric_name,
    'num_periods': len(period_values),
    'mean_value': mean_value,
    'period_values': period_values
  }



# ==============================================================================
# FUNCTION 2: consistency_sd_set2set()
# ==============================================================================
def consistency_sd_set2set(ppr_df, player_name, metric_name):
  
  """
    Calculate the standard deviation of a metric across sets.
    
    This measures how consistent a player's performance is from set to set.
    Lower std dev = more consistent performance.
    
    Args:
        ppr_df (DataFrame): Point-by-point dataframe
        player_name (str): Player to analyze
        metric_name (str): Which metric to calculate consistency for.
                          Options: 'fbhe', 'error_density', 'knockout', 
                                   'pass_oos', 'points', 'transition'
    
    Returns:
        dict: {
            'std_dev': float (standard deviation across sets),
            'metric_name': str,
            'num_sets': int (number of sets analyzed),
            'mean_value': float (average metric value across sets),
            'set_values': list (metric value for each set)
        }
    """

  # Filter for player
  player_df = ppr_df[
    (ppr_df['player_a1'] == player_name) | 
    (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) | 
    (ppr_df['player_b2'] == player_name)
    ].copy()

  if len(player_df) == 0:
    return {
      'std_dev': None,
      'metric_name': metric_name,
      'num_sets': 0,
      'mean_value': None,
      'set_values': [],
      'error': 'No data for player'
    }

  # Group by video_id and set to get unique sets
  set_values = []

  for (video_id, set_num), set_df in player_df.groupby(['video_id', 'set']):

    # Calculate the specified metric for this set
    metric_value = None

    if metric_name == 'fbhe':
      # First ball hitting efficiency
      attacks = set_df[set_df['att_player'] == player_name]
      if len(attacks) > 0:
        kills = len(attacks[attacks['point_outcome'] == 'FBK'])
        errors = len(attacks[attacks['point_outcome'] == 'FBE'])
        attempts = len(attacks)
        metric_value = (kills - errors) / attempts if attempts > 0 else None

    elif metric_name == 'error_density':
      # Total errors / total points
      att_errors = len(set_df[(set_df['point_outcome'] == 'FBE') & (set_df['att_player'] == player_name)])
      tran_errors = len(set_df[(set_df['point_outcome'] == 'TE') & (set_df['point_outcome_team'].str.contains(player_name, na=False))]) / 2
      serve_errors = len(set_df[(set_df['point_outcome'] == 'TSE') & (set_df['serve_player'] == player_name)])
      errors = att_errors + tran_errors + serve_errors
      points = len(set_df)
      metric_value = errors / points if points > 0 else None

    elif metric_name == 'knockout':
      # (Aces + opponent OOS passes) / serves
      serves = set_df[set_df['serve_player'] == player_name]
      if len(serves) > 0:
        aces = len(serves[serves['point_outcome'] == 'TSA'])
        oos = len(serves[serves['pass_oos'] != 0])
        metric_value = (aces + oos) / len(serves)

    elif metric_name == 'pass_oos':
      # Out of system passes / total passes
      passes = set_df[set_df['pass_player'] == player_name]
      if len(passes) > 0:
        oos = len(passes[passes['pass_oos'] != 0])
        metric_value = oos / len(passes)

    elif metric_name == 'points':
      # Points won / total points in set
      points_earned = len(set_df[
        ((set_df['point_outcome'] == 'FBK') | 
         (set_df['point_outcome'] == 'TK') | 
         (set_df['point_outcome'] == 'TSA')) &
        (set_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      metric_value = points_earned / len(set_df) if len(set_df) > 0 else None

    elif metric_name == 'transition':
      # Transition points won / total transition points
      tran_pts_won = len(set_df[
        (set_df['point_outcome'] == 'TK') & 
        (set_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      tran_pts_opp_err = len(set_df[
        (set_df['point_outcome'] == 'TE') & 
        (~set_df['point_outcome_team'].str.contains(player_name, na=False))
        ])
      tran_pts = tran_pts_won + tran_pts_opp_err

      total_tran = len(set_df[
        (set_df['point_outcome'] == 'TK') | 
        (set_df['point_outcome'] == 'TE')
        ])

      metric_value = tran_pts / total_tran if total_tran > 0 else None

    if metric_value is not None:
      set_values.append(metric_value)

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
    'set_values': set_values
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
  serves = ppr_df[ ppr_df['serve_player'] == player_name ].copy()

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
  aces = len(serves[serves['point_outcome'] == 'TSA'])

  # Count errors (serve out, net, foot fault)
  errors = len(serves[serves['point_outcome'] == 'TSE'])

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
  attacks = ppr_df[ ppr_df['att_player'] == player_name].copy()

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
    kills = len(angle_attacks[angle_attacks['point_outcome'] == 'FBK'])
    errors = len(angle_attacks[angle_attacks['point_outcome'] == 'FBE'])
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

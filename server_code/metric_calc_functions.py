import anvil.files
from anvil.files import data_files
"""
NEW FUNCTIONS FOR BEACH VOLLEYBALL METRICS
===========================================
These 4 functions complete the metric dictionary implementation.
Copy each function into your Anvil server code.
"""

import pandas as pd
import numpy as np
from datetime import datetime

from logger_utils import log_error


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
  serves = ppr_df[ ppr_df['serve_player'] == player_name.strip() ].copy()

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
# FUNCTION 5: build_margin_sequence()
# ==============================================================================
def build_margin_sequence(ppr_df, disp_player, set_number=None, half=None):
  """
    Build the ordered, running point-margin sequence for one set or one
    half-set, oriented to disp_player's side.

    ppr_df is already one row per point (serve/pass/set/attack/dig columns
    live wide on the same row), so this is a sort + filter + diff over
    a_score/b_score -- no point reconstruction needed.

    Half-set split uses the same total-score thresholds already used
    elsewhere in the dictionary: <=21 for sets 1-2, <=15 for set 3.
    For half='b', the margin is rebased to start at 0 at the beginning of
    that half (subtract the margin reached at the end of half 'a'), so the
    sequence reflects the half's own internal shape rather than carrying
    the absolute score in from the rest of the set. Half 'a' and whole-set
    sequences are already 0-based since a_score/b_score reset at set start.

    Args:
        ppr_df (DataFrame): Point-by-point dataframe. May already be
            sliced to a single set (as the metric_dictionary set-level
            harness delivers it) or span multiple sets (any other caller).
        disp_player (str): Player whose side orients the margin
        set_number (int | None): Set to build the sequence for. If None,
            inferred as the set of the first row -- the metric_dictionary
            harness's exec() namespace has no `set_number` variable to
            reference, so dictionary-driven calls must rely on inference;
            direct/test callers can still pass it explicitly.
        half (str | None): None for the whole set, 'a' or 'b' for a half

    Returns:
        Object (SimpleNamespace) with attributes:
            .margins (list[int]): running margin (own side - opponent) at
                each point, in point_no order
            .winners (list[str]): 'us'/'them' per point, same order/length
                as margins -- derived from whichever of a_score/b_score
                incremented versus the prior row
            .points (int): len(margins)
            .side (str | None): 'a' or 'b' -- disp_player's side in this
                set, or None if the set has no rows at all
    """
  from types import SimpleNamespace
  from generate_set_level_metrics import get_player_side

  if ppr_df is None or len(ppr_df) == 0:
    return SimpleNamespace(margins=[], winners=[], points=0, side=None)

  if set_number is None:
    set_number = int(ppr_df['set'].iloc[0])

  disp_player = disp_player.strip()
  empty = SimpleNamespace(margins=[], winners=[], points=0, side=None)

  set_df = ppr_df[ppr_df['set'] == set_number].sort_values('point_no')
  if set_df.empty:
    return empty

  first_row = set_df.iloc[0]
  side = get_player_side(first_row, disp_player)

  if side == 'a':
    margin = set_df['a_score'] - set_df['b_score']
  else:
    margin = set_df['b_score'] - set_df['a_score']

  # Winner per point: whichever score incremented versus the prior row.
  # Scores reset to 0-0 at the start of each set, so seeding prev=(0, 0)
  # is correct for the set's first row.
  prev_a = 0
  winner_side = []
  for a_s in set_df['a_score']:
    winner_side.append('a' if a_s > prev_a else 'b')
    prev_a = a_s

  threshold = 15 if set_number == 3 else 21
  total_score = set_df['a_score'] + set_df['b_score']
  in_half_a = total_score <= threshold

  if half == 'a':
    keep = in_half_a
    baseline = 0
  elif half == 'b':
    keep = ~in_half_a
    baseline = margin[in_half_a].iloc[-1] if in_half_a.any() else 0
  else:
    keep = pd.Series(True, index=set_df.index)
    baseline = 0

  if not keep.any():
    return SimpleNamespace(margins=[], winners=[], points=0, side=side)

  margins = (margin[keep] - baseline).tolist()
  winners = ['us' if w == side else 'them'
             for w, k in zip(winner_side, keep.tolist()) if k]

  return SimpleNamespace(
    margins=[int(m) for m in margins],
    winners=winners,
    points=len(margins),
    side=side
  )


# ==============================================================================
# FUNCTION 6: smooth_momentum()
# ==============================================================================
def smooth_momentum(margin_sequence, window=5):
  """
    Turn a raw margin sequence into a smoothed momentum series.

    Computes the point-to-point change in margin (first derivative), then
    applies a rolling average over `window` points to cut point-to-point
    noise. window=5 is a starting default, not a validated value -- pass a
    different value once real output has been reviewed.

    Args:
        margin_sequence (list[float]): running margin, in point order
        window (int): rolling-average window size over the derivative series

    Returns:
        list[float]: smoothed momentum series. Empty if there are fewer
            than `window` point-to-point deltas to average (too short a
            sequence to smooth meaningfully -- e.g. an early-terminated
            set/half).
    """
  if len(margin_sequence) < 2:
    return []

  deltas = [margin_sequence[i] - margin_sequence[i - 1]
            for i in range(1, len(margin_sequence))]

  if len(deltas) < window:
    return []

  smoothed = pd.Series(deltas).rolling(window=window).mean().dropna()
  return smoothed.tolist()


# ==============================================================================
# FUNCTION 7: calc_momentum_obj()
# ==============================================================================
def calc_momentum_obj(ppr_df, disp_player, set_number=None, half=None, window=5):
  """
    Extract within-set/half momentum and decline metrics for disp_player.

    Uses build_margin_sequence() + smooth_momentum() internally -- see
    those for the margin/smoothing conventions, including how set_number
    is inferred when not passed explicitly.

    Args:
        ppr_df (DataFrame): Point-by-point dataframe
        disp_player (str): Player to analyze
        set_number (int | None): Set to analyze; inferred from ppr_df if
            omitted (see build_margin_sequence)
        half (str | None): None for whole-set, 'a'/'b' for a half-set
        window (int): rolling-average window passed to smooth_momentum

    Returns:
        Object (SimpleNamespace) with attributes:
            .run_for_max (int | None): longest consecutive point-scoring
                streak by disp_player's side
            .run_against_max (int | None): same, opponent's side
            .margin_max_lead (int | None): max(margins)
            .margin_max_deficit (int | None): min(margins)
            .decline_sharpness (float | None): magnitude of the largest
                peak-to-trough decline (max drawdown) in the smoothed
                momentum series -- i.e. the biggest drop from any local
                high down to a later low, not just one step; 0.0 if
                momentum never actually declined
            .decline_location_pct (float | None): where that trough (the
                bottom of the steepest decline) occurred, 0-100 through
                the smoothed series; None if there was no decline to
                locate
            .attempts (int): number of points in the sequence -- exposed
                under this name (not .points) so it lines up with the
                other calc_*_obj results and the set-level formatter's
                "(n=X)" display

        All fields are None (aside from .attempts) when the underlying
        sequence is too short to compute them meaningfully, rather than
        raising or returning a misleading 0.
    """
  from types import SimpleNamespace

  seq = build_margin_sequence(ppr_df, disp_player, set_number, half=half)

  if seq.points == 0:
    return SimpleNamespace(
      run_for_max=None, run_against_max=None,
      margin_max_lead=None, margin_max_deficit=None,
      decline_sharpness=None, decline_location_pct=None,
      attempts=0
    )

  # Longest consecutive scoring runs, own side and opponent
  run_for_max = 0
  run_against_max = 0
  cur_for = 0
  cur_against = 0
  for w in seq.winners:
    if w == 'us':
      cur_for += 1
      cur_against = 0
    else:
      cur_against += 1
      cur_for = 0
    run_for_max = max(run_for_max, cur_for)
    run_against_max = max(run_against_max, cur_against)

  margin_max_lead = max(seq.margins)
  margin_max_deficit = min(seq.margins)

  smoothed = smooth_momentum(seq.margins, window=window)

  if len(smoothed) < 2:
    decline_sharpness = None
    decline_location_pct = None
  else:
    # Max drawdown: track the running peak of the smoothed series and,
    # at each later point, how far below that peak we've fallen. The
    # largest such fall is the steepest decline, however many points it
    # takes to unfold -- a single-step diff saturates near-instantly
    # here because the series is a window=5 rolling mean of +/-1 deltas,
    # so consecutive values can only differ by up to 2/5.
    running_peak = smoothed[0]
    max_drawdown = 0.0
    trough_idx = None
    for i, value in enumerate(smoothed):
      running_peak = max(running_peak, value)
      drawdown = running_peak - value
      if drawdown > max_drawdown:
        max_drawdown = drawdown
        trough_idx = i

    if trough_idx is not None:
      decline_sharpness = max_drawdown
      decline_location_pct = (trough_idx / (len(smoothed) - 1)) * 100
    else:
      # smoothed momentum never actually dropped below a prior peak
      decline_sharpness = 0.0
      decline_location_pct = None

  return SimpleNamespace(
    run_for_max=int(run_for_max),
    run_against_max=int(run_against_max),
    margin_max_lead=int(margin_max_lead),
    margin_max_deficit=int(margin_max_deficit),
    decline_sharpness=(float(decline_sharpness) if decline_sharpness is not None else None),
    decline_location_pct=(float(decline_location_pct) if decline_location_pct is not None else None),
    attempts=seq.points
  )


# ==============================================================================
# FUNCTION 8: calc_momentum_agg_obj()
# ==============================================================================
def calc_momentum_agg_obj(ppr_df, disp_player, window=5, min_points_per_set=10):
  """
    Aggregate momentum across every set disp_player has played in ppr_df.

    calc_momentum_obj() is inherently single-set (score margins reset each
    set, so "momentum" only means something within one set's boundaries).
    This groups ppr_df into (video_id, set) pairs, runs calc_momentum_obj()
    once per qualifying set, and summarizes each of its six fields as a
    mean/stdev across sets. This is what the metric_dictionary's
    aggregate-level momentum rows (run_for_max_avg/_sd, margin_max_lead_avg/
    _sd, etc.) call -- those rows must not run calc_momentum_obj() directly
    against a multi-set frame (see build_margin_sequence's set_number
    inference, which silently narrows to a single arbitrary set otherwise).

    Args:
        ppr_df (DataFrame): Point-by-point dataframe, any number of
            matches/sets -- the full player (or partner) aggregate frame
        disp_player (str): Player whose side orients each set's margins
        window (int): rolling-average window passed through to
            calc_momentum_obj / smooth_momentum
        min_points_per_set (int): sets with fewer points than this are
            dropped as too short to be a real set (mirrors the >=10-point
            convention used elsewhere to count valid sets)

    Returns:
        Object (SimpleNamespace) with, for each of run_for_max,
        run_against_max, margin_max_lead, margin_max_deficit,
        decline_sharpness, decline_location_pct:
            .<field>_avg (float | None): mean across qualifying sets that
                produced a value for this field; None if fewer than 2
            .<field>_sd (float | None): sample stdev (ddof=1) across the
                same sets; None under the same condition as _avg
        Plus:
            .sets (int): number of qualifying sets calc_momentum_obj() was
                run against -- shared attempts/CI-gating count for all
                twelve _avg/_sd metrics
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()

  player_df = ppr_df[
    (ppr_df['player_a1'].str.strip() == disp_player) |
    (ppr_df['player_a2'].str.strip() == disp_player) |
    (ppr_df['player_b1'].str.strip() == disp_player) |
    (ppr_df['player_b2'].str.strip() == disp_player)
  ]

  fields = [
    'run_for_max', 'run_against_max',
    'margin_max_lead', 'margin_max_deficit',
    'decline_sharpness', 'decline_location_pct',
  ]
  set_values = {field: [] for field in fields}
  num_sets = 0

  for (video_id, set_num), set_df in player_df.groupby(['video_id', 'set']):
    if len(set_df) < min_points_per_set:
      continue

    result = calc_momentum_obj(set_df, disp_player, set_number=int(set_num), window=window)
    if result.attempts == 0:
      continue

    num_sets += 1
    for field in fields:
      value = getattr(result, field)
      if value is not None:
        set_values[field].append(value)

  out = {'sets': num_sets}
  for field in fields:
    vals = set_values[field]
    if len(vals) >= 2:
      out[f'{field}_avg'] = float(np.mean(vals))
      out[f'{field}_sd']  = float(np.std(vals, ddof=1))
    else:
      out[f'{field}_avg'] = float(vals[0]) if vals else None
      out[f'{field}_sd']  = None

  return SimpleNamespace(**out)


# ==============================================================================
# FUNCTION 9: calc_pts_won_obj()
# ==============================================================================
def calc_pts_won_obj(ppr_df, disp_player):
  """
    Calculate disp_player's team's share of points won over ppr_df.

    Uses the actual recorded outcome of each point -- point_outcome_team
    combined with point_outcome -- rather than reconstructing anything from
    a_score/b_score. a_score/b_score only accumulate correctly within a
    single set (they reset to 0-0 at the start of each set), so they can't
    be read directly for an arbitrary window; point_outcome_team is set
    per-row regardless of window, so this works unmodified whether ppr_df
    is a whole match, a single set, a half of a set, or any other
    data_filter slice (zone, phase, etc.) -- same convention already used
    for point_pct in reports_player.py's player_tri_corr.

    point_outcome codes: TSA/FBK/TK win the point for point_outcome_team;
    TSE/FBE/TE lose the point for point_outcome_team (i.e. win it for the
    other side). Every ppr_df row is one point, so team_points +
    opponent_points should equal len(ppr_df).

    Args:
        ppr_df (DataFrame): Point-by-point dataframe, any window/filter
        disp_player (str): Player whose team's share to calculate

    Returns:
        Object (SimpleNamespace) with attributes:
            .per_pts_won (float | None): team_points / (team_points +
                opponent_points); bounded 0-1, 0.5 = even. None if there
                are no classifiable points in the window.
            .team_points (int): points won by disp_player's team
            .opponent_points (int): points won by the opponent
            .attempts (int): team_points + opponent_points
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()

  won_outcomes  = ['TSA', 'FBK', 'TK']
  lost_outcomes = ['TSE', 'FBE', 'TE']

  if (ppr_df is None or len(ppr_df) == 0
      or 'point_outcome_team' not in ppr_df.columns
      or 'point_outcome' not in ppr_df.columns):
    return SimpleNamespace(per_pts_won=None, team_points=0, opponent_points=0, attempts=0)

  player_is_outcome_team = ppr_df['point_outcome_team'].str.contains(
    disp_player, na=False, regex=False
  )

  team_points = len(ppr_df[
    (player_is_outcome_team & ppr_df['point_outcome'].isin(won_outcomes)) |
    (~player_is_outcome_team & ppr_df['point_outcome'].isin(lost_outcomes))
  ])
  opponent_points = len(ppr_df[
    (~player_is_outcome_team & ppr_df['point_outcome'].isin(won_outcomes)) |
    (player_is_outcome_team & ppr_df['point_outcome'].isin(lost_outcomes))
  ])

  attempts = team_points + opponent_points
  per_pts_won = (team_points / attempts) if attempts > 0 else None

  return SimpleNamespace(
    per_pts_won=per_pts_won,
    team_points=int(team_points),
    opponent_points=int(opponent_points),
    attempts=int(attempts)
  )


# ==============================================================================
# RUN METRICS
# ==============================================================================
#
# Derived from the PPR momentum-streak columns (streak_before_a/b,
# streak_after_a/b -- see calc_momentum_streak() in btd_ppr_conversion.py).
#
# These are PAIR-level facts -- a run happens to the team on court, not to
# either individual -- computed and stored per player through this same
# player-keyed engine, the same way run_for_max/run_against_max (momentum,
# above) already are. Both partners on a pair get identical values for
# every metric in this section; there's no separate schema tag for this,
# matching the existing momentum precedent.
#
# Every function below resolves player perspective exactly once, in
# _resolve_run_perspective(), the same way _resolve_my_streak_before()
# (server_functions.py) already does for streak_before alone, then reads
# ONLY the resulting _player columns -- never streak_before_a/b or
# streak_after_a/b directly. That's deliberate: an a/b branch repeated in
# each function below would eventually get inverted in one of them without
# looking wrong, and this way it can't.

RUN_MIN = 3                    # run_burden/run_burden_for: min run length that counts as "a run"
SIDEOUT_MIN_ATTEMPTS = 200     # sideout_rate/sideout_rate_opp: min receiving/serving points
RUN_BURDEN_MIN_ATTEMPTS = 500  # run_burden/run_burden_for/run_burden_resid: min total points
RUN_HAZARD_MIN_ATTEMPTS = 30   # run_hazard_k: min points entered at that streak value

RUN_WON_OUTCOMES = ['TSA', 'FBK', 'TK']
RUN_LOST_OUTCOMES = ['TSE', 'FBE', 'TE']


def _resolve_run_perspective(ppr_df, disp_player):
  """
  Filter ppr_df to disp_player's own points and add on_a/on_b plus the two
  perspective-resolved streak columns every run metric reads --
  streak_before_player/streak_after_player -- so nothing downstream ever
  branches on a/b again.

  Mirrors _resolve_my_streak_before() (server_functions.py), extended to
  streak_after and materialized as on_a/on_b + both _player columns on one
  dataframe rather than a single Series, since the run metrics need several
  of these together.
  """
  disp_player = disp_player.strip()

  df = ppr_df[
    (ppr_df['player_a1'].str.strip() == disp_player) |
    (ppr_df['player_a2'].str.strip() == disp_player) |
    (ppr_df['player_b1'].str.strip() == disp_player) |
    (ppr_df['player_b2'].str.strip() == disp_player)
  ].copy()

  if len(df) == 0:
    for col in ('on_a', 'on_b', 'streak_before_player', 'streak_after_player'):
      df[col] = pd.Series(dtype=float)
    return df

  df['on_a'] = (df['player_a1'].str.strip() == disp_player) | (df['player_a2'].str.strip() == disp_player)
  df['on_b'] = (df['player_b1'].str.strip() == disp_player) | (df['player_b2'].str.strip() == disp_player)
  df['streak_before_player'] = np.where(
    df['on_a'], df['streak_before_a'],
    np.where(df['on_b'], df['streak_before_b'], np.nan)
  )
  df['streak_after_player'] = np.where(
    df['on_a'], df['streak_after_a'],
    np.where(df['on_b'], df['streak_after_b'], np.nan)
  )
  return df


def _resolve_serve_side(df):
  """
  Which side served, resolved by matching serve_player against the four
  player columns -- not against on_a/on_b, which is the RECEIVING player's
  own side, resolved separately above. Returns (own_pair_served,
  opp_pair_served, unresolved_count); points where serve_player doesn't
  match any of the four columns are excluded from both booleans and counted
  in unresolved_count so the caller can log it.
  """
  serve_is_a = (df['serve_player'] == df['player_a1']) | (df['serve_player'] == df['player_a2'])
  serve_is_b = (df['serve_player'] == df['player_b1']) | (df['serve_player'] == df['player_b2'])
  serve_resolved = serve_is_a | serve_is_b

  own_pair_served = (df['on_a'] & serve_is_a) | (df['on_b'] & serve_is_b)
  opp_pair_served = serve_resolved & ~own_pair_served
  unresolved_count = int((~serve_resolved).sum())

  return own_pair_served, opp_pair_served, unresolved_count


def _pair_won_point(df, disp_player):
  """
  Same win/loss convention as calc_pts_won_obj() above: point_outcome_team
  combined with point_outcome, not streak/score reconstruction, so this
  works on any row subset. Returns (pair_won, classifiable) boolean Series.
  """
  player_is_outcome_team = df['point_outcome_team'].str.contains(disp_player, na=False, regex=False)
  pair_won = (
    (player_is_outcome_team & df['point_outcome'].isin(RUN_WON_OUTCOMES)) |
    (~player_is_outcome_team & df['point_outcome'].isin(RUN_LOST_OUTCOMES))
  )
  classifiable = df['point_outcome'].isin(RUN_WON_OUTCOMES + RUN_LOST_OUTCOMES)
  return pair_won, classifiable


def sideout_rate_obj(ppr_df, disp_player):
  """
    Of the points the OPPONENT served, the fraction disp_player's pair won.

    Returns:
        Object (SimpleNamespace):
            .sideout_rate (float | None): 0-1; None if there are no
                classifiable receiving points
            .attempts (int): classifiable receiving points (opponent
                served, point_outcome resolves a winner) -- gate at
                SIDEOUT_MIN_ATTEMPTS
            .unresolved_server (int): points skipped (and logged) because
                serve_player didn't match any of player_a1/a2/b1/b2
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()
  df = _resolve_run_perspective(ppr_df, disp_player)
  if len(df) == 0:
    return SimpleNamespace(sideout_rate=None, attempts=0, unresolved_server=0)

  _, opp_pair_served, unresolved_count = _resolve_serve_side(df)
  if unresolved_count:
    log_error(
      f"sideout_rate: {unresolved_count} of {len(df)} points for {disp_player} "
      f"skipped -- serve_player did not match player_a1/a2/b1/b2"
    )

  receiving = df[opp_pair_served]
  pair_won, classifiable = _pair_won_point(receiving, disp_player)
  attempts = int(classifiable.sum())
  sideout_rate = float(pair_won[classifiable].mean()) if attempts > 0 else None

  return SimpleNamespace(
    sideout_rate=sideout_rate,
    attempts=attempts,
    unresolved_server=unresolved_count
  )


def sideout_rate_opp_obj(ppr_df, disp_player):
  """
    Of the points THIS pair served, the fraction the opponent won.

    Returns:
        Object (SimpleNamespace):
            .sideout_rate_opp (float | None): 0-1; None if there are no
                classifiable serving points
            .attempts (int): classifiable serving points -- gate at
                SIDEOUT_MIN_ATTEMPTS
            .unresolved_server (int): points skipped (and logged) because
                serve_player didn't match any of player_a1/a2/b1/b2
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()
  df = _resolve_run_perspective(ppr_df, disp_player)
  if len(df) == 0:
    return SimpleNamespace(sideout_rate_opp=None, attempts=0, unresolved_server=0)

  own_pair_served, _, unresolved_count = _resolve_serve_side(df)
  if unresolved_count:
    log_error(
      f"sideout_rate_opp: {unresolved_count} of {len(df)} points for {disp_player} "
      f"skipped -- serve_player did not match player_a1/a2/b1/b2"
    )

  serving = df[own_pair_served]
  pair_won, classifiable = _pair_won_point(serving, disp_player)
  attempts = int(classifiable.sum())
  sideout_rate_opp = float((~pair_won[classifiable]).mean()) if attempts > 0 else None

  return SimpleNamespace(
    sideout_rate_opp=sideout_rate_opp,
    attempts=attempts,
    unresolved_server=unresolved_count
  )


def _add_run_final_length(df):
  """
  Add 'run_final_length': for every row, the largest abs(streak_after_player)
  reached inside the run (maximal same-sign block of streak_after_player)
  that row belongs to, computed separately within each (video_id, set) --
  a run never crosses a set boundary since the streak resets to 0 there.

  streak_update() (btd_ppr_conversion.py) only ever increments magnitude
  while a run's sign holds and resets to 1 the instant it flips, so a run's
  values are non-decreasing in magnitude across its own span -- its final
  length is always its last value, and .max() over the block reads the same
  thing while being simpler to reason about here.
  """
  df = df.copy()
  df['run_final_length'] = np.nan

  for _, idx in df.groupby(['video_id', 'set'], sort=False).groups.items():
    group = df.loc[idx].sort_index()
    signs = np.sign(group['streak_after_player'])
    new_run = signs.ne(signs.shift(1))
    run_id = new_run.cumsum()
    final_length = group['streak_after_player'].abs().groupby(run_id).transform('max')
    df.loc[group.index, 'run_final_length'] = final_length

  return df


def run_burden_obj(ppr_df, disp_player):
  """
    Points lost/won inside runs of RUN_MIN+ consecutive points, per 100
    points.

    A run is a maximal block of consecutive points, within one set, where
    sign(streak_after_player) doesn't flip; its final length is the largest
    abs(streak_after_player) reached inside it (_add_run_final_length).

    Returns:
        Object (SimpleNamespace):
            .run_burden (float | None): 100 * (points with
                streak_after_player < 0 inside a run whose final length >=
                RUN_MIN) / total points; None if there are no points at all
            .run_burden_for (float | None): same, streak_after_player > 0
            .attempts (int): total points -- gate at RUN_BURDEN_MIN_ATTEMPTS
                for both run_burden and run_burden_for
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()
  df = _resolve_run_perspective(ppr_df, disp_player)
  total_points = len(df)
  if total_points == 0:
    return SimpleNamespace(run_burden=None, run_burden_for=None, attempts=0)

  df = _add_run_final_length(df)
  in_long_run = df['run_final_length'] >= RUN_MIN

  burden_points = int(((df['streak_after_player'] < 0) & in_long_run).sum())
  for_points = int(((df['streak_after_player'] > 0) & in_long_run).sum())

  return SimpleNamespace(
    run_burden=float(burden_points / total_points * 100.0),
    run_burden_for=float(for_points / total_points * 100.0),
    attempts=int(total_points)
  )


# rows = own side-out rate, cols = opponent side-out rate
RUN_BURDEN_EXP_BREAKPOINTS = [0.50, 0.54, 0.58, 0.62, 0.66, 0.70, 0.74]
RUN_BURDEN_EXP_GRID = np.array([
  [23.6, 24.8, 25.5, 26.6, 27.0, 27.9, 28.4],
  [20.0, 21.0, 21.7, 22.4, 23.4, 23.8, 24.3],
  [16.7, 17.6, 18.3, 18.6, 19.2, 19.8, 20.5],
  [13.7, 14.5, 14.8, 15.4, 16.0, 16.4, 16.6],
  [11.0, 11.7, 11.9, 12.5, 12.7, 13.2, 13.5],
  [ 8.6,  9.0,  9.1,  9.5,  9.8, 10.4, 10.4],
  [ 6.3,  6.6,  7.1,  7.2,  7.4,  7.6,  8.0],
])


def _grid_bracket(value, breakpoints):
  """
  Clamp value into [breakpoints[0], breakpoints[-1]] (rather than
  extrapolate) and return (lo_index, hi_index, fraction from lo to hi) for
  linear interpolation. fraction lands on exactly 0.0 or 1.0 at any
  breakpoint, including the last one, so a lookup at a grid point returns
  that cell with no interpolation drift.
  """
  clamped = min(max(value, breakpoints[0]), breakpoints[-1])
  hi = 1
  while hi < len(breakpoints) - 1 and breakpoints[hi] < clamped:
    hi += 1
  lo = hi - 1
  span = breakpoints[hi] - breakpoints[lo]
  fraction = (clamped - breakpoints[lo]) / span if span else 0.0
  return lo, hi, fraction


def _run_burden_exp_lookup(own_sideout, opp_sideout):
  """Bilinear interpolation of RUN_BURDEN_EXP_GRID at (own_sideout,
  opp_sideout), clamped to the grid range rather than extrapolated."""
  row_lo, row_hi, row_frac = _grid_bracket(own_sideout, RUN_BURDEN_EXP_BREAKPOINTS)
  col_lo, col_hi, col_frac = _grid_bracket(opp_sideout, RUN_BURDEN_EXP_BREAKPOINTS)

  top = (RUN_BURDEN_EXP_GRID[row_lo, col_lo]
         + (RUN_BURDEN_EXP_GRID[row_lo, col_hi] - RUN_BURDEN_EXP_GRID[row_lo, col_lo]) * col_frac)
  bottom = (RUN_BURDEN_EXP_GRID[row_hi, col_lo]
            + (RUN_BURDEN_EXP_GRID[row_hi, col_hi] - RUN_BURDEN_EXP_GRID[row_hi, col_lo]) * col_frac)
  return float(top + (bottom - top) * row_frac)


def run_burden_exp_obj(ppr_df, disp_player):
  """
    Expected run_burden looked up from RUN_BURDEN_EXP_GRID given this
    pair's own and opponent side-out rates, bilinearly interpolated.

    Calls sideout_rate_obj/sideout_rate_opp_obj directly as plain functions
    rather than reading their metric_dictionary rows' results, so this
    bypasses the dictionary engine's generic attempts_path gating --
    SIDEOUT_MIN_ATTEMPTS is re-checked explicitly here for that reason
    (both side-out rates must be present, per spec).

    Returns:
        Object (SimpleNamespace):
            .run_burden_exp (float | None): None unless both side-out
                rates clear SIDEOUT_MIN_ATTEMPTS
            .attempts (int): the smaller of the two side-out attempt
                counts, exposed for explainability even when None (e.g.
                AI narration can say why it's missing)
  """
  from types import SimpleNamespace

  so = sideout_rate_obj(ppr_df, disp_player)
  so_opp = sideout_rate_opp_obj(ppr_df, disp_player)
  attempts = min(so.attempts, so_opp.attempts)

  if (so.sideout_rate is None or so_opp.sideout_rate_opp is None
      or so.attempts < SIDEOUT_MIN_ATTEMPTS or so_opp.attempts < SIDEOUT_MIN_ATTEMPTS):
    return SimpleNamespace(run_burden_exp=None, attempts=attempts)

  value = _run_burden_exp_lookup(so.sideout_rate, so_opp.sideout_rate_opp)
  return SimpleNamespace(run_burden_exp=value, attempts=attempts)


def run_burden_resid_obj(ppr_df, disp_player):
  """
    run_burden - run_burden_exp -- the part of a pair's run_burden not
    explained by how often each side sides out. This is the one meant for
    coaching output; raw run_burden is confounded by opponent strength (see
    run_burden_obj).

    Returns:
        Object (SimpleNamespace):
            .run_burden_resid (float | None): None unless both run_burden
                and run_burden_exp are available
            .attempts (int): total points (same denominator as
                run_burden) -- gate at RUN_BURDEN_MIN_ATTEMPTS
  """
  from types import SimpleNamespace

  rb = run_burden_obj(ppr_df, disp_player)
  exp = run_burden_exp_obj(ppr_df, disp_player)

  if rb.run_burden is None or exp.run_burden_exp is None:
    return SimpleNamespace(run_burden_resid=None, attempts=rb.attempts)

  return SimpleNamespace(
    run_burden_resid=float(rb.run_burden - exp.run_burden_exp),
    attempts=rb.attempts
  )


def run_hazard_obj(ppr_df, disp_player, k):
  """
    Of the points entered at streak_before_player == -k, the percent where
    streak_after_player < 0 (the run extended rather than broke).

    Args:
        k (int): magnitude of the losing streak entering the point (pass
            1, 2, or 3 for run_hazard_1/2/3)

    Returns:
        Object (SimpleNamespace):
            .run_hazard (float | None): 0-100; None if no points were
                entered at streak_before_player == -k
            .attempts (int): points entered at that streak value -- gate
                at RUN_HAZARD_MIN_ATTEMPTS
  """
  from types import SimpleNamespace

  disp_player = disp_player.strip()
  df = _resolve_run_perspective(ppr_df, disp_player)
  entered = df[df['streak_before_player'] == -k]
  attempts = len(entered)
  if attempts == 0:
    return SimpleNamespace(run_hazard=None, attempts=0)

  extended = int((entered['streak_after_player'] < 0).sum())
  return SimpleNamespace(run_hazard=float(extended / attempts * 100.0), attempts=int(attempts))


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

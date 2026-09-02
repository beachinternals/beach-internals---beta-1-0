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

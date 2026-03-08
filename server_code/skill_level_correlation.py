"""
Skill Level Correlation Analysis
=================================
Calculates Fisher Z-weighted correlations between set-level metrics
and point_pct (points won %) across all defined skill levels.

New Anvil data tables required:
  skill_level_def     — defines each skill level and its player list
  skill_level_results — stores the output of each analysis run

How to call from browser:
  anvil.server.call('run_skill_level_correlation_analysis')
  This launches a background task and returns a task_id for polling.

Author: Beach Volleyball Analytics
"""

import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

from .logger_utils import log_debug, log_info, log_error, log_critical
from server_functions import (
get_ppr_data,
monitor_performance,
MONITORING_LEVEL_OFF,
MONITORING_LEVEL_CRITICAL,
MONITORING_LEVEL_IMPORTANT,
MONITORING_LEVEL_DETAILED,
MONITORING_LEVEL_VERBOSE
)
from generate_set_level_metrics import (
get_core_metrics_from_dictionary,
calculate_metric_for_set,
get_set_metadata
)


# ============================================================================
# ANALYSIS METRICS BUILDER
# ============================================================================

def get_analysis_metrics():
  """
  Return the full list of metric rows to calculate per set.

  Combines:
    1. All metric_dictionary rows where metric_category = 'core'
    2. Any additional rows listed in EXTRA_METRIC_IDS (by metric_id),
       regardless of their category.

  Returns:
    list of metric_dictionary row objects (duplicates removed)
  """
  core_metrics = get_core_metrics_from_dictionary()
  core_ids     = {m['metric_id'] for m in core_metrics}

  extra_metrics = []
  for metric_id in EXTRA_METRIC_IDS:
    if metric_id in core_ids:
      log_debug(f"Extra metric '{metric_id}' already in core — skipping duplicate")
      continue
    try:
      row = app_tables.metric_dictionary.get(metric_id=metric_id)
      if row:
        extra_metrics.append(row)
        log_debug(f"Added extra metric: {metric_id}")
      else:
        log_error(f"Extra metric '{metric_id}' not found in metric_dictionary — skipping")
    except Exception as e:
      log_error(f"Error fetching extra metric '{metric_id}': {e}")

  all_metrics = core_metrics + extra_metrics
  log_info(f"Analysis metrics: {len(core_metrics)} core + {len(extra_metrics)} extra "
           f"= {len(all_metrics)} total")
  return all_metrics


# ============================================================================
# CONSTANTS
# ============================================================================

# Point outcomes that count as a WIN for the player's team
WON_OUTCOMES  = ['TSA', 'FBK', 'TK']
LOST_OUTCOMES = ['TSE', 'FBE', 'TE']

# Minimum sets needed per player for reliable correlation
# 500 points / 37 points per set ≈ 13-14 sets minimum
MIN_POINTS_DEFAULT = 500

# De-identified leagues — ppr uses player_uuid instead of "team number shortname"
# Add new leagues here as they are de-identified
DEIDENTIFIED_LEAGUES = [
  {'league': 'NCAA', 'gender': 'W', 'year': '2026'},
  {'league': 'NCAA', 'gender': 'W', 'year': '2025'},
  {'league': 'NCAA', 'gender': 'W', 'year': '2024'},
  {'league': 'FIVB', 'gender': 'W', 'year': '2024'},
]

# ---------------------------------------------------------------------------
# EXTRA METRICS
# ---------------------------------------------------------------------------
# Metric IDs from metric_dictionary that are NOT category='core' but we still
# want included in the skill-level correlation analysis.
# Add new metric_ids here — no other code changes needed.
# ---------------------------------------------------------------------------
EXTRA_METRIC_IDS = [
  'tcr_r',       # TCR when receiving serve
  'tcr_s',       # TCR when serving
  't_eff_r',     # Transition efficiency when receiving
  't_eff_s',     # Transition efficiency when serving
  'opp_fbhe',    # Opponent first-ball hitting efficiency (defense measure)
]


# ============================================================================
# FISHER Z HELPERS
# ============================================================================

def fisher_z(r):
  """
  Convert a Pearson correlation coefficient to Fisher Z score.
  Clips r to (-0.9999, 0.9999) to avoid arctanh blowing up at ±1.
  """
  r_clipped = float(np.clip(r, -0.9999, 0.9999))
  return float(np.arctanh(r_clipped))


def z_to_r(z):
  """Convert a Fisher Z score back to a Pearson correlation coefficient."""
  return float(np.tanh(z))


def fisher_z_weighted_mean(correlations, weights):
  """
  Calculate a Fisher Z weighted mean correlation.

  Args:
    correlations : list of float  — Pearson r values per player
    weights      : list of float  — weight per player (typically n_sets)

  Returns:
    float: weighted mean correlation (back in r space), or None if no valid data
  """
  valid = [
    (r, w) for r, w in zip(correlations, weights)
    if r is not None and w is not None and not np.isnan(r) and w > 0
  ]
  if not valid:
    return None

  z_values = [fisher_z(r) * w for r, w in valid]
  total_weight = sum(w for _, w in valid)

  if total_weight == 0:
    return None

  return z_to_r(sum(z_values) / total_weight)


# ============================================================================
# PLAYER IDENTIFIER HELPERS
# ============================================================================

def is_deidentified(league, gender, year):
  """
  Return True if this league/gender/year uses player_uuid in ppr_df
  instead of 'team number shortname'.
  """
  year_str = str(year)
  for d in DEIDENTIFIED_LEAGUES:
    if d['league'] == league and d['gender'] == gender and d['year'] == year_str:
      return True
  return False


def resolve_player_identifier(p_row):
  """
  Given a master_player row, return the string used to identify
  this player in ppr_df — either player_uuid or 'team number shortname'.

  Args:
    p_row: Anvil row from master_player table

  Returns:
    str: player identifier for filtering ppr_df, or None on failure
  """
  league = p_row['league']
  gender = p_row['gender']
  year   = str(p_row['year'])

  if is_deidentified(league, gender, year):
    uid = p_row['player_uuid']
    if uid:
      log_debug(f"De-id league: using uuid {uid} for {p_row['shortname']}")
      return uid
    else:
      log_error(f"No player_uuid for {p_row['team']} {p_row['number']} {p_row['shortname']}")
      return None
  else:
    identifier = f"{p_row['team']} {p_row['number']} {p_row['shortname']}"
    log_debug(f"Named league: using '{identifier}'")
    return identifier


# ============================================================================
# PPR DATA FETCHING
# ============================================================================

def get_ppr_for_player_row(p_row):
  """
  Fetch the ppr_df for the league/gender/year of a master_player row.
  Always fetches as team='League' to get all data.

  Args:
    p_row: Anvil row from master_player

  Returns:
    pd.DataFrame or None
  """
  league = p_row['league']
  gender = p_row['gender']
  year   = str(p_row['year'])

  log_debug(f"Fetching ppr for {league} | {gender} | {year} | League")

  try:
    ppr_df = get_ppr_data(league, gender, year, 'League', scout=False)
    if ppr_df is None or (isinstance(ppr_df, list)):
      log_error(f"get_ppr_data returned no data for {league}|{gender}|{year}")
      return None
    if ppr_df.empty:
      log_error(f"ppr_df is empty for {league}|{gender}|{year}")
      return None
    log_info(f"Fetched {len(ppr_df)} rows for {league}|{gender}|{year}")
    return ppr_df
  except Exception as e:
    log_error(f"Error fetching ppr for {league}|{gender}|{year}: {e}")
    return None


# ============================================================================
# SET-LEVEL DATA BUILDER
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def build_set_level_df_for_player(ppr_df, player_id, min_points):
  """
  Build a flat DataFrame with one row per set for this player.
  Each row has all core metric values + point_pct (points won %).

  Args:
    ppr_df     : full ppr DataFrame for this league/year
    player_id  : string identifier (uuid or 'team number shortname')
    min_points : minimum total points for player to be included

  Returns:
    dict with keys:
      'set_df'        : pd.DataFrame (one row per set) or None
      'n_points'      : total points found for this player
      'n_sets'        : sets included
      'n_sets_excl'   : sets excluded (too few points in set)
      'excluded'      : True if player excluded due to min_points
  """
  result = {
    'set_df': None,
    'n_points': 0,
    'n_sets': 0,
    'n_sets_excl': 0,
    'excluded': False
  }

  # Filter ppr_df to rows involving this player
  player_df = ppr_df[
    (ppr_df['player_a1'] == player_id) |
    (ppr_df['player_a2'] == player_id) |
    (ppr_df['player_b1'] == player_id) |
    (ppr_df['player_b2'] == player_id)
    ]

  if player_df.empty:
    log_info(f"No ppr rows found for player {player_id}")
    result['excluded'] = True
    return result

  n_points = len(player_df)
  result['n_points'] = n_points

  if n_points < min_points:
    log_info(f"Player {player_id}: {n_points} points < {min_points} minimum — excluded")
    result['excluded'] = True
    return result

  log_info(f"Player {player_id}: {n_points} points — proceeding")

  # Get metrics to calculate: core + any extras defined in EXTRA_METRIC_IDS
  core_metrics = get_analysis_metrics()
  if not core_metrics:
    log_error("No metrics found — check metric_dictionary and EXTRA_METRIC_IDS")
    return result

  # Find all unique (video_id, set) combinations for this player
  set_combos = player_df.groupby(['video_id', 'set']).size().reset_index(name='pt_count')

  rows = []

  for _, combo_row in set_combos.iterrows():
    video_id  = combo_row['video_id']
    set_num   = combo_row['set']
    pt_count  = combo_row['pt_count']

    # Skip sets that are too short to be real sets
    if pt_count < 10:
      log_debug(f"Skipping set {video_id}-{set_num}: only {pt_count} points")
      result['n_sets_excl'] += 1
      continue

    # Filter to this set
    set_df = player_df[
      (player_df['video_id'] == video_id) &
      (player_df['set']      == set_num)
      ]

    # ── Calculate point_pct ──────────────────────────────────────────────────
    if 'point_outcome_team' not in set_df.columns or 'point_outcome' not in set_df.columns:
      log_error(f"Missing point_outcome columns in set {video_id}-{set_num}")
      result['n_sets_excl'] += 1
      continue

    player_is_outcome_team = set_df['point_outcome_team'].str.contains(
      player_id, na=False, regex=False
    )
    points_won = len(set_df[
      (player_is_outcome_team  & set_df['point_outcome'].isin(WON_OUTCOMES)) |
      (~player_is_outcome_team & set_df['point_outcome'].isin(LOST_OUTCOMES))
      ])
    total_pts  = len(set_df)
    point_pct  = points_won / total_pts if total_pts > 0 else None

    if point_pct is None:
      result['n_sets_excl'] += 1
      continue

    # ── Calculate core metrics for this set ─────────────────────────────────
    row = {
      'video_id'    : video_id,
      'set'         : set_num,
      'total_points': total_pts,
      'points_won'  : points_won,
      'point_pct'   : point_pct,
    }

    for metric_row in core_metrics:
      metric_result = calculate_metric_for_set(metric_row, set_df, player_id)
      if metric_result and metric_result['value'] is not None:
        row[metric_result['metric_id']] = metric_result['value']

    rows.append(row)
    result['n_sets'] += 1

  if not rows:
    log_info(f"No valid sets built for player {player_id}")
    return result

  set_df_flat = pd.DataFrame(rows)
  result['set_df'] = set_df_flat
  log_info(f"Player {player_id}: built set_df with {len(set_df_flat)} sets, "
           f"{len(set_df_flat.columns)} columns")
  return result


# ============================================================================
# PLAYER-LEVEL CORRELATIONS
# ============================================================================

def calculate_player_correlations(set_df):
  """
  Calculate Pearson correlation of every metric column vs point_pct
  for a single player's set-level DataFrame.

  Skips _n columns (attempt counts) and non-numeric columns.

  Args:
    set_df: pd.DataFrame with one row per set

  Returns:
    dict: { metric_id: {'r': float, 'n': int} }
         r = correlation, n = number of sets used
  """
  results = {}

  if set_df is None or set_df.empty:
    return results

  if 'point_pct' not in set_df.columns:
    log_error("point_pct column missing from set_df")
    return results

  # Keep only numeric columns
  numeric_df = set_df.select_dtypes(include=['float64', 'int64', 'float32', 'int32']).copy()

  # Drop housekeeping columns
  drop_cols = ['point_pct', 'points_won', 'total_points', 'video_id', 'set']
  numeric_df = numeric_df.drop(columns=[c for c in drop_cols if c in numeric_df.columns])

  # Drop _n columns (attempt counts — not meaningful to correlate)
  n_cols = [c for c in numeric_df.columns if c.endswith('_n')]
  numeric_df = numeric_df.drop(columns=n_cols)

  # Drop zero-variance columns
  numeric_df = numeric_df.loc[:, numeric_df.std() > 0]

  point_pct = set_df['point_pct'].fillna(set_df['point_pct'].mean())

  for col in numeric_df.columns:
    series = numeric_df[col].fillna(numeric_df[col].mean())
    valid  = pd.concat([series, point_pct], axis=1).dropna()

    if len(valid) < 4:   # Need at least 4 sets for any meaningful correlation
      continue

    try:
      r = float(valid[col].corr(valid['point_pct']))
      if not np.isnan(r):
        results[col] = {'r': round(r, 4), 'n': len(valid)}
    except Exception as e:
      log_debug(f"Correlation failed for {col}: {e}")

  return results


# ============================================================================
# LEVEL-LEVEL AGGREGATION
# ============================================================================

def aggregate_level_correlations(player_corr_list):
  """
  Aggregate per-player correlations into level-mean correlations
  using Fisher Z weighting by number of sets.

  Args:
    player_corr_list: list of dicts from calculate_player_correlations()
                      Each dict: { metric_id: {'r': float, 'n': int} }

  Returns:
    dict: { metric_id: {
              'mean_r'         : float,   Fisher Z weighted mean
              'n_players'      : int,     players contributing
              'mean_n_sets'    : float,   average sets per player
              'total_sets'     : int,     total sets across all players
            } }
  """
  # Collect all metric ids seen
  all_metrics = set()
  for pc in player_corr_list:
    all_metrics.update(pc.keys())

  results = {}

  for metric_id in all_metrics:
    correlations = []
    weights      = []
    n_sets_list  = []

    for pc in player_corr_list:
      if metric_id in pc:
        r = pc[metric_id]['r']
        n = pc[metric_id]['n']
        if r is not None and n is not None and n > 0:
          correlations.append(r)
          weights.append(n)       # weight by number of sets
          n_sets_list.append(n)

    if not correlations:
      continue

    mean_r = fisher_z_weighted_mean(correlations, weights)
    if mean_r is None:
      continue

    results[metric_id] = {
      'mean_r'       : round(mean_r, 4),
      'n_players'    : len(correlations),
      'mean_n_sets'  : round(sum(n_sets_list) / len(n_sets_list), 1),
      'total_sets'   : sum(n_sets_list),
    }

  return results


# ============================================================================
# MAIN ANALYSIS FUNCTION — one skill level
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def analyse_skill_level(level_row, run_id):
  """
  Run the full correlation analysis for one skill level.

  Args:
    level_row : Anvil row from skill_level_def table
    run_id    : str — unique ID for this analysis run

  Returns:
    dict: {
      'level_id'         : int,
      'level_name'       : str,
      'n_players_used'   : int,
      'n_players_excl'   : int,
      'metric_results'   : dict  from aggregate_level_correlations()
      'player_summaries' : list  — one dict per player
      'mean_values'      : dict  { metric_id: mean_value }
    }
  """
  level_id   = level_row['level_id']
  level_name = level_row['level_name']
  min_points = level_row['min_points'] or MIN_POINTS_DEFAULT

  log_info(f"=== Analysing level {level_id}: {level_name} ===")
  log_info(f"Min points threshold: {min_points}")

  player_rows = list(level_row['player_list'])  # linked master_player rows

  if not player_rows:
    log_error(f"No players defined for level {level_id} ({level_name})")
    return None

  log_info(f"Level {level_id}: {len(player_rows)} players defined")

  # ── Cache ppr_df by league/gender/year to avoid re-fetching ─────────────
  ppr_cache = {}   # key: "league|gender|year"

  player_corr_list  = []   # one entry per included player
  player_summaries  = []   # human-readable summary per player
  n_excluded        = 0

  # ── Per-player value lists for mean_values ───────────────────────────────
  metric_value_lists = {}  # { metric_id: [values] }

  for p_row in player_rows:
    league = p_row['league']
    gender = p_row['gender']
    year   = str(p_row['year'])
    cache_key = f"{league}|{gender}|{year}"

    # Fetch ppr_df (use cache if already loaded)
    if cache_key not in ppr_cache:
      ppr_df = get_ppr_for_player_row(p_row)
      ppr_cache[cache_key] = ppr_df
    else:
      ppr_df = ppr_cache[cache_key]

    if ppr_df is None:
      log_error(f"No ppr data for {cache_key} — skipping player {p_row['shortname']}")
      n_excluded += 1
      continue

    # Resolve identifier (uuid or name string)
    player_id = resolve_player_identifier(p_row)
    if not player_id:
      n_excluded += 1
      continue

    player_label = f"{p_row['team']} {p_row['number']} {p_row['shortname']}"

    # Build set-level DataFrame for this player
    build_result = build_set_level_df_for_player(ppr_df, player_id, min_points)

    if build_result['excluded'] or build_result['set_df'] is None:
      log_info(f"Player {player_label} excluded "
               f"(points={build_result['n_points']}, min={min_points})")
      n_excluded += 1
      player_summaries.append({
        'player'   : player_label,
        'league'   : league,
        'year'     : year,
        'status'   : 'excluded',
        'n_points' : build_result['n_points'],
        'n_sets'   : 0,
        'reason'   : 'below_min_points' if build_result['n_points'] < min_points else 'no_data'
      })
      continue

    set_df     = build_result['set_df']
    n_sets     = build_result['n_sets']
    n_points   = build_result['n_points']

    log_info(f"Player {player_label}: {n_sets} sets, {n_points} points")

    # Calculate per-player correlations
    player_corr = calculate_player_correlations(set_df)

    if not player_corr:
      log_info(f"Player {player_label}: no correlations calculated — excluding")
      n_excluded += 1
      continue

    player_corr_list.append(player_corr)

    # Collect metric values for level means
    metric_cols = [c for c in set_df.columns
                   if c not in ('video_id','set','total_points','points_won','point_pct')
                   and not c.endswith('_n')]
    for col in metric_cols:
      vals = set_df[col].dropna().tolist()
      if vals:
        if col not in metric_value_lists:
          metric_value_lists[col] = []
        metric_value_lists[col].extend(vals)

    player_summaries.append({
      'player'       : player_label,
      'league'       : league,
      'year'         : year,
      'status'       : 'included',
      'n_points'     : n_points,
      'n_sets'       : n_sets,
      'n_metrics'    : len(player_corr),
      'correlations' : player_corr   # per-metric {r, n}
    })

  log_info(f"Level {level_id}: {len(player_corr_list)} players included, "
           f"{n_excluded} excluded")

  if not player_corr_list:
    log_error(f"Level {level_id}: no players with valid data — cannot aggregate")
    return None

  # ── Aggregate to level means ─────────────────────────────────────────────
  metric_results = aggregate_level_correlations(player_corr_list)

  # ── Mean metric values across all included players ───────────────────────
  mean_values = {
    metric_id: round(float(np.mean(vals)), 4)
    for metric_id, vals in metric_value_lists.items()
    if vals
  }

  log_info(f"Level {level_id}: aggregated {len(metric_results)} metrics")

  return {
    'level_id'         : level_id,
    'level_name'       : level_name,
    'n_players_used'   : len(player_corr_list),
    'n_players_excl'   : n_excluded,
    'metric_results'   : metric_results,
    'player_summaries' : player_summaries,
    'mean_values'      : mean_values,
  }


# ============================================================================
# RESULTS WRITER
# ============================================================================

def save_level_results(level_result, run_id):
  """
  Save the aggregated results for one skill level to skill_level_results table.
  One row per metric per level.

  skill_level_results columns needed:
    run_id, level_id, level_name, metric_id,
    mean_correlation, mean_value,
    n_players, n_players_excluded, mean_n_sets, total_sets,
    created_at
  """
  if not level_result:
    return

  level_id   = level_result['level_id']
  level_name = level_result['level_name']
  n_excl     = level_result['n_players_excl']
  created_at = datetime.now()

  metric_results = level_result['metric_results']
  mean_values    = level_result['mean_values']

  rows_saved = 0

  for metric_id, stats in metric_results.items():
    try:
      app_tables.skill_level_results.add_row(
        run_id            = run_id,
        level_id          = level_id,
        level_name        = level_name,
        metric_id         = metric_id,
        mean_correlation  = stats['mean_r'],
        mean_value        = mean_values.get(metric_id),
        n_players         = stats['n_players'],
        n_players_excluded= n_excl,
        mean_n_sets       = stats['mean_n_sets'],
        total_sets        = stats['total_sets'],
        created_at        = created_at
      )
      rows_saved += 1
    except Exception as e:
      log_error(f"Error saving result for {metric_id} level {level_id}: {e}")

  log_info(f"Level {level_id}: saved {rows_saved} metric result rows")




# ============================================================================
# POINT-LEVEL CORRELATION ANALYSIS
# ============================================================================
#
# Answers: "What physical/tactical execution on individual points wins first-
# ball (FBK) or transition (TK) opportunities?"
#
# Two separate correlation targets:
#   first_ball : FBK=+1, FBE=-1  (first ball outcome)
#   transition : TK=+1,  TE=-1   (transition outcome)
#
# team parameter is explicit throughout so this code can be used for both:
#   team='League'  — skill-level / research analysis (all data)
#   team='FSU'     — player-specific analysis (team-only data)
# ============================================================================

# Physical measurement columns in ppr_df to correlate against point outcome.
# These are raw per-point measurements — no aggregation.
POINT_LEVEL_COLUMNS = [
  'serve_dist', 'serve_dur', 'serve_speed', 'serve_angle', 'serve_height',
  'pass_dist',  'pass_dur',  'pass_speed',  'pass_angle',  'pass_height',
  'pass_rtg_btd', 'pass_oos',
  'set_dist',   'set_dur',   'set_speed',   'set_angle',   'set_height',
  'att_dist',   'att_dur',   'att_speed',   'att_angle',
  'att_height', 'att_touch_height',
]

# Minimum first-ball / transition points for a player to be included
# Lower than set-level minimum because we're working with raw points not sets
MIN_FB_POINTS    = 50   # minimum FBK+FBE rows
MIN_TRANS_POINTS = 30   # minimum TK+TE rows (transition is rarer)


@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def build_point_level_df(ppr_df, player_id, outcome_type):
  """
  Build a flat DataFrame of individual points for point-level correlation.

  Args:
    ppr_df       : full ppr DataFrame for this league/year (already fetched)
    player_id    : string identifier (uuid or 'team number shortname')
    outcome_type : 'first_ball' — FBK/FBE rows only
                   'transition' — TK/TE rows only

  Returns:
    dict with keys:
      'point_df'   : pd.DataFrame (one row per qualifying point) or None
      'n_points'   : number of qualifying points found
      'excluded'   : True if below minimum threshold
  """
  result = {'point_df': None, 'n_points': 0, 'excluded': False}

  # Filter to rows where this player is the attacker
  # For first ball: att_player must be this player AND outcome is FBK or FBE
  # For transition: same — TK and TE are outcomes where this player attacked
  if outcome_type == 'first_ball':
    target_outcomes = ['FBK', 'FBE']
    encode_map      = {'FBK': 1, 'FBE': -1}
    min_pts         = MIN_FB_POINTS
  elif outcome_type == 'transition':
    target_outcomes = ['TK', 'TE']
    encode_map      = {'TK': 1, 'TE': -1}
    min_pts         = MIN_TRANS_POINTS
  else:
    log_error(f"Unknown outcome_type: {outcome_type}")
    return result

  # Filter: player must be the attacker AND outcome must be one of our targets
  if 'att_player' not in ppr_df.columns:
    log_error("att_player column missing from ppr_df")
    result['excluded'] = True
    return result

  player_df = ppr_df[
    (ppr_df['att_player'] == player_id) &
    (ppr_df['point_outcome'].isin(target_outcomes))
    ].copy()

  n_points = len(player_df)
  result['n_points'] = n_points

  if n_points < min_pts:
    log_debug(f"Player {player_id}: {n_points} {outcome_type} points < {min_pts} — excluded")
    result['excluded'] = True
    return result

  # Encode outcome as numeric
  player_df['outcome'] = player_df['point_outcome'].map(encode_map)
  player_df = player_df.dropna(subset=['outcome'])
  player_df['outcome'] = player_df['outcome'].astype(int)

  # Check we have both outcome values (can't correlate if all FBK or all FBE)
  if player_df['outcome'].nunique() < 2:
    log_debug(f"Player {player_id}: only one outcome value in {outcome_type} — excluded")
    result['excluded'] = True
    return result

  # Keep only measurement columns that exist in this ppr_df + outcome
  available_cols = [c for c in POINT_LEVEL_COLUMNS if c in player_df.columns]
  if not available_cols:
    log_error(f"No measurement columns found in ppr_df for player {player_id}")
    result['excluded'] = True
    return result

  point_df = player_df[available_cols + ['outcome']].copy()

  # Clean: replace inf/-inf, clip extreme outliers at p99, fill NaN with median
  for col in available_cols:
    point_df[col] = point_df[col].replace([np.inf, -np.inf], np.nan)
    if not point_df[col].isna().all():
      p99 = point_df[col].quantile(0.99)
      p01 = point_df[col].quantile(0.01)
      point_df[col] = point_df[col].clip(lower=p01, upper=p99)
      point_df[col] = point_df[col].fillna(point_df[col].median())

  # Drop zero-variance columns
  point_df = point_df.loc[:, (point_df.std() > 0) | (point_df.columns == 'outcome')]

  result['point_df'] = point_df
  result['n_points'] = len(point_df)
  log_debug(f"Player {player_id}: {len(point_df)} {outcome_type} points, "
            f"{len(available_cols)} measurement columns")
  return result


@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def calculate_point_level_correlations(point_df):
  """
  Calculate Pearson correlation of each measurement column vs outcome (+1/-1).

  Args:
    point_df : DataFrame with measurement columns + 'outcome' column

  Returns:
    dict: { column_name: {'r': float, 'n': int, 'p': float} }
          n = number of valid (non-NaN) rows used for that column
  """
  results = {}

  if point_df is None or point_df.empty:
    return results

  from scipy import stats as scipy_stats

  measure_cols = [c for c in point_df.columns if c != 'outcome']
  outcome      = point_df['outcome']

  for col in measure_cols:
    valid = point_df[[col, 'outcome']].dropna()
    if len(valid) < 10 or valid[col].std() == 0:
      continue
    try:
      r, p = scipy_stats.pearsonr(valid[col], valid['outcome'])
      if not np.isnan(r):
        results[col] = {
          'r': round(float(r), 4),
          'n': len(valid),
          'p': round(float(p), 4),
        }
    except Exception as e:
      log_debug(f"Point-level correlation failed for {col}: {e}")

  return results


@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def analyse_point_level_for_level(level_row, ppr_cache, run_id, outcome_type, team):
  """
  Run point-level correlation analysis for one skill level and one outcome type.

  Args:
    level_row    : Anvil row from skill_level_def
    ppr_cache    : dict cache of already-fetched ppr DataFrames {cache_key: df}
                   Shared with set-level analysis to avoid double-fetching.
    run_id       : str — unique ID for this run
    outcome_type : 'first_ball' or 'transition'
    team         : team to pass to get_ppr_data() — 'League' for research,
                   specific team name for player-level analysis

  Returns:
    dict with metric_results and summary, or None on failure
  """
  level_id   = level_row['level_id']
  level_name = level_row['level_name']

  log_info(f"=== Point-level ({outcome_type}) analysis: level {level_id} ({level_name}) ===")

  player_rows  = list(level_row['player_list'])
  if not player_rows:
    log_error(f"No players for level {level_id}")
    return None

  player_corr_list = []
  n_excluded       = 0

  for p_row in player_rows:
    league = p_row['league']
    gender = p_row['gender']
    year   = str(p_row['year'])
    cache_key = f"{league}|{gender}|{year}|{team}"

    # Fetch ppr_df — use cache keyed by league+gender+year+team
    if cache_key not in ppr_cache:
      try:
        ppr_df = get_ppr_data(league, gender, year, team, scout=False)
        if ppr_df is None or isinstance(ppr_df, list) or (hasattr(ppr_df, 'empty') and ppr_df.empty):
          log_error(f"No ppr data for {cache_key}")
          ppr_cache[cache_key] = None
        else:
          ppr_cache[cache_key] = ppr_df
          log_info(f"Cached ppr for {cache_key}: {len(ppr_df)} rows")
      except Exception as e:
        log_error(f"Error fetching ppr for {cache_key}: {e}")
        ppr_cache[cache_key] = None

    ppr_df = ppr_cache[cache_key]
    if ppr_df is None:
      n_excluded += 1
      continue

    player_id    = resolve_player_identifier(p_row)
    player_label = f"{p_row['team']} {p_row['number']} {p_row['shortname']}"

    if not player_id:
      n_excluded += 1
      continue

    build_result = build_point_level_df(ppr_df, player_id, outcome_type)

    if build_result['excluded'] or build_result['point_df'] is None:
      log_debug(f"Player {player_label}: excluded from {outcome_type} "
                f"(n={build_result['n_points']})")
      n_excluded += 1
      continue

    point_df = build_result['point_df']
    n_points = build_result['n_points']

    corr = calculate_point_level_correlations(point_df)
    if not corr:
      log_info(f"Player {player_label}: no point-level correlations — skipping")
      n_excluded += 1
      continue

    player_corr_list.append(corr)
    log_info(f"Player {player_label}: {n_points} {outcome_type} points, "
             f"{len(corr)} correlations")

  if not player_corr_list:
    log_error(f"Level {level_id} ({outcome_type}): no players with valid data")
    return None

  # Aggregate using Fisher Z weighting by n points (not n sets)
  metric_results = aggregate_level_correlations(player_corr_list)

  log_info(f"Level {level_id} ({outcome_type}): "
           f"{len(player_corr_list)} players, {len(metric_results)} metrics aggregated")

  return {
    'level_id'       : level_id,
    'level_name'     : level_name,
    'outcome_type'   : outcome_type,
    'n_players_used' : len(player_corr_list),
    'n_players_excl' : n_excluded,
    'metric_results' : metric_results,
  }


def save_point_level_results(point_result, run_id, analysis_type):
  """
  Save point-level correlation results to skill_level_results table.
  Uses the same table as set-level results — distinguished by analysis_type column.

  New columns needed in skill_level_results:
    analysis_type : Text  — 'set_level', 'point_level_fb', 'point_level_trans'
    outcome_type  : Text  — 'point_pct', 'first_ball', 'transition'

  For existing set-level rows: analysis_type='set_level', outcome_type='point_pct'
  For new point-level rows:    analysis_type as passed, outcome_type from result

  Note: mean_value and mean_n_sets are not applicable for point-level results
  and will be saved as None. total_sets = total points for point-level rows.
  """
  if not point_result:
    return

  level_id     = point_result['level_id']
  level_name   = point_result['level_name']
  outcome_type = point_result['outcome_type']
  n_excl       = point_result['n_players_excl']
  created_at   = datetime.now()
  rows_saved   = 0

  for metric_id, stats in point_result['metric_results'].items():
    try:
      app_tables.skill_level_results.add_row(
        run_id             = run_id,
        level_id           = level_id,
        level_name         = level_name,
        metric_id          = metric_id,
        mean_correlation   = stats['mean_r'],
        mean_value         = None,          # not applicable at point level
        n_players          = stats['n_players'],
        n_players_excluded = n_excl,
        mean_n_sets        = stats.get('mean_n_sets'),   # here = mean points per player
        total_sets         = stats.get('total_sets'),    # here = total points
        analysis_type      = analysis_type,
        outcome_type       = outcome_type,
        created_at         = created_at,
      )
      rows_saved += 1
    except Exception as e:
      log_error(f"Error saving point-level result {metric_id} level {level_id}: {e}")

  log_info(f"Level {level_id} ({outcome_type}): saved {rows_saved} point-level rows")

# ============================================================================
# BACKGROUND TASK
# ============================================================================

@anvil.server.background_task
def run_skill_level_correlations():
  """
  Background task: loop over all active skill levels in skill_level_def.
  Runs BOTH analyses for each level:
    1. Set-level   — metrics vs point_pct (points won % per set)
    2. Point-level — physical measurements vs FBK/FBE and TK/TE outcomes

  All three result types are saved to skill_level_results, distinguished
  by the analysis_type column:
    'set_level'         — set-level metric correlations (original analysis)
    'point_level_fb'    — point-level first-ball correlations (FBK/FBE)
    'point_level_trans' — point-level transition correlations (TK/TE)

  ppr_df is fetched once per league/gender/year/team combination and cached
  across all three analyses to avoid redundant data fetching.

  team defaults to 'League' for the skill-level research analysis.
  For future player-specific analysis, call analyse_point_level_for_level()
  directly with the appropriate team name.
  """
  run_id = str(uuid.uuid4())[:8]
  team   = 'League'             # always 'League' for skill-level research

  log_info(f"=== skill_level_correlations START | run_id={run_id} | team={team} ===")

  try:
    level_rows = list(app_tables.skill_level_def.search(active=True))
  except Exception as e:
    log_critical(f"Could not fetch skill_level_def: {e}")
    return {'status': 'error', 'message': str(e)}

  level_rows = sorted(level_rows, key=lambda r: str(r['level_id']))
  log_info(f"Found {len(level_rows)} active skill levels to process")

  # Shared ppr cache — populated on first access, reused across all analyses
  # Key: "league|gender|year|team"
  ppr_cache = {}

  results_summary = []

  for level_row in level_rows:
    level_id   = level_row['level_id']
    level_name = level_row['level_name']
    level_ok   = True

    # ── 1. Set-level analysis ──────────────────────────────────────────────
    try:
      level_result = analyse_skill_level(level_row, run_id)
      if level_result:
        save_level_results(level_result, run_id)
        n_players_set = level_result['n_players_used']
        n_metrics_set = len(level_result['metric_results'])
        log_info(f"Set-level OK: {level_id} — {n_players_set} players, "
                 f"{n_metrics_set} metrics")

        # Populate ppr_cache from what analyse_skill_level fetched
        # (It fetches internally; we re-use those results for point-level)
        # NOTE: analyse_skill_level uses get_ppr_for_player_row() internally
        # and doesn't expose the cache. We prime the point-level cache here.
        for p_row in list(level_row['player_list']):
          league = p_row['league']
          gender = p_row['gender']
          year   = str(p_row['year'])
          cache_key = f"{league}|{gender}|{year}|{team}"
          if cache_key not in ppr_cache:
            try:
              df = get_ppr_data(league, gender, year, team, scout=False)
              ppr_cache[cache_key] = df if (
                df is not None and
                not isinstance(df, list) and
                not (hasattr(df, 'empty') and df.empty)
              ) else None
            except Exception as ce:
              log_error(f"Cache prime failed for {cache_key}: {ce}")
              ppr_cache[cache_key] = None
      else:
        log_error(f"Set-level analysis returned None for level {level_id}")
        level_ok = False

    except Exception as e:
      log_critical(f"Set-level error for {level_id}: {e}")
      level_ok = False

    # ── 2. Point-level: first ball (FBK / FBE) ────────────────────────────
    try:
      fb_result = analyse_point_level_for_level(
        level_row, ppr_cache, run_id,
        outcome_type='first_ball',
        team=team,
      )
      if fb_result:
        save_point_level_results(fb_result, run_id, analysis_type='point_level_fb')
        log_info(f"Point FB OK: {level_id} — {fb_result['n_players_used']} players, "
                 f"{len(fb_result['metric_results'])} metrics")
      else:
        log_error(f"Point-level first_ball returned None for {level_id}")

    except Exception as e:
      log_critical(f"Point-level first_ball error for {level_id}: {e}")

    # ── 3. Point-level: transition (TK / TE) ─────────────────────────────
    try:
      trans_result = analyse_point_level_for_level(
        level_row, ppr_cache, run_id,
        outcome_type='transition',
        team=team,
      )
      if trans_result:
        save_point_level_results(trans_result, run_id, analysis_type='point_level_trans')
        log_info(f"Point TRANS OK: {level_id} — {trans_result['n_players_used']} players, "
                 f"{len(trans_result['metric_results'])} metrics")
      else:
        log_error(f"Point-level transition returned None for {level_id}")

    except Exception as e:
      log_critical(f"Point-level transition error for {level_id}: {e}")

    # ── Summary entry for this level ──────────────────────────────────────
    results_summary.append({
      'level_id'  : level_id,
      'level_name': level_name,
      'status'    : 'ok' if level_ok else 'partial',
    })

  log_info(f"=== skill_level_correlations COMPLETE | run_id={run_id} ===")
  for s in results_summary:
    log_info(f"  {s['level_id']} ({s['level_name']}): {s['status']}")

  return {'status': 'complete', 'run_id': run_id, 'levels': results_summary}


# ============================================================================
# SERVER-CALLABLE ENTRY POINTS
# ============================================================================

@anvil.server.callable
def run_skill_level_correlation_analysis():
  """
  Launch the full background analysis (set-level + point-level).
  Called from browser — returns a task object the browser can poll.

  Usage:
    task = anvil.server.call('run_skill_level_correlation_analysis')
    # poll: task.get_state()  ->  'completed' | 'failed' | 'running'
    # result: task.get_return_value()
  """
  log_info("run_skill_level_correlation_analysis called from browser")
  task = anvil.server.launch_background_task('run_skill_level_correlations')
  return task


# ============================================================================
# RESULTS RETRIEVAL
# ============================================================================

@anvil.server.callable
def get_skill_level_results(run_id=None, analysis_type=None):
  """
  Retrieve results from skill_level_results table.

  Args:
    run_id        : optional — if None, returns the most recent run
    analysis_type : optional filter — one of:
                      'set_level'         (original set-level correlations)
                      'point_level_fb'    (first-ball point correlations)
                      'point_level_trans' (transition point correlations)
                    If None, returns all three types for the run.

  Returns:
    list of dicts. Each dict has:
      run_id, level_id, level_name, metric_id,
      mean_correlation, mean_value,
      n_players, n_players_excluded,
      mean_n_sets,    <- for point-level rows: mean points per player
      total_sets,     <- for point-level rows: total points across players
      analysis_type, outcome_type,
      created_at
  """
  try:
    if run_id:
      all_rows = list(app_tables.skill_level_results.search(run_id=run_id))
    else:
      # Most recent run — find the latest run_id first
      all_rows = list(app_tables.skill_level_results.search(
        tables.order_by('created_at', ascending=False)
      ))
      if not all_rows:
        return []
      latest_run_id = all_rows[0]['run_id']
      all_rows = [r for r in all_rows if r['run_id'] == latest_run_id]

    # Optional filter by analysis_type
    if analysis_type:
      all_rows = [r for r in all_rows if r.get('analysis_type') == analysis_type]

    return [
      {
        'run_id'             : r['run_id'],
        'level_id'           : r['level_id'],
        'level_name'         : r['level_name'],
        'metric_id'          : r['metric_id'],
        'mean_correlation'   : r['mean_correlation'],
        'mean_value'         : r['mean_value'],
        'n_players'          : r['n_players'],
        'n_players_excluded' : r['n_players_excluded'],
        'mean_n_sets'        : r['mean_n_sets'],
        'total_sets'         : r['total_sets'],
        'analysis_type'      : r.get('analysis_type', 'set_level'),
        'outcome_type'       : r.get('outcome_type', 'point_pct'),
        'created_at'         : str(r['created_at']),
      }
      for r in all_rows
    ]

  except Exception as e:
    log_error(f"Error retrieving skill_level_results: {e}")
    return []
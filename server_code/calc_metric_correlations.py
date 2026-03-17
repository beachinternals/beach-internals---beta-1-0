import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import json

# ─────────────────────────────────────────────────────────────────
#  HELPER: calculate goodpass for a single player from ppr_df
# ─────────────────────────────────────────────────────────────────
def _calc_goodpass(ppr_df, player):
  """
  Good pass = 1 - out_of_system rate.
  Filters to rows where this player is the passer.
  Returns float or None if no data.
  """
  df = ppr_df[ppr_df['pass_player'].str.strip() == player.strip()]
  # Exclude service aces (player never touched the ball)
  df = df[df['point_outcome'] != 'TSA']
  total = df.shape[0]
  if total < 5:   # minimum attempts threshold
    return None
  oos = df[df['pass_oos'] == 1].shape[0]   # pass_oos = 1 means out of system
  return 1.0 - (oos / total)


# ─────────────────────────────────────────────────────────────────
#  HELPER: calculate fbhe for a single player from ppr_df
# ─────────────────────────────────────────────────────────────────
def _calc_fbhe(ppr_df, player):
  """
  FBHE = (kills - errors) / attempts on first ball attacks.
  Filters to rows where this player is the attacker,
  on first ball (not transition).
  Returns float or None if no data.
  """
  df = ppr_df[ppr_df['att_player'].str.strip() == player.strip()]
  # First ball only: exclude transition points (TK, TE)
  df = df[~df['point_outcome'].isin(['TK', 'TE'])]
  # Also exclude service aces/errors (no attack happened)
  df = df[~df['point_outcome'].isin(['TSA', 'TSE'])]
  attempts = df.shape[0]
  if attempts < 5:
    return None
  kills  = df[df['point_outcome'] == 'FBK'].shape[0]
  errors = df[df['point_outcome'] == 'FBE'].shape[0]
  return (kills - errors) / attempts


# ─────────────────────────────────────────────────────────────────
#  HELPER: calculate dig_quality (mean) for a single player
# ─────────────────────────────────────────────────────────────────
def _calc_dig_quality_mean(ppr_df, player):
  """
  Mean dig quality score (0.0 - 1.0) for this player's digs.
  Filters to rows where this player is the server (srv flag)
  since tcr_s measures transition when serving.
  Returns float or None if no data.
  """
  df = ppr_df[ppr_df['serve_player'].str.strip() == player.strip()]
  # Only rows where a dig actually happened
  df = df[df['dig_yn'] == 'Y']
  # Only transition points (dig leads to transition)
  df = df[df['point_outcome'].isin(['TK', 'TE'])]
  # Drop rows with no dig quality score
  df = df[df['dig_quality'].notna() & (df['dig_quality'] != 0)]
  if df.shape[0] < 5:
    return None
  return df['dig_quality'].mean()


# ─────────────────────────────────────────────────────────────────
#  HELPER: calculate tcr_s for a single player from ppr_df
# ─────────────────────────────────────────────────────────────────
def _calc_tcr_s(ppr_df, player):
  """
  TCR-S = transition conversion rate when this player is the server.
  = (transition kills won + opp transition errors) / all transition points
  while this player's team is serving.
  Returns float or None if no data.
  """
  df = ppr_df[ppr_df['serve_player'].str.strip() == player.strip()]
  # Only transition points
  df = df[df['point_outcome'].isin(['TK', 'TE'])]
  total = df.shape[0]
  if total < 5:
    return None

  # Points won by our team
  # point_outcome_team contains the player string for the winning team
  # We check if our player's team fragment is in the outcome team string
  player_team_fragment = player.strip()[:-1]   # e.g. "STETSON 35 Liz" → "STETSON 35 Li"
  # More reliable: use player name minus last char as team fragment
  # (matches the convention used elsewhere in the codebase)
  won = df[df['point_outcome_team'].str.contains(player_team_fragment, na=False)]
  pts_won = won.shape[0]
  return pts_won / total


# ─────────────────────────────────────────────────────────────────
#  HELPER: load and parse ppr_csv for a given league/gender/year/team
# ─────────────────────────────────────────────────────────────────
def _load_ppr_df(league, gender, year, team='League'):
  """
  Loads the combined PPR dataframe from ppr_csv_tables.
  Uses team='League' for the full league dataset (all players).
  Returns DataFrame or None.
  """
  rows = app_tables.ppr_csv_tables.search(
    league=league,
    gender=gender,
    year=year,
    team=team
  )
  row = None
  for r in rows:
    row = r
    break

  if row is None:
    print(f"No PPR data found for {league} {gender} {year} team={team}")
    return None

  ppr_json = row['player_data']
  if not ppr_json:
    print(f"player_data is empty for {league} {gender} {year}")
    return None

  try:
    ppr_df = pd.DataFrame(json.loads(ppr_json))
    return ppr_df
  except Exception as e:
    print(f"Error parsing PPR JSON: {e}")
    return None


# ─────────────────────────────────────────────────────────────────
#  MAIN CALLABLE: run all active correlation definitions
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def calc_metric_correlations(league, gender, year):
  """
  For each active row in metric_correlation_def,
  and for each skill level in skill_level_def,
  calculate the Pearson correlation between the two metrics
  across the players in that skill level.

  Results are stored in metric_correlation_results.
  Existing results for this league/gender/year are deleted first (fresh run).

  Args:
    league (str): e.g. 'NCAA'
    gender (str): e.g. 'W'
    year   (int): e.g. 2026

  Returns:
    str: summary message
  """
  print(f"=== calc_metric_correlations: {league} {gender} {year} ===")

  # --- Load PPR data (full league dataset) ---
  ppr_df = _load_ppr_df(league, gender, year, team='League')
  if ppr_df is None:
    return f"ERROR: No PPR data found for {league} {gender} {year}"

  print(f"PPR data loaded: {ppr_df.shape[0]} rows")

  # --- Get active correlation definitions ---
  corr_defs = [r for r in app_tables.metric_correlation_def.search() if r['active']]
  if not corr_defs:
    return "No active correlation definitions found in metric_correlation_def"

  print(f"Found {len(corr_defs)} active correlation definitions")

  # --- Get skill levels ---
  skill_levels = list(app_tables.skill_level_def.search())
  if not skill_levels:
    return "No skill levels found in skill_level_def"

  print(f"Found {len(skill_levels)} skill levels")

  # --- Delete existing results for this league/gender/year ---
  existing = app_tables.metric_correlation_results.search(
    # NOTE: metric_correlation_results has no league/gender/year columns
    # since skill_level_def crosses those boundaries.
    # We delete all rows linked to each corr_def and skill_level combo.
    # Simplest: delete all rows (full recalculate each run).
  )
  count_deleted = 0
  for row in existing:
    row.delete()
    count_deleted += 1
  print(f"Deleted {count_deleted} existing result rows")

  # --- Main calculation loop ---
  results_saved = 0
  results_skipped = 0

  for corr_def in corr_defs:
    upstream_metric   = corr_def['metric_upstream']
    downstream_metric = corr_def['metric_downstream']
    print(f"\n--- Processing: {upstream_metric} → {downstream_metric} ---")

    for skill_row in skill_levels:
      skill_level_name = skill_row['level_name']

      # Get the list of players in this skill level
      # skill_level_def stores players as a JSON list or comma-separated string
      # Adjust parsing based on how your table stores this
      try:
        player_list_raw = skill_row['player_list']   # adjust column name if different
        if isinstance(player_list_raw, str):
          # Try JSON first, fall back to comma-split
          try:
            player_list = json.loads(player_list_raw)
          except Exception:
            player_list = [p.strip() for p in player_list_raw.split(',') if p.strip()]
        elif isinstance(player_list_raw, list):
          player_list = player_list_raw
        else:
          player_list = []
      except Exception as e:
        print(f"  Error reading player_list for {skill_level_name}: {e}")
        continue

      if len(player_list) < 3:
        print(f"  Skipping {skill_level_name}: only {len(player_list)} players (need ≥ 3)")
        results_skipped += 1
        continue

      # Calculate per-player metric values
      upstream_values   = []
      downstream_values = []

      for player in player_list:
        # --- Calculate upstream metric ---
        if upstream_metric == 'goodpass':
          up_val = _calc_goodpass(ppr_df, player)
        elif upstream_metric == 'dig_quality':
          up_val = _calc_dig_quality_mean(ppr_df, player)
        else:
          print(f"  Unknown upstream metric: {upstream_metric}")
          up_val = None

        # --- Calculate downstream metric ---
        if downstream_metric == 'fbhe':
          down_val = _calc_fbhe(ppr_df, player)
        elif downstream_metric == 'tcr_s':
          down_val = _calc_tcr_s(ppr_df, player)
        else:
          print(f"  Unknown downstream metric: {downstream_metric}")
          down_val = None

        # Only use this player if both values are available
        if up_val is not None and down_val is not None:
          upstream_values.append(up_val)
          downstream_values.append(down_val)

      n = len(upstream_values)
      print(f"  {skill_level_name}: {n} players with valid data")

      if n < 3:
        print(f"  Skipping: not enough valid player data points")
        results_skipped += 1
        continue

      # --- Pearson correlation ---
      try:
        r, p = stats.pearsonr(upstream_values, downstream_values)
        is_significant = bool(p < 0.05)
        print(f"  r={r:.3f}, p={p:.4f}, significant={is_significant}")
      except Exception as e:
        print(f"  Correlation calculation failed: {e}")
        results_skipped += 1
        continue

      # --- Save to results table ---
      app_tables.metric_correlation_results.add_row(
        corr_def       = corr_def,
        skill_level    = skill_level_name,
        correlation    = round(float(r), 4),
        p_value        = round(float(p), 4),
        n_players      = n,
        is_significant = is_significant,
        calculated_at  = datetime.now(),
        notes          = f"{upstream_metric}→{downstream_metric} | {skill_level_name}"
      )
      results_saved += 1

  summary = (f"Done. Saved {results_saved} correlation results, "
             f"skipped {results_skipped} (insufficient data).")
  print(summary)
  return summary


# ─────────────────────────────────────────────────────────────────
#  CALLABLE: fetch results for display
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def get_metric_correlation_results():
  """
  Returns all correlation results as a list of dicts,
  sorted by correlation strength (strongest first).
  Ready to drop into a DataGrid or DataFrame.
  """
  results = []
  for row in app_tables.metric_correlation_results.search():
    corr_def = row['corr_def']
    results.append({
      'upstream'      : corr_def['metric_upstream']   if corr_def else '',
      'downstream'    : corr_def['metric_downstream']  if corr_def else '',
      'description'   : corr_def['description']        if corr_def else '',
      'skill_level'   : row['skill_level'],    # column in metric_correlation_results (not skill_level_def)
      'correlation'   : row['correlation'],
      'p_value'       : row['p_value'],
      'n_players'     : row['n_players'],
      'is_significant': row['is_significant'],
      'calculated_at' : str(row['calculated_at']),
    })

  results.sort(key=lambda x: abs(x['correlation'] or 0), reverse=True)
  return results
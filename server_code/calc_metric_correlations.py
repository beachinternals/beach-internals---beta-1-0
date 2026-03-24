import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
from server_functions import get_ppr_data
from metric_functions import calc_metric_from_dict


# ─────────────────────────────────────────────────────────────────
#  HELPER: load PPR dataframe for a league/gender/year
# ─────────────────────────────────────────────────────────────────
def _load_ppr_df(league, gender, year):
  try:
    ppr_df = get_ppr_data(league, gender, year, 'League', scout=False)
    if isinstance(ppr_df, list):
      print(f"  No PPR data found for {league} {gender} {year}")
      return None
    print(f"  Loaded PPR: {league} {gender} {year} — {ppr_df.shape[0]} rows")
    return ppr_df
  except Exception as e:
    print(f"  Error loading PPR for {league} {gender} {year}: {e}")
    return None


# ─────────────────────────────────────────────────────────────────
#  HELPER: load metric dictionary once as a DataFrame
# ─────────────────────────────────────────────────────────────────
def _load_metric_dict():
  """
  Load the metric_dictionary table as a pandas DataFrame.
  This is the single source of truth for how every metric is calculated.
  Same table used by generate_player_metrics_json.py.
  """
  dict_rows = list(app_tables.metric_dictionary.search())
  if not dict_rows:
    raise ValueError("metric_dictionary table is empty")
  column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
  return pd.DataFrame([
    {col: row[col] for col in column_names}
    for row in dict_rows
  ])


# ─────────────────────────────────────────────────────────────────
#  MAIN CALCULATION
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def calc_metric_correlations():
  """
  For each active row in metric_correlation_def,
  and for each skill level in skill_level_def,
  calculate Pearson correlation between the two metrics
  across all players in that skill level.

  Metric calculations use the metric_dictionary table as the single
  source of truth — the same functions and logic as player reports.

  All existing results are deleted first (full recalculate).
  Returns a summary string.
  """
  print("=== calc_metric_correlations: starting ===")

  # --- Load metric dictionary once (shared across all correlations) ---
  try:
    metric_dict_df = _load_metric_dict()
    print(f"Loaded metric dictionary: {len(metric_dict_df)} metrics")
  except Exception as e:
    return f"Failed to load metric dictionary: {e}"

  # --- Active correlation pairs ---
  corr_defs = [r for r in app_tables.metric_correlation_def.search() if r['active']]
  if not corr_defs:
    return "No active correlation definitions found"
  print(f"Found {len(corr_defs)} active correlation definitions")

  # --- Skill levels ---
  skill_levels = list(app_tables.skill_level_def.search())
  if not skill_levels:
    return "No skill levels found in skill_level_def"
  print(f"Found {len(skill_levels)} skill levels")

  # --- Wipe existing results ---
  count_deleted = 0
  for row in app_tables.metric_correlation_results.search():
    row.delete()
    count_deleted += 1
  print(f"Deleted {count_deleted} existing result rows")

  results_saved   = 0
  results_skipped = 0

  for corr_def in corr_defs:
    upstream_metric   = corr_def['metric_upstream']
    downstream_metric = corr_def['metric_downstream']
    print(f"\n--- {upstream_metric} → {downstream_metric} ---")

    # Validate both metrics exist in the dictionary before looping players
    if metric_dict_df[metric_dict_df['metric_id'] == upstream_metric].empty:
      print(f"  Skipping: '{upstream_metric}' not found in metric_dictionary")
      results_skipped += len(skill_levels)
      continue
    if metric_dict_df[metric_dict_df['metric_id'] == downstream_metric].empty:
      print(f"  Skipping: '{downstream_metric}' not found in metric_dictionary")
      results_skipped += len(skill_levels)
      continue

    for skill_row in skill_levels:
      skill_level_name = skill_row['level_name']
      player_list      = skill_row['player_list']

      if not player_list or len(player_list) < 3:
        print(f"  Skipping {skill_level_name}: fewer than 3 players")
        results_skipped += 1
        continue

      upstream_values   = []
      downstream_values = []

      # Cache PPR dataframe — only reload when league/gender/year changes
      current_lgy    = None
      current_ppr_df = None

      for player_row in player_list:
        try:
          league      = player_row['league']
          gender      = player_row['gender']
          year        = player_row['year']
          player_uuid = player_row['player_uuid']
        except Exception as e:
          print(f"  Error reading master_player row: {e}")
          continue

        lgy = f"{league}|{gender}|{year}"
        if lgy != current_lgy:
          current_ppr_df = _load_ppr_df(league, gender, year)
          current_lgy    = lgy

        if current_ppr_df is None:
          continue

        # Use metric_dictionary as single source of truth for both metrics
        up_val   = calc_metric_from_dict(upstream_metric,   current_ppr_df, player_uuid, metric_dict_df)
        down_val = calc_metric_from_dict(downstream_metric, current_ppr_df, player_uuid, metric_dict_df)

        if up_val is not None and down_val is not None:
          upstream_values.append(up_val)
          downstream_values.append(down_val)

      # --- Correlate ---
      n = len(upstream_values)
      print(f"  {skill_level_name}: {n} players with valid data")

      if n < 3:
        print(f"  Skipping: insufficient valid data")
        results_skipped += 1
        continue

      try:
        r, p = stats.pearsonr(upstream_values, downstream_values)
        is_significant = bool(p < 0.05)
        print(f"  r={r:.3f}  p={p:.4f}  n={n}  sig={is_significant}")
      except Exception as e:
        print(f"  Correlation failed: {e}")
        results_skipped += 1
        continue

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

  summary = (f"Done. {results_saved} results saved, "
             f"{results_skipped} skipped (insufficient data).")
  print(summary)
  return summary


# ─────────────────────────────────────────────────────────────────
#  BACKGROUND TASK + LAUNCHER
# ─────────────────────────────────────────────────────────────────
@anvil.server.background_task
def bg_calc_metric_correlations():
  result = calc_metric_correlations()
  anvil.server.task_state['status']  = 'complete'
  anvil.server.task_state['message'] = result


@anvil.server.callable
def launch_metric_correlations():
  task = anvil.server.launch_background_task('bg_calc_metric_correlations')
  return task.get_id()


@anvil.server.callable
def get_correlation_task_status(task_id):
  try:
    task = anvil.server.get_background_task(task_id)
    if task.is_completed():
      state = task.get_state()
      return {'done': True,  'status': state.get('status', 'complete'), 'message': state.get('message', 'Done')}
    elif task.get_error():
      return {'done': True,  'status': 'error',   'message': str(task.get_error())}
    else:
      return {'done': False, 'status': 'running', 'message': 'Calculating...'}
  except Exception as e:
    return {'done': True, 'status': 'error', 'message': str(e)}


# ─────────────────────────────────────────────────────────────────
#  FETCH RESULTS FOR DISPLAY
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def get_metric_correlation_results():
  """Returns all results as list of dicts, sorted by correlation strength."""
  results = []
  for row in app_tables.metric_correlation_results.search():
    cd = row['corr_def']
    results.append({
      'upstream'      : cd['metric_upstream']   if cd else '',
      'downstream'    : cd['metric_downstream']  if cd else '',
      'description'   : cd['description']        if cd else '',
      'skill_level'   : row['skill_level'],
      'correlation'   : row['correlation'],
      'p_value'       : row['p_value'],
      'n_players'     : row['n_players'],
      'is_significant': row['is_significant'],
      'calculated_at' : str(row['calculated_at']),
    })
  results.sort(key=lambda x: abs(x['correlation'] or 0), reverse=True)
  return results
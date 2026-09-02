"""
calc_player_data_dictionary.py
===============================
Metric_dictionary-driven implementation of
calc_player_data.calculate_player_data_not_background(). Now that the
migration itself is validated and cut over (2026-09-02), this is
COMPREHENSIVE by default: every metric_dictionary row with
aggregate_level='Yes' is emitted as a player_data column under its own
metric_id, automatically -- no manual step needed when a new metric is added
to the dictionary. COLUMN_ALIAS_MAP now only handles the exceptions: legacy
column-name renames (e.g. fbhe_bang -> fbhe_harddriven) and scale fixes, kept
for the handful of downstream consumers that still hardcode old names
(reports_league.py, dashboard.py, s_w_report.py, reports_dashboard.py,
calc_traingle_scoring.py).

distribution / distribution_setheight metrics (e.g. srv_dest_per,
set_height_dist) aren't single values -- calculate_all_metrics returns them
as a rolled-up payload (one exec() call computing every cell at once, rather
than one call per cell, for speed). _flatten_distribution() unpacks that
payload into one player_data column per cell, so every cell is still a real
number player_data_stats can compute a population mean/stdev for.
"""

import io
import numpy as np
import pandas as pd
import anvil.tables.query as q
from anvil.tables import app_tables

from logger_utils import log_info, log_error
from generate_player_metrics_json_server import calculate_all_metrics
from player_data_column_alias_map import (
  COLUMN_ALIAS_MAP,
  PER_RATIO_SPECS,
  SCALE_TRANSFORMS,
)


def _load_metric_dictionary_df():
  dict_rows = list(app_tables.metric_dictionary.search())
  column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
  return pd.DataFrame([{col: row[col] for col in column_names} for row in dict_rows])


def _load_league_ppr(c_league, c_gender, c_year):
  """Port of the loading block in calc_player_data.calculate_player_data_not_background
  (league ppr_df + min_att), duplicated rather than imported so this module has
  zero coupling to the untouched legacy function."""
  ppr_csv_row = app_tables.ppr_csv_tables.get(
    q.all_of(league=c_league, gender=c_gender, year=c_year, team="League"))
  if not ppr_csv_row:
    return None, None
  ppr_df = pd.read_csv(io.BytesIO(ppr_csv_row['ppr_csv'].get_bytes()))
  return ppr_csv_row, ppr_df


def _get_player_list(ppr_df):
  """Port of calc_player_data.py's player-list construction."""
  p_list = np.unique(np.concatenate([
    ppr_df['player_a1'].unique(), ppr_df['player_a2'].unique(),
    ppr_df['player_b1'].unique(), ppr_df['player_b2'].unique(),
  ]))
  return p_list


def _parse_team(player_name):
  """Port of calc_player_data.py's team-parsing logic."""
  if player_name.startswith('PLYR-'):
    return 'League'
  return player_name[:player_name.index(" ")].strip()


def _compute_point_per(ppr_df, player_name):
  """Bespoke, non-dictionary calc -- port of calc_player_data.py's points-earned
  logic. Kept as direct pandas logic since it isn't shaped like a
  metric_dictionary metric."""
  tmp_df = ppr_df[
    (ppr_df['player_a1'] == player_name) | (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) | (ppr_df['player_b2'] == player_name)
  ]
  tot_pts = tmp_df.shape[0]
  if tot_pts == 0:
    return None
  tmp1 = tmp_df[tmp_df['point_outcome_team'].str.contains(player_name)]
  pts_earned = tmp1[tmp1['point_outcome'].isin(['FBK', 'TK', 'TSA'])].shape[0]
  tmp1 = tmp_df[~tmp_df['point_outcome_team'].str.contains(player_name)]
  pts_earned += tmp1[tmp1['point_outcome'].isin(['FBE', 'TE', 'TSE'])].shape[0]
  return pts_earned / tot_pts


def _flat_metric_lookup(metrics_output):
  """Flatten calculate_all_metrics()['metrics'] (nested by category) into a
  single metric_id -> info dict."""
  flat = {}
  for category_metrics in metrics_output.values():
    flat.update(category_metrics)
  return flat


def _flatten_distribution(metric_id, dist, row):
  """Unpacks one distribution-shaped metric result into player_data columns.

  build_setheight_payload() (server_functions.py) shape:
    {'kind': 'setheight', 'total': N, 'order': [bucket names],
     'cells': {bucket: {'vol':, 'fbhe':, 'n':}}}
  -> {metric_id}_n = total; {metric_id}__{bucket}_vol/_fbhe/_n per bucket.

  build_distribution_payload() shape:
    {'cells_full': {cell_key: pct}, 'cells_dest': {...}, 'n':, 'err_rate':}
  -> {metric_id}_n = n; {metric_id}_err = err_rate (if present);
     {metric_id}__{cell_key} = pct per cell in cells_full. cells_dest is a
     strict rollup of cells_full (summed by destination) and is deliberately
     NOT also flattened -- it's fully derivable from cells_full later, so
     keeping only the granular version avoids doubling the column count for
     no new information.
  """
  if dist.get('kind') == 'setheight':
    row[f"{metric_id}_n"] = dist.get('total')
    for bucket in dist.get('order', []):
      cell = dist.get('cells', {}).get(bucket)
      if not cell:
        continue
      row[f"{metric_id}__{bucket}_vol"] = cell.get('vol')
      row[f"{metric_id}__{bucket}_fbhe"] = cell.get('fbhe')
      row[f"{metric_id}__{bucket}_n"] = cell.get('n')
  else:
    row[f"{metric_id}_n"] = dist.get('n')
    if dist.get('err_rate') is not None:
      row[f"{metric_id}_err"] = dist.get('err_rate')
    for cell_key, pct in dist.get('cells_full', {}).items():
      row[f"{metric_id}__{cell_key}"] = pct


def _flatten_row(flat_metrics, skipped_columns):
  """Two passes over one player's calculate_all_metrics() output:

  1. Comprehensive: every metric_id the dictionary computed (aggregate_level
     already filtered by calculate_all_metrics itself) becomes a column
     under its own name -- <metric_id> (value) + <metric_id>_n (attempts)
     for scalars, or the flattened cell columns from _flatten_distribution
     for distribution-shaped results. New metrics added to the dictionary
     show up here automatically, no code change required.

  2. Legacy aliasing: COLUMN_ALIAS_MAP adds a second, differently-named copy
     for the handful of columns downstream consumers still hardcode under
     the old player_data name (e.g. fbhe_bang duplicates fbhe_harddriven).
     For entries where metric_id == legacy_col this just re-writes the same
     value under the same key -- redundant but harmless, so the map doesn't
     need pruning as more of it becomes covered by the comprehensive pass.

  SCALE_TRANSFORMS corrects columns (expected, err_den, ...) that were
  historically stored on a 0-100 scale (parsed from a '{:.2%}'-formatted
  string in legacy) while the dictionary's equivalent metric is a plain 0-1
  float, matching every other ratio metric in the dictionary -- applied by
  whichever name (native or legacy-aliased) is actually being written."""
  row = {}

  for metric_id, info in flat_metrics.items():
    dist = info.get('distribution')
    if dist:
      _flatten_distribution(metric_id, dist, row)
      continue
    value = info['value'] if info.get('sufficient_data', True) else None
    if value is not None and metric_id in SCALE_TRANSFORMS:
      value = value * SCALE_TRANSFORMS[metric_id]
    row[metric_id] = value
    row[f"{metric_id}_n"] = info.get('attempts')

  for legacy_col, (metric_id, _confidence) in COLUMN_ALIAS_MAP.items():
    if metric_id is None:
      skipped_columns.add(legacy_col)
      continue
    info = flat_metrics.get(metric_id)
    if info is None:
      skipped_columns.add(legacy_col)
      continue
    value = info['value'] if info.get('sufficient_data', True) else None
    if value is not None and legacy_col in SCALE_TRANSFORMS:
      value = value * SCALE_TRANSFORMS[legacy_col]
    row[legacy_col] = value
    row[f"{legacy_col}_n"] = info.get('attempts')

  return row


def _compute_fbhe_range(row):
  """Bespoke derived column -- min/max spread across fbhe1..fbhe5, matching
  calc_player_data.py's fbhe_range calculation."""
  vals = [row.get(f"fbhe{j}") for j in (1, 2, 3, 4, 5)]
  vals = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
  if not vals:
    return None
  return float("{:.3f}".format(max(vals) - min(vals)))


def _compute_per_ratios(ppr_df, player_name, row, skipped_columns):
  """Implements PER_RATIO_SPECS.

  'total_pass'/'total_both' denominators are computed directly from ppr_df,
  filtered the same way fbhe_obj() does it (server_functions.py), since these
  ratios are not shaped like a single metric_dictionary metric.

  'sibling_sum' denominators are deliberately NOT computed from ppr_df --
  they're the sum of two already-dictionary-computed numerator columns
  (e.g. fbhe_oos_n + fbhe_insys_n), so numerator and denominator are
  guaranteed to share the same scope the dictionary itself uses, rather than
  risking a population mismatch against an independently-recomputed total."""
  name = player_name.strip()
  total_pass = ppr_df[ppr_df['pass_player'].str.strip() == name].shape[0]
  total_both = ppr_df[
    (ppr_df['pass_player'].str.strip() == name) &
    (ppr_df['att_player'].str.strip() == name)
  ].shape[0]

  for legacy_col, spec in PER_RATIO_SPECS.items():
    numerator = row.get(spec['legacy_numerator_col'])
    if numerator is None:
      row[legacy_col] = None
      skipped_columns.add(legacy_col)
      continue

    denom_kind = spec['denominator']
    if denom_kind == 'sibling_sum':
      sibling = row.get(spec['sibling_col'])
      denom = (numerator + sibling) if sibling is not None else None
    elif denom_kind == 'total_pass':
      denom = total_pass
    else:
      denom = total_both

    row[legacy_col] = (numerator / denom) if denom else None

  return row


def _calculate_one_player_row(ppr_df, player_name, metric_dict, skipped_columns):
  """
  Computes one player's full player_data row. Shared by the league-wide loop
  and the single-player entry point below, so there's exactly one place that
  defines "what a player's row contains."

  Filters ppr_df to this player's own points BEFORE running the ~300
  dictionary formulas, rather than handing every formula the full league
  ppr_df to re-filter from scratch. A player appears in player_a1/a2/b1/b2
  for every point of every match they played (positions don't change
  mid-match), so this loses nothing a per-player metric would ever need --
  it just avoids redoing a full-league scan hundreds of times per player,
  which is what was exhausting memory on large leagues.
  """
  player_ppr_df = ppr_df[
    (ppr_df['player_a1'] == player_name) | (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) | (ppr_df['player_b2'] == player_name)
  ]

  metrics_result = calculate_all_metrics(metric_dict, player_ppr_df, player_name)
  flat_metrics = _flat_metric_lookup(metrics_result['metrics'])

  row = {'pair': '', 'player': player_name, 'team': _parse_team(player_name)}
  row.update(_flatten_row(flat_metrics, skipped_columns))
  row['fbhe_range'] = _compute_fbhe_range(row)
  row = _compute_per_ratios(player_ppr_df, player_name, row, skipped_columns)
  row['point_per'] = _compute_point_per(player_ppr_df, player_name)
  return row


def calculate_player_data_via_dictionary(c_league, c_gender, c_year):
  """
  Parallel, metric_dictionary-driven implementation of
  calc_player_data.calculate_player_data_not_background().

  Returns:
      (player_df, player_stats_df, meta) where meta = {
          'skipped_columns': sorted list of legacy columns not resolved this
                              run (unmapped OR mapped-but-missing),
          'num_players': int,
      }
      or (None, None, {'error': ...}) on structural failure (no ppr_csv_row /
      empty ppr_df), mirroring calculate_player_data_not_background's
      ["No Rows"] early-return behavior.
  """
  log_info(f"[dict-engine] League:{c_league}, Gender:{c_gender}, Year:{c_year}")
  ppr_csv_row, ppr_df = _load_league_ppr(c_league, c_gender, c_year)
  if ppr_csv_row is None:
    log_error(f"[dict-engine] No ppr_csv_row for {c_league} {c_gender} {c_year}")
    return None, None, {'error': 'No Rows'}
  if ppr_df is None or ppr_df.shape[0] == 0:
    log_error(f"[dict-engine] ppr_df has 0 rows for {c_league} {c_gender} {c_year}")
    return None, None, {'error': 'No Rows'}

  metric_dict = _load_metric_dictionary_df()
  p_list = _get_player_list(ppr_df)
  skipped_columns = set()
  rows = []

  for player_name in p_list:
    log_info(f"[dict-engine] Calculating {player_name}")
    rows.append(_calculate_one_player_row(ppr_df, player_name, metric_dict, skipped_columns))

  player_df = pd.DataFrame(rows)
  player_stats_df = _build_player_stats_df(player_df)

  meta = {'skipped_columns': sorted(skipped_columns), 'num_players': len(p_list)}
  log_info(f"[dict-engine] Done. {len(skipped_columns)} legacy columns unresolved this run.")
  return player_df, player_stats_df, meta


def calculate_single_player_data_via_dictionary(c_league, c_gender, c_year, player_name):
  """
  Same engine as calculate_player_data_via_dictionary, but for exactly one
  player -- cheap enough to run synchronously (same scale as the existing
  per-player JSON/markdown reports), for validating the metric_id mapping
  without the league-wide memory cost.

  Returns:
      (player_row_df, meta) where player_row_df is a 1-row DataFrame (or
      None on structural failure, with meta['error'] set). There is no
      per-player equivalent of player_data_stats -- that's a league-wide
      mean/stdev across every player, which this intentionally does not
      compute.
  """
  log_info(f"[dict-engine] Single player: {player_name} ({c_league}, {c_gender}, {c_year})")
  ppr_csv_row, ppr_df = _load_league_ppr(c_league, c_gender, c_year)
  if ppr_csv_row is None:
    log_error(f"[dict-engine] No ppr_csv_row for {c_league} {c_gender} {c_year}")
    return None, {'error': 'No Rows'}
  if ppr_df is None or ppr_df.shape[0] == 0:
    log_error(f"[dict-engine] ppr_df has 0 rows for {c_league} {c_gender} {c_year}")
    return None, {'error': 'No Rows'}

  metric_dict = _load_metric_dictionary_df()
  skipped_columns = set()
  row = _calculate_one_player_row(ppr_df, player_name, metric_dict, skipped_columns)

  meta = {'skipped_columns': sorted(skipped_columns)}
  log_info(f"[dict-engine] Done. {len(skipped_columns)} legacy columns unresolved this run.")
  return pd.DataFrame([row]), meta


def _build_player_stats_df(player_df):
  """Generic replacement for calc_player_data.py's ~150 hand-written
  .mean()/.std() lines. Comprehensive over every numeric column in
  player_df (which is itself comprehensive now, see _flatten_row) --
  'pair'/'player'/'team' are the only non-numeric columns and are excluded
  by select_dtypes automatically."""
  numeric_df = player_df.select_dtypes(include='number')
  stats_row = {}
  for col in numeric_df.columns:
    series = numeric_df[col]
    stats_row[f"{col}_mean"] = series.mean(skipna=True)
    stats_row[f"{col}_stdev"] = series.std(skipna=True)
  return pd.DataFrame([stats_row])

"""
metric_dictionary_diff.py
==========================
Verification tool for the calc_player_data_dictionary.py migration. Computes
the new metric_dictionary-driven player_data/player_data_stats IN MEMORY and
diffs it against the CURRENTLY STORED blobs (read via server_functions.py's
get_player_data()) -- no schema change, no writes, safe to run against a live
league at any time.
"""

import time

import anvil.server
from anvil import BlobMedia
import numpy as np
import pandas as pd

from server_functions import get_player_data
from calc_player_data import _require_internals
from calc_player_data_dictionary import calculate_player_data_via_dictionary
from player_data_column_alias_map import DROPPED_LEGACY_COLUMNS


def _diff_player_dataframes(old_df, new_df, key_col='player'):
  if old_df.empty or new_df.empty:
    return {'error': 'one or both player_data frames are empty'}

  old_players = set(old_df[key_col])
  new_players = set(new_df[key_col])

  old_cols = set(old_df.columns) - {key_col}
  new_cols = set(new_df.columns) - {key_col}

  missing_in_new = sorted((old_cols - new_cols) - DROPPED_LEGACY_COLUMNS)
  intentionally_dropped = sorted((old_cols - new_cols) & DROPPED_LEGACY_COLUMNS)
  new_only = sorted(new_cols - old_cols)

  merged = pd.merge(old_df, new_df, on=key_col, suffixes=('_old', '_new'), how='inner')

  column_report = []
  for col in sorted(old_cols & new_cols):
    old_series = pd.to_numeric(merged[f"{col}_old"], errors='coerce')
    new_series = pd.to_numeric(merged[f"{col}_new"], errors='coerce')
    both_present = old_series.notna() & new_series.notna()
    abs_diff = (old_series - new_series).abs()
    column_report.append({
      'column': col,
      'n_players_compared': int(both_present.sum()),
      'mean_abs_diff': float(abs_diff[both_present].mean()) if both_present.any() else None,
      'max_abs_diff': float(abs_diff[both_present].max()) if both_present.any() else None,
      'n_only_in_old': int((old_series.notna() & new_series.isna()).sum()),
      'n_only_in_new': int((old_series.isna() & new_series.notna()).sum()),
    })

  return {
    'players_only_in_old': sorted(old_players - new_players),
    'players_only_in_new': sorted(new_players - old_players),
    'missing_in_new': missing_in_new,          # real gaps to fix
    'intentionally_dropped': intentionally_dropped,
    'new_only': new_only,                      # bonus coverage, review not fix
    'column_report': column_report,
  }


def _diff_stats_dataframes(old_df, new_df):
  if old_df.empty or new_df.empty:
    return {'error': 'one or both player_data_stats frames are empty'}
  old_cols = set(old_df.columns)
  new_cols = set(new_df.columns)
  rows = []
  for col in sorted(old_cols & new_cols):
    old_v, new_v = old_df.at[0, col], new_df.at[0, col]
    abs_diff = None
    if pd.notna(old_v) and pd.notna(new_v):
      abs_diff = abs(old_v - new_v)
    rows.append({'column': col, 'old': old_v, 'new': new_v, 'abs_diff': abs_diff})
  return {
    'missing_in_new': sorted((old_cols - new_cols) - DROPPED_LEGACY_COLUMNS),
    'new_only': sorted(new_cols - old_cols),
    'column_report': rows,
  }


def diff_player_data_dictionary_vs_legacy(c_league, c_gender, c_year):
  """
  Plain internal entry point (not itself server-callable -- see the
  background-task wrapper below). Computes the new dictionary-driven
  player_data/player_data_stats and diffs against the currently stored blobs.
  """
  new_player_df, new_stats_df, meta = calculate_player_data_via_dictionary(c_league, c_gender, c_year)
  if new_player_df is None:
    return {'error': meta.get('error', 'unknown failure')}

  old_player_df, old_stats_df = get_player_data(c_league, c_gender, c_year)

  return {
    'league': c_league, 'gender': c_gender, 'year': c_year,
    'skipped_columns': meta['skipped_columns'],
    'player_count_old': len(old_player_df), 'player_count_new': len(new_player_df),
    'player_data_diff': _diff_player_dataframes(old_player_df, new_player_df),
    'player_stats_diff': _diff_stats_dataframes(old_stats_df, new_stats_df),
  }


@anvil.server.callable
def diff_player_data_dictionary_vs_legacy_background(c_league, c_gender, c_year):
  """Launches the diff as a background task, so the actual pandas-heavy
  computation runs in its own process with generous resource limits instead
  of inside a synchronous server call (which is what was getting killed).
  Polls server-side (real time.sleep, unlike client-side Skulpt) and returns
  the finished BlobMedia directly -- so the client still only needs one
  simple blocking anvil.server.call(), no Task/Timer handling required."""
  _require_internals()
  task = anvil.server.launch_background_task(
    'diff_player_data_dictionary_vs_legacy_task', c_league, c_gender, c_year)
  while task.is_running():
    time.sleep(2)
  error = task.get_error()
  if error is not None:
    raise error
  return task.get_return_value()


@anvil.server.background_task
def diff_player_data_dictionary_vs_legacy_task(c_league, c_gender, c_year):
  """Runs the full diff and packages it as a downloadable BlobMedia."""
  report = diff_player_data_dictionary_vs_legacy(c_league, c_gender, c_year)
  markdown = format_diff_report_as_markdown(report)
  filename = f"player_data_diff_{c_league}_{c_gender}_{c_year}.md"
  return BlobMedia("text/markdown", markdown.encode("utf-8"), name=filename)


def format_diff_report_as_markdown(report):
  """Human-readable rendering via tabulate (already a project dependency,
  used in calc_player_data.py)."""
  from tabulate import tabulate

  if 'error' in report:
    return f"# Diff Report Failed\n\n{report['error']}"

  lines = [
    f"# Diff Report: {report['league']} {report['gender']} {report['year']}",
    f"Players: old={report['player_count_old']} new={report['player_count_new']}",
    f"Skipped (unresolved) columns: {len(report['skipped_columns'])}",
  ]
  if report['skipped_columns']:
    lines.append(f"  {report['skipped_columns']}")

  pdd = report['player_data_diff']
  if 'error' in pdd:
    lines.append(f"\n## player_data diff failed: {pdd['error']}")
  else:
    lines.append(f"\n## player_data -- missing_in_new ({len(pdd['missing_in_new'])}): {pdd['missing_in_new']}")
    lines.append(f"## player_data -- new_only ({len(pdd['new_only'])}): {pdd['new_only']}")
    lines.append("\n## Per-column comparison")
    lines.append(tabulate(pdd['column_report'], headers='keys', floatfmt='.4f'))

  psd = report['player_stats_diff']
  if 'error' in psd:
    lines.append(f"\n## player_data_stats diff failed: {psd['error']}")
  else:
    lines.append(f"\n## player_data_stats -- missing_in_new ({len(psd['missing_in_new'])}): {psd['missing_in_new']}")
    lines.append(f"## player_data_stats -- new_only ({len(psd['new_only'])}): {psd['new_only']}")
    lines.append("\n## Per-column comparison")
    lines.append(tabulate(psd['column_report'], headers='keys', floatfmt='.4f'))

  return "\n".join(lines)

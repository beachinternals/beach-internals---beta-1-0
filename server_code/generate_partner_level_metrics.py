"""
Generate Partner-Level Metrics for AI Export

This module calculates metrics for a single player broken out by each
partner they have played with in the (already filtered) PPR data. It
mirrors the shape of generate_set_level_metrics.py, but groups a player's
points by partner instead of by set.

Metric selection is driven by the 'partner_level' Yes/No flag column on
metric_dictionary (same pattern as set_level_whole/set_level_half). Each
partner's metrics are calculated by re-running the existing aggregate
calculation engine (calculate_all_metrics) on just that partner's points,
so the same min_attempts_for_ci gating already used for aggregate metrics
applies per partner — there is no separate partner-level point threshold.

Author: Beach Volleyball Analytics
Created: 2026-07-13
"""

import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import pandas as pd
import numpy as np
import json

# ============================================================================
# LOGGING IMPORTS
# ============================================================================
from logger_utils import log_debug, log_info, log_error, log_critical

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

from generate_player_metrics_json_server import calculate_all_metrics

# Reuse existing helpers rather than duplicating comp_level / uuid lookups
# and value-formatting logic.
from generate_set_level_metrics import (
  flag_is_yes,
  get_comp_level_for_player,
  get_player_uuid,
  _fmt_val,
  _fmt_comp_level,
)


# ============================================================================
# METRIC DICTIONARY: partner_level=Yes rows, as a DataFrame
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_partner_level_metric_dict():
  """
  Load metric_dictionary and filter to rows flagged partner_level=Yes.

  Returns:
      DataFrame: subset of metric_dictionary columns/rows suitable for
                 calculate_all_metrics(). Empty DataFrame if the column
                 doesn't exist yet or nothing is flagged.
  """
  log_info("Loading metric_dictionary for partner-level metrics (partner_level=Yes)...")

  dict_rows = list(app_tables.metric_dictionary.search())
  if len(dict_rows) == 0:
    log_error("metric_dictionary is empty")
    return pd.DataFrame()

  column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
  metric_dict = pd.DataFrame([{col: row[col] for col in column_names} for row in dict_rows])

  if 'partner_level' not in metric_dict.columns:
    log_error("metric_dictionary has no 'partner_level' column — no partner metrics will be calculated")
    return metric_dict.iloc[0:0]

  selected = metric_dict[metric_dict['partner_level'].apply(flag_is_yes)].reset_index(drop=True)
  log_info(f"Found {len(selected)} metrics flagged partner_level=Yes (of {len(metric_dict)} in dictionary)")
  return selected


# ============================================================================
# PARTNER RESOLUTION
# ============================================================================

def _resolve_partner_series(df, player_name):
  """
  For each row, return the name of player_name's teammate on that point.

  player_name can be on team A (player_a1/a2) or team B (player_b1/b2) on
  any given row — matches are resolved per-row rather than assumed
  constant across the dataframe.
  """
  conditions = [
    df['player_a1'] == player_name,
    df['player_a2'] == player_name,
    df['player_b1'] == player_name,
    df['player_b2'] == player_name,
  ]
  choices = [
    df['player_a2'],
    df['player_a1'],
    df['player_b2'],
    df['player_b1'],
  ]
  return np.select(conditions, choices, default=None)


# ============================================================================
# MAIN GENERATION FUNCTION
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def generate_partner_level_metrics_for_player(ppr_df, player_name, league_value, team):
  """
  Generate metrics broken out by partner for a single player.

  Args:
      ppr_df:       Filtered PPR dataframe (already filtered for date/comp/etc.)
      player_name:  Player name in format "TEAM NUMBER SHORTNAME"
      league_value: League string (e.g., "NCAA | W | 2026")
      team:         Team name

  Returns:
      dict: {
          'player', 'league', 'team',
          'partners': [ {partner, partner_uuid, comp_level, sets_together,
                          points_together, date_start, date_end, metrics} ],
          'summary': { total_partners, total_points, metrics_per_partner }
      }
      None if there is no data for this player or no partner_level metrics
      are defined.
  """
  log_info(f"Generating partner-level metrics for {player_name}")

  partner_metric_dict = get_partner_level_metric_dict()
  if len(partner_metric_dict) == 0:
    log_error("No metrics flagged partner_level=Yes in metric_dictionary")
    return None

  player_df = ppr_df[
    (ppr_df['player_a1'] == player_name) |
    (ppr_df['player_a2'] == player_name) |
    (ppr_df['player_b1'] == player_name) |
    (ppr_df['player_b2'] == player_name)
  ].copy()

  if len(player_df) == 0:
    log_error(f"No data found for player {player_name}")
    return None

  log_info(f"Found {len(player_df)} total points for player")

  player_df['partner'] = _resolve_partner_series(player_df, player_name)
  points_before_partner_filter = len(player_df)
  player_df = player_df[player_df['partner'].notna() & (player_df['partner'] != '')]

  dropped_unresolved = points_before_partner_filter - len(player_df)
  if dropped_unresolved > 0:
    log_error(
      f"Partner-level: dropped {dropped_unresolved} of {points_before_partner_filter} "
      f"points for {player_name} — partner could not be resolved from "
      f"player_a1/a2/b1/b2 (malformed or blank row)"
    )

  unique_partners = sorted(player_df['partner'].unique())
  log_info(f"Found {len(unique_partners)} unique partner(s) for {player_name}")

  partners_data = []

  for partner_name in unique_partners:
    partner_df = player_df[player_df['partner'] == partner_name]

    sets_together = partner_df.groupby(['video_id', 'set']).ngroups
    points_together = len(partner_df)

    date_start = str(partner_df['game_date'].min()) if 'game_date' in partner_df.columns else None
    date_end   = str(partner_df['game_date'].max()) if 'game_date' in partner_df.columns else None

    comp_partner = get_comp_level_for_player(partner_name, league_value)
    uuid_partner = get_player_uuid(partner_name, league_value)

    metrics_result = calculate_all_metrics(partner_metric_dict, partner_df, player_name)

    partners_data.append({
      'partner':          partner_name,
      'partner_uuid':     uuid_partner,
      'comp_level':       comp_partner,
      'sets_together':    sets_together,
      'points_together':  points_together,
      'date_start':       date_start,
      'date_end':         date_end,
      'metrics':          metrics_result['metrics'],
      'metrics_calculated': metrics_result['successful']
    })

    log_debug(
      f"Partner {partner_name}: {points_together} pts, {sets_together} sets, "
      f"{metrics_result['successful']} metrics with sufficient data"
    )

  # Most-played partner first
  partners_data.sort(key=lambda p: p['points_together'], reverse=True)

  summary = {
    'total_partners':               len(partners_data),
    'total_points':                 len(player_df),
    'metrics_per_partner':          len(partner_metric_dict),
    'points_excluded_unresolved_partner': dropped_unresolved
  }

  log_info(f"Partner-level processing complete: {len(partners_data)} partners")

  return {
    'player':   player_name,
    'league':   league_value,
    'team':     team,
    'partners': partners_data,
    'summary':  summary
  }


# ============================================================================
# FORMAT: HUMAN-READABLE  (ai_optimized=False)
# ============================================================================

def _format_partner_level_human(partner_level_data, display_name=None, display_team=None):
  """Human-readable markdown format."""
  log_info("Formatting partner-level data as markdown (human)...")

  content_name = display_name if display_name else partner_level_data['player']
  content_team = display_team if display_team else partner_level_data['team']

  md = []

  md.append(f"# {content_name}")
  md.append(f"")
  md.append(f"**League:** {partner_level_data['league']}")
  md.append(f"**Team:** {content_team}")
  md.append(f"")

  summary = partner_level_data['summary']
  md.append(f"## Partner Summary")
  md.append(f"- Total Partners: {summary['total_partners']}")
  md.append(f"- Total Points (all partners): {summary['total_points']}")
  md.append(f"- Metrics Per Partner: {summary['metrics_per_partner']}")
  md.append(f"")
  md.append(f"---")
  md.append(f"")

  md.append(f"## Performance By Partner")
  md.append(f"")

  for partner_data in partner_level_data['partners']:
    md.append(f"### Partner: {partner_data['partner']}")
    md.append(f"")
    md.append(f"**Partner Information:**")
    md.append(f"- Points Together: {partner_data['points_together']}")
    md.append(f"- Sets Together: {partner_data['sets_together']}")
    if partner_data.get('date_start'):
      md.append(f"- Date Range: {partner_data['date_start']} to {partner_data['date_end']}")
    md.append(f"- Partner Competitive Level: {_fmt_comp_level(partner_data.get('comp_level'))}")
    md.append(f"")

    md.append(f"**Metrics:**")
    md.append(f"")
    metrics_dict = partner_data['metrics']
    for category in sorted(metrics_dict.keys()):
      md.append(f"#### {category}")
      md.append(f"")
      metrics_in_category = metrics_dict[category]
      for metric_id in sorted(metrics_in_category.keys()):
        metric_info = metrics_in_category[metric_id]
        value_str = _fmt_val(metric_info['value'])
        attempts  = metric_info.get('attempts')
        if attempts is not None:
          md.append(f"- **{metric_info['metric_name']}** (`{metric_id}`): {value_str} (n={attempts})")
        else:
          md.append(f"- **{metric_info['metric_name']}** (`{metric_id}`): {value_str}")
      md.append(f"")

    md.append(f"---")
    md.append(f"")

  return "\n".join(md)


# ============================================================================
# FORMAT: AI-OPTIMIZED DENSE  (ai_optimized=True)
# ============================================================================

def _format_partner_level_dense(partner_level_data, display_name=None, display_team=None):
  """
  Token-efficient dense format for AI consumption.

  PARTNER header line:
    PARTNER|partner_uuid|sets:N|pts:N|comp:rank/score|dates:start..end
  Category line:
    [category]
  Metrics line:
    metric_id:value(n=att) ...
  """
  log_info("Formatting partner-level data as markdown (ai_optimized dense)...")

  content_name = display_name if display_name else partner_level_data['player']
  content_team = display_team if display_team else partner_level_data['team']

  summary = partner_level_data['summary']

  md = []

  md.append(
    f"PLAYER|{content_name}|{partner_level_data['league']}|{content_team}|"
    f"partners:{summary['total_partners']}|pts:{summary['total_points']}"
  )
  md.append(f"")

  md.append("# Partner-Level Metrics")
  md.append("# Format: PARTNER|partner_uuid|sets:N|pts:N|comp:rank/score|dates:start..end")
  md.append("# [category] then metric_id:value(n=attempts) tokens")
  md.append("# Decode metric_id codes using the metric_dictionary file.")
  md.append("")

  for partner_data in partner_level_data['partners']:
    comp_dict = partner_data.get('comp_level') or {}
    rank  = comp_dict.get('comp_level_rank')
    score = comp_dict.get('comp_level_score')
    if rank is None and score is None:
      comp_str = "N/A"
    else:
      comp_str = f"{rank if rank is not None else '?'}/{f'{score:.2f}' if score is not None else '?'}"

    date_start = partner_data.get('date_start') or ''
    date_end   = partner_data.get('date_end') or ''
    date_str   = f"{date_start}..{date_end}" if date_start else "all"

    md.append(
      f"PARTNER|{partner_data['partner_uuid']}|"
      f"sets:{partner_data['sets_together']}|pts:{partner_data['points_together']}|"
      f"comp:{comp_str}|dates:{date_str}"
    )

    metrics_dict = partner_data['metrics']
    for category in sorted(metrics_dict.keys()):
      metrics_in_category = metrics_dict[category]
      parts = []
      for metric_id in sorted(metrics_in_category.keys()):
        metric_info = metrics_in_category[metric_id]
        metric_value = metric_info['value']
        sufficient   = metric_info.get('sufficient_data', True)

        if not sufficient or metric_value is None:
          continue  # skip insufficient in optimized mode

        if isinstance(metric_value, float):
          formatted_value = f"{metric_value:.3f}".rstrip('0').rstrip('.')
        else:
          formatted_value = str(metric_value)

        attempts = metric_info.get('attempts')
        att_str  = f"(n={attempts})" if attempts is not None else ""
        parts.append(f"{metric_id}:{formatted_value}{att_str}")

      if parts:
        md.append(f"[{category}]")
        md.append(" ".join(parts))

    md.append("")  # blank line between partners

  return "\n".join(md)


# ============================================================================
# PUBLIC FORMAT ENTRY POINT
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_partner_level_data_as_markdown(
  partner_level_data,
  display_name=None,
  display_team=None,
  ai_optimized=False
):
  """
  Format partner-level data as markdown.

  Args:
      partner_level_data: Output from generate_partner_level_metrics_for_player
      display_name      : Optional override for player name in headers
      display_team      : Optional override for team name in headers
      ai_optimized      : If True, use dense token-efficient format

  Returns:
      str: Markdown formatted text
  """
  if not partner_level_data:
    return ""

  if ai_optimized:
    return _format_partner_level_dense(partner_level_data, display_name, display_team)
  else:
    return _format_partner_level_human(partner_level_data, display_name, display_team)


# ============================================================================
# JSON FORMAT
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def format_partner_level_data_as_json(partner_level_data):
  """
  Format partner-level data as JSON.

  Returns:
      str: JSON formatted text
  """
  if not partner_level_data:
    return "{}"

  log_info("Formatting partner-level data as JSON...")

  json_data = {
    'player':   partner_level_data['player'],
    'league':   partner_level_data['league'],
    'team':     partner_level_data['team'],
    'summary':  partner_level_data['summary'],
    'partners': partner_level_data['partners']
  }

  return json.dumps(json_data, indent=2, default=str)

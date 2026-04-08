"""
SERVER FUNCTION: generate_player_metrics_markdown()
====================================================
Modified version that generates Markdown exports instead of JSON,
optimized for NotebookLM consumption.

Based on generate_player_metrics_json.py but outputs structured Markdown.

UPDATED: Added ai_optimized flag for token-efficient dense output.
"""

import anvil.tables as tables
from anvil.tables import app_tables
import anvil.users
import anvil.server
import anvil.media
import pandas as pd
import numpy as np
import json
from datetime import datetime
import hashlib
import io
import re

# Import your logging utilities
from logger_utils import log_info, log_error, log_debug, log_critical

# ============================================================================
#
#  AUTH HELPERS
#  _require_own_team(team) — logged-in AND requesting own team's data
#  _require_internals()    — INTERNALS team only
#
# ============================================================================

def _require_own_team(team):
  """
  Verify the caller is logged in AND is either on the INTERNALS team
  (can access any team) or requesting their own team's data only.
  Returns the user row or raises Exception.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS' and team != user['team']:
    raise Exception("Access denied: you can only access your own team's data.")
  return user

def _require_internals():
  """
  Verify the caller is logged in AND is on the INTERNALS team.
  Raises Exception if not authorized. Returns user row.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS':
    raise Exception("Access denied: this function is for admins only.")
  return user



from server_functions import *
from metric_calc_functions import *
from generate_player_metrics_json_server import calculate_all_metrics


def sanitize_name_for_filename(name):
  """Sanitize player name for use in filenames."""
  if not name:
    return "Unknown_Player"
  sanitized = re.sub(r'[^\w\s-]', '', name)
  sanitized = re.sub(r'[-\s]+', '_', sanitized)
  return sanitized.strip('_')


def format_metric_value(value, metric_row=None):
  """
    Format metric values for display in Markdown.
    
    Args:
        value: The value to format
        metric_row: Optional row from metric_dictionary with context
    
    Returns:
        str: Formatted value string
    """
  if value is None or pd.isna(value):
    return "-"

  if isinstance(value, (int, float, np.number)):
    # Format numbers with appropriate precision
    if isinstance(value, bool):
      return "Yes" if value else "No"

    if abs(value) < 0.001 and value != 0:
      return f"{value:.4f}".rstrip('0').rstrip('.')
    elif abs(value) < 0.1:
      return f"{value:.3f}".rstrip('0').rstrip('.')
    elif abs(value) < 1:
      return f"{value:.2f}".rstrip('0').rstrip('.')
    else:
      return f"{value:.2f}".rstrip('0').rstrip('.')

  return str(value)


def determine_metric_context(metric_row):
  """
    Determine context/goal for a metric based on dictionary metadata.
    
    Args:
        metric_row: Row from metric_dictionary DataFrame
    
    Returns:
        str: Context or goal description
    """
  metric_id = metric_row.get('metric_id', '').lower()
  metric_name = metric_row.get('metric_name', '').lower()
  description = metric_row.get('description', '')

  # Use description if available
  if pd.notna(description) and description.strip():
    # Truncate long descriptions
    if len(description) > 80:
      return description[:77] + "..."
    return description

    # Infer from metric_id patterns
  if 'consistency' in metric_id or 'std' in metric_id or 'cv' in metric_id:
    return "Lower = more consistent"

  if 'error' in metric_id or 'fault' in metric_id:
    return "Lower is better"

  if 'efficiency' in metric_id or 'fbhe' in metric_id or 'eso' in metric_id:
    return "Higher is better"

  if 'pct' in metric_id or 'percentage' in metric_id:
    return "Performance rate"

  if 'count' in metric_id or 'total' in metric_id:
    return "Volume indicator"

  if 'area' in metric_id or 'zone' in metric_id:
    return "Spatial metric"

  return "Performance indicator"


def generate_player_overview(player_name, metadata, metrics_by_category):
  """
    Generate a 3-sentence narrative overview based on calculated metrics.
    
    Args:
        player_name: Name of the player
        metadata: Metadata dict with filters, etc.
        metrics_by_category: Dict of calculated metrics by category
    
    Returns:
        str: 3-sentence overview
    """
  total_points = metadata.get('total_points_analyzed', 0)
  total_sets = metadata.get('total_sets_analyzed', 0)

  # Identify strengths from metrics
  strengths = []

  # Check passing metrics
  passing_metrics = metrics_by_category.get('Passing', {})
  if 'fbhe_overall' in passing_metrics:
    fbhe_val = passing_metrics['fbhe_overall'].get('value')
    if fbhe_val and fbhe_val > 0.60:
      strengths.append("strong passing efficiency")

    # Check attacking metrics
  attacking_metrics = metrics_by_category.get('Attacking', {})
  if 'attack_efficiency_overall' in attacking_metrics:
    att_eff = attacking_metrics['attack_efficiency_overall'].get('value')
    if att_eff and att_eff > 0.40:
      strengths.append("effective attacking")

    # Check serving metrics
  serving_metrics = metrics_by_category.get('Serving', {})
  if 'ace_percentage' in serving_metrics:
    ace_pct = serving_metrics['ace_percentage'].get('value')
    if ace_pct and ace_pct > 0.15:
      strengths.append("powerful serving")

    # Construct sentences
  sentence1 = f"{player_name} is a beach volleyball player with comprehensive performance data spanning {total_points} points across {total_sets} sets."

  if strengths:
    sentence2 = f"Key strengths identified include {', '.join(strengths[:3])}."
  else:
    sentence2 = f"This analysis covers {len(metrics_by_category)} major skill categories with detailed performance indicators."

  date_range = metadata.get('filters_applied', {})
  if 'start_date' in date_range and 'end_date' in date_range:
    sentence3 = f"Data analyzed from {date_range['start_date']} to {date_range['end_date']}, providing insights for targeted skill development."
  else:
    sentence3 = "This comprehensive report provides metrics for tracking progress and identifying development opportunities."

  return f"{sentence1} {sentence2} {sentence3}"


def generate_descriptive_header(player_name, metadata, metrics_by_category):
  """
    Generate 2-3 descriptive paragraphs for the report header.
    Optimized for NotebookLM understanding.
    
    Args:
        player_name: Name of the player
        metadata: Metadata dict
        metrics_by_category: Dict of calculated metrics by category
        
    Returns:
        str: Multi-paragraph header description
    """
  paragraphs = []

  # Paragraph 1: Player introduction and data scope
  total_points = metadata.get('total_points_analyzed', 0)
  total_sets = metadata.get('total_sets_analyzed', 0)
  league = metadata.get('league', '')
  gender = metadata.get('gender', '')
  year = metadata.get('year', '')
  team = metadata.get('team', '')

  para1 = (f"This performance analysis report presents comprehensive beach volleyball metrics for "
           f"{player_name}, competing for {team} in the {league} {gender} {year} season. "
           f"The analysis encompasses {total_points} individual rally outcomes across {total_sets} competitive match sets, "
           f"providing a detailed statistical profile of performance across all major skill categories including "
           f"serving, passing, attacking, defense, and blocking.")
  paragraphs.append(para1)

  # Paragraph 2: Filters and data collection context
  filters = metadata.get('filters_applied', {})
  if filters:
    filter_desc = []
    if 'start_date' in filters and 'end_date' in filters:
      filter_desc.append(f"date range from {filters['start_date']} to {filters['end_date']}")
    if 'comp_l1' in filters:
      filter_desc.append(f"competition level {filters['comp_l1']}")
    if 'comp_l2' in filters:
      filter_desc.append(f"tournament classification {filters['comp_l2']}")

    if filter_desc:
      para2 = (f"Data collection was filtered to include only matches meeting specific criteria: "
               f"{', '.join(filter_desc)}. "
               f"This targeted approach ensures the metrics reflect performance under consistent competitive conditions, "
               f"allowing for meaningful trend analysis and performance comparison.")
    else:
      para2 = (f"This report includes all available match data for the specified time period, "
               f"providing a comprehensive view of overall performance without filtering by competition level or opponent quality.")
  else:
    para2 = (f"This report includes all available match data for {player_name} during the {year} season, "
             f"providing a comprehensive view of performance across all competition levels and opponents.")
  paragraphs.append(para2)

  # Paragraph 3: Purpose and usage
  metrics_calculated = metadata.get('summary_stats', {}).get('metrics_with_sufficient_data', 0)
  para3 = (f"The metrics in this report are organized by skill category and calculated using a comprehensive "
           f"metric dictionary with standardized formulas and minimum attempt thresholds to ensure statistical validity. "
           f"Of the metrics evaluated, {metrics_calculated} met the minimum sample size requirements for reliable analysis. "
           f"Each metric includes contextual guidance on interpretation and performance benchmarks. "
           f"This report is designed for use by coaches, players, and analysts to identify strengths, "
           f"target areas for improvement, and track performance trends over time.")
  paragraphs.append(para3)

  return "\n\n".join(paragraphs)


def filter_metrics_by_attempts(metrics_dict, metric_dict_df):
  """
    Filter out metrics where attempts (_n suffix) is 0, unless it's a core metric.
    """
  filtered_metrics = {}
  metrics_to_skip = set()

  # First pass: identify metrics with zero attempts
  for metric_id, metric_info in metrics_dict.items():
    if metric_id.endswith('_n'):
      value = metric_info.get('value')
      if value == 0 or value == 0.0:
        parent_id = metric_id[:-2]
        metrics_to_skip.add(parent_id)
        metrics_to_skip.add(metric_id)

  # Second pass: filter metrics
  for metric_id, metric_info in metrics_dict.items():
    if metric_id in metrics_to_skip:
      continue

    metric_row = metric_dict_df[metric_dict_df['metric_id'] == metric_id]
    is_core = False
    if len(metric_row) > 0:
      parent_metric = metric_row.iloc[0].get('parent_metric')
      is_core = pd.isna(parent_metric) or parent_metric == ''

    value = metric_info.get('value')

    if is_core:
      filtered_metrics[metric_id] = metric_info
      continue

    if isinstance(value, int) and value == 0:
      continue

    filtered_metrics[metric_id] = metric_info

  return filtered_metrics


def organize_metrics_hierarchically(metrics_dict, metric_dict_df):
  """
    Organize metrics into hierarchical structure based on metric_id patterns.
    """
  core_metrics = []
  derived_metrics = {}

  for metric_id, metric_info in metrics_dict.items():
    metric_row = metric_dict_df[metric_dict_df['metric_id'] == metric_id]
    if len(metric_row) == 0:
      continue

    metric_row = metric_row.iloc[0]
    parent_metric = metric_row.get('parent_metric')

    if pd.isna(parent_metric) or parent_metric == '':
      core_metrics.append((metric_id, metric_info, metric_row))
    else:
      if parent_metric not in derived_metrics:
        derived_metrics[parent_metric] = []
      derived_metrics[parent_metric].append((metric_id, metric_info, metric_row))

  if not derived_metrics:
    for metric_id, metric_info in metrics_dict.items():
      metric_row = metric_dict_df[metric_dict_df['metric_id'] == metric_id]
      if len(metric_row) == 0:
        continue

      metric_row = metric_row.iloc[0]

      if not any(c.isdigit() for c in metric_id) and '_' not in metric_id:
        if (metric_id, metric_info, metric_row) not in core_metrics:
          core_metrics.append((metric_id, metric_info, metric_row))
      else:
        parent_name = metric_id.split('_')[0].split('1')[0].split('2')[0].split('3')[0].split('4')[0].split('5')[0]
        if parent_name not in derived_metrics:
          derived_metrics[parent_name] = []
        derived_metrics[parent_name].append((metric_id, metric_info, metric_row))

  return {'core': core_metrics, 'derived': derived_metrics}


def create_hierarchical_metrics_tables(metrics_dict, metric_dict_df, category_name):
  """
  Create hierarchically organized Markdown tables for a category.
  Includes a Plays column when any metric in the group has video links.
  """
  if not metrics_dict:
    return ""

  md = f"## {category_name} Metrics\n\n"

  filtered_metrics = filter_metrics_by_attempts(metrics_dict, metric_dict_df)

  if not filtered_metrics:
    md += "*No metrics with sufficient data for this category.*\n\n"
    return md

  hierarchy = organize_metrics_hierarchically(filtered_metrics, metric_dict_df)

  # --- Core metrics ---
  if hierarchy['core']:
    core_has_video = any(
      metric_info.get('video_links', '')
      for _, metric_info, _ in hierarchy['core']
    )

    md += "### Core Performance Indicators\n\n"
    if core_has_video:
      md += "| Metric | Value | Context/Goal | Plays |\n"
      md += "| :--- | :--- | :--- | :--- |\n"
    else:
      md += "| Metric | Value | Context/Goal |\n"
      md += "| :--- | :--- | :--- |\n"

    for metric_id, metric_info, metric_row in sorted(hierarchy['core'], key=lambda x: x[0]):
      metric_name = metric_info.get('metric_name', metric_id)
      value = format_metric_value(metric_info.get('value'), metric_row)
      context = determine_metric_context(metric_row)
      video_links = metric_info.get('video_links', '')

      if core_has_video:
        md += f"| {metric_name} | {value} | {context} | {video_links} |\n"
      else:
        md += f"| {metric_name} | {value} | {context} |\n"

    md += "\n"

  # --- Derived/breakdown metrics ---
  if hierarchy['derived']:
    md += "### Detailed Breakdowns\n\n"

    for parent_name in sorted(hierarchy['derived'].keys()):
      child_metrics = hierarchy['derived'][parent_name]

      group_has_video = any(
        metric_info.get('video_links', '')
        for _, metric_info, _ in child_metrics
      )

      parent_display = parent_name.replace('_', ' ').title()
      md += f"#### {parent_display}\n\n"

      if group_has_video:
        md += "| Metric | Value | Context/Goal | Plays |\n"
        md += "| :--- | :--- | :--- | :--- |\n"
      else:
        md += "| Metric | Value | Context/Goal |\n"
        md += "| :--- | :--- | :--- |\n"

      for metric_id, metric_info, metric_row in sorted(child_metrics, key=lambda x: x[0]):
        metric_name = metric_info.get('metric_name', metric_id)
        value = format_metric_value(metric_info.get('value'), metric_row)
        context = determine_metric_context(metric_row)
        video_links = metric_info.get('video_links', '')

        if '_' in metric_id or any(c.isdigit() for c in metric_id):
          metric_name = "  " + metric_name

        if group_has_video:
          md += f"| {metric_name} | {value} | {context} | {video_links} |\n"
        else:
          md += f"| {metric_name} | {value} | {context} |\n"

      md += "\n"

  return md


def format_metric_line(metric_id, metric_info, metric_dict_df):
  """
    Format a single metric as a markdown list line.
    """
  metric_name = metric_info.get('metric_name', metric_id)
  value       = metric_info.get('value')
  attempts    = metric_info.get('attempts')
  sufficient  = metric_info.get('sufficient_data', True)
  min_att     = metric_info.get('min_attempts')

  if not sufficient or value is None:
    if attempts is not None and min_att is not None:
      value_str = f"insufficient data (n={attempts}, min={min_att})"
    else:
      value_str = "insufficient data"
  elif isinstance(value, float):
    if abs(value) < 0.001 and value != 0:
      value_str = f"{value:.4f}".rstrip('0').rstrip('.')
    elif abs(value) < 1:
      value_str = f"{value:.3f}".rstrip('0').rstrip('.')
    else:
      value_str = f"{value:.2f}".rstrip('0').rstrip('.')
  else:
    value_str = str(value)

  att_str = f" (n={attempts})" if attempts is not None else ""

  return f"- **{metric_name}** (`{metric_id}`): {value_str}{att_str}"


def generate_coach_view_sections(metrics_by_category, metric_dict_df, ai_optimized=False):
  """
    Generate the coach-view organized metrics sections.

    Args:
        metrics_by_category: dict from calculate_all_metrics
        metric_dict_df: full metric dictionary DataFrame
        ai_optimized (bool): If True, emit dense key:value lines instead of tables.

    Returns:
        str: Markdown content for all coach-view sections
    """

  COACH_VIEW_ORDER = [
    ('attacking',                  '1. Attacking — Overall'),
    ('attacking_net',              '1b. Attacking — Along the Net'),
    ('attacking_angle',            '1c. Attacking — By Angle'),
    ('attacking_sets',             '1d. Attacking — By Set Type'),
    ('attacking_style',            '1e. Attacking — By Attack Style'),
    ('attacking_situation',        '1f. Attacking — By Situation'),
    ('attacking_serve',            '1g. Attacking — By Serve Destination'),
    ('attacking_serve_source',     '1h. Attacking — By Serve Source'),
    ('serve_receive',              '2a. Serve Receive — Overall'),
    ('serve_receive_body',         '2b. Serve Receive — By Body Position'),
    ('serve_receive_source_body',  '2c. Serve Receive — Source × Body'),
    ('transition',                 '3a. Transition — Overall'),
    ('transition_serving',         '3b. Transition — When Serving'),
    ('transition_receiving',       '3c. Transition — When Receiving'),
    ('serving',                    '4a. Serving — Overall'),
    ('serving_source',             '4b. Serving — By Source Zone'),
    ('serving_opp_fbhe',           '4c. Serving — Opponent FBHE'),
    ('serving_opp_net',            '4d. Serving — Opponent FBHE by Net Zone'),
    ('serving_opp_fbhe_source',    '4e. Serving — Opponent FBHE by Destination'),
    ('serving_opp_fbhe_pass',      '4f. Serving — Opponent Pass Area'),
    ('errors',                     '5. Errors'),
    ('consistency_match',          '6a. Consistency — During Match'),
    ('consistency_set2set',        '6b. Consistency — Set to Set'),
  ]

  # Flatten all metrics
  all_metrics = {}
  for cat_metrics in metrics_by_category.values():
    all_metrics.update(cat_metrics)

  # Build coach_view lookup
  cv_lookup = {}
  if 'coach_view' in metric_dict_df.columns:
    for _, row in metric_dict_df.iterrows():
      mid = row['metric_id']
      cv  = row.get('coach_view', '')
      if pd.notna(cv) and str(cv).strip():
        cv_lookup[mid] = str(cv).strip()

  # Group by coach_view
  cv_groups = {}
  unassigned = {}
  for metric_id, metric_info in all_metrics.items():
    cv = cv_lookup.get(metric_id, '')
    if cv:
      cv_groups.setdefault(cv, {})[metric_id] = metric_info
    else:
      unassigned[metric_id] = metric_info

  def zone_key(metric_id):
    m = re.search(r'(?:^|[_])([1-5])(?:[_]|$)', metric_id)
    if m:
      return int(m.group(1))
    m = re.search(r'(\d)$', metric_id.split('_')[0])
    if m and int(m.group(1)) in range(1, 6):
      return int(m.group(1))
    return 0

  def sort_metrics(metrics_dict):
    def sort_key(item):
      metric_id, metric_info = item
      attempts = metric_info.get('attempts') or 0
      return (zone_key(metric_id), -attempts)
    return sorted(metrics_dict.items(), key=sort_key)

  md = []

  for cv_key, section_title in COACH_VIEW_ORDER:
    section_metrics = cv_groups.get(cv_key, {})
    if not section_metrics:
      continue

    if ai_optimized:
      # ── DENSE: section header + one metric per short line ───────────────
      md.append(f"[{cv_key}]")
      metric_parts = []
      for metric_id, metric_info in sort_metrics(section_metrics):
        value       = metric_info.get('value')
        attempts    = metric_info.get('attempts')
        sufficient  = metric_info.get('sufficient_data', True)

        if not sufficient or value is None:
          continue  # skip insufficient in ai_optimized — saves tokens

        # Format value
        if isinstance(value, float):
          if abs(value) < 0.001 and value != 0:
            value_str = f"{value:.4f}".rstrip('0').rstrip('.')
          elif abs(value) < 1:
            value_str = f"{value:.3f}".rstrip('0').rstrip('.')
          else:
            value_str = f"{value:.2f}".rstrip('0').rstrip('.')
        else:
          value_str = str(value)

        att_str = f"(n={attempts})" if attempts is not None else ""
        metric_parts.append(f"{metric_id}:{value_str}{att_str}")

      # Put all metrics for this section on ONE line
      if metric_parts:
        md.append(" ".join(metric_parts))
      md.append("")

    else:
      # ── HUMAN: table format (original) ──────────────────────────────────
      md.append(f"## {section_title}\n")

      section_has_video = any(
        metric_info.get('video_links', '')
        for _, metric_info in sort_metrics(section_metrics)
      )

      if section_has_video:
        md.append("| Metric | Value | n | Plays |")
        md.append("| :--- | :--- | :--- | :--- |")
      else:
        md.append("| Metric | Value | n |")
        md.append("| :--- | :--- | :--- |")

      for metric_id, metric_info in sort_metrics(section_metrics):
        metric_name = metric_info.get('metric_name', metric_id)
        value       = metric_info.get('value')
        attempts    = metric_info.get('attempts')
        sufficient  = metric_info.get('sufficient_data', True)
        min_att     = metric_info.get('min_attempts')
        video_links = metric_info.get('video_links', '')

        if not sufficient or value is None:
          if attempts is not None and min_att is not None:
            value_str = f"insufficient data (n={attempts}, min={min_att})"
          else:
            value_str = "insufficient data"
        elif isinstance(value, float):
          if abs(value) < 0.001 and value != 0:
            value_str = f"{value:.4f}".rstrip('0').rstrip('.')
          elif abs(value) < 1:
            value_str = f"{value:.3f}".rstrip('0').rstrip('.')
          else:
            value_str = f"{value:.2f}".rstrip('0').rstrip('.')
        else:
          value_str = str(value)

        att_str = str(attempts) if attempts is not None else "-"

        if section_has_video:
          md.append(f"| {metric_name} | {value_str} | {att_str} | {video_links} |")
        else:
          md.append(f"| {metric_name} | {value_str} | {att_str} |")

      md.append("")

  # Unassigned metrics
  if unassigned:
    if ai_optimized:
      md.append("[other]")
      parts = []
      for metric_id, metric_info in sort_metrics(unassigned):
        value = metric_info.get('value')
        attempts = metric_info.get('attempts')
        sufficient = metric_info.get('sufficient_data', True)
        if not sufficient or value is None:
          continue
        if isinstance(value, float):
          value_str = f"{value:.3f}".rstrip('0').rstrip('.')
        else:
          value_str = str(value)
        att_str = f"(n={attempts})" if attempts is not None else ""
        parts.append(f"{metric_id}:{value_str}{att_str}")
      if parts:
        md.append(" ".join(parts))
      md.append("")
    else:
      md.append("## Other Metrics\n")
      for metric_id, metric_info in sort_metrics(unassigned):
        md.append(format_metric_line(metric_id, metric_info, metric_dict_df))
      md.append("")

  return "\n".join(md)


def generate_metric_index(metrics_by_category, metric_dict_df, ai_optimized=False):
  """
    Generate a flat alphabetical index of all metrics.

    Args:
        metrics_by_category: dict from calculate_all_metrics
        metric_dict_df: full metric dictionary DataFrame
        ai_optimized (bool): If True, emit compact key:value pairs instead of
                             bullet lines. Insufficient metrics are omitted.

    Returns:
        str: Markdown content for the metric index section
    """
  all_metrics = {}
  for cat_metrics in metrics_by_category.values():
    all_metrics.update(cat_metrics)

  if ai_optimized:
    # Dense: "metric_id:value(n=att)" all on sorted lines, 10 per line
    md = ["# Aggregate Metrics",
          "# Format: metric_id:value(n=attempts)  — decode via metric_dictionary file",
          ""]

    parts = []
    for metric_id in sorted(all_metrics.keys()):
      metric_info = all_metrics[metric_id]
      value      = metric_info.get('value')
      attempts   = metric_info.get('attempts')
      sufficient = metric_info.get('sufficient_data', True)

      if not sufficient or value is None:
        continue  # skip insufficient — saves tokens

      if isinstance(value, float):
        if abs(value) < 0.001 and value != 0:
          value_str = f"{value:.4f}".rstrip('0').rstrip('.')
        elif abs(value) < 1:
          value_str = f"{value:.3f}".rstrip('0').rstrip('.')
        else:
          value_str = f"{value:.2f}".rstrip('0').rstrip('.')
      else:
        value_str = str(value)

      att_str = f"(n={attempts})" if attempts is not None else ""
      parts.append(f"{metric_id}:{value_str}{att_str}")

    # Group into rows of 8 for readability without wasting tokens
    ROW_SIZE = 8
    for i in range(0, len(parts), ROW_SIZE):
      md.append(" ".join(parts[i:i + ROW_SIZE]))

    md.append("")
    return "\n".join(md)

  else:
    # Human: original bullet-list format
    md = ["## Metric Index\n",
          "*Alphabetical index of all calculated metrics for fast lookup.*\n"]

    for metric_id in sorted(all_metrics.keys()):
      metric_info = all_metrics[metric_id]
      md.append(format_metric_line(metric_id, metric_info, metric_dict_df))

    md.append("")
    return "\n".join(md)


def generate_markdown_content(player_name, metadata, metrics_by_category, metric_dict_df, ai_optimized=False):
  """
    Generate complete Markdown content for player export.

    Args:
        player_name: Player name
        metadata: Metadata dict
        metrics_by_category: Dict of metrics organized by metric_category
        metric_dict_df: Full metric dictionary DataFrame
        ai_optimized (bool): If True, use dense token-efficient format throughout.

    Returns:
        str: Complete Markdown content
    """
  md_content = []

  if ai_optimized:
    # ── AI OPTIMIZED HEADER ────────────────────────────────────────────────
    # Compact single-line header instead of paragraphs
    md_content.append(
      f"PLAYER|{player_name}|{metadata['league']}|{metadata['gender']}|"
      f"{metadata['year']}|{metadata['team']}|"
      f"pts:{metadata['total_points_analyzed']}|sets:{metadata['total_sets_analyzed']}|"
      f"generated:{metadata['generated_at']}"
    )
    filters = metadata.get('filters_applied', {})
    if filters:
      filter_str = " ".join(f"{k}:{v}" for k, v in filters.items())
      md_content.append(f"FILTERS|{filter_str}")
    md_content.append("")

    # Metric index (dense)
    md_content.append(generate_metric_index(metrics_by_category, metric_dict_df, ai_optimized=True))

    # Coach view sections (dense)
    md_content.append("# Coach View Sections")
    md_content.append("# Format: [section_key] then metric_id:value(n=att) ...")
    md_content.append("")
    md_content.append(generate_coach_view_sections(metrics_by_category, metric_dict_df, ai_optimized=True))

    # Compact summary footer
    md_content.append(
      f"SUMMARY|metrics_calculated:{metadata['summary_stats']['total_metrics_calculated']}|"
      f"sufficient:{metadata['summary_stats']['metrics_with_sufficient_data']}|"
      f"insufficient:{metadata['summary_stats']['metrics_below_min_attempts']}"
    )

  else:
    # ── HUMAN READABLE (original format) ──────────────────────────────────
    md_content.append(f"# {player_name} — Performance Analysis\n")

    header_paragraphs = generate_descriptive_header(
      player_name, metadata, metrics_by_category
    )
    md_content.append(header_paragraphs)
    md_content.append("\n")

    md_content.append("## Data Collection Details\n")
    md_content.append(f"- **Report Generated**: {metadata['generated_at']}")
    md_content.append(
      f"- **League**: {metadata['league']} {metadata['gender']} {metadata['year']}"
    )
    md_content.append(f"- **Team**: {metadata['team']}")
    md_content.append(
      f"- **Data Points Analyzed**: {metadata['total_points_analyzed']} rally outcomes"
    )
    md_content.append(
      f"- **Match Sets Analyzed**: {metadata['total_sets_analyzed']} competitive sets"
    )
    md_content.append(
      f"- **Metric Dictionary Version**: {metadata['dictionary_version']}"
    )

    filters = metadata.get('filters_applied', {})
    if filters:
      md_content.append("\n**Data Filters Applied:**")
      for key, value in filters.items():
        filter_name = key.replace('_', ' ').title()
        md_content.append(f"- {filter_name}: {value}")

    md_content.append("\n")

    md_content.append("---\n")
    md_content.append(generate_metric_index(metrics_by_category, metric_dict_df, ai_optimized=False))

    md_content.append("---\n")
    md_content.append("# Performance Metrics — Coach View\n")
    md_content.append(
      "*Metrics organized by the Coach View Template. "
      "See `01_Coach_View_Template.md` for interpretation guidance.*\n"
    )
    md_content.append(
      generate_coach_view_sections(metrics_by_category, metric_dict_df, ai_optimized=False)
    )

    md_content.append("---\n")
    md_content.append("## Calculation Summary\n")
    md_content.append(
      f"- **Total Metrics Calculated**: "
      f"{metadata['summary_stats']['total_metrics_calculated']}"
    )
    md_content.append(
      f"- **Metrics with Sufficient Data**: "
      f"{metadata['summary_stats']['metrics_with_sufficient_data']}"
    )
    md_content.append(
      f"- **Metrics Below Minimum Attempts**: "
      f"{metadata['summary_stats']['metrics_below_min_attempts']}"
    )
    md_content.append("\n")

    md_content.append("---\n")
    md_content.append("## Reference Notes\n\n")
    md_content.append(
      "*Metric definitions, level benchmarks, and interpretation guidance are in "
      "`00_Global_Context_Philosophy.md`. "
      "Coach View organization is defined in `01_Coach_View_Template.md`. "
      "Scouting analysis uses `02_Scouting_Template.md`. "
      "Statistical analysis uses `03_Statistical_View_Template.md`.*\n\n"
    )
    md_content.append(
      f"*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
      f"using metric dictionary version {metadata['dictionary_version']}*\n"
    )

  return "\n".join(md_content)


def generate_player_metrics_markdown(league_value, team, use_direct_data=False, ai_optimized=False, **json_filters):
  """
    Generate comprehensive player metrics MARKDOWN file (instead of JSON).
    
    Args:
        league_value (str): League|Gender|Year format (e.g., "AVP|W|2024")
        team (str): Team name
        use_direct_data (bool): If True, bypass user check (for background jobs)
        ai_optimized (bool): If True, use token-efficient dense format.
                             If False (default), use human-readable format.
        **json_filters: Filter parameters (player, start_date, end_date, etc.)
    
    Returns:
        dict: {
            'media_obj': BlobMedia with .md file,
            'filename': str,
            'summary': dict with metadata
        }
    """

  try:
    log_info("=== MARKDOWN Generation Started ===")
    log_info(f"Player: {json_filters.get('player')}")
    log_info(f"League: {league_value}")
    log_info(f"Team: {team}")
    log_info(f"ai_optimized: {ai_optimized}")

    # Parse league value
    str_loc = league_value.index("|")
    league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    gender = league_value[: str_loc - 1].strip()
    year = str(int(league_value[str_loc + 1 :].strip()))

    log_info(f"Parsed - League: {league}, Gender: {gender}, Year: {year}")

    # Get player name (required)
    player_name = json_filters.get('player')
    if not player_name:
      log_error("Player name not provided", with_traceback=False)
      raise ValueError("Player name is required")

    player_shortname = json_filters.get('player_shortname', 'player')

    # Load metric dictionary
    log_info("Loading metric dictionary from database...")
    dict_rows = list(app_tables.metric_dictionary.search())

    if len(dict_rows) == 0:
      log_error("No metrics found in metric_dictionary table", with_traceback=False)
      raise ValueError("Metric dictionary is empty")

    column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]

    metric_dict = pd.DataFrame([
      {col: row[col] for col in column_names}
      for row in dict_rows
    ])
    log_info(f"✓ Loaded {len(metric_dict)} metrics from dictionary")

    # Get PPR data with filters
    log_info("Retrieving and filtering PPR data...")
    if use_direct_data:
      from ai_export_manager import get_filtered_ppr_data_direct
      ppr_df = get_filtered_ppr_data_direct(league, gender, year, team, **json_filters)
    else:
      ppr_df = get_filtered_ppr_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(ppr_df)} points from PPR data")

    if len(ppr_df) == 0:
      log_error("No data found for the specified filters", with_traceback=False)
      raise ValueError("No data found for the specified filters")

      # Count points / sets for this player
    player_mask = (
      (ppr_df['player_a1'] == player_name) |
      (ppr_df['player_a2'] == player_name) |
      (ppr_df['player_b1'] == player_name) |
      (ppr_df['player_b2'] == player_name)
    )
    player_ppr_count = len(ppr_df[player_mask])

    set_point_counts = (
      ppr_df[player_mask]
        .groupby(['video_id', 'set'])
        .size()
        .reset_index(name='point_count')
    )
    player_set_count = int(
      set_point_counts[set_point_counts['point_count'] >= 10].shape[0]
    )
    log_info(f"✓ Counted {player_set_count} sets for {player_name} (>=10 points each)")

    # Build metadata
    metadata = {
      'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
      'player_name': player_name,
      'player_shortname': player_shortname,
      'league': league,
      'gender': gender,
      'year': year,
      'team': team,
      'filters_applied': {k: str(v) for k, v in json_filters.items() 
                          if k not in ['player', 'player_shortname']},
      'total_points_analyzed': player_ppr_count,
      'total_sets_analyzed': player_set_count,
      'dictionary_version': '1.0'
    }

    # Calculate all metrics
    log_info("Starting metric calculations...")
    metrics_result = calculate_all_metrics(metric_dict, ppr_df, player_name)
    log_info(f"✓ Calculated {metrics_result['successful']} / {metrics_result['total_calculated']} metrics")
    log_info(f"  ({metrics_result['insufficient_data']} metrics had insufficient data)")

    metadata['summary_stats'] = {
      'total_metrics_calculated': metrics_result['total_calculated'],
      'metrics_with_sufficient_data': metrics_result['successful'],
      'metrics_below_min_attempts': metrics_result['insufficient_data']
    }

    # Generate Markdown content
    log_info("Building Markdown output...")
    markdown_content = generate_markdown_content(
      player_name,
      metadata,
      metrics_result['metrics'],
      metric_dict,
      ai_optimized=ai_optimized
    )

    # Generate filename (include _opt suffix for optimized files)
    safe_name = sanitize_name_for_filename(player_name)
    opt_suffix = "_opt" if ai_optimized else ""
    filename = f"{safe_name}_{league}_{gender}_{year}{opt_suffix}.md"
    log_info(f"Generated filename: {filename}")

    log_info(f"Markdown size: {len(markdown_content)} bytes")
    media_obj = anvil.BlobMedia(
      'text/markdown',
      markdown_content.encode('utf-8'),
      name=filename
    )

    log_info("=== MARKDOWN Generation Complete ===")

    return {
      'media_obj': media_obj,
      'filename': filename,
      'summary': metadata
    }

  except Exception as e:
    log_error(f"Markdown generation failed: {str(e)}", with_traceback=True)
    raise


def generate_global_context_markdown():
  """
    Generate the global context/philosophy file for beach volleyball metrics.
    This should be generated once and referenced by all player Markdown files.
    
    Returns:
        anvil.BlobMedia: The global context Markdown file
    """
  content = """# Beach Volleyball Analytics - Global Context & Philosophy

## Core Metric Definitions

### FBHE (First Ball High Efficiency)
First Ball High Efficiency measures a player's ability to deliver a high-quality first contact that allows their partner to attack effectively. A pass is considered "high efficiency" when it enables an aggressive offensive play.

**Calculation**: (Kills on First Ball) / (Total First Ball Attack Attempts)

**Benchmarks**:
- Elite: > 70%
- Good: 50-70%
- Developing: 30-50%
- Needs Focus: < 30%

### ESO (Expected Sideout)
Expected Sideout measures the probability of winning a rally when receiving serve, based on pass quality and attack tendencies. It combines passing effectiveness with offensive conversion rates.

**Application**: Higher ESO indicates better pass-to-attack conversion and overall offensive system efficiency.

### Consistency Metrics
Consistency measures a player's ability to maintain performance quality across different contexts (sets, scores, opponents). Lower standard deviation indicates more consistent performance.

**The 13% Rule**: A performance variance threshold. If a player's metric varies by more than 13% across contexts, it indicates a consistency issue requiring attention.

### Error Density
Error density measures the concentration of errors within match segments, helping identify whether errors cluster (pressure-related) or are distributed evenly (skill-related).

**Analysis**: High error density in critical moments suggests mental/pressure issues; distributed errors suggest technical development needs.

## Performance Standards by Skill

### Passing
- **Elite FBHE**: > 65%
- **Good FBHE**: 50-65%
- **Developing**: 35-50%
- **Critical Threshold**: < 35%

### Attacking
- **Elite Efficiency**: > 50%
- **Strong Efficiency**: 40-50%
- **Average Efficiency**: 30-40%
- **Needs Development**: < 30%

### Serving
- **Ace Rate Target**: 12-18%
- **Error Rate Maximum**: < 15%
- **Pressure Serves**: > 40% (causing difficult passes)

### Defense
- **Dig Efficiency**: > 60% good digs
- **Transition Kill Rate**: > 35%
- **Coverage Success**: > 45%

## Spatial Metrics

### Attack Zones
The court is divided into 6 attack zones for tendency analysis:
- Left Pin (Zone 1)
- Left Slot (Zone 2)
- Middle Left (Zone 3)
- Middle Right (Zone 4)
- Right Slot (Zone 5)
- Right Pin (Zone 6)

**Analysis**: Effective attackers show balance across zones with higher efficiency in primary zones.

### Serve Target Zones
Serve placement analyzed across 9 zones to identify:
- Most effective target areas
- Avoidance zones (opponent's strengths)
- Pressure serve patterns

## Data Collection Philosophy

This analytics system is built on the principle that **accurate data drives better decisions**:

1. **Completeness**: Every rally, every contact, every outcome
2. **Context**: Not just what happened, but when and why
3. **Actionability**: Metrics that directly inform training decisions
4. **Longitudinal Tracking**: Trends over time matter more than single-game snapshots

## Metric Categories

### Performance Metrics (Volume)
- Attack attempts, total passes, serve counts
- **Use**: Track workload and involvement

### Efficiency Metrics (Quality)
- FBHE, attack efficiency, serve effectiveness
- **Use**: Measure skill execution quality

### Consistency Metrics (Reliability)
- Standard deviations, coefficient of variation
- **Use**: Identify performance stability

### Psychological Metrics (Mental)
- Pressure performance, error recovery, momentum maintenance
- **Use**: Assess competitive readiness

### Spatial Metrics (Tendencies)
- Zone preferences, court coverage, shot selection
- **Use**: Inform tactical decisions

## Using These Metrics

### For Coaches
- Focus on trends, not single-game anomalies
- Use comparative metrics (player vs. team average, current vs. historical)
- Identify 2-3 key development areas per athlete
- Track progress weekly, adjust monthly
- Combine multiple metrics for complete picture

### For Players
- Understand your baseline performance
- Identify your unique strengths to leverage
- Target specific skill development areas
- Use metrics to set concrete, measurable goals
- Track improvement over training cycles

### For Data Analysis
- Metrics are calculated from point-by-point (PPR) data
- Some metrics require minimum attempt thresholds
- Missing metrics indicate insufficient sample size
- Combine metrics for deeper insights

## Minimum Sample Sizes

To ensure statistical validity, metrics require minimum attempts:
- **Attack Efficiency**: 10 attempts
- **FBHE**: 15 first ball contacts
- **Serve Metrics**: 20 serves
- **Consistency Metrics**: 3+ sets/matches
- **Spatial Metrics**: 8+ attempts per zone

**Note**: Metrics calculated with insufficient data are flagged or excluded from reports.

## Interpretation Guidelines

### Reading Tables
All metric tables follow this format:
| Metric | Value | Context/Goal |

- **Metric**: Name of the performance indicator
- **Value**: Calculated result (with appropriate precision)
- **Context/Goal**: What this metric means and target ranges

### Understanding Context
- "Higher is better": Efficiency and success metrics
- "Lower is better": Error rates and consistency measures
- "Volume indicator": Shows involvement/workload
- "Performance metric": Skill execution measure

## Version Control

This document represents the canonical definitions for:
- Metric calculations
- Performance benchmarks
- Interpretation guidelines

**Current Version**: 1.0  
**Last Updated**: 2025-01-27  
**Maintained By**: Beach Internals Analytics Team

---

*This document serves as the reference guide for all player-specific performance reports and metric analyses.*
"""

  return anvil.BlobMedia(
    'text/markdown',
    content.encode('utf-8'),
    name='00_Global_Context_Philosophy.md'
  )


# Keep the original helper functions from generate_player_metrics_json.py
# These remain unchanged:

def get_filtered_ppr_data(league, gender, year, team, **filters):
  """
    Retrieve and filter point-by-point data.
    (Original function - unchanged)
    """
  try:
    current_user = anvil.users.get_user()
    if not current_user:
      log_error("No user logged in", with_traceback=False)
      return pd.DataFrame()

    user_team = current_user['team']
    log_info(f"User team from login: {user_team}")

    if user_team == 'INTERNALS':
      search_team = 'League'
      log_info(f"INTERNALS user - using team='{search_team}'")
    else:
      search_team = user_team

    log_info(f"Querying PPR data for {league}/{gender}/{year}/team={search_team}...")

    ppr_rows = list(app_tables.ppr_csv_tables.search(
      league=league,
      gender=gender,
      year=year,
      team=search_team
    ))

    if len(ppr_rows) == 0:
      log_error(f"No PPR data found for {league}/{gender}/{year}/team={search_team}", with_traceback=False)
      return pd.DataFrame()

    log_info(f"Found {len(ppr_rows)} PPR data record(s)")

    ppr_row = ppr_rows[0]
    column_names = [col['name'] for col in app_tables.ppr_csv_tables.list_columns()]

    if 'ppr_csv' not in column_names:
      log_error("ppr_csv column not found in ppr_csv_tables", with_traceback=False)
      return pd.DataFrame()

    ppr_csv_data = ppr_row['ppr_csv']

    if hasattr(ppr_csv_data, 'get_bytes'):
      ppr_csv_string = ppr_csv_data.get_bytes().decode('utf-8')
      log_debug("Loaded ppr_csv from Media object")
    else:
      ppr_csv_string = ppr_csv_data
      log_debug("Loaded ppr_csv as string")

    ppr_df = pd.read_csv(io.StringIO(ppr_csv_string))
    log_info(f"Loaded {len(ppr_df)} raw points from PPR")

    log_info("Applying filters using filter_ppr_df()...")
    ppr_df = filter_ppr_df(ppr_df, **filters)
    log_info(f"After filtering: {len(ppr_df)} points retained")

    return ppr_df

  except Exception as e:
    log_error(f"Error in get_filtered_ppr_data: {str(e)}", with_traceback=True)
    return pd.DataFrame()


def get_filtered_triangle_data(league, gender, year, team, **filters):
  """
    Retrieve and filter triangle (set-level) data.
    (Original function - unchanged)
    """
  try:
    log_info("Querying triangle data...")

    has_dates = 'start_date' in filters and 'end_date' in filters

    if has_dates:
      date_checked = True
      disp_start_date = filters['start_date']
      disp_end_date = filters['end_date']
      log_info(f"Using date range: {disp_start_date} to {disp_end_date}")
    else:
      date_checked = False
      disp_start_date = None
      disp_end_date = None
      log_info("No date range specified - retrieving all triangle data")

    tri_df, tri_found = get_tri_data(
      league,
      gender,
      year,
      date_checked,
      disp_start_date,
      disp_end_date
    )

    if not tri_found or len(tri_df) == 0:
      log_info("No triangle data found")
      return pd.DataFrame()

    log_info(f"Loaded {len(tri_df)} raw sets from triangle data")

    player_name = filters.get('player')
    if player_name and 'player' in tri_df.columns:
      tri_df = tri_df[tri_df['player'] == player_name]
      log_debug(f"After player filter: {len(tri_df)} sets")
    elif player_name:
      if 'player1' in tri_df.columns and 'player2' in tri_df.columns:
        tri_df = tri_df[(tri_df['player1'] == player_name) | (tri_df['player2'] == player_name)]
        log_debug(f"After player filter (player1/player2): {len(tri_df)} sets")

    if 'comp_l1' in filters and 'comp_l1' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l1'] == filters['comp_l1']]
      log_debug(f"After comp_l1 filter: {len(tri_df)} sets")

    if 'comp_l2' in filters and 'comp_l2' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l2'] == filters['comp_l2']]
      log_debug(f"After comp_l2 filter: {len(tri_df)} sets")

    if 'comp_l3' in filters and 'comp_l3' in tri_df.columns:
      tri_df = tri_df[tri_df['comp_l3'] == filters['comp_l3']]
      log_debug(f"After comp_l3 filter: {len(tri_df)} sets")

    log_info(f"After all filtering: {len(tri_df)} sets")
    return tri_df

  except Exception as e:
    log_error(f"Error in get_filtered_triangle_data: {str(e)}", with_traceback=True)
    return pd.DataFrame()


@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_player_metrics_markdown_content(ppr_df, player_name, league_value, team, ai_optimized=False):
  """
    Generate player metrics markdown content WITHOUT the top-level file headers.
    This is used when combining multiple datasets into one file.
    
    Args:
        ppr_df: Filtered PPR dataframe
        player_name: Player name
        league_value: League string
        team: Team name
        ai_optimized (bool): If True, use dense token-efficient format.
        
    Returns:
        dict: {
            'content': str (markdown content without top headers),
            'summary': dict (summary statistics)
        }
    """
  log_info(f"Generating player metrics content (no headers) for {player_name}, ai_optimized={ai_optimized}")

  from generate_player_metrics_json_server import generate_player_metrics_json

  json_result = generate_player_metrics_json(
    league_value=league_value,
    team=team,
    player=player_name
  )

  if not json_result or 'data' not in json_result:
    log_error(f"Failed to generate JSON data for {player_name}")
    return None

  player_data = json_result['data']

  md_lines = []

  categories = [
    ('attacking', 'Attacking'),
    ('passing', 'Passing'),
    ('serving', 'Serving'),
    ('defense', 'Defense'),
    ('transition', 'Transition')
  ]

  for cat_key, cat_name in categories:
    if cat_key in player_data and player_data[cat_key]:
      cat_data = player_data[cat_key]

      if ai_optimized:
        # Dense: [category] then metric_id:value(n=att)
        md_lines.append(f"[{cat_key}]")
        parts = []
        for metric_key, metric_data in cat_data.items():
          if isinstance(metric_data, dict):
            value = metric_data.get('value', 'N/A')
            attempts = metric_data.get('attempts', '')
            att_str = f"(n={attempts})" if attempts else ""
            parts.append(f"{metric_key}:{value}{att_str}")
        if parts:
          md_lines.append(" ".join(parts))
        md_lines.append("")
      else:
        # Human: original bulleted format
        md_lines.append(f"### {cat_name}")
        md_lines.append("")
        for metric_key, metric_data in cat_data.items():
          if isinstance(metric_data, dict):
            value = metric_data.get('value', 'N/A')
            attempts = metric_data.get('attempts', '')
            metric_name = metric_data.get('name', metric_key)
            if attempts:
              md_lines.append(f"- **{metric_name}**: {value} (n={attempts})")
            else:
              md_lines.append(f"- **{metric_name}**: {value}")
        md_lines.append("")

  content = "\n".join(md_lines)
  summary = json_result.get('summary', {})

  return {
    'content': content,
    'summary': summary
  }
"""
AI Export Dataset Combiner
==========================
Combines multiple datasets (player aggregate, set-level, etc.)
into a single markdown file for AI analysis (NotebookLM, Gemini, etc.)

KEY DESIGN:
- Each row in ai_export_dataset_list defines ONE data slice
- A slice = a function call + a set of filters (comp_l1/2/3, dates, days_before)
- The combiner loops over the linked dataset rows, resolves filters,
  calls the right function, and assembles one markdown file per player

HOW TO ADD A NEW DATASET SLICE (e.g., FIVB Elite 16):
1. Add a row to ai_export_dataset_list in Anvil
2. Set dataset_type = 'aggregate' (or 'set_level')
3. Set comp_l1 = 'Elite 16'  (or whatever the comp level is called)
4. Set section_title, order, etc.
5. Link it to the relevant ai_export_mgr rows
No code changes required.

Author: Beach Volleyball Analytics
Updated: 2026-02-22
"""

import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from anvil import BlobMedia
from datetime import datetime, timedelta
import json
import pandas as pd

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

# ============================================================================
# METRIC GENERATION IMPORTS
# ============================================================================
from generate_player_metrics_json_server import (
calculate_all_metrics,
get_filtered_triangle_data
)

from generate_set_level_metrics import (
generate_set_level_metrics_for_player,
format_set_level_data_as_markdown,
format_set_level_data_as_json
)


# ============================================================================
# CORE METRICS - the key metrics shown at the top of every aggregate section.
# Order here controls the order they appear in the scorecard table.
# The _n metrics are attempt counts for the corresponding core metric.
# ============================================================================
CORE_METRIC_IDS = [
  # --- Primary performance metrics ---
  'fbhe',          # First Ball High Efficiency (attack efficiency)
  'fbso',          # First Ball Side Out (side-out %)
  'eso',           # Expected Side-Out
  'expected',      # Expected Value (EV)
  'knockout',      # Knockout %
  'tcr',           # Transition Conversion Rate
  't_create',      # Transition Creates
  't_eff',         # Transition Effectiveness
  'err_den',       # Error Density

  # --- Serving metrics ---
  'src_ace_per',   # Ace %
  'srv_err_per',   # Serve Error %
  'srv_n',         # Serve attempts (count)

  # --- Passing / spatial metrics ---
  'goodpass',      # Good Pass %
  'pass_ea',       # Pass Ellipse Area (spatial)
  'att_ht_mean',   # Attack Height (mean)

  # --- Attempt counts (n values) for the primary metrics ---
  'fbhe_n',        # FBHE attempts
  'fbso_n',        # FBSO attempts
  'eso_n',         # ESO attempts
  'expected_n',    # EV attempts
  'att_ht_n',      # Attack height attempts
]


# ============================================================================
# FILTER RESOLUTION
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def resolve_dataset_filters(dataset_row):
  """
  Resolve the concrete filter dict for a dataset row.

  Priority / logic:
    1. comp_l1 / comp_l2 / comp_l3  →  passed directly if not blank
    2. days_before (rolling window)  →  calculates start_date / end_date from today
    3. start_date / end_date (fixed) →  passed directly if set
    4. If none of the above, no date/comp filter is added (all data)

  Args:
      dataset_row: Row from ai_export_dataset_list table

  Returns:
      dict: Filter kwargs ready to pass to metric functions
            e.g. {'comp_l1': 'regular season', 'start_date': date(...), 'end_date': date(...)}
  """
  filters = {}

  # --- Competition level filters ---
  comp_l1 = dataset_row['comp_l1']
  comp_l2 = dataset_row['comp_l2']
  comp_l3 = dataset_row['comp_l3']

  if comp_l1 and str(comp_l1).strip():
    filters['comp_l1'] = str(comp_l1).strip()
    log_info(f"Filter: comp_l1 = '{filters['comp_l1']}'")

  if comp_l2 and str(comp_l2).strip():
    filters['comp_l2'] = str(comp_l2).strip()
    log_info(f"Filter: comp_l2 = '{filters['comp_l2']}'")

  if comp_l3 and str(comp_l3).strip():
    filters['comp_l3'] = str(comp_l3).strip()
    log_info(f"Filter: comp_l3 = '{filters['comp_l3']}'")

  # --- Date filters ---
  days_before = dataset_row.get('days_before')

  if days_before and not pd.isna(days_before) and int(days_before) > 0:
    # Rolling window: last N days ending today
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=int(days_before))
    filters['start_date'] = start_date
    filters['end_date'] = end_date
    log_info(f"Filter: rolling {days_before} days → {start_date} to {end_date}")

  else:
    # Fixed dates (optional)
    start_date = dataset_row.get('start_date')
    end_date = dataset_row.get('end_date')

    if start_date and not pd.isna(start_date):
      filters['start_date'] = start_date
      log_info(f"Filter: start_date = {start_date}")

    if end_date and not pd.isna(end_date):
      filters['end_date'] = end_date
      log_info(f"Filter: end_date = {end_date}")

  if not filters:
    log_info("No filters applied — using all available data for this slice")

  return filters


# ============================================================================
# CORE METRICS SCORECARD
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def build_core_metrics_scorecard(metrics_by_category, section_label=""):
  """
  Build a compact 'Core Metrics Scorecard' markdown table.

  Pulls the ~14 key metrics from wherever they appear in metrics_by_category
  and renders them in a simple two-column table (Metric | Value).

  This is placed at the TOP of the player document so that both humans
  and AI tools see the most important numbers immediately.

  Args:
      metrics_by_category: Dict of {category: {metric_id: {value, metric_name, ...}}}
      section_label: Optional label like "Regular Season" to add context

  Returns:
      str: Markdown table, or empty string if no core metrics found
  """
  # Flatten all metrics into one lookup dict: metric_id → metric_info
  all_metrics = {}
  for cat_metrics in metrics_by_category.values():
    for mid, minfo in cat_metrics.items():
      all_metrics[mid] = minfo

  rows = []
  for mid in CORE_METRIC_IDS:
    if mid in all_metrics:
      info = all_metrics[mid]
      name = info.get('metric_name', mid)
      value = info.get('value')

      # Format value
      if value is None or (isinstance(value, float) and pd.isna(value)):
        val_str = "-"
      elif isinstance(value, float):
        val_str = f"{value:.3f}".rstrip('0').rstrip('.')
      else:
        val_str = str(value)

      rows.append((name, val_str))

  if not rows:
    log_info("No core metrics found for scorecard — skipping")
    return ""

  label = f" — {section_label}" if section_label else ""
  md = [f"### Core Metrics Scorecard{label}", ""]
  md.append("| Metric | Value |")
  md.append("| :--- | :--- |")
  for name, val in rows:
    md.append(f"| {name} | {val} |")
  md.append("")

  return "\n".join(md)


# ============================================================================
# AGGREGATE DATA GENERATION
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_aggregate_section(ppr_df_full, player_name, league_value, team, extra_filters):
  """
  Generate player aggregate metrics for ONE filtered slice.

  Args:
      ppr_df_full: The FULL (unfiltered) PPR dataframe for this player
      player_name: Player name
      league_value: e.g. "NCAA | W | 2026"
      team: Team name
      extra_filters: Dict of filters from resolve_dataset_filters()
                     (comp_l1, start_date, etc.)

  Returns:
      dict: {
          'content': str (markdown),
          'scorecard': str (core metrics scorecard markdown),
          'summary': dict,
          'points': int,
          'sets': int
      }
      or None on failure
  """
  log_info(f"Generating aggregate section for {player_name} with filters: {extra_filters}")

  try:
    # Apply filters to the pre-loaded ppr_df
    # We re-filter here so we don't re-query the database for each slice
    from server_functions import filter_ppr_df
    filtered_ppr = filter_ppr_df(ppr_df_full, player=player_name, **extra_filters)

    if len(filtered_ppr) == 0:
      log_info(f"No data after applying filters {extra_filters} for {player_name}")
      return None

    log_info(f"Filtered PPR: {len(filtered_ppr)} points")

    # Parse league_value
    league_parts = [p.strip() for p in league_value.split('|')]
    league_str = league_parts[0]
    gender = league_parts[1]
    year = league_parts[2]

    # Get triangle data with same filters
    tri_filters = {'player': player_name}
    tri_filters.update(extra_filters)
    tri_df = get_filtered_triangle_data(league_str, gender, year, team, **tri_filters)

    # Load metric dictionary
    dict_rows = list(app_tables.metric_dictionary.search())
    column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
    metric_dict = pd.DataFrame([{col: row[col] for col in column_names} for row in dict_rows])

    # Calculate metrics
    metrics_result = calculate_all_metrics(metric_dict, filtered_ppr, tri_df, player_name)

    # Build scorecard (core metrics at the top)
    scorecard = build_core_metrics_scorecard(metrics_result['metrics'])

    # Build detailed metrics content
    content = format_aggregate_metrics_as_markdown(metrics_result['metrics'])

    return {
      'content': content,
      'scorecard': scorecard,
    'summary': {
        'total_points': len(filtered_ppr),
        'total_sets': len(tri_df),
        'metrics_calculated': metrics_result['successful'],
        'filters_applied': extra_filters
      },
      'points': len(filtered_ppr),
      'sets': len(tri_df)
    }

  except Exception as e:
    log_error(f"Error in generate_aggregate_section: {str(e)}")
    return None


# ============================================================================
# SET LEVEL DATA GENERATION
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_set_level_section(ppr_df_full, player_name, league_value, team, extra_filters):
  """
  Generate set-level metrics for ONE filtered slice.

  Args:
      ppr_df_full: The FULL (unfiltered) PPR dataframe for this player
      player_name: Player name
      league_value: e.g. "NCAA | W | 2026"
      team: Team name
      extra_filters: Dict of filters from resolve_dataset_filters()

  Returns:
      dict: {'content': str, 'scorecard': str, 'summary': dict} or None
  """
  log_info(f"Generating set-level section for {player_name} with filters: {extra_filters}")

  try:
    # Apply filters to the pre-loaded ppr_df
    from server_functions import filter_ppr_df
    filtered_ppr = filter_ppr_df(ppr_df_full, player=player_name, **extra_filters)

    if len(filtered_ppr) == 0:
      log_info(f"No data after applying filters {extra_filters} for {player_name}")
      return None

    log_info(f"Filtered PPR: {len(filtered_ppr)} points for set-level analysis")

    set_level_data = generate_set_level_metrics_for_player(
      ppr_df=filtered_ppr,
      player_name=player_name,
      league_value=league_value,
      team=team
    )

    if not set_level_data:
      log_error(f"generate_set_level_metrics_for_player returned None for {player_name}")
      return None

    content = format_set_level_data_as_markdown(set_level_data)

    # No scorecard for set-level (it's per-set, not aggregate)
    return {
      'content': content,
      'scorecard': "",
      'summary': set_level_data.get('summary', {}),
      'points': len(filtered_ppr),
      'sets': set_level_data.get('summary', {}).get('total_sets_analyzed', 0)
    }

  except Exception as e:
    log_error(f"Error in generate_set_level_section: {str(e)}")
    return None


# ============================================================================
# FORMAT HELPERS
# ============================================================================

def format_aggregate_metrics_as_markdown(metrics_dict):
  """
  Format aggregate metrics dictionary as markdown text.
  (Detailed breakdown — goes AFTER the scorecard)

  Args:
      metrics_dict: Dictionary of metrics organized by category

  Returns:
      str: Formatted markdown text
  """
  lines = []

  for category in sorted(metrics_dict.keys()):
    lines.append(f"#### {category}")
    lines.append("")

    metrics_in_category = metrics_dict[category]
    for metric_id in sorted(metrics_in_category.keys()):
      metric_info = metrics_in_category[metric_id]
      metric_name = metric_info['metric_name']
      metric_value = metric_info['value']
      parent_metric = metric_info.get('parent_metric')

      if isinstance(metric_value, float):
        if pd.isna(metric_value):
          formatted_value = "-"
        else:
          formatted_value = f"{metric_value:.3f}".rstrip('0').rstrip('.')
      else:
        formatted_value = str(metric_value) if metric_value is not None else "-"

      if parent_metric and parent_metric != metric_id:
        lines.append(f"- **{metric_name}** (`{metric_id}` | parent: `{parent_metric}`): {formatted_value}")
      else:
        lines.append(f"- **{metric_name}** (`{metric_id}`): {formatted_value}")

    lines.append("")

  return "\n".join(lines)


def _describe_filters(extra_filters):
  """Return a human-readable one-line description of the active filters."""
  parts = []
  if 'comp_l1' in extra_filters:
    parts.append(f"Competition: {extra_filters['comp_l1']}")
  if 'comp_l2' in extra_filters:
    parts.append(f"Sub-competition: {extra_filters['comp_l2']}")
  if 'comp_l3' in extra_filters:
    parts.append(f"Tournament: {extra_filters['comp_l3']}")
  if 'start_date' in extra_filters and 'end_date' in extra_filters:
    parts.append(f"Dates: {extra_filters['start_date']} → {extra_filters['end_date']}")
  elif 'start_date' in extra_filters:
    parts.append(f"From: {extra_filters['start_date']}")
  elif 'end_date' in extra_filters:
    parts.append(f"To: {extra_filters['end_date']}")
  return " | ".join(parts) if parts else "All data (no filter)"


# ============================================================================
# DATASET LIST ACCESS
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_enabled_datasets(export_row):
  """
  Get the list of datasets linked to this export row, sorted by 'order'.

  Falls back to a default 'player_aggregate' dataset if none are linked,
  for backward compatibility with older export rows.

  Args:
      export_row: Row from ai_export_mgr table

  Returns:
      list: List of dataset rows from ai_export_dataset_list
  """
  log_info("Getting enabled datasets for export...")

  datasets_included = export_row['datasets_included']

  if datasets_included is None or len(datasets_included) == 0:
    log_info("No datasets linked — falling back to default: player_aggregate")
    default_dataset = app_tables.ai_export_dataset_list.get(dataset_id='player_aggregate')
    return [default_dataset] if default_dataset else []

  # Filter to active only, then sort by order
  dataset_list = [ds for ds in datasets_included if ds.get('active', True)]
  dataset_list.sort(key=lambda x: x['order'] if x['order'] is not None else 999)

  log_info(f"Found {len(dataset_list)} active datasets:")
  for ds in dataset_list:
    log_info(f"  [{ds['order']}] {ds['dataset_name']}  type={ds['dataset_type']}")

  return dataset_list


# ============================================================================
# MAIN ENTRY POINT: COMBINED PLAYER EXPORT
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def generate_combined_player_export(
  ppr_df_full,
  player_name,
  league_value,
  team,
  datasets_to_include,
  output_format='markdown'
):
  """
  Generate one combined export file for a single player.

  Loops over datasets_to_include, resolves each slice's filters,
  calls the right generation function, and assembles the results
  into a single markdown document.

  Document structure:
    # Player Name - Performance Report
    (metadata block)
    ---
    ## [Section Title]          ← one per dataset row
    **Filters:** ...
    ### Core Metrics Scorecard  ← always at top of aggregate sections
    | Metric | Value |
    ...
    ### Detailed Breakdown
    (full metrics tables)
    ---

  Args:
      ppr_df_full: Fully loaded PPR dataframe for this player's league/team.
                   Filtering per slice happens INSIDE this function.
      player_name: Player name in format "TEAM NUMBER SHORTNAME"
      league_value: e.g. "NCAA | W | 2026"
      team: Team name
      datasets_to_include: Ordered list of rows from ai_export_dataset_list
      output_format: 'markdown' (only supported format right now)

  Returns:
      dict: {
          'success': bool,
          'content': str or None,
          'media_obj': BlobMedia or None,
          'filename': str,
          'summary': dict
      }
  """
  log_info(f"=== Generating combined export for {player_name} ===")
  log_info(f"Datasets: {[ds['dataset_name'] for ds in datasets_to_include]}")

  if output_format != 'markdown':
    log_error(f"Unsupported output format: {output_format}")
    return {'success': False, 'content': None, 'media_obj': None,
            'filename': None, 'summary': {}}

  combined_content = []
  combined_summary = {
    'player': player_name,
    'league': league_value,
    'team': team,
    'datasets_included': [],
    'generation_timestamp': str(datetime.now())
  }

  # ── Document header ──────────────────────────────────────────────────────
  combined_content += [
    f"# Player Performance Report: {player_name}",
    f"",
    f"**League:** {league_value}",
    f"**Team:** {team}",
    f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    f"",
    f"---",
    f"",
  ]

  # ── Process each dataset slice ────────────────────────────────────────────
  for dataset in datasets_to_include:
    dataset_id   = dataset['dataset_id']
    dataset_name = dataset['dataset_name']
    dataset_type = dataset['dataset_type']
    section_title = dataset.get('output_section_title') or dataset_name

    log_info(f"--- Processing slice: {dataset_name} (type={dataset_type}) ---")

    try:
      # Resolve the filters for this slice
      extra_filters = resolve_dataset_filters(dataset)
      filter_desc   = _describe_filters(extra_filters)

      # ── Aggregate (player-level) metrics ──────────────────────────────
      if dataset_type == 'aggregate':
        result = generate_aggregate_section(
          ppr_df_full, player_name, league_value, team, extra_filters
        )

        if result:
          combined_content += [
            f"## {section_title}",
            f"",
            f"**Data filter:** {filter_desc}",
            f"**Points analyzed:** {result['points']} | **Sets:** {result['sets']}",
            f"",
          ]

          # Core scorecard first
          if result['scorecard']:
            combined_content.append(result['scorecard'])

          # Then detailed breakdown
          combined_content += [
            "### Detailed Metric Breakdown",
            "",
            result['content'],
            "",
            "---",
            "",
          ]

          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': True,
            'filter_desc': filter_desc,
            'summary': result['summary']
          })

        else:
          log_info(f"No data for slice '{dataset_name}' — skipping section")
          combined_content += [
            f"## {section_title}",
            f"",
            f"**Data filter:** {filter_desc}",
            f"",
            f"*No data available for this filter combination.*",
            f"",
            f"---",
            f"",
          ]
          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': False,
            'filter_desc': filter_desc,
            'error': 'No data after filtering'
          })

      # ── Set-level metrics ─────────────────────────────────────────────
      elif dataset_type == 'set_level':
        result = generate_set_level_section(
          ppr_df_full, player_name, league_value, team, extra_filters
        )

        if result:
          combined_content += [
            f"## {section_title}",
            f"",
            f"**Data filter:** {filter_desc}",
            f"**Points analyzed:** {result['points']} | **Sets:** {result['sets']}",
            f"",
            result['content'],
            "",
            "---",
            "",
          ]
          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': True,
            'filter_desc': filter_desc,
            'summary': result['summary']
          })
        else:
          log_info(f"No set-level data for slice '{dataset_name}' — skipping section")
          combined_content += [
            f"## {section_title}",
            f"",
            f"**Data filter:** {filter_desc}",
            f"",
            f"*No data available for this filter combination.*",
            f"",
            f"---",
            f"",
          ]
          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': False,
            'filter_desc': filter_desc,
            'error': 'No data after filtering'
          })

      # ── Unknown dataset type ──────────────────────────────────────────
      else:
        log_error(f"Unknown dataset_type='{dataset_type}' for dataset '{dataset_name}'")
        combined_summary['datasets_included'].append({
          'dataset': dataset_name,
          'success': False,
          'error': f"Unknown dataset_type: {dataset_type}"
        })

    except Exception as e:
      log_error(f"Exception processing dataset '{dataset_name}': {str(e)}")
      combined_summary['datasets_included'].append({
        'dataset': dataset_name,
        'success': False,
        'error': str(e)
      })

  # ── Assemble final document ───────────────────────────────────────────────
  final_content = "\n".join(combined_content)

  safe_name = player_name.replace(' ', '_').replace('/', '_')
  filename = f"{safe_name}_combined.md"

  media_obj = BlobMedia(
    'text/markdown',
    final_content.encode('utf-8'),
    name=filename
  )

  log_info(f"=== Combined export complete: {filename} ({len(final_content)} chars) ===")

  return {
    'success': True,
    'content': final_content,
    'media_obj': media_obj,
    'filename': filename,
    'summary': combined_summary
  }


# ============================================================================
# OPTIONAL: SEPARATE FILES PER DATASET (alternative to combined)
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_separate_dataset_files(
  ppr_df_full,
  player_name,
  league_value,
  team,
  datasets_to_include,
  output_format='markdown'
):
  """
  Generate one separate file per dataset slice (alternative to combined).
  Useful if you want to upload individual slices to NotebookLM separately.

  Returns:
      list: List of result dicts, one per dataset
  """
  log_info(f"Generating {len(datasets_to_include)} separate files for {player_name}")
  results = []

  for dataset in datasets_to_include:
    result = generate_combined_player_export(
      ppr_df_full=ppr_df_full,
      player_name=player_name,
      league_value=league_value,
      team=team,
      datasets_to_include=[dataset],
      output_format=output_format
    )

    if result['success']:
      # Give the file a slice-specific name
      safe_name = player_name.replace(' ', '_')
      dataset_suffix = dataset['dataset_id'].replace('_', '-')
      result['filename'] = f"{safe_name}_{dataset_suffix}.md"
      results.append(result)
    else:
      log_error(f"Failed to generate file for dataset: {dataset['dataset_name']}")

  log_info(f"Generated {len(results)} separate files")
  return results
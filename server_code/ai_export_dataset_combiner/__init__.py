"""
AI Export Dataset Combiner

This module combines multiple datasets (player aggregate, set-level, etc.)
into a single output file (markdown or JSON) for AI analysis.

Each dataset row in ai_export_dataset_list can specify its own filters:
  - comp_l1, comp_l2, comp_l3  : competition level filters
  - days_before                 : if set, use (today - days_before) as start date
  - date_start, date_end        : explicit date range (overridden by days_before)

Datasets are processed in 'order' column sequence and appended into one file per player.

UPDATED: Added ai_optimized flag support for token-efficient combined files.
         When ai_optimized=True, each dataset section uses the dense format,
         and the combined file header is also compact.
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
# HELPER: Format aggregate metrics as markdown
# ============================================================================

def format_aggregate_metrics_as_markdown(metrics_dict, player_name, ai_optimized=False):
  """
  Format aggregate metrics dictionary as markdown text.

  Args:
      metrics_dict: dict of {category: {metric_id: metric_info}}
      player_name: Player name (used in human format headers)
      ai_optimized (bool): If True, emit dense [category] + key:value lines.
                           If False (default), emit original bulleted format.
  """
  lines = []

  if ai_optimized:
    # ── DENSE FORMAT ─────────────────────────────────────────────────────
    for category in sorted(metrics_dict.keys()):
      lines.append(f"[{category}]")
      metrics_in_category = metrics_dict[category]
      parts = []
      for metric_id in sorted(metrics_in_category.keys()):
        metric_info = metrics_in_category[metric_id]
        metric_value = metric_info['value']
        sufficient = metric_info.get('sufficient_data', True)

        if not sufficient or metric_value is None:
          continue  # skip insufficient in optimized mode

        if isinstance(metric_value, float):
          formatted_value = f"{metric_value:.3f}".rstrip('0').rstrip('.')
        else:
          formatted_value = str(metric_value)

        attempts = metric_info.get('attempts')
        att_str = f"(n={attempts})" if attempts is not None else ""
        parts.append(f"{metric_id}:{formatted_value}{att_str}")

      if parts:
        lines.append(" ".join(parts))
      lines.append("")

  else:
    # ── HUMAN FORMAT (original) ───────────────────────────────────────────
    for category in sorted(metrics_dict.keys()):
      lines.append(f"### {category}")
      lines.append("")

      metrics_in_category = metrics_dict[category]
      for metric_id in sorted(metrics_in_category.keys()):
        metric_info = metrics_in_category[metric_id]
        metric_name = metric_info['metric_name']
        metric_value = metric_info['value']
        parent_metric = metric_info.get('parent_metric')

        if isinstance(metric_value, float):
          formatted_value = f"{metric_value:.3f}"
        else:
          formatted_value = str(metric_value)

        if parent_metric and parent_metric != metric_id:
          lines.append(f"- **{metric_name}** (`{metric_id}` | parent: `{parent_metric}`): {formatted_value}")
        else:
          lines.append(f"- **{metric_name}** (`{metric_id}`): {formatted_value}")

      lines.append("")

  return "\n".join(lines)


# ============================================================================
# HELPER: Apply dataset-level filters to a ppr_df
# ============================================================================

def apply_dataset_filters(ppr_df, dataset):
  """
  Apply the filter columns from a dataset row (ai_export_dataset_list) to ppr_df.

  Filters applied (in order):
    1. comp_l1  - competition level 1
    2. comp_l2  - competition level 2
    3. comp_l3  - competition level 3
    4. days_before - if set, overrides date range: use (today - days_before) to today
    5. date_start / date_end - explicit date range (only if days_before is empty)

  Args:
      ppr_df: Full player PPR dataframe (already filtered by player name)
      dataset: Row from ai_export_dataset_list table

  Returns:
      tuple: (filtered_df, filters_applied_dict)
  """
  filtered_df = ppr_df.copy()
  filters_applied = {}

  # --- comp_l1 ---
  comp_l1 = dataset.get('comp_l1') or None
  if comp_l1 and str(comp_l1).strip():
    comp_l1 = str(comp_l1).strip()
    if 'comp_l1' in filtered_df.columns:
      before = len(filtered_df)
      filtered_df = filtered_df[filtered_df['comp_l1'] == comp_l1]
      log_info(f"  comp_l1='{comp_l1}': {before} -> {len(filtered_df)} points")
      filters_applied['comp_l1'] = comp_l1
    else:
      log_error("  comp_l1 filter requested but 'comp_l1' column not in ppr_df")

  # --- comp_l2 ---
  comp_l2 = dataset.get('comp_l2') or None
  if comp_l2 and str(comp_l2).strip():
    comp_l2 = str(comp_l2).strip()
    if 'comp_l2' in filtered_df.columns:
      before = len(filtered_df)
      filtered_df = filtered_df[filtered_df['comp_l2'] == comp_l2]
      log_info(f"  comp_l2='{comp_l2}': {before} -> {len(filtered_df)} points")
      filters_applied['comp_l2'] = comp_l2
    else:
      log_error("  comp_l2 filter requested but 'comp_l2' column not in ppr_df")

  # --- comp_l3 ---
  comp_l3 = dataset.get('comp_l3') or None
  if comp_l3 and str(comp_l3).strip():
    comp_l3 = str(comp_l3).strip()
    if 'comp_l3' in filtered_df.columns:
      before = len(filtered_df)
      filtered_df = filtered_df[filtered_df['comp_l3'] == comp_l3]
      log_info(f"  comp_l3='{comp_l3}': {before} -> {len(filtered_df)} points")
      filters_applied['comp_l3'] = comp_l3
    else:
      log_error("  comp_l3 filter requested but 'comp_l3' column not in ppr_df")

  # --- Date filtering: days_before takes priority over date_start/date_end ---
  days_before = dataset.get('days_before') or None
  date_start  = dataset.get('date_start')  or None
  date_end    = dataset.get('date_end')    or None

  date_col = None
  for col in ['date', 'match_date', 'game_date']:
    if col in filtered_df.columns:
      date_col = col
      break

  if days_before:
    try:
      cutoff = datetime.now() - timedelta(days=int(days_before))
      if date_col:
        before = len(filtered_df)
        filtered_df[date_col] = pd.to_datetime(filtered_df[date_col], errors='coerce')
        filtered_df = filtered_df[filtered_df[date_col] >= cutoff]
        log_info(f"  days_before={days_before} (cutoff {cutoff.date()}): {before} -> {len(filtered_df)} points")
        filters_applied['days_before'] = days_before
      else:
        log_error("  days_before filter requested but no date column found in ppr_df")
    except Exception as e:
      log_error(f"  Error applying days_before filter: {e}")

  elif date_start or date_end:
    if date_col:
      filtered_df[date_col] = pd.to_datetime(filtered_df[date_col], errors='coerce')
      if date_start:
        before = len(filtered_df)
        filtered_df = filtered_df[filtered_df[date_col] >= pd.to_datetime(date_start)]
        log_info(f"  date_start='{date_start}': {before} -> {len(filtered_df)} points")
        filters_applied['date_start'] = str(date_start)
      if date_end:
        before = len(filtered_df)
        filtered_df = filtered_df[filtered_df[date_col] <= pd.to_datetime(date_end)]
        log_info(f"  date_end='{date_end}': {before} -> {len(filtered_df)} points")
        filters_applied['date_end'] = str(date_end)
    else:
      log_error("  date filter requested but no date column found in ppr_df")

  return filtered_df, filters_applied


# ============================================================================
# MAIN: Get enabled datasets (sorted by order)
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_enabled_datasets(export_row):
  """
  Get the list of datasets to include in this export, sorted by 'order'.

  Args:
      export_row: Row from ai_export_mgr table

  Returns:
      list: Dataset rows from ai_export_dataset_list, sorted by order
  """
  log_info("Getting enabled datasets for export...")

  datasets_included = export_row['datasets_included']

  if datasets_included is None or len(datasets_included) == 0:
    log_info("No datasets specified, using default: player_aggregate")
    default_dataset = app_tables.ai_export_dataset_list.get(dataset_id='player_aggregate')
    return [default_dataset] if default_dataset else []

  dataset_list = list(datasets_included)
  dataset_list.sort(key=lambda x: x['order'] or 0)

  log_info(f"Found {len(dataset_list)} datasets to include:")
  for ds in dataset_list:
    log_info(
      f"  [{ds['order']}] {ds['dataset_name']} "
      f"(type={ds['dataset_type']}, "
      f"comp_l1='{ds.get('comp_l1','') or ''}', "
      f"days_before={ds.get('days_before') or 'None'})"
    )

  return dataset_list


# ============================================================================
# MAIN: Generate combined export for one player
# ============================================================================

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def generate_combined_player_export(
  ppr_df,
  player_name,
  league_value,
  team,
  datasets_to_include,
  output_format='markdown',
  ai_optimized=False
):
  """
  Generate combined export for a single player across multiple datasets.

  For each dataset (in 'order' sequence):
    1. Read dataset-specific filters from ai_export_dataset_list
       (comp_l1, comp_l2, comp_l3, days_before, date_start, date_end)
    2. Apply those filters to ppr_df
    3. Get triangle data with the same comp filters
    4. Calculate metrics on the filtered data
    5. Append the section to the combined output

  Args:
      ppr_df: Player PPR dataframe (already filtered by player name only)
      player_name: e.g. "STETSON 02 Julia"
      league_value: e.g. "NCAA | W | 2026"
      team: e.g. "STETSON"
      datasets_to_include: List of rows from ai_export_dataset_list (sorted by order)
      output_format: 'markdown' or 'json'
      ai_optimized (bool): If True, use dense token-efficient format throughout.
                           If False (default), use human-readable format.

  Returns:
      dict: {
          'success': bool,
          'content': str,
          'media_obj': BlobMedia,
          'filename': str,
          'summary': dict
      }
  """
  log_info(f"Generating combined export for {player_name}, format={output_format}, ai_optimized={ai_optimized}")
  log_info(f"Full ppr_df size (player-filtered): {len(ppr_df)} points")
  log_info(f"Datasets: {[ds['dataset_name'] for ds in datasets_to_include]}")

  # Parse league components
  league_parts = league_value.split('|')
  league_str = league_parts[0].strip()
  gender     = league_parts[1].strip()
  year       = league_parts[2].strip()

  combined_content = []
  combined_summary = {
    'player': player_name,
    'league': league_value,
    'team': team,
    'datasets_included': [],
    'generation_timestamp': str(datetime.now())
  }

  # ── File-level header (written once at the top) ──────────────────────────
  if output_format == 'markdown':
    if ai_optimized:
      # Single compact header line
      combined_content.append(
        f"PLAYER|{player_name}|{league_value}|{team}|"
        f"generated:{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
      )
      combined_content.append("# Decode metric codes using the metric_dictionary file.")
      combined_content.append("")
    else:
      combined_content.append(f"# Player Performance Report: {player_name}")
      combined_content.append(f"")
      combined_content.append(f"**League:** {league_value}")
      combined_content.append(f"**Team:** {team}")
      combined_content.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
      combined_content.append(f"")
      combined_content.append(f"---")
      combined_content.append(f"")

  # Load metric dictionary ONCE (shared across all datasets)
  log_info("Loading metric dictionary (once, shared across datasets)...")
  dict_rows = list(app_tables.metric_dictionary.search())
  col_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
  metric_dict = pd.DataFrame([{col: row[col] for col in col_names} for row in dict_rows])
  log_info(f"Loaded {len(metric_dict)} metrics from dictionary")

  # =========================================================================
  # Process each dataset in order
  # =========================================================================
  for dataset in datasets_to_include:
    dataset_name  = dataset['dataset_name']
    dataset_type  = dataset['dataset_type']
    section_title = dataset.get('output_section_title') or dataset_name

    log_info(f"\n=== Dataset: {dataset_name} (type={dataset_type}) ===")

    try:
      # ----------------------------------------------------------------
      # STEP 1: Apply this dataset's filters to ppr_df
      # ----------------------------------------------------------------
      dataset_ppr_df, filters_applied = apply_dataset_filters(ppr_df, dataset)

      log_info(f"After filters: {len(dataset_ppr_df)} / {len(ppr_df)} points retained")

      if len(dataset_ppr_df) == 0:
        log_error(f"No data after filters for '{dataset_name}' - skipping section")

        if ai_optimized:
          combined_content.append(f"DATASET|{section_title}|NO_DATA")
        else:
          combined_content.append(f"---")
          combined_content.append(f"# DATASET: {section_title}")
          combined_content.append(f"")
          combined_content.append(f"*No data available for these filter criteria.*")
          combined_content.append(f"")

        combined_summary['datasets_included'].append({
          'dataset': dataset_name,
          'success': False,
          'error': 'No data after filtering',
          'filters_applied': filters_applied
        })
        continue

      # ----------------------------------------------------------------
      # STEP 2: Get triangle data with the SAME comp filters
      # ----------------------------------------------------------------
      tri_filters = {'player': player_name}
      if filters_applied.get('comp_l1'):
        tri_filters['comp_l1'] = filters_applied['comp_l1']
      if filters_applied.get('comp_l2'):
        tri_filters['comp_l2'] = filters_applied['comp_l2']
      if filters_applied.get('comp_l3'):
        tri_filters['comp_l3'] = filters_applied['comp_l3']

      tri_df = get_filtered_triangle_data(league_str, gender, year, team, **tri_filters)
      log_info(f"Triangle data: {len(tri_df)} sets")

      # ----------------------------------------------------------------
      # STEP 3: Generate section content
      # ----------------------------------------------------------------
      if dataset_type == 'aggregate':
        log_info(f"Calculating aggregate metrics ({len(dataset_ppr_df)} pts, {len(tri_df)} sets)...")
        metrics_result = calculate_all_metrics(metric_dict, dataset_ppr_df, tri_df, player_name)
        log_info(f"Metrics: {metrics_result['successful']} calculated, {metrics_result['insufficient_data']} insufficient data")

        section_content = format_aggregate_metrics_as_markdown(
          metrics_result['metrics'],
          player_name,
          ai_optimized=ai_optimized
        )

        if output_format == 'markdown':
          if ai_optimized:
            # Compact section divider
            filter_desc = "|".join(f"{k}:{v}" for k, v in filters_applied.items()) if filters_applied else "all"
            combined_content.append(f"DATASET|{section_title}|pts:{len(dataset_ppr_df)}|sets:{len(tri_df)}|filters:{filter_desc}")
            combined_content.append("")
          else:
            combined_content.append(f"---")
            combined_content.append(f"# DATASET: {section_title}")
            combined_content.append(f"")
            if filters_applied:
              filter_desc = ", ".join([f"{k}: {v}" for k, v in filters_applied.items()])
              combined_content.append(f"*Filters applied: {filter_desc}*")
            combined_content.append(f"*Data Points: {len(dataset_ppr_df)} | Sets Analyzed: {len(tri_df)}*")
            combined_content.append(f"")

          combined_content.append(section_content)
          combined_content.append(f"")

        combined_summary['datasets_included'].append({
          'dataset': dataset_name,
          'success': True,
          'summary': {
            'total_points': len(dataset_ppr_df),
            'total_sets': len(tri_df),
            'metrics_calculated': metrics_result['successful'],
            'filters_applied': filters_applied
          }
        })

      elif dataset_type == 'set_level':
        log_info(f"Generating set-level metrics ({len(dataset_ppr_df)} pts)...")

        set_level_data = generate_set_level_metrics_for_player(
          ppr_df=dataset_ppr_df,
          player_name=player_name,
          league_value=league_value,
          team=team
        )

        if set_level_data:
          if output_format == 'markdown':
            # Pass ai_optimized flag through to the formatter
            set_md = format_set_level_data_as_markdown(
              set_level_data,
              ai_optimized=ai_optimized
            )

            if ai_optimized:
              filter_desc = "|".join(f"{k}:{v}" for k, v in filters_applied.items()) if filters_applied else "all"
              combined_content.append(f"DATASET|{section_title}|pts:{len(dataset_ppr_df)}|filters:{filter_desc}")
              combined_content.append("")
            else:
              combined_content.append(f"---")
              combined_content.append(f"# DATASET: {section_title}")
              combined_content.append(f"")
              if filters_applied:
                filter_desc = ", ".join([f"{k}: {v}" for k, v in filters_applied.items()])
                combined_content.append(f"*Filters applied: {filter_desc}*")
              combined_content.append(f"*Data Points: {len(dataset_ppr_df)}*")
              combined_content.append(f"")

            combined_content.append(set_md)
            combined_content.append(f"")

          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': True,
            'summary': {
              **set_level_data.get('summary', {}),
              'filters_applied': filters_applied
            }
          })
        else:
          log_error(f"generate_set_level_metrics_for_player returned no data")

          if ai_optimized:
            combined_content.append(f"DATASET|{section_title}|ERROR:no_set_data")
          else:
            combined_content.append(f"---")
            combined_content.append(f"# DATASET: {section_title}")
            combined_content.append(f"")
            combined_content.append(f"*Error generating this section: no set-level data returned*")
            combined_content.append(f"")

          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': False,
            'error': 'No set-level data returned'
          })

      else:
        log_error(f"Unknown dataset_type: '{dataset_type}'")
        combined_summary['datasets_included'].append({
          'dataset': dataset_name,
          'success': False,
          'error': f'Unknown dataset type: {dataset_type}'
        })

    except Exception as e:
      import traceback
      err_msg = str(e)
      log_error(f"Error processing dataset '{dataset_name}': {err_msg}")
      log_error(traceback.format_exc())

      if ai_optimized:
        combined_content.append(f"DATASET|{section_title}|ERROR:{err_msg[:80]}")
      else:
        combined_content.append(f"---")
        combined_content.append(f"# DATASET: {section_title}")
        combined_content.append(f"")
        combined_content.append(f"*Error generating this section: {err_msg}*")
        combined_content.append(f"")

      combined_summary['datasets_included'].append({
        'dataset': dataset_name,
        'success': False,
        'error': err_msg
      })

  # =========================================================================
  # Assemble final file
  # =========================================================================
  safe_name = player_name.replace(' ', '_')

  if output_format == 'markdown':
    final_content = "\n".join(combined_content)
    # Include _opt suffix in filename so it's easy to tell which format
    opt_suffix = "_opt" if ai_optimized else ""
    filename = f"{safe_name}_combined{opt_suffix}.md"
    mime_type = 'text/plain'

  elif output_format == 'json':
    json_obj = {
      'player': player_name,
      'league': league_value,
      'team': team,
      'generated': str(datetime.now()),
      'summary': combined_summary,
      'datasets': {}
    }
    final_content = json.dumps(json_obj, indent=2, default=str)
    filename = f"{safe_name}_combined.json"
    mime_type = 'application/json'

  else:
    log_error(f"Unknown output format: {output_format}")
    return {
      'success': False,
      'content': None,
      'media_obj': None,
      'filename': None,
      'summary': combined_summary
    }

  media_obj = BlobMedia(mime_type, final_content.encode('utf-8'), name=filename)

  log_info(f"Combined export complete: {filename} ({len(final_content):,} bytes)")

  return {
    'success': True,
    'content': final_content,
    'media_obj': media_obj,
    'filename': filename,
    'summary': combined_summary
  }
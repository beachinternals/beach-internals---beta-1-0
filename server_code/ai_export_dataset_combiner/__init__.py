"""
AI Export Dataset Combiner

This module combines multiple datasets (player aggregate, set-level, etc.)
into a single output file (markdown or JSON) for AI analysis.

Author: Beach Volleyball Analytics
Created: 2026-02-05
"""

import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from anvil import BlobMedia
from datetime import datetime
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

# Import the generation functions
from generate_player_metrics_markdown import (
generate_player_metrics_markdown, 
generate_global_context_markdown
)
from generate_set_level_metrics import (
generate_set_level_metrics_for_player,
format_set_level_data_as_markdown,
format_set_level_data_as_json
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_aggregate_metrics_as_markdown(metrics_dict, player_name):
  """
  Format aggregate metrics dictionary as markdown text.
  
  Args:
      metrics_dict (dict): Dictionary of metrics organized by category
      player_name (str): Player name for the header
      
  Returns:
      str: Formatted markdown text
  """
  lines = []

  # Sort categories alphabetically for consistent output
  for category in sorted(metrics_dict.keys()):
    lines.append(f"### {category}")
    lines.append("")

    # Sort metrics within category by metric_id
    metrics_in_category = metrics_dict[category]
    for metric_id in sorted(metrics_in_category.keys()):
      metric_info = metrics_in_category[metric_id]
      metric_name = metric_info['metric_name']
      metric_value = metric_info['value']
      parent_metric = metric_info.get('parent_metric')

      # Format the value appropriately
      if isinstance(metric_value, float):
        formatted_value = f"{metric_value:.3f}"
      else:
        formatted_value = str(metric_value)

      # Format the line with parent metric context if available
      if parent_metric and parent_metric != metric_id:
        # Show: metric_name (metric_id | parent: parent_metric): value
        lines.append(f"- **{metric_name}** (`{metric_id}` | parent: `{parent_metric}`): {formatted_value}")
      else:
        # No parent or metric is its own parent - simpler format
        lines.append(f"- **{metric_name}** (`{metric_id}`): {formatted_value}")

    lines.append("")  # Blank line between categories

  return "\n".join(lines)


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================



@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def get_enabled_datasets(export_row):
  """
    Get the list of datasets to include in this export.
    
    Args:
        export_row: Row from ai_export_mgr table
        
    Returns:
        list: List of dataset rows from ai_export_dataset_list, ordered by 'order' field
    """
  log_info("Getting enabled datasets for export...")

  # Get datasets_included from export_row
  datasets_included = export_row['datasets_included']

  if datasets_included is None or len(datasets_included) == 0:
    # Default: use player_aggregate only (backward compatibility)
    log_info("No datasets specified, using default: player_aggregate")
    default_dataset = app_tables.ai_export_dataset_list.get(dataset_id='player_aggregate')
    return [default_dataset] if default_dataset else []

    # Return the linked datasets, sorted by order
  dataset_list = list(datasets_included)
  dataset_list.sort(key=lambda x: x['order'])

  log_info(f"Found {len(dataset_list)} datasets to include:")
  for ds in dataset_list:
    log_info(f"  - {ds['dataset_name']} (order: {ds['order']})")

  return dataset_list


@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def generate_combined_player_export(
  ppr_df, 
  player_name, 
  league_value, 
  team, 
  datasets_to_include,
  output_format='markdown'
):
  """
    Generate combined export for a single player with multiple datasets.
    
    Args:
        ppr_df: Filtered PPR dataframe
        player_name: Player name in format "TEAM NUMBER SHORTNAME"
        league_value: League string (e.g., "NCAA | W | 2026")
        team: Team name
        datasets_to_include: List of dataset rows from ai_export_dataset_list
        output_format: 'markdown' or 'json'
        
    Returns:
        dict: {
            'success': bool,
            'content': str or None,
            'media_obj': BlobMedia or None,
            'filename': str,
            'summary': dict
        }
    """
  log_info(f"Generating combined export for {player_name}, format={output_format}")
  log_info(f"Datasets to include: {[ds['dataset_name'] for ds in datasets_to_include]}")

  combined_content = []
  combined_summary = {
    'player': player_name,
    'league': league_value,
    'team': team,
    'datasets_included': [],
    'generation_timestamp': str(datetime.now())
  }

  # Generate header
  if output_format == 'markdown':
    combined_content.append(f"# Player Performance Report: {player_name}")
    combined_content.append(f"")
    combined_content.append(f"**League:** {league_value}")
    combined_content.append(f"**Team:** {team}")
    combined_content.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    combined_content.append(f"")
    combined_content.append(f"---")
    combined_content.append(f"")

    # Process each dataset
  for dataset in datasets_to_include:
    dataset_id = dataset['dataset_id']
    dataset_name = dataset['dataset_name']
    dataset_type = dataset['dataset_type']
    function_name = dataset['function_name']
    try:
      section_title = dataset['output_section_title']
    except (KeyError, AttributeError):
      section_title = dataset_name

    log_info(f"Processing dataset: {dataset_name} (type: {dataset_type})")

    try:
      # Generate dataset content
      if dataset_type == 'aggregate':
        # This is the existing player aggregate metrics
        log_info(f"Calling {function_name} for aggregate data...")

        # Get metric dictionary
        dict_rows = list(app_tables.metric_dictionary.search())
        column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
        metric_dict = pd.DataFrame([{col: row[col] for col in column_names} for row in dict_rows])
        league_parts = league_value.split('|')
        league_str = league_parts[0].strip()
        gender = league_parts[1].strip()
        year = league_parts[2].strip()

        tri_df = get_filtered_triangle_data(league_str, gender, year, team)

        # Calculate all metrics using the PRE-LOADED ppr_df
        log_info(f"Calculating aggregate metrics with {len(ppr_df)} points...")
        metrics_result = calculate_all_metrics(metric_dict, ppr_df, tri_df, player_name)

        # Format as markdown content
        aggregate_content = format_aggregate_metrics_as_markdown(
          metrics_result['metrics'],
          player_name
        )

        aggregate_result = {
          'content': aggregate_content,
          'summary': {
            'total_points': len(ppr_df),
            'total_sets': len(tri_df),
            'metrics_calculated': metrics_result['successful']
          }
        }

        if aggregate_result:
          if output_format == 'markdown':
            combined_content.append(f"## {section_title}")
            combined_content.append(f"")
            combined_content.append(aggregate_result['content'])
            combined_content.append(f"")
            combined_content.append(f"---")
            combined_content.append(f"")

          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': True,
            'summary': aggregate_result.get('summary', {})
          })
        else:
          log_error(f"Failed to generate aggregate data for {player_name}")
          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': False,
            'error': 'Generation failed'
          })

      elif dataset_type == 'set_level':
        # Generate set-level metrics
        log_info("Generating set-level metrics...")

        set_level_data = generate_set_level_metrics_for_player(
          ppr_df=ppr_df,
          player_name=player_name,
        league_value=league_value,
          team=team
        )

        if set_level_data:
          if output_format == 'markdown':
            set_md = format_set_level_data_as_markdown(set_level_data)
            combined_content.append(f"## {section_title}")
            combined_content.append(f"")
            combined_content.append(set_md)
            combined_content.append(f"")
            combined_content.append(f"---")
            combined_content.append(f"")
          elif output_format == 'json':
            # For JSON, we'll combine differently below
            pass

          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': True,
            'summary': set_level_data.get('summary', {})
          })
        else:
          log_error(f"Failed to generate set-level data for {player_name}")
          combined_summary['datasets_included'].append({
            'dataset': dataset_name,
            'success': False,
            'error': 'Generation failed'
          })
            
      else:
        log_error(f"Unknown dataset type: {dataset_type}")
        combined_summary['datasets_included'].append({
          'dataset': dataset_name,
          'success': False,
          'error': f'Unknown dataset type: {dataset_type}'
        })
                
    except Exception as e:
      log_error(f"Error processing dataset {dataset_name}: {str(e)}")
      combined_summary['datasets_included'].append({
        'dataset': dataset_name,
        'success': False,
        'error': str(e)
      })
  
  # Combine all content (AFTER the loop completes)
  if output_format == 'markdown':
    final_content = "\n".join(combined_content)
    filename = f"{player_name.replace(' ', '_')}_combined.md"
  elif output_format == 'json':
    # For JSON, create a structured object
    json_obj = {
      'player': player_name,
      'league': league_value,
      'team': team,
      'generated': str(datetime.now()),
      'datasets': {}
    }
    
    # TODO: Add actual JSON content from each dataset
    final_content = json.dumps(json_obj, indent=2, default=str)
    filename = f"{player_name.replace(' ', '_')}_combined.json"
  else:
    log_error(f"Unknown output format: {output_format}")
    return {
      'success': False,
      'content': None,
      'media_obj': None,
      'filename': None,
      'summary': combined_summary
    }
  
  # Create BlobMedia object
  media_obj = BlobMedia(
    'text/plain' if output_format == 'markdown' else 'application/json',
    final_content.encode('utf-8'),
    name=filename
  )
  
  log_info(f"Successfully generated combined export: {filename}")
  
  return {
    'success': True,
    'content': final_content,
    'media_obj': media_obj,
    'filename': filename,
    'summary': combined_summary
  }


@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_separate_dataset_files(
    ppr_df,
    player_name,
    league_value,
    team,
    datasets_to_include,
    output_format='markdown'
):
    """
    Generate separate files for each dataset (alternative to combined approach).
    
    Args:
        ppr_df: Filtered PPR dataframe
        player_name: Player name
        league_value: League string
        team: Team name
        datasets_to_include: List of dataset rows
        output_format: 'markdown' or 'json'
        
    Returns:
        list: List of dicts with file info for each dataset
    """
    log_info(f"Generating separate files for {len(datasets_to_include)} datasets")
    
    results = []
    
    for dataset in datasets_to_include:
        dataset_id = dataset['dataset_id']
        dataset_name = dataset['dataset_name']
        
        log_info(f"Generating separate file for: {dataset_name}")
        
        # Generate the dataset
        result = generate_combined_player_export(
            ppr_df=ppr_df,
            player_name=player_name,
            league_value=league_value,
            team=team,
            datasets_to_include=[dataset],  # Just this one dataset
            output_format=output_format
        )
        
        if result['success']:
            # Modify filename to include dataset name
            base_name = player_name.replace(' ', '_')
            dataset_suffix = dataset_id.replace('_', '-')
            if output_format == 'markdown':
                filename = f"{base_name}_{dataset_suffix}.md"
            else:
                filename = f"{base_name}_{dataset_suffix}.json"
            
            result['filename'] = filename
            results.append(result)
        else:
            log_error(f"Failed to generate file for dataset: {dataset_name}")
    
    log_info(f"Generated {len(results)} separate dataset files")
    return results
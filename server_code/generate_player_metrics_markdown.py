"""
SERVER FUNCTION: generate_player_metrics_markdown()
====================================================
Modified version that generates Markdown exports instead of JSON,
optimized for NotebookLM consumption.

Based on generate_player_metrics_json.py but outputs structured Markdown.
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

from server_functions import *
from metric_calc_functions import *


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


def create_metrics_table(metrics_dict, metric_dict_df):
  """
    Create a Markdown table from metrics dictionary.
    
    Args:
        metrics_dict: Dict of {metric_id: {'value': ..., 'metric_name': ...}}
        metric_dict_df: Full metric dictionary DataFrame for context
    
    Returns:
        str: Markdown table
    """
  if not metrics_dict:
    return ""

  md = "| Metric | Value | Context/Goal |\n"
  md += "| :--- | :--- | :--- |\n"

  # Sort metrics by metric_id for consistency
  for metric_id in sorted(metrics_dict.keys()):
    metric_info = metrics_dict[metric_id]
    value = metric_info.get('value')
    metric_name = metric_info.get('metric_name', metric_id)

    # Get full metric row for context
    metric_row = metric_dict_df[metric_dict_df['metric_id'] == metric_id]
    if len(metric_row) > 0:
      context = determine_metric_context(metric_row.iloc[0])
      formatted_value = format_metric_value(value, metric_row.iloc[0])
    else:
      context = "Performance metric"
      formatted_value = format_metric_value(value)

    md += f"| {metric_name} | {formatted_value} | {context} |\n"

  md += "\n"
  return md


def generate_markdown_content(player_name, metadata, metrics_by_category, metric_dict_df):
  """
    Generate complete Markdown content for player export.
    
    Args:
        player_name: Player name
        metadata: Metadata dict
        metrics_by_category: Dict of metrics organized by category
        metric_dict_df: Full metric dictionary DataFrame
    
    Returns:
        str: Complete Markdown content
    """
  md_content = []

  # Header
  md_content.append(f"# {player_name} - Performance Metrics Analysis\n")

  # Overview
  overview = generate_player_overview(player_name, metadata, metrics_by_category)
  md_content.append(f"## Overview\n\n{overview}\n")

  # Metadata section
  md_content.append("## Analysis Details\n")
  md_content.append(f"- **Generated**: {metadata['generated_at']}")
  md_content.append(f"- **League**: {metadata['league']} {metadata['gender']} {metadata['year']}")
  md_content.append(f"- **Team**: {metadata['team']}")
  md_content.append(f"- **Total Points Analyzed**: {metadata['total_points_analyzed']}")
  md_content.append(f"- **Total Sets Analyzed**: {metadata['total_sets_analyzed']}")

  # Show filters if any applied
  filters = metadata.get('filters_applied', {})
  if filters:
    md_content.append("\n**Filters Applied:**")
    for key, value in filters.items():
      md_content.append(f"- {key}: {value}")

  md_content.append("\n")

  # Metrics by category
  md_content.append("## Performance Metrics\n")

  # Define category order for logical flow
  preferred_order = [
    'Passing',
    'Setting', 
    'Attacking',
    'Serving',
    'Defense',
    'Blocking',
    'Consistency',
    'Psychological',
    'Spatial',
    'Transition',
    'Other'
  ]

  # Sort categories - preferred first, then alphabetically
  categories = list(metrics_by_category.keys())
  ordered_categories = [c for c in preferred_order if c in categories]
  remaining_categories = sorted([c for c in categories if c not in ordered_categories])
  all_categories = ordered_categories + remaining_categories

  for category in all_categories:
    metrics = metrics_by_category[category]

    if not metrics:
      continue

    md_content.append(f"### {category}\n")

    # Create table for this category
    table = create_metrics_table(metrics, metric_dict_df)
    md_content.append(table)

    # Summary statistics
  md_content.append("---\n")
  md_content.append("## Calculation Summary\n")
  md_content.append(f"- **Total Metrics Calculated**: {metadata['summary_stats']['total_metrics_calculated']}")
  md_content.append(f"- **Metrics with Sufficient Data**: {metadata['summary_stats']['metrics_with_sufficient_data']}")
  md_content.append(f"- **Metrics Below Minimum Attempts**: {metadata['summary_stats']['metrics_below_min_attempts']}")
  md_content.append("\n")

  # Footer with cross-references
  md_content.append("---\n")
  md_content.append("## Reference Notes\n\n")
  md_content.append("*Definitions for FBHE (First Ball High Efficiency), ESO (Expected Sideout), ")
  md_content.append("and other advanced metrics are located in `00_Global_Context_Philosophy.md`.*\n\n")
  md_content.append(f"*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
  md_content.append(f"using metric dictionary version {metadata['dictionary_version']}*\n")

  return "\n".join(md_content)


@anvil.server.callable
def generate_player_metrics_markdown(league_value, team, **json_filters):
  """
    Generate comprehensive player metrics MARKDOWN file (instead of JSON).
    
    This is the modified version optimized for NotebookLM:
    - Structured Markdown format
    - Token-efficient (only meaningful metrics)
    - 3-sentence overview at top
    - Standardized tables with context
    - Cross-references to global philosophy
    
    Args:
        league_value (str): League|Gender|Year format (e.g., "AVP|W|2024")
        team (str): Team name
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

      # Get column names from the table schema
    column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]

    # Convert to DataFrame
    metric_dict = pd.DataFrame([
      {col: row[col] for col in column_names}
      for row in dict_rows
    ])
    log_info(f"✓ Loaded {len(metric_dict)} metrics from dictionary")

    # Get PPR data with filters
    log_info("Retrieving and filtering PPR data...")
    ppr_df = get_filtered_ppr_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(ppr_df)} points from PPR data")

    # Get triangle data (for set-to-set consistency)
    log_info("Retrieving triangle data...")
    tri_df = get_filtered_triangle_data(league, gender, year, team, **json_filters)
    log_info(f"✓ Loaded {len(tri_df)} sets from triangle data")

    if len(ppr_df) == 0:
      log_error("No data found for the specified filters", with_traceback=False)
      raise ValueError("No data found for the specified filters")

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
      'total_points_analyzed': len(ppr_df),
      'total_sets_analyzed': len(tri_df) if len(tri_df) > 0 else 0,
      'dictionary_version': '1.0'
    }

    # Calculate all metrics
    log_info("Starting metric calculations...")
    metrics_result = calculate_all_metrics(metric_dict, ppr_df, tri_df, player_name)
    log_info(f"✓ Calculated {metrics_result['successful']} / {metrics_result['total_calculated']} metrics")
    log_info(f"  ({metrics_result['insufficient_data']} metrics had insufficient data)")

    # Add summary stats to metadata
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
      metric_dict
    )

    # Generate filename
    safe_name = sanitize_name_for_filename(player_name)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{league}_{gender}_{year}_{safe_name}_metrics_{timestamp}.md"
    log_info(f"Generated filename: {filename}")

    # Create media object
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


@anvil.server.callable
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
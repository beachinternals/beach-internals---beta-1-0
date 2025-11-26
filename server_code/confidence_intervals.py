"""
Confidence Interval Report for Beach Volleyball Analytics - FIXED VERSION
Anvil.works Integration

FIXED: Uses correct column names from your system:
- 'att_player' for attacking player
- 'player_a1', 'player_a2', 'player_b1', 'player_b2' for match players
- 'action' column for filtering plays
"""

import pandas as pd
import numpy as np
from scipy import stats
import math
import inspect
from anvil.tables import app_tables

# import error logging funcitons
from logger_utils import log_info, log_error, log_critical, log_debug

# Import other modules
from pair_functions import *
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *


def report_confidence_intervals(lgy, team, **rpt_filters):
  '''
    Report Functions:
        - Calculate and display confidence intervals for player metrics
        - Show how reliability changes with sample size
        - Compare actual player data with theoretical margins of error
    
    INPUT Parameters:
        - lgy : league, gender, year combination (as in dropdowns)
        - team : the team of the user calling the report
        - rpt_filters : the list of filters to limit the data
            - player : specific player to analyze
            - comp_l1, comp_l2, comp_l3 : competition filters
            - start_date, end_date : date range filters
    
    OUTPUT Return Parameters:
        - title_list : a list of up to 10 titles to display on the report
        - label_list : a list of up to 10 labels to display on the report
        - image_list : a list of up to 10 images to plot data on the report
        - df_list : a list of up to 10 data frames to display tables
        - df_desc_list : descriptions for each dataframe
        - image_desc_list : descriptions for each image
    '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch information from report_list
  #------------------------------------------------------------------------------------------------------

  # Initialize the calculated lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Get player from filters
  disp_player = rpt_filters.get('player')

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Filter the ppr dataframe by general filters
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # FIXED: Filter for player using correct column names
  if disp_player:
    ppr_df = ppr_df[(ppr_df['player_a1'] == disp_player) | 
      (ppr_df['player_a2'] == disp_player) |
      (ppr_df['player_b1'] == disp_player) |
      (ppr_df['player_b2'] == disp_player)]

    # Fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)

  if rpt_row:
    title_list[0] = rpt_row['rpt_title']
    title_list[1] = rpt_row['rpt_sub_title']
    title_list[2] = rpt_row['rpt_section_title1']
    title_list[3] = rpt_filters.get('lgy')
    title_list[4] = rpt_row['team_name']
    title_list[5] = rpt_row['rpt_type']
    title_list[6] = rpt_row['filter_text']
    title_list[7] = rpt_row['explain_text']
    title_list[8] = disp_player if disp_player else 'All Players'
    title_list[9] = rpt_filters.get('pair', '')

    label_list[0] = rpt_row['box1_title']
    label_list[1] = rpt_row['box2_title']
    label_list[2] = rpt_row['box3_title']
    label_list[3] = rpt_row['box4_title']
    label_list[4] = rpt_row['box5_title']
    label_list[5] = rpt_row['box6_title']
    label_list[6] = rpt_row['box7_title']
    label_list[7] = rpt_row['box8_title']
    label_list[8] = rpt_row['box9_title']
    label_list[9] = rpt_row['box10_title']

    df_desc_list[0] = rpt_row['df_desc_1']
    df_desc_list[1] = rpt_row['df_desc_2']
    df_desc_list[2] = rpt_row['df_desc_3']
    df_desc_list[3] = rpt_row['df_desc_4']
    df_desc_list[4] = rpt_row['df_desc_5']
    df_desc_list[5] = rpt_row['df_desc_6']

    #------------------------------------------------------------------------------------------------------
    #            Create the images and dataframes with filtered ppr data for report
    #------------------------------------------------------------------------------------------------------

    # DF 0: Theoretical Margin of Error Reference Table
  df_list[0] = create_margin_of_error_table().to_dict('records')

  # DF 1: Player's Actual Metrics with Confidence Intervals (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[1] = create_player_ci_metrics(ppr_df, disp_player, player_data_stats_df).to_dict('records')

    # DF 2: Reliability Guidelines
  df_list[2] = create_reliability_guidelines().to_dict('records')

  # DF 3: Sample Size Requirements for Target Precision
  df_list[3] = create_sample_size_requirements().to_dict('records')

  # DF 4: Player Zone Analysis with CIs (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[4] = create_zone_confidence_analysis(ppr_df, disp_player, player_data_stats_df).to_dict('records')

    # DF 5: How Metrics Change Over Season (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[5] = create_cumulative_ci_analysis(ppr_df, disp_player).to_dict('records')

    # IMAGE 0: Margin of Error vs Sample Size Plot
  image_list[0] = plot_margin_vs_sample_size()

  # IMAGE 1: Confidence Interval Visualization for Player (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    image_list[1] = plot_player_ci_visualization(ppr_df, disp_player)

    # IMAGE 2: Zone Comparison with Error Bars (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    image_list[2] = plot_zone_comparison_with_ci(ppr_df, disp_player)

  log_error(f"Data Frames returned from report_confidence_intervals: {df_list}")
  
  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


#------------------------------------------------------------------------------------------------------
#                           Helper Functions for Confidence Interval Calculations
#------------------------------------------------------------------------------------------------------

def wilson_score_ci(successes, attempts, confidence=0.95):
  """Calculate Wilson Score Confidence Interval for a proportion."""
  if attempts == 0:
    return {'lower': 0, 'upper': 0, 'margin': 0}

  p = successes / attempts
  n = attempts
  z = stats.norm.ppf(1 - (1 - confidence) / 2)
  z_squared = z * z

  denominator = 1 + z_squared / n
  center = (p + z_squared / (2 * n)) / denominator
  margin = (z * math.sqrt(p * (1 - p) / n + z_squared / (4 * n * n))) / denominator

  return {
    'lower': max(0, center - margin),
    'upper': min(1, center + margin),
    'margin': margin
  }


def calculate_fbhe_ci(kills, errors, attempts, confidence=0.95):
  """Calculate confidence interval for FBHE metric."""
  if attempts == 0:
    return {
      'fbhe': 0,
      'lower': 0,
      'upper': 0,
      'margin': 0
    }

  fbhe = (kills - errors) / attempts

  # Transform FBHE from [-1, 1] to [0, 1] scale
  fbhe_scaled = (fbhe + 1) / 2
  successes_scaled = fbhe_scaled * attempts

  # Calculate CI
  ci = wilson_score_ci(successes_scaled, attempts, confidence)

  # Transform back to FBHE scale
  return {
    'fbhe': fbhe,
    'lower': ci['lower'] * 2 - 1,
    'upper': ci['upper'] * 2 - 1,
    'margin': ci['margin'] * 2
  }


def get_reliability_level(attempts):
  """Return reliability level based on sample size."""
  if attempts < 10:
    return "Very Limited"
  elif attempts < 20:
    return "Limited"
  elif attempts < 50:
    return "Moderate"
  elif attempts < 100:
    return "Good"
  else:
    return "Very Good"


#------------------------------------------------------------------------------------------------------
#                           Dataframe Creation Functions
#------------------------------------------------------------------------------------------------------

def create_margin_of_error_table():
  """Create reference table showing margin of error at different sample sizes."""

  sample_sizes = [5, 10, 15, 20, 30, 50, 75, 100, 150, 200]
  fbhe_values = [0.10, 0.20, 0.30, 0.40, 0.50]

  data = []
  for n in sample_sizes:
    row = {'Attempts': n}
    for fbhe_val in fbhe_values:
      # Estimate kills and errors for this FBHE
      kills = int(0.6 * n)
      errors = int(kills - fbhe_val * n)

      ci = calculate_fbhe_ci(kills, errors, n)
      row[f'FBHE={fbhe_val:.2f}'] = f"±{ci['margin']:.3f}"

    reliability = get_reliability_level(n)
    row['Reliability'] = reliability
    data.append(row)

  return pd.DataFrame(data)


def create_player_ci_metrics(ppr_df, disp_player, player_data_stats_df):
  """Create table of player's key metrics with confidence intervals."""

  metrics_data = []

  # FBHE - Attacking (FIXED: use 'att_player' column)
  att_df = ppr_df[ppr_df['att_player'] == disp_player]
  if att_df.shape[0] > 0:
    fbhe_result = fbhe_obj(att_df, disp_player, 'att', False)
    ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

    metrics_data.append({
      'Metric': 'FBHE (Attacking)',
      'Value': f"{ci['fbhe']:.3f}",
      '95% CI Lower': f"{ci['lower']:.3f}",
      '95% CI Upper': f"{ci['upper']:.3f}",
      'Margin (±)': f"{ci['margin']:.3f}",
      'Attempts': fbhe_result.attempts,
      'Reliability': get_reliability_level(fbhe_result.attempts)
    })

    # FBHE - Serving (FIXED: check if 'action' column exists)
  if 'action' in ppr_df.columns:
    srv_df = ppr_df[ppr_df['serve_player'] == disp_player]
    if srv_df.shape[0] > 0:
      fbhe_result = fbhe_obj(srv_df, disp_player, 'srv', False)
      ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

      metrics_data.append({
        'Metric': 'FBHE (Serving)',
        'Value': f"{ci['fbhe']:.3f}",
        '95% CI Lower': f"{ci['lower']:.3f}",
        '95% CI Upper': f"{ci['upper']:.3f}",
        'Margin (±)': f"{ci['margin']:.3f}",
        'Attempts': fbhe_result.attempts,
        'Reliability': get_reliability_level(fbhe_result.attempts)
      })

    # Good Pass Percentage (FIXED: check if 'action' column exists)
  if 'action' in ppr_df.columns:
    pass_df = ppr_df[ppr_df['pass_player'] == disp_player]
    if pass_df.shape[0] > 0:
      oos_result = count_out_of_system(pass_df, disp_player, 'pass')
      good_passes = int((1 - oos_result[1]) * oos_result[2])

      ci = wilson_score_ci(good_passes, oos_result[2])

      metrics_data.append({
        'Metric': 'Good Pass %',
        'Value': f"{(1-oos_result[1])*100:.1f}%",
        '95% CI Lower': f"{ci['lower']*100:.1f}%",
        '95% CI Upper': f"{ci['upper']*100:.1f}%",
        'Margin (±)': f"{ci['margin']*100:.1f}%",
        'Attempts': oos_result[2],
        'Reliability': get_reliability_level(oos_result[2])
      })

  if not metrics_data:
    #return pd.DataFrame({'Message': ['No data available for this player']})
    return ' '  # on return it checks for empty dataframe.

  return pd.DataFrame(metrics_data)


def create_reliability_guidelines():
  """Create table of reliability guidelines."""

  data = [
    {
      'Attempts': '< 10',
      'Reliability': 'Very Limited',
      'Typical Margin': '> ±0.50',
      'Best Use': 'Early scouting, trend spotting only',
      'Decision Level': '⚠️ Use with extreme caution'
    },
    {
      'Attempts': '10-19',
      'Reliability': 'Limited',
      'Typical Margin': '±0.35 to ±0.50',
      'Best Use': 'Identify major patterns',
      'Decision Level': '⚠️ General tendencies only'
    },
    {
      'Attempts': '20-49',
      'Reliability': 'Moderate',
      'Typical Margin': '±0.25 to ±0.35',
      'Best Use': 'Game planning, lineup decisions',
  'Decision Level': '✓ Coaching decisions'
        },
        {
            'Attempts': '50-99',
            'Reliability': 'Good',
            'Typical Margin': '±0.15 to ±0.25',
            'Best Use': 'Season evaluation, recruiting',
            'Decision Level': '✓ Detailed analysis'
        },
        {
            'Attempts': '100+',
            'Reliability': 'Very Good',
            'Typical Margin': '< ±0.15',
            'Best Use': 'All purposes',
            'Decision Level': '✓✓ High confidence'
        }
    ]
    
  return pd.DataFrame(data)


def create_sample_size_requirements():
    """Create table showing required sample sizes for target precision."""
    
    data = []
    desired_margins = [0.20, 0.15, 0.10, 0.05, 0.03]
    
    for margin in desired_margins:
        # Calculate required n for proportion around 0.65 (scaled from FBHE 0.30)
        z = stats.norm.ppf(0.975)  # 95% CI
        p = 0.65
        n = int(math.ceil((z * z * p * (1 - p)) / (margin * margin)))
        
        # Estimate matches needed (assume ~25 attempts per match)
        matches = int(math.ceil(n / 25))
        
        timeframe = ""
        if matches <= 2:
            timeframe = "1-2 matches"
        elif matches <= 5:
            timeframe = "3-5 matches"
        elif matches <= 10:
            timeframe = "Half season"
        elif matches <= 15:
            timeframe = "Full season"
        else:
            timeframe = "Multiple seasons"
        
        data.append({
            'Desired Precision': f"±{margin:.2f} ({margin*100:.0f}%)",
            'Attempts Needed': f"~{n}",
            'Est. Matches': f"~{matches}",
            'Typical Timeframe': timeframe
        })
    
    return pd.DataFrame(data)


def create_zone_confidence_analysis(ppr_df, disp_player, player_data_stats_df):
    """Create table analyzing FBHE by zone with confidence intervals."""
    
    # FIXED: Use 'att_player' column and check for 'attack_zone' or 'att_src_zone_net'
    player_att = ppr_df[ppr_df['att_player'] == disp_player]
    
    if player_att.shape[0] == 0:
        return pd.DataFrame({'Message': ['No attacking data available']})
    
    # Determine which zone column to use
    zone_col = None
    if 'attack_zone' in player_att.columns:
        zone_col = 'attack_zone'
    elif 'att_src_zone_net' in player_att.columns:
        zone_col = 'att_src_zone_net'
    
    if zone_col is None:
        return pd.DataFrame({'Message': ['Zone column not found in data']})
    
    zones_data = []
    
    for zone in [1, 3, 5]:
        zone_df = player_att[player_att[zone_col] == zone]
        
        if zone_df.shape[0] > 0:
            fbhe_result = fbhe_obj(zone_df, disp_player, 'att', False)
            ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
            
            zones_data.append({
                'Zone': f"Zone {zone}",
                'FBHE': f"{ci['fbhe']:.3f}",
                'CI Lower': f"{ci['lower']:.3f}",
                'CI Upper': f"{ci['upper']:.3f}",
                'Margin': f"±{ci['margin']:.3f}",
                'Attempts': fbhe_result.attempts,
                'Reliability': get_reliability_level(fbhe_result.attempts),
                'Kills': fbhe_result.kills,
                'Errors': fbhe_result.errors
            })
    
    # Add overall
    fbhe_result = fbhe_obj(player_att, disp_player, 'att', False)
    ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
    
    zones_data.append({
        'Zone': 'All Zones',
        'FBHE': f"{ci['fbhe']:.3f}",
        'CI Lower': f"{ci['lower']:.3f}",
        'CI Upper': f"{ci['upper']:.3f}",
        'Margin': f"±{ci['margin']:.3f}",
        'Attempts': fbhe_result.attempts,
        'Reliability': get_reliability_level(fbhe_result.attempts),
        'Kills': fbhe_result.kills,
        'Errors': fbhe_result.errors
    })
    
    if not zones_data:
        return pd.DataFrame({'Message': ['No zone data available']})
    
    return pd.DataFrame(zones_data)


def create_cumulative_ci_analysis(ppr_df, disp_player):
    """Show how confidence intervals narrow over time."""
    
    # FIXED: Use 'att_player' column
    player_att = ppr_df[ppr_df['att_player'] == disp_player]
    
    if player_att.shape[0] < 10:
        return pd.DataFrame({'Message': ['Insufficient data for cumulative analysis (need 10+ attempts)']})
    
    # Sort by date if available
    if 'game_date' in player_att.columns:
        player_att = player_att.sort_values('game_date')
    
    data = []
    cumulative_kills = 0
    cumulative_errors = 0
    cumulative_attempts = 0
    
    # Sample at specific intervals
    intervals = [10, 20, 30, 50, 75, 100, 150, 200]
    current_interval_idx = 0
    
    # FIXED: Check if 'result' column exists
    if 'result' not in player_att.columns:
        return pd.DataFrame({'Message': ['Result column not found - cannot analyze cumulative data']})
    
    for idx, row in player_att.iterrows():
        if row['result'] == 'kill':
            cumulative_kills += 1
        elif row['result'] == 'error':
            cumulative_errors += 1
        cumulative_attempts += 1
        
        # Check if we've reached next interval
        if current_interval_idx < len(intervals) and cumulative_attempts >= intervals[current_interval_idx]:
            ci = calculate_fbhe_ci(cumulative_kills, cumulative_errors, cumulative_attempts)
            
            data.append({
                'After N Attempts': cumulative_attempts,
                'FBHE': f"{ci['fbhe']:.3f}",
                'CI Width': f"{ci['upper'] - ci['lower']:.3f}",
                'Margin': f"±{ci['margin']:.3f}",
                'Reliability': get_reliability_level(cumulative_attempts)
            })
            
            current_interval_idx += 1
    
    # Add final cumulative
    if cumulative_attempts not in [d['After N Attempts'] for d in data]:
        ci = calculate_fbhe_ci(cumulative_kills, cumulative_errors, cumulative_attempts)
        data.append({
            'After N Attempts': cumulative_attempts,
            'FBHE': f"{ci['fbhe']:.3f}",
            'CI Width': f"{ci['upper'] - ci['lower']:.3f}",
            'Margin': f"±{ci['margin']:.3f}",
            'Reliability': get_reliability_level(cumulative_attempts)
        })
    
    if not data:
        return pd.DataFrame({'Message': ['Not enough attempts reached intervals']})
    
    return pd.DataFrame(data)


#------------------------------------------------------------------------------------------------------
#                           Plot/Image Creation Functions
#------------------------------------------------------------------------------------------------------
def plot_margin_vs_sample_size():
  """Create plot showing margin of error decreasing with sample size."""
  import matplotlib.pyplot as plt

  sample_sizes = list(range(5, 201, 5))
  margins = []

  for n in sample_sizes:
    kills = int(0.6 * n)
    errors = int(kills - 0.30 * n)
    ci = calculate_fbhe_ci(kills, errors, n)
    margins.append(ci['margin'])

  fig, ax = plt.subplots(figsize=(12, 8))
  ax.plot(sample_sizes, margins, 'o-', color='#1f77b4', linewidth=3, 
          markersize=6, label='Margin of Error')
  ax.axhline(y=0.35, color='red', linestyle='--', linewidth=2, 
             label='Moderate (±0.35)')
  ax.axhline(y=0.25, color='orange', linestyle='--', linewidth=2,
             label='Good (±0.25)')
  ax.axhline(y=0.18, color='green', linestyle='--', linewidth=2,
             label='Very Good (±0.18)')
  ax.set_title("How Margin of Error Decreases with Sample Size", fontsize=16)
  ax.set_xlabel("Number of Attempts", fontsize=14)
  ax.set_ylabel("Margin of Error (±)", fontsize=14)
  ax.legend(loc='upper right')
  ax.grid(True, alpha=0.3)

  img = anvil.mpl_util.plot_image()
  plt.close()
  return img


def plot_player_ci_visualization(ppr_df, disp_player):
  """Create visualization of player's metrics with error bars."""
  import matplotlib.pyplot as plt

  metrics = []
  values = []
  errors_lower = []
  errors_upper = []

  att_df = ppr_df[ppr_df['att_player'] == disp_player]
  if att_df.shape[0] > 0:
    try:
      fbhe_result = fbhe_obj(att_df, disp_player, 'att', False)
      ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
      metrics.append(f'Attacking\n(n={fbhe_result.attempts})')
      values.append(ci['fbhe'])
      errors_lower.append(ci['fbhe'] - ci['lower'])
      errors_upper.append(ci['upper'] - ci['fbhe'])
    except:
      pass

  if 'serve_player' in ppr_df.columns:
    srv_df = ppr_df[ppr_df['serve_player'] == disp_player]
    if srv_df.shape[0] > 0:
      try:
        fbhe_result = fbhe_obj(srv_df, disp_player, 'srv', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
        metrics.append(f'Serving\n(n={fbhe_result.attempts})')
        values.append(ci['fbhe'])
        errors_lower.append(ci['fbhe'] - ci['lower'])
        errors_upper.append(ci['upper'] - ci['fbhe'])
      except:
        pass

  if not metrics:
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=14)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    img = anvil.mpl_util.plot_image()
    plt.close()
    return img

  fig, ax = plt.subplots(figsize=(10, 8))
  x_pos = range(len(metrics))
  ax.bar(x_pos, values, yerr=[errors_lower, errors_upper], 
         capsize=10, color='lightblue', edgecolor='navy', linewidth=2)
  ax.set_xticks(x_pos)
  ax.set_xticklabels(metrics)
  ax.set_ylabel('FBHE', fontsize=14)
  ax.set_title(f"{disp_player} - FBHE with 95% Confidence Intervals", fontsize=16)
  ax.set_ylim(-1, 1)
  ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
  ax.grid(True, alpha=0.3, axis='y')

  img = anvil.mpl_util.plot_image()
  plt.close()
  return img


def plot_zone_comparison_with_ci(ppr_df, disp_player):
  """Create zone comparison plot with error bars."""
  import matplotlib.pyplot as plt
  from matplotlib.patches import Patch

  player_att = ppr_df[ppr_df['att_player'] == disp_player]
  zone_col = 'att_src_zone_net'

  if zone_col not in player_att.columns:
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.text(0.5, 0.5, 'Zone data not available', ha='center', va='center', fontsize=14)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    img = anvil.mpl_util.plot_image()
    plt.close()
    return img

  zones = []
  fbhe_values = []
  errors_lower = []
  errors_upper = []
  colors = []

  for zone in [1, 2, 3, 4, 5]:
    zone_df = player_att[player_att[zone_col] == zone]
    if zone_df.shape[0] > 0:
      try:
        fbhe_result = fbhe_obj(zone_df, disp_player, 'att', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
        zones.append(f'Zone {zone}\n(n={fbhe_result.attempts})')
        fbhe_values.append(ci['fbhe'])
        errors_lower.append(ci['fbhe'] - ci['lower'])
        errors_upper.append(ci['upper'] - ci['fbhe'])
        reliability = get_reliability_level(fbhe_result.attempts)
        if reliability in ['Good', 'Very Good']:
          colors.append('green')
        elif reliability == 'Moderate':
          colors.append('orange')
        else:
          colors.append('red')
      except:
        pass

  if not zones:
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.text(0.5, 0.5, 'No zone data available', ha='center', va='center', fontsize=14)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    img = anvil.mpl_util.plot_image()
    plt.close()
    return img

  fig, ax = plt.subplots(figsize=(12, 8))
  x_pos = range(len(zones))
  bars = ax.bar(x_pos, fbhe_values, yerr=[errors_lower, errors_upper],
                  capsize=10, color=colors, edgecolor='black', linewidth=2, alpha=0.7)
  ax.set_xticks(x_pos)
  ax.set_xticklabels(zones)
  ax.set_ylabel('FBHE', fontsize=14)
  ax.set_title(f"{disp_player} - FBHE by Zone with 95% CI", fontsize=16)
  ax.set_ylim(-1, 1)
  ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
  ax.grid(True, alpha=0.3, axis='y')
    
  legend_elements = [
        Patch(facecolor='green', alpha=0.7, label='Good/Very Good'),
        Patch(facecolor='orange', alpha=0.7, label='Moderate'),
        Patch(facecolor='red', alpha=0.7, label='Limited')
  ]
  ax.legend(handles=legend_elements, loc='upper right')
    
  img = anvil.mpl_util.plot_image()
  plt.close()
  return img

#===================================================================================
#
#
#
#===================================================================================

def report_statistical_guidelines(lgy, team, **rpt_filters):
  '''
    Statistical Guidelines Report - League-Wide Analysis (Per-Player Metrics)
    
    Provides statistical guidance for the 5 key beach volleyball metrics:
    - First Ball Hitting Efficiency (FBHE)
    - Transition Conversion Rate (TCR)
    - Error Density
    - Knockout Rate
    - In-System Passing %
    
    Shows sample size requirements, reliability guidelines, and margin of error curves
    to help coaches understand how much data is needed for reliable PER-PLAYER metrics.
    
    NOTE: All calculations are adjusted for individual player analysis. Since attempts
    are shared among multiple players (4 attackers, 2 servers, 2 passers), the 
    "attempts per point" values account for this distribution.
    
    INPUT Parameters:
        - lgy : league, gender, year combination
        - team : the team of the user calling the report
        - rpt_filters : filters to limit the data (comp_l1, comp_l2, comp_l3, date ranges, etc.)
    
    OUTPUT Return Parameters:
        - title_list : report titles
        - label_list : section labels
        - image_list : plots and charts
        - df_list : data tables
        - df_desc_list : descriptions for dataframes
        - image_desc_list : descriptions for images
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize and Fetch Data
  #------------------------------------------------------------------------------------------------------

  # Initialize lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  # Unpack lgy
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch and filter data
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # Get report metadata from database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)

  if rpt_row:
    title_list[0] = rpt_row['rpt_title']
    title_list[1] = rpt_row['rpt_sub_title']
    title_list[2] = rpt_row['rpt_section_title1']
    title_list[3] = rpt_filters.get('lgy')
    title_list[4] = rpt_row['team_name']
    title_list[5] = rpt_row['rpt_type']
    title_list[6] = rpt_row['filter_text']
    title_list[7] = rpt_row['explain_text']
    title_list[8] = 'League-Wide Analysis'
    title_list[9] = ''

    label_list[0] = rpt_row['box1_title']
    label_list[1] = rpt_row['box2_title']
    label_list[2] = rpt_row['box3_title']
    label_list[3] = rpt_row['box4_title']
    label_list[4] = rpt_row['box5_title']
    label_list[5] = rpt_row['box6_title']
    label_list[6] = rpt_row['box7_title']
    label_list[7] = rpt_row['box8_title']
    label_list[8] = rpt_row['box9_title']
    label_list[9] = rpt_row['box10_title']

    df_desc_list[0] = rpt_row['df_desc_1']
    df_desc_list[1] = rpt_row['df_desc_2']
    df_desc_list[2] = rpt_row['df_desc_3']
    df_desc_list[3] = rpt_row['df_desc_4']
    df_desc_list[4] = rpt_row['df_desc_5']
    df_desc_list[5] = rpt_row['df_desc_6']

  #------------------------------------------------------------------------------------------------------
  #            Calculate League-Wide Percentages for Each Metric Type
  #------------------------------------------------------------------------------------------------------

  total_points = len(ppr_df)

  if total_points == 0:
    # Return empty report if no data
    df_list[0] = pd.DataFrame({'Message': ['No data available for selected filters']})
    return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

  # Calculate actual percentages in this league/filter
  # Adjusted for PER-PLAYER analysis (4 players total: 2 per team)

  # First ball attacks: Only 1 player attacks per first ball point
  # Each of 4 players gets approximately 1/4 of all first ball attempts
  fb_points = len(ppr_df[ppr_df['point_outcome'] != 'TSE'])
  fb_pct = (fb_points / total_points / 4) if total_points > 0 else 0.22

  # Transition: All 4 players have opportunity on transition points (no division)
  trans_points = len(ppr_df[ppr_df['point_outcome'].isin(['TK', 'TE'])])
  trans_pct = (trans_points / total_points) if total_points > 0 else 0.40

  # Error Density: All 4 players have opportunity to make errors (no division)
  error_pct = 1.0

  # Knockout (serving): Only 1 of 4 players serves each point
  # Each player serves approximately 1/4 of all points
  knockout_points = len(ppr_df[ppr_df['point_outcome'] != 'TSE'])
  knockout_pct = (knockout_points / total_points / 4) if total_points > 0 else 0.22

  # In-System Passing: Only 1 of 4 players passes each point
  # Each player passes approximately 1/4 of all points
  passing_points = len(ppr_df[~ppr_df['point_outcome'].isin(['TSE', 'TSA'])])
  passing_pct = (passing_points / total_points / 4) if total_points > 0 else 0.21

  # Define metrics with their characteristics
  metrics_info = {
    'FBHE': {
      'name': 'First Ball Hitting Efficiency',
      'range': '(-1 to 1)',
      'type': 'efficiency',
      'attempts_per_point': fb_pct,
      'typical_value': 0.30
    },
    'TCR': {
      'name': 'Transition Conversion Rate',
      'range': '(0 to 1)',
      'type': 'proportion',
      'attempts_per_point': trans_pct,
      'typical_value': 0.50
    },
    'Error Density': {
      'name': 'Error Density',
      'range': '(0 to 1)',
      'type': 'proportion',
      'attempts_per_point': error_pct,
      'typical_value': 0.20
    },
    'Knockout Rate': {
      'name': 'Knockout Rate',
      'range': '(0 to 1)',
      'type': 'proportion',
      'attempts_per_point': knockout_pct,
      'typical_value': 0.15
    },
    'In-System %': {
      'name': 'In-System Passing',
      'range': '(0 to 1)',
      'type': 'proportion',
      'attempts_per_point': passing_pct,
      'typical_value': 0.70
    }
  }

  #------------------------------------------------------------------------------------------------------
  #            Create DataFrames and Images
  #------------------------------------------------------------------------------------------------------

  # DF 0: Dataset Summary (total points and matches)
  df_list[0] = create_league_data_summary(total_points, metrics_info).to_dict('records')

  # DF 1: Metric Occurrence Percentages
  df_list[1] = create_metric_percentages_table(metrics_info).to_dict('records')

  # DF 2: Sample Size Requirements (in points)
  df_list[2] = create_sample_size_by_points(metrics_info).to_dict('records')

  # DF 3: Reliability Guidelines
  df_list[3] = create_reliability_guidelines_league().to_dict('records')

  # DF 4: Points-to-Matches Conversion
  df_list[4] = create_points_to_matches_table().to_dict('records')

  # DF 5: Detailed Margin of Error by Metric (by points)
  df_list[5] = create_detailed_margin_table(metrics_info).to_dict('records')

  # DF 6: Margin of Error by Attempts (for single-actor metrics)
  df_list[6] = create_margin_by_attempts_table().to_dict('records')

  # IMAGE 0: Margin of Error Curves for All Metrics
  plot_all_metrics_margin_curves(metrics_info)
  image_list[0] = anvil.mpl_util.plot_image()

  # IMAGE 1: Sample Size Comparison Chart
  plot_sample_size_comparison(metrics_info)
  image_list[1] = anvil.mpl_util.plot_image()

  # IMAGE 2: Attempts per Match Visualization
  plot_attempts_per_match(metrics_info)
  image_list[2] = anvil.mpl_util.plot_image()

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


#------------------------------------------------------------------------------------------------------
#                    Helper Functions for Statistical Guidelines Report
#------------------------------------------------------------------------------------------------------

def calculate_se_proportion(p, n):
  """Calculate standard error for a simple proportion (0 to 1)."""
  if n == 0:
    return 0
  return math.sqrt(p * (1 - p) / n)


def calculate_se_efficiency(p_good, p_bad, n):
  """Calculate standard error for an efficiency metric (-1 to 1)."""
  if n == 0:
    return 0
  return math.sqrt((p_good * (1 - p_good) / n) + (p_bad * (1 - p_bad) / n))


def calculate_margin_of_error(metric_info, n, confidence=0.95):
  """Calculate margin of error for a given metric and sample size."""
  z = stats.norm.ppf(1 - (1 - confidence) / 2)  # 1.96 for 95% CI

  if metric_info['type'] == 'efficiency':
    # For efficiency metrics like FBHE
    typical_val = metric_info['typical_value']
    # Estimate p_good and p_bad
    p_good = (typical_val + 1) / 2  # Convert from [-1,1] to [0,1] scale
    p_bad = 1 - p_good
    se = calculate_se_efficiency(p_good, p_bad, n)
  else:
    # For simple proportions
    p = metric_info['typical_value']
    se = calculate_se_proportion(p, n)
  
  return z * se


def get_sample_size_for_margin(metric_info, target_margin, confidence=0.95):
  """Calculate required sample size for a target margin of error."""
  z = stats.norm.ppf(1 - (1 - confidence) / 2)
  
  if metric_info['type'] == 'efficiency':
    # Conservative estimate using p=0.5 for maximum variance
    p_good = 0.5
    p_bad = 0.5
    # n = z^2 * (var_good + var_bad) / margin^2
    n = (z ** 2) * (p_good * (1 - p_good) + p_bad * (1 - p_bad)) / (target_margin ** 2)
  else:
    # Use p=0.5 for maximum variance (conservative)
    p = 0.5
    n = (z ** 2) * p * (1 - p) / (target_margin ** 2)
  
  return int(math.ceil(n))


#------------------------------------------------------------------------------------------------------
#                    DataFrame Creation Functions
#------------------------------------------------------------------------------------------------------

def create_league_data_summary(total_points, metrics_info):
  """Create summary of league data - just the totals."""
  
  data = [{
    'Statistic': 'Total Points in Dataset',
    'Value': f"{total_points}"
  }, {
    'Statistic': 'Estimated Matches',
    'Value': f"{total_points / 85:.1f}"
  }]
  
  return pd.DataFrame(data)


def create_metric_percentages_table(metrics_info):
  """Create table showing what percentage of points each metric occurs on."""
  
  data = []
  for metric_key, info in metrics_info.items():
    pct = info['attempts_per_point'] * 100
    data.append({
      'Metric': info['name'],
      'Attempts per Point': f"{info['attempts_per_point']:.2f}",
      'Percentage of Points': f"{pct:.1f}%"
    })
  
  return pd.DataFrame(data)


def create_sample_size_by_points(metrics_info):
  """Create table showing required points for different accuracy levels."""
  
  target_margins = [0.05, 0.075, 0.10, 0.15, 0.20]
  
  data = []
  for target_margin in target_margins:
    row = {'Target Accuracy (±)': f"±{target_margin:.1%}"}
    
    for metric_key, info in metrics_info.items():
      required_attempts = get_sample_size_for_margin(info, target_margin)
      # Convert to points
      required_points = int(required_attempts / info['attempts_per_point']) if info['attempts_per_point'] > 0 else 0
      row[metric_key] = f"{required_points} pts"
    
    data.append(row)
  
  return pd.DataFrame(data)


def create_reliability_guidelines_league():
  """Create reliability guidelines based on number of points."""
  
  data = [
    {
      'Points': '< 100',
      'Matches (≈)': '< 1',
      'Reliability': 'Very Limited',
      'Guidance': 'Insufficient data for reliable conclusions'
    },
    {
      'Points': '100-250',
      'Matches (≈)': '1-3',
      'Reliability': 'Limited',
      'Guidance': 'Use with caution; significant uncertainty'
    },
    {
      'Points': '250-500',
      'Matches (≈)': '3-6',
      'Reliability': 'Moderate',
      'Guidance': 'Reasonable for general trends'
    },
    {
      'Points': '500-1000',
      'Matches (≈)': '6-12',
      'Reliability': 'Good',
      'Guidance': 'Reliable for most decisions'
    },
    {
      'Points': '> 1000',
      'Matches (≈)': '> 12',
      'Reliability': 'Very Good',
      'Guidance': 'High confidence in metrics'
    }
  ]
  
  return pd.DataFrame(data)


def create_points_to_matches_table():
  """Create conversion table from points to matches."""
  
  matches = [1, 2, 3, 5, 10, 15, 20, 30, 50]
  
  data = []
  for m in matches:
    points = m * 85
    row = {
      'Matches': m,
      'Points (≈85/match)': points,
      'Range': f"{m * 75} - {m * 100}"
    }
    data.append(row)
  
  return pd.DataFrame(data)


def create_detailed_margin_table(metrics_info):
  """Create detailed margin of error table for each metric at different sample sizes."""
  
  point_counts = [100, 250, 500, 750, 1000, 1500, 2000]
  
  data = []
  for points in point_counts:
    row = {
      'Points': points,
      'Matches (≈)': f"{points / 85:.1f}"
    }
    
    for metric_key, info in metrics_info.items():
      # Calculate actual attempts for this metric
      attempts = int(points * info['attempts_per_point'])
      if attempts > 0:
        margin = calculate_margin_of_error(info, attempts)
        row[metric_key] = f"±{margin:.3f}"
      else:
        row[metric_key] = "N/A"
    
    data.append(row)
  
  return pd.DataFrame(data)


def create_margin_by_attempts_table():
  """Create margin of error table based on number of attempts for single-actor metrics."""
  
  attempt_counts = [10, 25, 50, 100, 150, 200, 300, 400]
  
  data = []
  for attempts in attempt_counts:
    # FBHE uses efficiency formula (range -1 to 1)
    # Use p=0.5 for conservative estimate (matches the chart calculations)
    fbhe_metric = {
      'type': 'efficiency',
      'typical_value': 0.00  # This gives p_good=0.5, p_bad=0.5 (maximum variance)
    }
    fbhe_margin = calculate_margin_of_error(fbhe_metric, attempts)
    
    # Knockout and In-System use proportion formula (range 0 to 1)
    # Use p=0.5 for conservative estimate
    proportion_metric = {
      'type': 'proportion',
      'typical_value': 0.50
    }
    knockout_margin = calculate_margin_of_error(proportion_metric, attempts)
    in_system_margin = calculate_margin_of_error(proportion_metric, attempts)
    
    # Calculate equivalent matches for single-actor metrics (÷4 factor)
    # attempts * 4 = points needed, then / 85 = matches
    matches_needed = (attempts * 4) / 85
    
    row = {
      'Attempts': attempts,
      'FBHE': f"±{fbhe_margin:.3f}",
      'Knockout': f"±{knockout_margin:.3f}",
      'In-System %': f"±{in_system_margin:.3f}",
      'Matches Needed': f"{matches_needed:.1f}",
      'Reliability': get_reliability_level(attempts)
    }
    data.append(row)
  
  return pd.DataFrame(data)


#------------------------------------------------------------------------------------------------------
#                    Plot/Image Creation Functions
#------------------------------------------------------------------------------------------------------

def plot_all_metrics_margin_curves(metrics_info):
  """Create plot showing margin of error curves for all 5 metrics using matplotlib."""
  
  plt.figure(figsize=(12, 8))
  
  point_range = list(range(100, 2001, 50))
  
  colors = {
    'FBHE': '#1f77b4',
    'TCR': '#ff7f0e',
    'Error Density': '#2ca02c',
    'Knockout Rate': '#d62728',
    'In-System %': '#9467bd'
  }
  
  for metric_key, info in metrics_info.items():
    margins = []
    
    for points in point_range:
      attempts = int(points * info['attempts_per_point'])
      if attempts > 0:
        margin = calculate_margin_of_error(info, attempts)
        margins.append(margin)
      else:
        margins.append(None)
    
    plt.plot(point_range, margins, 
             label=metric_key, 
             color=colors.get(metric_key, '#000000'), 
             linewidth=2)
  
  # Add reference lines
  plt.axhline(y=0.10, linestyle='--', color='green', alpha=0.7, label='±10% (Good)')
  plt.axhline(y=0.15, linestyle='--', color='orange', alpha=0.7, label='±15% (Moderate)')
  
  plt.title("Margin of Error vs. Number of Points (All Metrics)", fontsize=14, fontweight='bold')
  plt.xlabel("Number of Points", fontsize=12)
  plt.ylabel("Margin of Error (±)", fontsize=12)
  plt.legend(loc='upper right', fontsize=10)
  plt.grid(True, alpha=0.3)
  plt.tight_layout()


def plot_sample_size_comparison(metrics_info):
  """Create bar chart comparing required sample sizes for ±10% accuracy using matplotlib."""
  
  target_margin = 0.10
  
  metrics = []
  required_points = []
  required_matches = []
  
  for metric_key, info in metrics_info.items():
    required_attempts = get_sample_size_for_margin(info, target_margin)
    points = int(required_attempts / info['attempts_per_point']) if info['attempts_per_point'] > 0 else 0
    matches = points / 85
    
    metrics.append(metric_key)
    required_points.append(points)
    required_matches.append(matches)
  
  plt.figure(figsize=(10, 6))
  
  colors_list = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
  bars = plt.bar(metrics, required_points, color=colors_list)
  
  # Add value labels on bars
  for i, (bar, points, matches) in enumerate(zip(bars, required_points, required_matches)):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{points} pts\n({matches:.1f} matches)',
             ha='center', va='bottom', fontsize=9)
  
  plt.title("Points Required for ±10% Accuracy (95% Confidence)", fontsize=14, fontweight='bold')
  plt.ylabel("Number of Points Required", fontsize=12)
  plt.xlabel("Metric", fontsize=12)
  plt.xticks(rotation=45, ha='right')
  plt.grid(True, axis='y', alpha=0.3)
  plt.tight_layout()


def plot_attempts_per_match(metrics_info):
  """Visualize how many attempts per match for each metric type using matplotlib."""
  
  metrics = []
  attempts_per_match = []
  
  for metric_key, info in metrics_info.items():
    metrics.append(metric_key)
    # Assuming 85 points per match
    attempts_per_match.append(info['attempts_per_point'] * 85)
  
  plt.figure(figsize=(10, 6))
  
  colors_list = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
  bars = plt.bar(metrics, attempts_per_match, color=colors_list)
  
  # Add value labels on bars
  for bar, attempts in zip(bars, attempts_per_match):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{attempts:.1f}',
             ha='center', va='bottom', fontsize=10)
  
  plt.title("Expected Attempts per Match (Based on Filtered Data)", fontsize=14, fontweight='bold')
  plt.ylabel("Attempts per Match (≈85 points)", fontsize=12)
  plt.xlabel("Metric", fontsize=12)
  plt.xticks(rotation=45, ha='right')
  plt.grid(True, axis='y', alpha=0.3)
  plt.tight_layout()
  
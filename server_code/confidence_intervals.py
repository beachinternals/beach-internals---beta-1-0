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
  df_list[0] = create_margin_of_error_table()

  # DF 1: Player's Actual Metrics with Confidence Intervals (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[1] = create_player_ci_metrics(ppr_df, disp_player, player_data_stats_df)

    # DF 2: Reliability Guidelines
  df_list[2] = create_reliability_guidelines()

  # DF 3: Sample Size Requirements for Target Precision
  df_list[3] = create_sample_size_requirements()

  # DF 4: Player Zone Analysis with CIs (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[4] = create_zone_confidence_analysis(ppr_df, disp_player, player_data_stats_df)

    # DF 5: How Metrics Change Over Season (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[5] = create_cumulative_ci_analysis(ppr_df, disp_player)

    # IMAGE 0: Margin of Error vs Sample Size Plot
  image_list[0] = plot_margin_vs_sample_size()

  # IMAGE 1: Confidence Interval Visualization for Player (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    image_list[1] = plot_player_ci_visualization(ppr_df, disp_player)

    # IMAGE 2: Zone Comparison with Error Bars (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    image_list[2] = plot_zone_comparison_with_ci(ppr_df, disp_player)

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
    srv_df = ppr_df[ppr_df['action'] == 'srv']
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
    pass_df = ppr_df[ppr_df['action'] == 'pass']
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
    return pd.DataFrame({'Message': ['No data available for this player']})

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
    import plotly.graph_objects as go
    
    sample_sizes = list(range(5, 201, 5))
    margins = []
    
    for n in sample_sizes:
        # Assume typical FBHE around 0.30
        kills = int(0.6 * n)
        errors = int(kills - 0.30 * n)
        ci = calculate_fbhe_ci(kills, errors, n)
        margins.append(ci['margin'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=sample_sizes,
        y=margins,
        mode='lines+markers',
        name='Margin of Error',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=6)
    ))
    
    # Add reference lines
    fig.add_hline(y=0.35, line_dash="dash", line_color="red", 
                  annotation_text="Moderate (±0.35)", annotation_position="right")
    fig.add_hline(y=0.25, line_dash="dash", line_color="orange",
                  annotation_text="Good (±0.25)", annotation_position="right")
    fig.add_hline(y=0.18, line_dash="dash", line_color="green",
                  annotation_text="Very Good (±0.18)", annotation_position="right")
    
    fig.update_layout(
        title="How Margin of Error Decreases with Sample Size",
        xaxis_title="Number of Attempts",
        yaxis_title="Margin of Error (±)",
        hovermode='x unified',
        showlegend=True
    )
    
    return fig


def plot_player_ci_visualization(ppr_df, disp_player):
    """Create visualization of player's metrics with error bars."""
    import plotly.graph_objects as go
    
    metrics = []
    values = []
    lower_errors = []
    upper_errors = []
    
    # Attacking (FIXED: use 'att_player')
    att_df = ppr_df[ppr_df['att_player'] == disp_player]
    if att_df.shape[0] > 0:
        fbhe_result = fbhe_obj(att_df, disp_player, 'att', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
        
        metrics.append(f'Attacking\n(n={fbhe_result.attempts})')
        values.append(ci['fbhe'])
        lower_errors.append(ci['fbhe'] - ci['lower'])
        upper_errors.append(ci['upper'] - ci['fbhe'])
    
    # Serving (FIXED: check for 'action' column)
    if 'action' in ppr_df.columns:
        srv_df = ppr_df[ppr_df['action'] == 'srv']
        if srv_df.shape[0] > 0:
            fbhe_result = fbhe_obj(srv_df, disp_player, 'srv', False)
            ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
            
            metrics.append(f'Serving\n(n={fbhe_result.attempts})')
            values.append(ci['fbhe'])
            lower_errors.append(ci['fbhe'] - ci['lower'])
            upper_errors.append(ci['upper'] - ci['fbhe'])
    
    if not metrics:
        # Return empty figure with message
        fig = go.Figure()
        fig.add_annotation(text="No data available for visualization",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False)
        return fig
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=metrics,
        y=values,
        error_y=dict(
            type='data',
            symmetric=False,
            array=upper_errors,
            arrayminus=lower_errors
        ),
        marker_color='lightblue',
        name='FBHE with 95% CI'
    ))
    
    fig.update_layout(
        title=f"{disp_player} - FBHE with 95% Confidence Intervals",
        yaxis_title="FBHE",
        yaxis=dict(range=[-1, 1]),
        showlegend=False
    )
    
    return fig


def plot_zone_comparison_with_ci(ppr_df, disp_player):
    """Create zone comparison plot with error bars."""
    import plotly.graph_objects as go
    
    # FIXED: Use 'att_player' and check zone column
    player_att = ppr_df[ppr_df['att_player'] == disp_player]
    
    # Determine zone column
    zone_col = None
    if 'attack_zone' in player_att.columns:
        zone_col = 'attack_zone'
    elif 'att_src_zone_net' in player_att.columns:
        zone_col = 'att_src_zone_net'
    
    if zone_col is None:
        fig = go.Figure()
        fig.add_annotation(text="Zone data not available",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False)
        return fig
    
    zones = []
    fbhe_values = []
    lower_errors = []
    upper_errors = []
    colors = []
    
    for zone in [1, 3, 5]:
        zone_df = player_att[player_att[zone_col] == zone]
        
        if zone_df.shape[0] > 0:
            fbhe_result = fbhe_obj(zone_df, disp_player, 'att', False)
            ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
            
            zones.append(f'Zone {zone}\n(n={fbhe_result.attempts})')
            fbhe_values.append(ci['fbhe'])
            lower_errors.append(ci['fbhe'] - ci['lower'])
            upper_errors.append(ci['upper'] - ci['fbhe'])
            
            # Color by reliability
            reliability = get_reliability_level(fbhe_result.attempts)
            if reliability in ['Good', 'Very Good']:
                colors.append('green')
            elif reliability == 'Moderate':
                colors.append('orange')
            else:
                colors.append('red')
    
    if not zones:
        fig = go.Figure()
        fig.add_annotation(text="No zone data available",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False)
        return fig
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=zones,
        y=fbhe_values,
        error_y=dict(
            type='data',
            symmetric=False,
            array=upper_errors,
            arrayminus=lower_errors
        ),
        marker_color=colors,
        name='FBHE by Zone'
    ))
    
    fig.update_layout(
        title=f"{disp_player} - FBHE by Zone with 95% CI",
        yaxis_title="FBHE",
        yaxis=dict(range=[-1, 1]),
        showlegend=False
    )
    
    return fig
import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
import anvil.server
import pandas as pd
import io
import math
import inspect
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Ellipse
import numpy as np
import scipy.stats as stats
from scipy.stats import chi2
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import mutual_info_classif
#import sklearn
import seaborn as sns

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import anvil.media
from io import BytesIO


from tabulate import tabulate
from anvil import pdf
from pair_functions import *
#from matchup_reports import player_45_serves
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *

# Create logger with formatting
from anvil_extras.logging import Logger
import logging
logger = Logger()
# If the library supports standard Python logging formatting:
formatter = logging.Formatter('%(levelname)s - %(funcName)s:%(lineno)d - %(message)s')


def report_stub(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================


  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_dashboard_key_metrics(lgy, team, **rpt_filters):
  """
    Dashboard report showing key metrics for all players on a team.
    
    Args:
        lgy: League+gender+year string
        team: Team name
        **rpt_filters: Additional report filters (should include 'team' filter)
        
    Returns:
        tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
    """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = [''] * 10
  df_list = [''] * 10

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Filter the ppr dataframe with any additional filters
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # Get list of all players for this team from master_player or from ppr_df
  team_players = get_team_players(disp_league, disp_gender, disp_year, team)

  # do not want to do this, as this will give you all players int he db!
  #if not team_players:
  #  # Fallback: get unique players from ppr_df
  #  team_players = set()
  #  for col in ['player_a1', 'player_a2', 'player_b1', 'player_b2']:
  #    team_players.update(ppr_df[col].dropna().unique())
  #  team_players = list(team_players)

  print(f"Generating dashboard for {len(team_players)} players on team {team}")

  # Initialize comprehensive metrics dataframe
  metrics_data = {
    'Player': [],
    'FBHE': [],           # First Ball Hitting Efficiency
    'FBSO': [],           # First Ball Side Out
    'TCR': [],            # Transition Conversion Rate
    'ESO': [],            # Expected Side Out
    'Expected': [], # Expected Value
    'Good_Pass_Pct': [],  # Good Pass Percentage
    'Knockout': [], # Knockout Ratio
    'Ace/Error': [], # Ace to Error Ratio
    'Consistency_Errors': [], # Consistency in Errors
    'Error_Density': []   # Error Density
  }

  # Helper function to parse percentage strings
  def parse_percentage(value, player, column):
    try:
      if isinstance(value, str):
        if value == '56%32%0%0%56%58%55%70%':  # Explicitly handle malformed string
          print(f"Found malformed {column} for {player}: {value}")
          return 0.0
        if '%' in value:
          cleaned_value = value.replace('%', '')
          return float(cleaned_value) / 100  # Convert to decimal
      return float(value)  # Handle numeric or string numeric values
    except (ValueError, TypeError):
      print(f"Invalid {column} for {player}: {value}")
      return 0.0  # Default to 0 if conversion fails

    # Helper function to convert NumPy types to Python types
  def to_python_type(value):
    if isinstance(value, (np.floating, np.integer)):
      return float(value) if isinstance(value, np.floating) else int(value)
    return value

    # Calculate metrics for each player
  for player in team_players:
    #print(f"Processing player: {player}")
    try:
      # Add player name
      metrics_data['Player'].append(player)

      # Get FBHE and FBSO from fbhe_obj function
      fbhe_result = fbhe_obj(ppr_df, player, 'att', False)
      metrics_data['FBHE'].append(to_python_type(fbhe_result.fbhe) if fbhe_result else 0)
      metrics_data['FBSO'].append(to_python_type(fbhe_result.fbso) if fbhe_result else 0)

      # Get TCR from calc_trans_obj function
      tcr_result = calc_trans_obj(ppr_df, player, 'all')
      metrics_data['TCR'].append(tcr_result.get('tcr')) 

      # Get ESO from calc_eso function
      eso_result = calc_player_eso_obj(ppr_df, player)
      metrics_data['ESO'].append(eso_result.get('eso'))

        # Get Expected Value from calc_ev_obj function
      ev_result = calc_ev_obj(ppr_df, player)
      metrics_data['Expected'].append(to_python_type(ev_result.get('expected_value')) )

      # Get Good Pass percentage from count_good_passes_obj function
      good_pass_result = count_good_passes_obj(ppr_df, player, 'pass')
      good_pass_value = good_pass_result.get('percent') 
      #print(f"Raw Good_Pass_Pct for {player}: {good_pass_value} Type {type(good_pass_value)}")
      metrics_data['Good_Pass_Pct'].append(to_python_type(good_pass_value))

      # Get Knockout Ratio from calc_knockout_obj function
      knockout_result = calc_knock_out_obj(ppr_df, player)
      metrics_data['Knockout'].append(to_python_type(knockout_result.get('knock_out_rate', 0)))

      # Get Ace Error Ratio
      ace_error_result = calc_ace_error_ratio_from_ppr(ppr_df, player)
      metrics_data['Ace/Error'].append(ace_error_result.get('ratio', 0)) 

      # Get Consistency in Errors from player_data_stats_df
      consistency_result = get_consistency_errors_from_stats(player_data_df, player, 'cons_ed_sd_match')
      metrics_data['Consistency_Errors'].append(to_python_type(consistency_result))

      # Get Error Density from calc_error_density_obj function
      error_density_result = calc_error_density_obj(ppr_df, player)
      error_density_value = error_density_result.get('error_density') 
      #print(f"Raw Error_Density for {player}: {error_density_value}, Type : {type(error_density_value)}")
      metrics_data['Error_Density'].append(parse_percentage(error_density_value, player, 'Error_Density'))

      #print(f"Metrics Data Row:{metrics_data}")

    except Exception as e:
      print(f"Error processing player {player}: {str(e)}")
      # Remove the player name we just added if error occurs
      if len(metrics_data['Player']) > len(metrics_data['FBHE']):
        metrics_data['Player'].pop()

    # Create the main metrics DataFrame
  metrics_df = pd.DataFrame(metrics_data)
  #print(metrics_df.dtypes)

  # Sort by player name
  metrics_df = metrics_df.sort_values('Player').reset_index(drop=True)

  # Add team summary row
  if not metrics_df.empty:
    summary_row = {'Player': 'TEAM AVERAGE'}

    # Calculate averages for numeric columns
    numeric_cols = ['FBHE', 'FBSO', 'TCR', 'ESO', 'Expected', 'Knockout', 'Ace/Error', 'Consistency_Errors']
    for col in numeric_cols:
      summary_row[col] = round(metrics_df[col].mean(), 3) if not metrics_df[col].empty else 0

      # Calculate averages for percentage columns
    for col in ['Good_Pass_Pct', 'Error_Density']:
      valid_values = [v for v in metrics_df[col] if pd.notnull(v)]
      summary_row[col] = round(sum(valid_values) / len(valid_values), 3) if valid_values else 0.0

      # Convert percentage columns back to string format for display
    summary_row['Good_Pass_Pct'] = f"{summary_row['Good_Pass_Pct'] * 100:.0f}%"
    summary_row['Error_Density'] = f"{summary_row['Error_Density'] * 100:.1f}%"
    
    # Add summary row
    summary_df = pd.DataFrame([summary_row])
    metrics_df = pd.concat([metrics_df, summary_df], ignore_index=True)

  # Top performers table (top 5 in each category)
  if len(metrics_df) > 6:  # More than just team average + 5 players
    top_performers = []
    categories = [
            ('FBHE', 'First Ball Hitting Efficiency'),
            ('FBSO', "First Ball Side Out"),
            ('TCR', 'Transition Conversion Rate'),
            ('ESO', 'Earned Side Out'),
            ('Knockout','Knockout Ratio'),
            #('Error_Density', 'Error Density')
            ('Expected', 'Expected Value')
        ]

    for metric, desc in categories:
      temp_df = metrics_df[metrics_df['Player'] != 'TEAM AVERAGE'].copy()
      top_player = temp_df.nlargest(1, metric)
      if not top_player.empty:
        # Format value for display
        # Convert value to Python type and format for display
        value = to_python_type(top_player[metric].iloc[0])
        #if metric in ['Good_Pass_Pct', 'Error_Density']:
        #  value = f"{value * 100:.0f}%" if metric == 'Good_Pass_Pct' else f"{value * 100:.1f}%"
        top_performers.append({
          'Category': desc,
          'Player': top_player['Player'].iloc[0],
          'Value': value
          })
  
    if top_performers:
      top_performers_df = pd.DataFrame(top_performers)
      df_list[1] = top_performers_df.to_dict('records')
      print(f"Top Performers: {top_performers}")

  # format the metrics df:
  # Create a mask for rows where Player is not 'TEAM AVERAGE'

  # After concatenating the summary row
  metrics_df['Good_Pass_Pct'] = metrics_df['Good_Pass_Pct'].apply(lambda x: float(x.strip('%')) / 100 if isinstance(x, str) else x)
  metrics_df['Error_Density'] = metrics_df['Error_Density'].apply(lambda x: float(x.strip('%')) / 100 if isinstance(x, str) else x)

  # Ensure float64 type
  metrics_df['Good_Pass_Pct'] = pd.to_numeric(metrics_df['Good_Pass_Pct'], errors='coerce').astype('float64')
  metrics_df['Error_Density'] = pd.to_numeric(metrics_df['Error_Density'], errors='coerce').astype('float64')

  # Verify types
  #print(metrics_df.dtypes)

  metrics_df['FBHE'] = metrics_df['FBHE'].round(3).astype(str)
  metrics_df['FBSO'] = metrics_df['FBSO'].round(3).astype(str)
  metrics_df['TCR'] = metrics_df['TCR'].round(3).astype(str)
  metrics_df['ESO'] = metrics_df['ESO'].round(3).astype(str)
  metrics_df['Expected'] = metrics_df['Expected'].round(3).astype(str)
  #metrics_df['Good_Pass_Pct'] = metrics_df['Good_Pass_Pct'].round(3)
  metrics_df['Good_Pass_Pct'] = metrics_df['Good_Pass_Pct'].apply(lambda x: f"{x * 100:02.0f}%" if pd.notnull(x) else x)
  metrics_df['Knockout'] = metrics_df['Knockout'].round(3).astype(str)
  metrics_df['Ace/Error'] = metrics_df['Ace/Error'].round(3).astype(str)
  metrics_df['Consistency_Errors'] = metrics_df['Consistency_Errors'].round(2).astype(str)
  #metrics_df['Error_Density'] = metrics_df['Error_Density'].round(3)
  metrics_df['Error_Density'] = metrics_df['Error_Density'].apply(lambda x: f"{x * 100:02.0f}%" if pd.notnull(x) else x)

  # change column names to fit into the display
  metrics_df = metrics_df.rename(columns={
    'Good_Pass_Pct': 'Good Pass',
    'Consistency_Errors': 'Consistency',
    'Error_Density': 'Error Density'
  })
  
      # Store the main metrics table
  df_list[0] = metrics_df.to_dict('records')
  #print(f"Metrics df:{metrics_data}")
  #df_desc_list[0] = f"Key Performance Metrics - Team {team}"

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

    


  
def get_team_players(disp_league, disp_gender, disp_year, team):
  """
    Get list of players for a specific team from master_player Anvil Data Table.
    """
  try:
    # Debug: Print input parameters to verify
    print(f"Parameters: league={disp_league}, gender={disp_gender}, year={disp_year}, team={team}")

    # Query the master_player Data Table
    rows = app_tables.master_player.search(
      league=disp_league,
      gender=disp_gender,
      year=disp_year,
      team=team  # Case-sensitive exact match
    )

    # Extract unique player names and sort
    player_names = sorted(set(row['team']+' '+row['number']+' '+row['shortname'] for row in rows))

    # Debug: Print results or indicate if empty
    if not player_names:
      print(f"No players found for team={team}, league={disp_league}, gender={disp_gender}, year={disp_year}")

    print(f"Player Names: {player_names}")
    return player_names

  except Exception as e:
    print(f"Error getting team players: {e}")
    return []

    

def calc_ace_error_ratio_from_ppr(ppr_df, player):
  """
    Calculate ace to error ratio from ppr_df data.
    """
  try:
    # Filter for serves by this player
    player_serves = ppr_df[ppr_df['serve_player'] == player]

    if player_serves.empty:
      return {'ratio': 0, 'aces': 0, 'errors': 0}

      # Count aces and service errors
    aces = len(player_serves[player_serves['point_outcome'] == 'TSA'])
    service_errors = len(player_serves[player_serves['point_outcome'] == 'TSE'])

    # Calculate ratio
    if service_errors > 0:
      ratio = aces / service_errors
    elif aces > 0:
      ratio = aces  # If no errors but has aces
    else:
      ratio = 0

    return {'ratio': ratio, 'aces': aces, 'errors': service_errors}
  except Exception as e:
    return {'ratio': 0, 'aces': 0, 'errors': 0}


def get_consistency_errors_from_stats(player_data_df, player, var):
  """
    Get consistency in errors from player_data_df for a specific player.

    Args:
        player_data_df (pandas.DataFrame): DataFrame containing player statistics
        player (str): Player identifier to match against the 'player' column
        var (str): Variable/column name (not used in current implementation)

    Returns:
        float: Consistency error value or 0 if not found or on error
    """
  try:
    # Check if DataFrame is not None and not empty
    if player_data_df is not None and not player_data_df.empty:
      # Check if required columns exist
      if 'player' in player_data_df.columns and 'cons_ed_sd_match' in player_data_df.columns:
        # Filter DataFrame where 'player' column matches the input player
        matching_row = player_data_df[player_data_df['player'] == player]
        # If a matching row is found, return the 'cons_ed_sd_match' value
        if not matching_row.empty:
          return float(matching_row['cons_ed_sd_match'].iloc[0])
      return 0
    return 0
  except Exception:
    return 0


#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------
#
#  Claude generated Player Dashboard
#
#---------------------------------------------------------------------------------------------
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import anvil.mpl_util
from server_functions import unpack_lgy, get_ppr_data, filter_ppr_df

"""
INTEGRATED BEACH VOLLEYBALL PLAYER ANALYSIS REPORT
Combines: Player Profiles + Run Analysis + Struggle Sets + Partner Dynamics
Compatible with existing report structure
"""

# ============================================
# HELPER FUNCTIONS - DATA PREPARATION
# ============================================

def reshape_to_player_level(ppr_df):
  """Convert from point-per-row to player-per-row format"""
  player_rows = []

  for _, row in ppr_df.iterrows():
    set_id = f"{row['video_id']}_set{row['set']}"
    point_winner_team = row.get('point_outcome_team', '')

    # Process home team (team a) players
    for player_id in [row['player_a1'], row['player_a2']]:
      if pd.isna(player_id):
        continue

      partner_id = row['player_a2'] if player_id == row['player_a1'] else row['player_a1']
      team_id = row['teama']
      team_scored = (point_winner_team == team_id)

      attacked = (row.get('att_player') == player_id)
      kill = False
      error = False
      if attacked:
        outcome = row.get('point_outcome', '')
        if outcome in ['FBK', 'TK']:
          kill = True
        elif outcome in ['FBE', 'TR']:
          error = True

      player_rows.append({
        'player_id': player_id,
        'partner_id': partner_id,
        'team_id': team_id,
        'opponent_team': row['teamb'],
        'set_id': set_id,
        'video_id': row['video_id'],
        'set_number': row['set'],
        'point_num': row.get('point_no', 0),
        'team_scored': team_scored,
        'player_score': row['a_score'],
        'opp_score': row['b_score'],
        'comp_l2': row.get('comp_l2', ''),
        'attacked': attacked,
        'kill': kill,
        'error': error,
        'attempt': attacked,
      })

      # Process away team (team b) players
    for player_id in [row['player_b1'], row['player_b2']]:
      if pd.isna(player_id):
        continue

      partner_id = row['player_b2'] if player_id == row['player_b1'] else row['player_b1']
      team_id = row['teamb']
      team_scored = (point_winner_team == team_id)

      attacked = (row.get('att_player') == player_id)
      kill = False
      error = False
      if attacked:
        outcome = row.get('point_outcome', '')
        if outcome in ['FBK', 'TK']:
          kill = True
        elif outcome in ['FBE', 'TR']:
          error = True

      player_rows.append({
        'player_id': player_id,
        'partner_id': partner_id,
        'team_id': team_id,
        'opponent_team': row['teama'],
        'set_id': set_id,
        'video_id': row['video_id'],
        'set_number': row['set'],
        'point_num': row.get('point_no', 0),
        'team_scored': team_scored,
        'player_score': row['b_score'],
        'opp_score': row['a_score'],
        'comp_l2': row.get('comp_l2', ''),
        'attacked': attacked,
        'kill': kill,
        'error': error,
        'attempt': attacked,
      })

  return pd.DataFrame(player_rows)


def calculate_runs(player_ppr):
  """Identify scoring runs and tag each point"""
  player_ppr = player_ppr.sort_values(['set_id', 'point_num']).copy()

  player_ppr['scored_change'] = (
    (player_ppr['team_scored'] != player_ppr['team_scored'].shift()) |
    (player_ppr['set_id'] != player_ppr['set_id'].shift())
  )
  player_ppr['run_id'] = player_ppr['scored_change'].cumsum()
  player_ppr['run_length'] = player_ppr.groupby('run_id').cumcount() + 1

  player_ppr['opp_run_length'] = np.where(
    ~player_ppr['team_scored'],
    player_ppr['run_length'],
    0
  )

  return player_ppr


def calculate_set_stats(player_ppr):
  """Calculate per-set statistics for each player"""
  set_stats = player_ppr.groupby(['player_id', 'partner_id', 'set_id', 'opponent_team', 'comp_l2']).agg({
    'kill': 'sum',
    'error': 'sum',
    'attempt': 'sum',
    'team_scored': 'sum',
    'point_num': 'max',
    'set_number': 'first',
    'video_id': 'first',
  }).reset_index()

  set_stats.columns = ['player_id', 'partner_id', 'set_id', 'opponent_team', 'comp_l2',
                       'kills', 'errors', 'attempts', 'team_points', 'total_points', 
                       'set_number', 'video_id']

  set_stats['efficiency'] = np.where(
    set_stats['attempts'] > 0,
    (set_stats['kills'] - set_stats['errors']) / set_stats['attempts'],
    np.nan
  )

  return set_stats


# ============================================
# ANALYSIS FUNCTIONS
# ============================================

def calculate_player_profiles(player_ppr, set_stats, min_attempts=30):
  """Calculate core player profile metrics"""
  profiles = []

  for player_id in set_stats['player_id'].unique():
    player_sets = set_stats[set_stats['player_id'] == player_id].copy()
    player_points = player_ppr[player_ppr['player_id'] == player_id].copy()
    player_attacks = player_sets[player_sets['attempts'] > 0]

    if len(player_attacks) < 3 or player_sets['attempts'].sum() < min_attempts:
      continue

      # BASELINE EFFICIENCY
    total_kills = player_sets['kills'].sum()
    total_errors = player_sets['errors'].sum()
    total_attempts = player_sets['attempts'].sum()
    baseline_eff = (total_kills - total_errors) / total_attempts if total_attempts > 0 else 0

    # RELIABILITY
    cv = player_attacks['efficiency'].std() / baseline_eff if baseline_eff > 0 else 0

    # Recalibrated for beach volleyball (typical CV range: 0.45 to 1.05)
    # CV of 0.45 = score of 10 (most consistent)
    # CV of 1.05 = score of 1 (least consistent)
    reliability_score = max(1, min(10, 10 - (cv - 0.45) * 15))

    # RESILIENCE
    after_run_attacks = player_points[(player_points['opp_run_length'] >= 3) & player_points['attacked']]
    if len(after_run_attacks) >= 10:
      after_run_eff = (after_run_attacks['kill'].sum() - after_run_attacks['error'].sum()) / len(after_run_attacks)
      resilience_ratio = after_run_eff / baseline_eff if baseline_eff > 0 else 1
    else:
      resilience_ratio = 1.0
    resilience_score = max(1, min(10, (resilience_ratio - 0.40) * 12))

    # CLUTCH
    player_points['score_total'] = player_points['player_score'] + player_points['opp_score']
    player_points['score_diff'] = abs(player_points['player_score'] - player_points['opp_score'])
    player_points['late_close'] = (player_points['score_total'] >= 16) & (player_points['score_diff'] <= 2)

    clutch_attacks = player_points[player_points['late_close'] & player_points['attacked']]
    if len(clutch_attacks) >= 10:
      clutch_eff = (clutch_attacks['kill'].sum() - clutch_attacks['error'].sum()) / len(clutch_attacks)
      clutch_delta = clutch_eff - baseline_eff
    else:
      clutch_delta = 0
    clutch_score = max(1, min(10, 5.5 + clutch_delta * 25))

    # VS RANKED
    vs_top20 = player_sets[player_sets['comp_l2'].str.contains('Top 20', na=False)]
    if len(vs_top20) >= 3 and vs_top20['attempts'].sum() >= 20:
      top20_eff = (vs_top20['kills'].sum() - vs_top20['errors'].sum()) / vs_top20['attempts'].sum()
    else:
      top20_eff = baseline_eff

      # STRUGGLE SETS - Calculate and store for use in detailed metrics
    player_baseline = player_attacks['efficiency'].mean()
    player_std = player_attacks['efficiency'].std()
    struggle_threshold = player_baseline - player_std

    # Count struggle sets (efficiency < mean - 1 std dev)
    # This MUST match analyze_struggle_triggers() logic exactly
    struggle_sets_list = player_attacks[player_attacks['efficiency'] < struggle_threshold]
    struggle_rate = len(struggle_sets_list) / len(player_attacks) if len(player_attacks) > 0 else 0

    # DEBUG - enable temporarily to compare
    if player_id.startswith('FSU'):  # Only for your team
      print(f"PROFILE: {player_id}: Total={len(player_attacks)}, Struggles={len(struggle_sets_list)}, Rate={struggle_rate:.3f}")

    note = ""
    if top20_eff < baseline_eff - 0.12:
      note = "vs Ranked ↓"

    profiles.append({
      'Player': player_id,
      'Eff%': f"{baseline_eff:.1%}",
      'Reliability': round(reliability_score, 1),
      'Clutch': round(clutch_score, 1),
      'Resilience': round(resilience_score, 1),
      'Note': note,
      'attempts': int(total_attempts),
      'eff_numeric': baseline_eff,
      'cv': cv,
      'struggle_rate': struggle_rate,
      'vs_top20_eff': top20_eff,
    })

  return pd.DataFrame(profiles)


def analyze_partner_dynamics(set_stats):
  """Calculate how each player performs with different partners"""
  partner_results = []
  processed_pairs = set()

  for player_id in set_stats['player_id'].unique():
    player_sets = set_stats[set_stats['player_id'] == player_id].copy()
    overall_sets = player_sets[player_sets['attempts'] > 0]

    if len(overall_sets) < 3:
      continue

    overall_eff = (overall_sets['kills'].sum() - overall_sets['errors'].sum()) / overall_sets['attempts'].sum()

    for partner_id in player_sets['partner_id'].unique():
      pair_key = tuple(sorted([player_id, partner_id]))
      if pair_key in processed_pairs:
        continue
      processed_pairs.add(pair_key)

      with_partner = player_sets[player_sets['partner_id'] == partner_id]
      with_partner_attacks = with_partner[with_partner['attempts'] > 0]

      if len(with_partner_attacks) < 3:
        continue

      partner_eff = (with_partner_attacks['kills'].sum() - with_partner_attacks['errors'].sum()) / with_partner_attacks['attempts'].sum()
      partner_delta = partner_eff - overall_eff

      partner_results.append({
        'Player': player_id,
        'Partner': partner_id,
        'Sets': len(with_partner),
        'Efficiency': f"{partner_eff:.1%}",
        'vs Baseline': f"{partner_delta:+.1%}",
        'delta_numeric': partner_delta,
        'eff_numeric': partner_eff,
      })

  df = pd.DataFrame(partner_results)
  if len(df) > 0:
    df = df.sort_values(['Player', 'delta_numeric'], ascending=[True, False])
  return df


def analyze_struggle_triggers(player_ppr, set_stats):
  """Identify what triggers struggle sets"""

  struggle_analysis = []

  for player_id in set_stats['player_id'].unique():
    player_sets = set_stats[set_stats['player_id'] == player_id]
    player_attacks = player_sets[player_sets['attempts'] > 0]  # Only sets with attacks

    # Minimum 3 sets to analyze
    total_sets = len(player_attacks)
    if total_sets < 3:
      continue

      # Calculate baseline and threshold - SAME as calculate_player_profiles()
    baseline = player_attacks['efficiency'].mean()
    std = player_attacks['efficiency'].std()
    threshold = baseline - std

    # Count struggle sets - SAME logic as calculate_player_profiles()
    struggle_sets = player_attacks[player_attacks['efficiency'] < threshold]
    n_struggles = len(struggle_sets)

    # Calculate struggle rate
    struggle_rate_value = n_struggles / total_sets if total_sets > 0 else 0

    # DEBUG - enable temporarily to compare
    if player_id.startswith('FSU'):
      print(f"TRIGGER: {player_id}: Total={total_sets}, Struggles={n_struggles}, Rate={struggle_rate_value:.3f}")

      # Calculate triggers only if there are struggle sets
    if n_struggles > 0:
      top20_struggle_rate = struggle_sets['comp_l2'].str.contains('Top 20', na=False).sum() / n_struggles

      set3_struggles = len(struggle_sets[struggle_sets['set_number'] == 3])
      set3_total = len(player_attacks[player_attacks['set_number'] == 3])
      set3_struggle_rate = set3_struggles / set3_total if set3_total > 0 else 0

      struggle_set_ids = struggle_sets['set_id'].tolist()
      struggle_points = player_ppr[(player_ppr['player_id'] == player_id) & 
        (player_ppr['set_id'].isin(struggle_set_ids))]
      avg_max_opp_run = struggle_points.groupby('set_id')['opp_run_length'].max().mean() if len(struggle_points) > 0 else 0
    else:
      # No struggle sets
      top20_struggle_rate = 0
      set3_struggle_rate = 0
      avg_max_opp_run = 0

    struggle_analysis.append({
      'Player': player_id,
      'Total Sets': total_sets,
      'Struggle Sets': n_struggles,
      'Struggle Rate': f"{struggle_rate_value:.1%}",
      'vs Top 20 Trigger': f"{top20_struggle_rate:.1%}",
      'Set 3 Trigger': f"{set3_struggle_rate:.1%}",
      'Avg Max Opp Run': f"{avg_max_opp_run:.1f}",
    })

  return pd.DataFrame(struggle_analysis)


# ============================================
# VISUALIZATION FUNCTIONS
# ============================================

def create_profile_plot(profiles_df):
  """Create scatter plot"""
  if len(profiles_df) == 0:
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center')
    return fig

  fig, ax = plt.subplots(figsize=(12, 10))

  scatter = ax.scatter(
    profiles_df['Reliability'],
    profiles_df['Clutch'],
    s=profiles_df['eff_numeric'] * 3000,
    c=profiles_df['Resilience'],
    cmap='RdYlGn',
    alpha=0.6,
    edgecolors='black',
    linewidth=1.5,
    vmin=1,
    vmax=10
  )

  for idx, row in profiles_df.iterrows():
    name = str(row['Player'])[:15]
    ax.annotate(name, (row['Reliability'], row['Clutch']),
                xytext=(5, 5), textcoords='offset points',
                fontsize=8, fontweight='bold')

  ax.axhline(y=5.5, color='gray', linestyle='--', alpha=0.3)
  ax.axvline(x=5.5, color='gray', linestyle='--', alpha=0.3)

  ax.text(8.5, 9, 'STARS', ha='center', fontsize=10,
          bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))
  ax.text(2.5, 9, 'HIGH VARIANCE', ha='center', fontsize=10,
          bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3))
  ax.text(8.5, 2, 'GRINDERS', ha='center', fontsize=10,
          bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))
  ax.text(2.5, 2, 'DEVELOPMENT', ha='center', fontsize=10,
          bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.3))

  ax.set_xlabel('Reliability', fontsize=12, fontweight='bold')
  ax.set_ylabel('Clutch Factor', fontsize=12, fontweight='bold')
  ax.set_title('Player Profile Matrix', fontsize=14, fontweight='bold')
  ax.set_xlim(0, 11)
  ax.set_ylim(0, 11)

  plt.colorbar(scatter, ax=ax, label='Resilience')
  plt.tight_layout()
  return fig


def create_partner_heatmap(partner_df):
  """Create heatmap of partner chemistry"""
  if len(partner_df) == 0:
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center')
    return fig

  matrix = partner_df.pivot_table(
    index='Player',
    columns='Partner',
    values='delta_numeric',
    aggfunc='mean'
  )

  if matrix.empty or matrix.shape[0] < 2:
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.text(0.5, 0.5, 'Insufficient partnerships', ha='center', va='center')
    return fig

  fig, ax = plt.subplots(figsize=(10, 8))

  sns.heatmap(matrix, annot=True, fmt='.1%', cmap='RdYlGn', center=0,
              vmin=-0.15, vmax=0.15,
              cbar_kws={'label': 'Efficiency Delta'}, 
              linewidths=0.5,  # Add gridlines
              linecolor='gray',  # Gridline color
              ax=ax)

  ax.set_title('Partnership Chemistry Matrix', fontsize=14, fontweight='bold')
  ax.set_xlabel('Partner', fontsize=11)
  ax.set_ylabel('Player', fontsize=11)

  plt.tight_layout()
  return fig


# ============================================
# MAIN INTERNAL FUNCTION
# ============================================

def _report_integrated_player_profile_internal(lgy, team, **rpt_filters):
  """Internal function - use report_team_profile() or report_player_profile()"""

  # Initialize outputs
  #title_list = ['','','','','','','','','','']
  #label_list = ['','','','','','','','','','']
  #df_desc_list = ['','','','','','','','','','']
  #image_desc_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team, function_name='report_team_profile')

  log_debug(f"Title List from setup_report_basic {title_list} \nLabel_list: {label_list}")
  try:
    # Get data
    disp_league, disp_gender, disp_year = unpack_lgy(lgy)
    ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
    ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

    # Handle player filter
    disp_player = rpt_filters.get('player', '')
    if disp_player and disp_player.strip() != '':
      ppr_df = ppr_df[
        (ppr_df['player_a1'] == disp_player) | 
        (ppr_df['player_a2'] == disp_player) |
        (ppr_df['player_b1'] == disp_player) |
        (ppr_df['player_b2'] == disp_player)
        ]
      team_prefix = disp_player.split()[0] if ' ' in disp_player else team
    else:
      disp_player = None
      team_prefix = team

    if len(ppr_df) == 0:
      df_list[0] = pd.DataFrame({'Message': ['No data available']}).to_dict('records')
      return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

      # Process data
    player_ppr = reshape_to_player_level(ppr_df)
    player_ppr = player_ppr[player_ppr['player_id'].str.startswith(team_prefix, na=False)]

    if len(player_ppr) == 0:
      df_list[0] = pd.DataFrame({'Message': [f'No {team_prefix} players']}).to_dict('records')
      return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

    player_ppr = calculate_runs(player_ppr)
    set_stats = calculate_set_stats(player_ppr)

    # Analyze
    profiles_df = calculate_player_profiles(player_ppr, set_stats)
    partner_df = analyze_partner_dynamics(set_stats)

    # Make a copy of set_stats before passing to struggle analysis to avoid modification issues
    set_stats_copy = set_stats.copy()
    struggle_df = analyze_struggle_triggers(player_ppr, set_stats_copy)

    # DEBUG - remove after fixing
    print(f"Number of players in profiles: {len(profiles_df)}")
    print(f"Number of players in struggle_df: {len(struggle_df)}")
    if len(struggle_df) > 0:
      print(f"Struggle df columns: {struggle_df.columns.tolist()}")
      print(f"First few rows:\n{struggle_df.head()}")

      # Store dataframes
    if len(profiles_df) > 0:
      display_profiles = profiles_df[['Player', 'Eff%', 'Reliability', 'Clutch', 'Resilience', 'Note']].copy()
      df_list[0] = display_profiles.to_dict('records')
    else:
      df_list[0] = pd.DataFrame({'Message': ['No players with sufficient data']}).to_dict('records')

    if len(partner_df) > 0:
      display_partners = partner_df[['Player', 'Partner', 'Sets', 'Efficiency', 'vs Baseline']].copy()
      df_list[1] = display_partners.to_dict('records')  # All partnerships
    else:
      df_list[1] = pd.DataFrame({'Message': ['No partner data']}).to_dict('records')

    if len(struggle_df) > 0:
      df_list[2] = struggle_df.to_dict('records')
    else:
      df_list[2] = pd.DataFrame({'Message': ['No struggle data']}).to_dict('records')

    if len(profiles_df) > 0:
      detailed = profiles_df[['Player', 'Eff%', 'attempts', 'struggle_rate', 'vs_top20_eff']].copy()
      detailed['Struggle Rate'] = detailed['struggle_rate'].apply(lambda x: f"{x:.1%}")
      detailed['vs Top 20'] = detailed['vs_top20_eff'].apply(lambda x: f"{x:.1%}")
      detailed = detailed[['Player', 'Eff%', 'attempts', 'Struggle Rate', 'vs Top 20']]
      detailed.columns = ['Player', 'Efficiency', 'Attempts', 'Struggle Rate', 'vs Top 20']
      df_list[3] = detailed.to_dict('records')
    else:
      df_list[3] = pd.DataFrame({'Message': ['No detailed metrics']}).to_dict('records')

      # Create visualizations
    if len(profiles_df) > 0:
      try:
        create_profile_plot(profiles_df)
        image_list[0] = anvil.mpl_util.plot_image()
        plt.clf()
      except Exception as e:
        print(f"Plot error: {e}")

    if len(partner_df) > 0:
      try:
        create_partner_heatmap(partner_df)
        image_list[1] = anvil.mpl_util.plot_image()
        plt.clf()
      except Exception as e:
        print(f"Heatmap error: {e}")

  except Exception as e:
    import traceback
    print(f"Error: {e}\n{traceback.format_exc()}")
    df_list[0] = pd.DataFrame({'Error': [str(e)]}).to_dict('records')

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


# ============================================
# PUBLIC REPORT FUNCTIONS
# ============================================

def report_team_profile(lgy, team, **rpt_filters):
  """Team-Wide Player Profile Report - analyzes all players on team"""
  rpt_filters['player'] = ''
  return _report_integrated_player_profile_internal(lgy, team, **rpt_filters)


def report_player_profile(lgy, team, **rpt_filters):
  """Individual Player Profile Report - must include 'player' in rpt_filters"""
  if 'player' not in rpt_filters or not rpt_filters['player'] or rpt_filters['player'].strip() == '':
    title_list = ['','','','','','','','','','']
    label_list = ['','','','','','','','','','']
    df_desc_list = ['','','','','','','','','','']
    image_desc_list = ['','','','','','','','','','']
    image_list = ['','','','','','','','','','']
    df_list = ['','','','','','','','','','']
    df_list[0] = pd.DataFrame({'Error': ['player required in rpt_filters']}).to_dict('records')
    return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

  return _report_integrated_player_profile_internal(lgy, team, **rpt_filters)


def report_integrated_player_profile(lgy, team, **rpt_filters):
  """Legacy function - auto-detects team vs player report"""
  player = rpt_filters.get('player', '')
  if player and player.strip() != '':
    return report_player_profile(lgy, team, **rpt_filters)
  else:
    return report_team_profile(lgy, team, **rpt_filters)

    
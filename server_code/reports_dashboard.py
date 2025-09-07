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
from server_functions import *
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
    print(f"Processing player: {player}")
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
      #print(f"Raw Good_Pass_Pct for {player}: {good_pass_value}")
      metrics_data['Good_Pass_Pct'].append(to_python_type(good_pass_value))

      # Get Knockout Ratio from calc_knockout_obj function
      knockout_result = calc_knock_out_obj(ppr_df, player)
      metrics_data['Knockout'].append(to_python_type(knockout_result.get('ratio', 0)))

      # Get Ace Error Ratio
      ace_error_result = calc_ace_error_ratio_from_ppr(ppr_df, player)
      metrics_data['Ace/Error'].append(ace_error_result.get('ratio', 0)) 

      # Get Consistency in Errors from player_data_stats_df
      consistency_result = get_consistency_errors_from_stats(player_data_df, player, 'cons_ed_sd_match')
      metrics_data['Consistency_Errors'].append(to_python_type(consistency_result))

      # Get Error Density from calc_error_density_obj function
      error_density_result = calc_error_density_obj(ppr_df, player)
      error_density_value = error_density_result.get('error_density') 
      #print(f"Raw Error_Density for {player}: {error_density_value}")
      metrics_data['Error_Density'].append(parse_percentage(error_density_value, player, 'Error_Density'))

      #print(f"Metrics Data Row:{metrics_data}")

    except Exception as e:
      print(f"Error processing player {player}: {str(e)}")
      # Remove the player name we just added if error occurs
      if len(metrics_data['Player']) > len(metrics_data['FBHE']):
        metrics_data['Player'].pop()

    # Create the main metrics DataFrame
  metrics_df = pd.DataFrame(metrics_data)

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
  mask = metrics_df['Player'] != 'TEAM AVERAGE'

  # Convert the object columns to float64 only for rows where the mask is True
  metrics_df.loc[mask, 'Good_Pass_Pct'] = pd.to_numeric(metrics_df.loc[mask, 'Good_Pass_Pct'], errors='coerce').astype('float64')
  metrics_df.loc[mask, 'Error_Density'] = pd.to_numeric(metrics_df.loc[mask, 'Error_Density'], errors='coerce').astype('float64')
  
  print(metrics_df.dtypes)
  metrics_df['FBHE'] = metrics_df['FBHE'].round(3).astype(str)
  metrics_df['FBSO'] = metrics_df['FBSO'].round(3).astype(str)
  metrics_df['TCR'] = metrics_df['TCR'].round(3).astype(str)
  metrics_df['ESO'] = metrics_df['ESO'].round(3).astype(str)
  metrics_df['Expected'] = metrics_df['Expected'].round(3).astype(str)
  metrics_df['Good_Pass_Pct'] = metrics_df['Good_Pass_Pct'].round(3)
  metrics_df['Knockout'] = metrics_df['Knockout'].round(3).astype(str)
  metrics_df['Ace/Error'] = metrics_df['Ace/Error'].round(3).astype(str)
  metrics_df['Consistency_Errors'] = metrics_df['Consistency_Errors'].round(3).astype(str)
  metrics_df['Error_Density'] = metrics_df['Error_Density'].round(3)

  
      # Store the main metrics table
  df_list[0] = metrics_df.to_dict('records')
  print(f"Metrics df:{metrics_data}")
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

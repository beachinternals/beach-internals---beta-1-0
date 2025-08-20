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
from matchup_reports import player_45_serves
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
        **rpt_filters: Additional report filters (should include 'team' filter)
        
    Returns:
        tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
    """
  # Extract team from rpt_filters - this is required for this report
  #team = rpt_filters.get('team')
  #if not team:
  #  raise ValueError("Team filter is required for dashboard key metrics report")

    # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Filter the ppr dataframe with any additional filters
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # Dashboard Key Metrics for All Team Players
  # =============================================================================

  # Get list of all players for this team from master_player or from ppr_df
  team_players = get_team_players(disp_league, disp_gender, disp_year, team)

  if not team_players:
    # Fallback: get unique players from ppr_df
    team_players = set()
    for col in ['player_a1', 'player_a2', 'player_b1', 'player_b2']:
      team_players.update(ppr_df[col].dropna().unique())
    team_players = list(team_players)

  print(f"Generating dashboard for {len(team_players)} players on team {team}")

  # Initialize comprehensive metrics dataframe
  metrics_data = {
    'Player': [],
    'FBHE': [],           # First Ball Hitting Efficiency
    'FBSO': [],           # First Ball Side Out
    'TCR': [],            # Transition Conversion Rate
    'ESO': [],            # Expected Side Out
    'Expected_Value': [], # Expected Value
    'Good_Pass_Pct': [],  # Good Pass Percentage
    'Knockout_Ratio': [], # Knockout Ratio
    'Ace_Error_Ratio': [], # Ace to Error Ratio
    'Consistency_Errors': [], # Consistency in Errors
    'Error_Density': []   # Error Density
  }

  # Calculate metrics for each player
  for player in team_players:
    print(f"Processing player: {player}")

    try:
      # Add player name
      metrics_data['Player'].append(player)

      # Get FBHE and FBSO from fbhe_obj function
      fbhe_result = fbhe_obj(ppr_df, player)
      metrics_data['FBHE'].append(fbhe_result.get('fbhe', 0) if fbhe_result else 0)
      metrics_data['FBSO'].append(fbhe_result.get('fbso', 0) if fbhe_result else 0)

      # Get TCR from calc_trans_obj function
      tcr_result = calc_trans_obj(ppr_df, player)
      metrics_data['TCR'].append(tcr_result.get('tcr', 0) if tcr_result else 0)

      # Get ESO from calc_eso function
      eso_result = calc_eso(ppr_df, player)
      if isinstance(eso_result, dict):
        metrics_data['ESO'].append(eso_result.get('eso', 0))
      else:
        metrics_data['ESO'].append(eso_result if eso_result else 0)

        # Get Expected Value from calc_ev_obj function
      ev_result = calc_ev_obj(ppr_df, player)
      metrics_data['Expected_Value'].append(ev_result.get('expected_value', 0) if ev_result else 0)

      # Get Good Pass percentage from count_good_passes_obj function
      good_pass_result = count_good_passes_obj(ppr_df, player, 'pass')
      metrics_data['Good_Pass_Pct'].append(good_pass_result.get('percent_str', '0%') if good_pass_result else '0%')

      # Get Knockout Ratio from calc_knockout_obj function
      knockout_result = calc_knockout_obj(ppr_df, player)
      metrics_data['Knockout_Ratio'].append(knockout_result.get('ratio', 0) if knockout_result else 0)

      # Get Ace Error Ratio
      ace_error_result = calc_ace_error_ratio_from_ppr(ppr_df, player)
      metrics_data['Ace_Error_Ratio'].append(ace_error_result.get('ratio', 0) if ace_error_result else 0)

      # Get Consistency in Errors from player_data_stats_df
      consistency_result = get_consistency_errors_from_stats(player_data_stats_df, player)
      metrics_data['Consistency_Errors'].append(consistency_result if consistency_result else 0)

      # Get Error Density from calc_error_density_obj function
      error_density_result = calc_error_density_obj(ppr_df, player)
      metrics_data['Error_Density'].append(error_density_result.get('error_density', '0.00%') if error_density_result else '0.00%')

    except Exception as e:
      print(f"Error processing player {player}: {str(e)}")
      # Fill with default values if error occurs
      if len(metrics_data['Player']) > len(metrics_data['FBHE']):
        # Remove the player name we just added
        metrics_data['Player'].pop()

    # Create the main metrics DataFrame

  metrics_df = pd.DataFrame(metrics_data)

  # Sort by player name
  metrics_df = metrics_df.sort_values('Player').reset_index(drop=True)

  # Add team summary row
  if len(metrics_df) > 0:
    summary_row = {'Player': 'TEAM AVERAGE'}

    # Calculate averages for numeric columns
    numeric_cols = ['FBHE', 'FBSO', 'TCR', 'ESO', 'Expected_Value', 'Knockout_Ratio', 'Ace_Error_Ratio', 'Consistency_Errors']
    for col in numeric_cols:
      summary_row[col] = round(metrics_df[col].mean(), 3)

      # Handle percentage string columns
    good_pass_pcts = []
    error_densities = []

    for pct in metrics_df['Good_Pass_Pct']:
      try:
        if '%' in str(pct):
          good_pass_pcts.append(float(str(pct).replace('%', '')))
      except:
        pass

    for ed in metrics_df['Error_Density']:
      try:
        if '%' in str(ed):
          error_densities.append(float(str(ed).replace('%', '')))
      except:
        pass

    summary_row['Good_Pass_Pct'] = f"{round(sum(good_pass_pcts) / len(good_pass_pcts), 0):.0f}%" if good_pass_pcts else '0%'
    summary_row['Error_Density'] = f"{round(sum(error_densities) / len(error_densities), 1):.1f}%" if error_densities else '0.0%'

    # Add summary row
    summary_df = pd.DataFrame([summary_row])
    metrics_df = pd.concat([metrics_df, summary_df], ignore_index=True)

    # Store the main metrics table
  df_list[0] = metrics_df
  df_desc_list[0] = f"Key Performance Metrics - Team {team}"

  # Create additional summary tables/charts if needed

  # Top performers table (top 5 in each category)
  if len(metrics_df) > 6:  # More than just team average + 5 players
    top_performers = []
    categories = [
      ('FBHE', 'First Ball Hitting Efficiency'),
      ('Good_Pass_Pct', 'Good Pass Percentage'), 
      ('TCR', 'Transition Conversion Rate'),
      ('ESO', 'Expected Side Out')
    ]

    for metric, desc in categories:
      if metric in ['Good_Pass_Pct']:
        # Handle percentage strings
        temp_df = metrics_df[metrics_df['Player'] != 'TEAM AVERAGE'].copy()
        temp_df['sort_val'] = temp_df[metric].str.replace('%', '').astype(float)
        top_player = temp_df.nlargest(1, 'sort_val')
      else:
        # Handle numeric columns
        temp_df = metrics_df[metrics_df['Player'] != 'TEAM AVERAGE']
        top_player = temp_df.nlargest(1, metric)

      if not top_player.empty:
        top_performers.append({
          'Category': desc,
          'Player': top_player['Player'].iloc[0],
          'Value': top_player[metric].iloc[0]
        })

    if top_performers:
      top_performers_df = pd.DataFrame(top_performers)
      df_list[1] = top_performers_df
      df_desc_list[1] = "Top Performers by Category"

    # Update titles and labels
  title_list[0] = f"Team {team} - Key Metrics Dashboard"
  label_list[0] = f"Comprehensive performance metrics for all players"

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def get_team_players(disp_league, disp_gender, disp_year, team):
  """
    Get list of players for a specific team from master_player table.
    """
  try:
    import pandas as pd
    query = f"""
        SELECT DISTINCT player_name 
        FROM master_player 
        WHERE league = '{disp_league}' 
        AND gender = '{disp_gender}' 
        AND year = '{disp_year}' 
        AND team = '{team}'
        ORDER BY player_name
        """

    result_df = pd.read_sql(query, con)
    return result_df['player_name'].tolist() if not result_df.empty else []
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
    aces = len(player_serves[player_serves['point_outcome'] == 'ACE'])
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


def get_consistency_errors_from_stats(player_data_stats_df, player):
  """
    Get consistency in errors from player_data_stats_df.
    """
  try:
    # Access the specific field as mentioned: player_data_stats_df.at['0','cons_ed_sd_match']
    # This might need adjustment based on how your DataFrame is indexed
    if not player_data_stats_df.empty:
      # If indexed by player name
      if player in player_data_stats_df.index:
        return player_data_stats_df.at[player, 'cons_ed_sd_match']
        # If using row index '0' as specified
      elif 'cons_ed_sd_match' in player_data_stats_df.columns:
        return player_data_stats_df.at[0, 'cons_ed_sd_match']

    return 0
  except Exception as e:
    return 0

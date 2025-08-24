import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from pair_functions import *
import scipy.stats as stats
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import math
from plot_functions import *
import numpy as np
from server_functions import *




def report_scouting_overview(lgy, team, **rpt_filters):
  """
  Scouting overview report function - provides matchup analysis between two pairs.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters including:
      - pair_a: First pair for matchup
      - pair_b: Second pair for matchup
    
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

  # Get required filter parameters
  pair_a = rpt_filters.get('pair_a')
  pair_b = rpt_filters.get('pair_b')

  # Validate that both pairs are provided
  if not pair_a or not pair_b:
    # Return empty report if pairs not specified
    return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # Generate matchup analysis using the existing matchup functions
  # =============================================================================

  try:
    # Generate matchup outcome dataframe
    matchup_outcome_df = matchup_outcome_df(disp_league, disp_gender, disp_year, pair_a, pair_b, team)

    # Check if matchup data was found successfully
    if isinstance(matchup_outcome_df, str):
      # Error occurred - pair not found
      df_list[0] = matchup_outcome_df  # Store error message
      df_desc_list[0] = "Error: Pair Data Not Found"
    else:
      # Convert to markdown and store
      df_list[0] = pd.DataFrame.to_markdown(matchup_outcome_df, index=False)
      df_desc_list[0] = f"Matchup Outcome Analysis: {pair_a} vs {pair_b}"

    # Generate net matchup analysis
    matchup_net_df = matchup_net(disp_league, disp_gender, disp_year, pair_a, pair_b, team)

    # Check if net matchup data was found successfully
    if isinstance(matchup_net_df, str):
      # Error occurred - pair not found
      df_list[1] = matchup_net_df.to_dict("records")  # Store error message
      df_desc_list[1] = "Error: Net Matchup Data Not Found"
    else:
      # Convert to markdown and store
      df_list[1] = pd.DataFrame.to_markdown(matchup_net_df, index=False)
      df_desc_list[1] = f"Net Position Matchup Analysis: {pair_a} vs {pair_b}"

    # Generate 45-degree serves analysis
    matchup_45_serves_df = matchup_45_serves(disp_league, disp_gender, disp_year, pair_a, pair_b, team)

    # Check if 45 serves data was found successfully
    if isinstance(matchup_45_serves_df, str):
      # Error occurred
      df_list[2] = matchup_45_serves_df.to_dict("records")  # Store error message
      df_desc_list[2] = "Error: 45-Degree Serves Data Not Found"
    else:
      # Convert to markdown and store (top 15 results)
      df_list[2] = pd.DataFrame.to_markdown(matchup_45_serves_df.head(15), index=False)
      df_desc_list[2] = f"45-Degree Serves Analysis (Top 15): {pair_a} vs {pair_b}"

      # Also create FBHE sorted version
      if len(matchup_45_serves_df) > 0:
        matchup_45_serves_fbhe_df = matchup_45_serves_df.sort_values(by='fbhe', ascending=True)
        df_list[3] = pd.DataFrame.to_markdown(matchup_45_serves_fbhe_df.head(15), index=False)
        df_desc_list[3] = f"45-Degree Serves Analysis by FBHE (Top 15): {pair_a} vs {pair_b}"

    # Update title and labels for the specific matchup
    if title_list and len(title_list) > 0:
      title_list[0] = f"Scouting Overview: {pair_a} vs {pair_b}"
    if label_list and len(label_list) > 0:
      label_list[0] = f"Matchup Analysis Report"
      if len(label_list) > 1:
        label_list[1] = f"League: {disp_league}, Gender: {disp_gender}, Year: {disp_year}"
      if len(label_list) > 2:
        label_list[2] = f"Team: {team}"

  except Exception as e:
    # Handle any unexpected errors
    df_list[0] = f"Error generating scouting report: {str(e)}"
    df_desc_list[0] = "Report Generation Error"

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

def matchup_outcome_df(disp_league, disp_gender, disp_year, pair_a, pair_b, disp_team):
  """
    Generate a comprehensive matchup analysis DataFrame for two pairs.
    
    This function creates a detailed comparison matrix showing serving statistics,
    expected performance, and predicted outcomes for each player in both pairs.
    
    Args:
        disp_league (str): League identifier
        disp_gender (str): Gender category (e.g., 'M', 'F')
        disp_year (str): Year of competition
        pair_a (str): First pair identifier (format: "Player1/Player2")
        pair_b (str): Second pair identifier (format: "Player1/Player2")
        disp_team (str): Team identifier for filtering
    
    Returns:
        pd.DataFrame: Matchup analysis matrix with columns for each player and rows for:
            - Ace %: Service ace percentage
            - Err %: Service error percentage  
            - Expected: Expected win probability
            - Points A Serving: Points scored when Pair A serves
            - Points B Serving: Points scored when Pair B serves
            - Point Ratio: Overall point scoring ratio
            - Predicted Score: Predicted final score (scaled to 21)
        
        str: Error message if pair data cannot be found
    
    Raises:
        ValueError: If input parameters are invalid
        KeyError: If required data columns are missing
    """

  # Input validation
  if not all([disp_league, disp_gender, disp_year, pair_a, pair_b]):
    raise ValueError("All league, gender, year, and pair parameters must be provided")

    # Fetch matchup data for both pairs
  try:
    a1_matchup, a2_matchup, b1_matchup, b2_matchup = matchup_pair_data(
      disp_league, disp_gender, disp_year, pair_a, pair_b, disp_team
    )
  except Exception as e:
    return f"Error fetching matchup data: {str(e)}"

    # Check if pair data was found
  if isinstance(a1_matchup, str):
    print(f"matchup_outcome_df error: {a1_matchup}")
    return a1_matchup

    # Extract individual player names
  try:
    player_a1, player_a2 = pair_players(pair_a)
    player_b1, player_b2 = pair_players(pair_b)
  except Exception as e:
    return f"Error parsing player names: {str(e)}"

    # Define the structure of the matchup analysis table
  METRIC_LABELS = [
    'Ace %',           # Row 0: Service ace percentage
    'Err %',           # Row 1: Service error percentage
    'Expected',        # Row 2: Expected win probability
    'Points A Serving', # Row 3: Points when Pair A serves
    'Points B Serving', # Row 4: Points when Pair B serves
    'Point Ratio',     # Row 5: Overall point ratio
    'Predicted Score'  # Row 6: Predicted final score
  ]

  # Initialize the matchup DataFrame structure
  matchup_data = {
    ' ': METRIC_LABELS,                    # Row labels
    pair_a: ['', '', '', '', '', 0, 0],   # Pair A summary column
    player_a1: [0, 0, 0, 0, 0, ' ', ' '], # Player A1 data
    player_a2: [0, 0, 0, 0, 0, ' ', ' '], # Player A2 data
    pair_b: ['', '', '', '', '', 0, 0],   # Pair B summary column
    player_b1: [0, 0, 0, 0, 0, ' ', ' '], # Player B1 data
    player_b2: [0, 0, 0, 0, 0, ' ', ' ']  # Player B2 data
  }

  matchup_df = pd.DataFrame.from_dict(matchup_data)

  # Helper function to safely format numeric values
  def safe_format(value, decimal_places=2):
    """Safely format numeric values to specified decimal places."""
    try:
      return round(float(value), decimal_places)
    except (ValueError, TypeError):
      return 0.0

    # Populate individual player statistics
    # Matchup data indices: [0=pair, 1=player, 2=ace%, 3=err%, 4=expected, 
    #                       5=pts_a_serve, 6=pts_b_serve, 7=rcv%, 8=srv%, 9=point_ratio]

  try:
    # Row 0: Ace percentages
    matchup_df.iloc[0, 2] = safe_format(a1_matchup[2])  # Player A1 ace %
    matchup_df.iloc[0, 3] = safe_format(a2_matchup[2])  # Player A2 ace %
    matchup_df.iloc[0, 5] = safe_format(b1_matchup[2])  # Player B1 ace %
    matchup_df.iloc[0, 6] = safe_format(b2_matchup[2])  # Player B2 ace %

    # Row 1: Error percentages
    matchup_df.iloc[1, 2] = safe_format(a1_matchup[3])  # Player A1 error %
    matchup_df.iloc[1, 3] = safe_format(a2_matchup[3])  # Player A2 error %
    matchup_df.iloc[1, 5] = safe_format(b1_matchup[3])  # Player B1 error %
    matchup_df.iloc[1, 6] = safe_format(b2_matchup[3])  # Player B2 error %

    # Row 2: Expected win probability
    matchup_df.iloc[2, 2] = safe_format(a1_matchup[4])  # Player A1 expected
    matchup_df.iloc[2, 3] = safe_format(a2_matchup[4])  # Player A2 expected
    matchup_df.iloc[2, 5] = safe_format(b1_matchup[4])  # Player B1 expected
    matchup_df.iloc[2, 6] = safe_format(b2_matchup[4])  # Player B2 expected

    # Row 3: Points when Pair A serves
    matchup_df.iloc[3, 2] = safe_format(a1_matchup[5])  # Player A1 points on A serve
    matchup_df.iloc[3, 3] = safe_format(a2_matchup[5])  # Player A2 points on A serve
    matchup_df.iloc[3, 5] = safe_format(b1_matchup[5])  # Player B1 points on A serve
    matchup_df.iloc[3, 6] = safe_format(b2_matchup[5])  # Player B2 points on A serve

    # Row 4: Points when Pair B serves
    matchup_df.iloc[4, 2] = safe_format(a1_matchup[6])  # Player A1 points on B serve
    matchup_df.iloc[4, 3] = safe_format(a2_matchup[6])  # Player A2 points on B serve
    matchup_df.iloc[4, 5] = safe_format(b1_matchup[6])  # Player B1 points on B serve
    matchup_df.iloc[4, 6] = safe_format(b2_matchup[6])  # Player B2 points on B serve

    # Row 5: Point ratios (same for both players in each pair)
    pair_a_ratio = safe_format(a1_matchup[9])
    pair_b_ratio = safe_format(b1_matchup[9])

    matchup_df.iloc[5, 1] = pair_a_ratio  # Pair A point ratio
    matchup_df.iloc[5, 4] = pair_b_ratio  # Pair B point ratio

    # Row 6: Predicted scores (scaled to 21 points)
    # Scale the point ratios to a 21-point game
    max_ratio = max(pair_a_ratio, pair_b_ratio)
    if max_ratio > 0:
      scale_factor = 21 / max_ratio
      predicted_score_a = safe_format(pair_a_ratio * scale_factor)
      predicted_score_b = safe_format(pair_b_ratio * scale_factor)
    else:
      predicted_score_a = predicted_score_b = 0

    matchup_df.iloc[6, 1] = predicted_score_a  # Pair A predicted score
    matchup_df.iloc[6, 4] = predicted_score_b  # Pair B predicted score

  except (IndexError, KeyError, TypeError) as e:
    return f"Error populating matchup data: {str(e)}"

  return matchup_df


def get_matchup_summary(matchup_df):
  """
    Extract key summary statistics from a matchup DataFrame.
    
    Args:
        matchup_df (pd.DataFrame): Matchup analysis DataFrame
        
    Returns:
        dict: Summary statistics including predicted winner and key metrics
    """
  if isinstance(matchup_df, str):
    return {"error": matchup_df}

  try:
    # Extract predicted scores
    pair_a_score = matchup_df.iloc[6, 1]
    pair_b_score = matchup_df.iloc[6, 4]

    # Determine predicted winner
    if pair_a_score > pair_b_score:
      predicted_winner = matchup_df.columns[1]  # Pair A name
      win_margin = pair_a_score - pair_b_score
    else:
      predicted_winner = matchup_df.columns[4]  # Pair B name
      win_margin = pair_b_score - pair_a_score

    return {
      "predicted_winner": predicted_winner,
      "predicted_score": f"{max(pair_a_score, pair_b_score):.1f} - {min(pair_a_score, pair_b_score):.1f}",
      "win_margin": round(win_margin, 1),
      "pair_a_ratio": matchup_df.iloc[5, 1],
      "pair_b_ratio": matchup_df.iloc[5, 4]
    }
  except Exception as e:
    return {"error": f"Error generating summary: {str(e)}"}


def validate_matchup_data(a1_matchup, a2_matchup, b1_matchup, b2_matchup):
  """
    Validate that matchup data contains expected structure and values.
    
    Args:
        a1_matchup, a2_matchup, b1_matchup, b2_matchup: Matchup data lists
        
    Returns:
        bool: True if data is valid, False otherwise
        str: Error message if validation fails
    """
  required_length = 10

  for i, matchup in enumerate([a1_matchup, a2_matchup, b1_matchup, b2_matchup]):
    if not isinstance(matchup, list) or len(matchup) < required_length:
      return False, f"Matchup data {i+1} has invalid structure"

      # Check that numeric fields contain valid numbers
    for idx in [2, 3, 4, 5, 6, 7, 8, 9]:  # Numeric indices
      try:
        float(matchup[idx])
      except (ValueError, TypeError):
        return False, f"Matchup data {i+1} contains invalid numeric value at index {idx}"

  return True, "Validation successful"



def matchup_net(disp_league, disp_gender, disp_year, pair_a, pair_b, disp_team):
  """
    Generate a comprehensive net position matchup analysis between two pairs.
    
    Analyzes the serving vs receiving matchup across all 5 net zones (1-5), comparing
    each serving player's opponent FBHE (First Ball Hitting Effectiveness) statistics 
    against each receiving player's FBHE performance. The analysis includes percentile
    rankings and differential analysis to identify optimal serving strategies.
    
    Net Zones (1-5):
    - Zone 1: Left sideline area
    - Zone 2: Left-center area  
    - Zone 3: Center net area
    - Zone 4: Right-center area
    - Zone 5: Right sideline area
    
    Args:
        disp_league (str): League identifier
        disp_gender (str): Gender category (e.g., 'M', 'F', 'Mixed')
        disp_year (str): Competition year
        pair_a (str): First pair identifier (format: "Player1/Player2")
        pair_b (str): Second pair identifier (format: "Player1/Player2")
        disp_team (str): Team identifier for data filtering
        
    Returns:
        pd.DataFrame: Net matchup analysis with columns:
            - srv_player: Serving player name
            - rcv_player: Receiving player name  
            - zone: Net zone (1-5)
            - opp_fbhe: Serving player's opponent FBHE rating for this zone
            - opp_per: Opponent FBHE percentile rank (higher = better serving matchup)
            - fbhe: Receiving player's FBHE rating for this zone
            - fbhe_per: Receiving player's FBHE percentile rank
            - per_diff: Advantage differential (opp_per - fbhe_per)
                       Positive = serving advantage, Negative = receiving advantage
                       
        str: Error message if pair data cannot be found or processing fails
        
    Raises:
        ValueError: If input parameters are invalid
        KeyError: If required data columns are missing from pair data
        
    Example:
        >>> matchup_df = matchup_net('NPL', 'M', '2024', 'Smith/Jones', 'Brown/Davis', 'TeamA')
        >>> print(matchup_df[matchup_df['per_diff'] > 0.2])  # Show strong serving advantages
    """

  # Input validation
  if not all([disp_league, disp_gender, disp_year, pair_a, pair_b]):
    raise ValueError("All league, gender, year, and pair parameters must be provided")

    # Define the expected net zones
  NET_ZONES = [1, 2, 3, 4, 5]

  # Initialize the matchup DataFrame structure
  COLUMN_SCHEMA = {
    'srv_player': '',      # Serving player name
    'rcv_player': '',      # Receiving player name  
    'zone': 0,             # Net zone (1-5)
    'opp_fbhe': 0.0,      # Opponent FBHE rating
    'opp_per': 0.0,       # Opponent FBHE percentile
    'fbhe': 0.0,          # Receiver FBHE rating
    'fbhe_per': 0.0,      # Receiver FBHE percentile
    'per_diff': 0.0       # Percentile difference (serve advantage)
  }

  # Create empty DataFrame with proper schema
  matchup_df = pd.DataFrame([COLUMN_SCHEMA])

  try:
    # Fetch pair data and statistics
    pair_data_df, pair_stats_df = get_pair_data(disp_league, disp_gender, disp_year)

    if pair_data_df is None or pair_data_df.empty:
      return f"No pair data found for {disp_league}-{disp_gender}-{disp_year}"

    if pair_stats_df is None or pair_stats_df.empty:
      return f"No pair statistics found for {disp_league}-{disp_gender}-{disp_year}"

  except Exception as e:
    return f"Error fetching pair data: {str(e)}"

  try:
    # Extract individual player names
    player_a1, player_a2 = pair_players(pair_a)
    player_b1, player_b2 = pair_players(pair_b)
  except Exception as e:
    return f"Error parsing player names: {str(e)}"

    # Find data indices for each player
  try:
    pair_a_indices = _get_player_indices(pair_data_df, pair_a, [player_a1, player_a2])
    pair_b_indices = _get_player_indices(pair_data_df, pair_b, [player_b1, player_b2])

    if isinstance(pair_a_indices, str):
      return pair_a_indices
    if isinstance(pair_b_indices, str):
      return pair_b_indices

    pair_a1_index, pair_a2_index = pair_a_indices
    pair_b1_index, pair_b2_index = pair_b_indices

  except Exception as e:
    return f"Error locating player data indices: {str(e)}"

    # Generate all possible matchup combinations
  matchup_combinations = []
  players_a = [(player_a1, pair_a1_index), (player_a2, pair_a2_index)]
  players_b = [(player_b1, pair_b1_index), (player_b2, pair_b2_index)]

  try:
    for srv_player, srv_index in players_a:
      for rcv_player, rcv_index in players_b:
        for zone in NET_ZONES:

          # Calculate zone-specific metrics
          zone_metrics = _calculate_zone_metrics(
            pair_data_df, pair_stats_df, 
            srv_index, rcv_index, zone
          )

          if isinstance(zone_metrics, str):  # Error occurred
            logging.warning(f"Zone metrics calculation failed: {zone_metrics}")
            continue

            # Create matchup record
          matchup_record = {
            'srv_player': srv_player,
            'rcv_player': rcv_player,
            'zone': zone,
            'opp_fbhe': zone_metrics['opp_fbhe'],
            'opp_per': zone_metrics['opp_per'],
            'fbhe': zone_metrics['fbhe'],
            'fbhe_per': zone_metrics['fbhe_per'],
            'per_diff': zone_metrics['per_diff']
          }

          matchup_combinations.append(matchup_record)

  except Exception as e:
    return f"Error generating matchup combinations: {str(e)}"

    # Create the final DataFrame from all combinations
  if not matchup_combinations:
    return "No valid matchup combinations could be generated"

  try:
    matchup_df = pd.DataFrame(matchup_combinations)

    # Sort by zone (descending) then by advantage differential (descending)
    matchup_df = matchup_df.sort_values(
      by=['zone', 'per_diff'], 
      ascending=[False, False]
    ).reset_index(drop=True)

  except Exception as e:
    return f"Error creating final matchup DataFrame: {str(e)}"

  return matchup_df


def _get_player_indices(pair_data_df, pair_name, player_names):
  """
    Helper function to find data indices for both players in a pair.
    
    Args:
        pair_data_df (pd.DataFrame): Pair data DataFrame
        pair_name (str): Pair identifier
        player_names (list): List of two player names
        
    Returns:
        tuple: (player1_index, player2_index) or error string
    """
  try:
    if not pair_data_df['pair'].isin([pair_name]).any():
      return f"Pair not found in pair data: {pair_name}"

      # Find indices for both players
    player1_mask = (pair_data_df['pair'] == pair_name) & (pair_data_df['player'] == player_names[0])
    player2_mask = (pair_data_df['pair'] == pair_name) & (pair_data_df['player'] == player_names[1])

    player1_indices = pair_data_df.loc[player1_mask].index
    player2_indices = pair_data_df.loc[player2_mask].index

    if len(player1_indices) == 0:
      return f"Player {player_names[0]} not found in pair {pair_name}"
    if len(player2_indices) == 0:
      return f"Player {player_names[1]} not found in pair {pair_name}"

    return player1_indices[0], player2_indices[0]

  except Exception as e:
    return f"Error finding player indices: {str(e)}"


def _calculate_zone_metrics(pair_data_df, pair_stats_df, srv_index, rcv_index, zone):
  """
    Calculate zone-specific FBHE metrics and percentiles for a serving vs receiving matchup.
    
    Args:
        pair_data_df (pd.DataFrame): Pair performance data
        pair_stats_df (pd.DataFrame): Statistical distributions for normalization
        srv_index (int): Serving player's data index
        rcv_index (int): Receiving player's data index  
        zone (int): Net zone (1-5)
        
    Returns:
        dict: Zone metrics including FBHE values and percentiles, or error string
    """
  try:
    # Generate column names for this zone
    opp_fbhe_col = f'opp_fbhe{zone}'
    fbhe_col = f'fbhe{zone}'
    opp_mean_col = f'{opp_fbhe_col}_mean'
    opp_stdev_col = f'{opp_fbhe_col}_stdev'
    fbhe_mean_col = f'{fbhe_col}_mean'
    fbhe_stdev_col = f'{fbhe_col}_stdev'

    # Validate required columns exist
    required_data_cols = [opp_fbhe_col, fbhe_col]
    required_stats_cols = [opp_mean_col, opp_stdev_col, fbhe_mean_col, fbhe_stdev_col]

    missing_data_cols = [col for col in required_data_cols if col not in pair_data_df.columns]
    missing_stats_cols = [col for col in required_stats_cols if col not in pair_stats_df.columns]

    if missing_data_cols:
      return f"Missing data columns: {missing_data_cols}"
    if missing_stats_cols:
      return f"Missing statistics columns: {missing_stats_cols}"

      # Extract raw FBHE values
    opp_fbhe_value = pair_data_df.loc[srv_index, opp_fbhe_col]
    fbhe_value = pair_data_df.loc[rcv_index, fbhe_col]

    # Get statistical parameters for percentile calculations
    opp_mean = pair_stats_df.at[0, opp_mean_col]
    opp_stdev = pair_stats_df.at[0, opp_stdev_col]
    fbhe_mean = pair_stats_df.at[0, fbhe_mean_col]
    fbhe_stdev = pair_stats_df.at[0, fbhe_stdev_col]

    # Calculate percentiles using normal distribution
    # For opponent FBHE: Higher values are better for serving (use complement)
    opp_percentile = 1 - stats.norm.cdf((opp_fbhe_value - opp_mean) / opp_stdev)

    # For receiving FBHE: Higher values are better for receiving
    fbhe_percentile = stats.norm.cdf((fbhe_value - fbhe_mean) / fbhe_stdev)

    # Calculate advantage differential
    # Positive = serving advantage, Negative = receiving advantage
    percentile_diff = opp_percentile - fbhe_percentile

    return {
      'opp_fbhe': round(float(opp_fbhe_value), 4),
      'opp_per': round(float(opp_percentile), 4),
      'fbhe': round(float(fbhe_value), 4), 
      'fbhe_per': round(float(fbhe_percentile), 4),
      'per_diff': round(float(percentile_diff), 4)
    }

  except (KeyError, IndexError, ValueError, TypeError) as e:
    return f"Error calculating zone {zone} metrics: {str(e)}"
  except Exception as e:
    return f"Unexpected error in zone {zone} calculation: {str(e)}"


def get_net_matchup_summary(matchup_df, top_n=5):
  """
    Extract key insights from net matchup analysis.
    
    Args:
        matchup_df (pd.DataFrame): Net matchup analysis DataFrame
        top_n (int): Number of top advantages to return
        
    Returns:
        dict: Summary including best serving matchups and receiving weaknesses
    """
  if isinstance(matchup_df, str):
    return {"error": matchup_df}

  try:
    # Find best serving advantages (highest positive per_diff)
    best_serve_matchups = matchup_df.nlargest(top_n, 'per_diff')[
      ['srv_player', 'rcv_player', 'zone', 'per_diff']
      ].to_dict('records')

    # Find biggest receiving advantages (lowest per_diff) 
    best_receive_matchups = matchup_df.nsmallest(top_n, 'per_diff')[
      ['srv_player', 'rcv_player', 'zone', 'per_diff']
      ].to_dict('records')

    # Zone-wise analysis
    zone_summary = matchup_df.groupby('zone').agg({
      'per_diff': ['mean', 'max', 'min', 'count']
    }).round(4)

    return {
      "best_serving_matchups": best_serve_matchups,
      "best_receiving_matchups": best_receive_matchups,
      "zone_analysis": zone_summary.to_dict(),
      "total_matchups": len(matchup_df)
    }

  except Exception as e:
    return {"error": f"Error generating net matchup summary: {str(e)}"}


def validate_net_data_requirements(pair_data_df, pair_stats_df):
  """
    Validate that the required columns exist for net position analysis.
    
    Args:
        pair_data_df (pd.DataFrame): Pair performance data
        pair_stats_df (pd.DataFrame): Statistical reference data
        
    Returns:
        tuple: (is_valid: bool, error_message: str)
    """
  try:
    required_data_columns = []
    required_stats_columns = []

    # Generate required column names for all zones
    for zone in [1, 2, 3, 4, 5]:
      required_data_columns.extend([f'opp_fbhe{zone}', f'fbhe{zone}'])
      required_stats_columns.extend([
        f'opp_fbhe{zone}_mean', f'opp_fbhe{zone}_stdev',
        f'fbhe{zone}_mean', f'fbhe{zone}_stdev'
      ])

      # Check data columns
    missing_data = [col for col in required_data_columns if col not in pair_data_df.columns]
    if missing_data:
      return False, f"Missing pair data columns: {missing_data}"

      # Check statistics columns  
    missing_stats = [col for col in required_stats_columns if col not in pair_stats_df.columns]
    if missing_stats:
      return False, f"Missing statistics columns: {missing_stats}"

    return True, "All required columns present"

  except Exception as e:
    return False, f"Validation error: {str(e)}"

    
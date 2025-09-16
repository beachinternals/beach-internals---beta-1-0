import anvil.secrets
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
from typing import Union
from server_functions import *


def report_scouting_overview(lgy, team, **rpt_filters):
  """
    Scouting overview report function - provides detailed analysis for a single pair.
    
    This function generates comprehensive performance analysis for one pair including
    individual player statistics, serving/receiving breakdowns, and zone-specific
    performance metrics for tactical planning.
    
    Args:
        lgy (str): League+gender+year string (format: "LEAGUE_GENDER_YEAR")
        team (str): Team identifier
        **rpt_filters: Additional report filters including:
            - pair: Pair identifier for analysis (format: "Player1/Player2")
        
    Returns:
        tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
            - df_list[0]: Player performance statistics (fullname, hit side, FBHE, Expected Value, Serve Recommendation)
            - df_list[1]: Player tendencies (Attack Area, Top Angular Attack for each player)
            - df_list[2]: Serving recommendations based on optimal serve zones
  """

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = [''] * 10
  df_list = [''] * 10

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Get the ppr data
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # Get required filter parameters
  pair_a = rpt_filters.get('pair')

  # Validate that pair is provided
  if not pair_a:
    print("report_scouting_overview, Pair_a not found")
    error_df = pd.DataFrame({'Error': ["No pair specified for analysis"]})
    df_list[0] = error_df.to_dict('records')
    df_desc_list[0] = "Error: Missing Pair Parameter"
    return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

  try:
    # Extract player names and filter ppr_df to points involving pair_a
    player1, player2 = pair_players(pair_a)
    ppr_df = ppr_df[(ppr_df['teama'] == pair_a) | (ppr_df['teamb'] == pair_a)]

    # First Table: Player Performance Statistics
    performance_data = []
    total_receives = 0

    for player in [player1, player2]:
      # Get full name from master_player table using team + ' ' + number + ' ' + shortname
      try:
        player_row = get_player_row(player)
        full_name = player_row['fullname'] if player_row and 'fullname' in player_row else player
      except Exception as e:
        print(f"Error fetching fullname for player {player}: {str(e)}")
        full_name = player

      # Determine hitting side
      player_ppr_df = ppr_df[(ppr_df['att_yn'] == 'Y') & (ppr_df['att_player'] == player)]
      att12 = player_ppr_df[((player_ppr_df['att_src_zone_net'] == 1) | 
                             (player_ppr_df['att_src_zone_net'] == 2)) & 
        (player_ppr_df['tactic'] != 'behind')].shape[0]
      att45 = player_ppr_df[((player_ppr_df['att_src_zone_net'] == 4) | 
                             (player_ppr_df['att_src_zone_net'] == 5)) & 
        (player_ppr_df['tactic'] != 'behind')].shape[0]
      hit_side = 'Left' if att12 > att45 else 'Right'

      # Calculate FBHE and Expected Value
      fbhe_result = fbhe_obj(ppr_df, player, 'both', False)
      fbhe = round(fbhe_result.fbhe, 2) if hasattr(fbhe_result, 'fbhe') else 0.0
      ev_result = calc_ev_obj(ppr_df, player)
      expected = round(ev_result.get('expected_value', 0.0), 2)

      # Calculate Serve Receive %
      player_receives = len(ppr_df[ppr_df['pass_player'] == player])
      total_receives += player_receives

      performance_data.append({
        'Full Name': full_name,
        'Hit Side': hit_side,
        'FBHE': fbhe,
        'Expected Value': expected,
        'Serve Receive %': player_receives,  # Temporary value, will be converted to % later
        'Serve Recommendation': ''  # Placeholder, will be filled after both players are processed
      })

    # Convert Serve Receive to percentage and compute Serve Recommendation
    fbhe1, fbhe2 = performance_data[0]['FBHE'], performance_data[1]['FBHE']
    ev1, ev2 = performance_data[0]['Expected Value'], performance_data[1]['Expected Value']

    # Use FBHE if available, otherwise Expected Value
    metric1, metric2 = (fbhe1, fbhe2) if fbhe1 != 0.0 and fbhe2 != 0.0 else (ev1, ev2)

    if metric1 != 0 and metric2 != 0:
      if metric1 < metric2 * 0.75:  # 25% lower
        performance_data[0]['Serve Recommendation'] = 'Strongly Recommended'
        performance_data[1]['Serve Recommendation'] = ''
      elif metric1 < metric2 * 0.85:  # 15% lower
        performance_data[0]['Serve Recommendation'] = 'Recommended'
        performance_data[1]['Serve Recommendation'] = ''
      elif metric2 < metric1 * 0.75:
        performance_data[1]['Serve Recommendation'] = 'Strongly Recommended'
        performance_data[0]['Serve Recommendation'] = ''
      elif metric2 < metric1 * 0.85:
        performance_data[1]['Serve Recommendation'] = 'Recommended'
        performance_data[0]['Serve Recommendation'] = ''
      else:
        performance_data[0]['Serve Recommendation'] = 'Neutral'
        performance_data[1]['Serve Recommendation'] = 'Neutral'

    for data in performance_data:
      data['Serve Receive %'] = round(data['Serve Receive %'] / total_receives * 100, 1) if total_receives > 0 else 0.0

    performance_df = pd.DataFrame(performance_data, index=[player1, player2])
    df_list[0] = performance_df.to_dict('records')
    df_desc_list[0] = f"Player Performance Statistics: {pair_a}"

    # Second Table: Player Tendencies
    recommendation_data = {
      'Attack Area': ['Front - Pin', 'Front - Slot', 'Middle', 'Behind'],
      player1: ['', '', '', ''],
      player2: ['', '', '', '']
    }

    # Define attack area mappings based on hit side
    for i, player in enumerate([player1, player2]):
      player_ppr_df = ppr_df[(ppr_df['att_yn'] == 'Y') & (ppr_df['att_player'] == player) & (ppr_df['pass_player'] == player)]
      att12 = player_ppr_df[((player_ppr_df['att_src_zone_net'] == 1) | 
                             (player_ppr_df['att_src_zone_net'] == 2)) & 
        (player_ppr_df['tactic'] != 'behind')].shape[0]
      att45 = player_ppr_df[((player_ppr_df['att_src_zone_net'] == 4) | 
                             (player_ppr_df['att_src_zone_net'] == 5)) & 
        (player_ppr_df['tactic'] != 'behind')].shape[0]
      hit_side = 'Left' if att12 > att45 else 'Right'

      # Define attack area mappings based on hit side
      attack_area_mappings = {
        'Front - Pin': [(1, 'not behind')] if hit_side == 'Left' else [(5, 'not behind')],
        'Front - Slot': [(2, 'not behind')] if hit_side == 'Left' else [(4, 'not behind')],
        'Middle': [(5, 'not behind'), (4, 'not behind'), (3, 'not behind')] if hit_side == 'Left' else [(1, 'not behind'), (2, 'not behind'), (3, 'not behind')],
        'Behind': [(1, 'behind'), (2, 'behind'), (3, 'behind'), (4, 'behind'), (5, 'behind')]
      }

      # Process each attack area
      for idx, area in enumerate(['Front - Pin', 'Front - Slot', 'Middle', 'Behind']):
        print(f" in the loop to build the df, idx: {idx}, area {area}")
        zones_tactics = attack_area_mappings[area]
        print(f" Zone Tactics : {zones_tactics}")
        area_df = pd.DataFrame()
        for zone, tactic in zones_tactics:
          print(f" filtering for zone {zone} and tactic {tactic}")
          if tactic == 'any':
            temp_df = player_ppr_df[player_ppr_df['att_src_zone_net'] == zone]
            print(f"Size of temp_df after filter for zone: {zone}, size {temp_df.shape[0]}")
          else:
            temp_df = player_ppr_df[(player_ppr_df['att_src_zone_net'] == zone) & 
              (player_ppr_df['tactic'] == tactic if tactic == 'behind' else player_ppr_df['tactic'] != 'behind')]
            print(f"Size of temp_df after filter for zone: {zone}, tactic {tactic}, df size: {temp_df.shape[0]}")
          area_df = pd.concat([area_df, temp_df])

        if not area_df.empty:
          print(f"Size of area_df passed to angular attack table {area_df.shape[0]}")
          area_angle_table = get_player_angular_attack_table(area_df, None, player)
          ang_labels = ['Cut-Left', 'Angle-Left', 'Over-Middle', 'Angle-Right', 'Cut-Right']
          max_attempts = 0
          top_angle = 'N/A'
          top_pct = '0%'
          top_fbhe = 0.0

          print(f" Angular Table: for player {player}, for {area} \n{area_angle_table}")
          for ang_label in ang_labels:
            attempts = area_angle_table.loc[area_angle_table[' '] == 'Attempts', ang_label].iloc[0]
            if isinstance(attempts, str):
              attempts = float(attempts.replace('%', '')) if '%' in attempts else float(attempts)
            if attempts > max_attempts:
              max_attempts = attempts
              top_angle = ang_label
              top_pct = area_angle_table.loc[area_angle_table[' '] == '% of Attempts', ang_label].iloc[0]
              top_fbhe = area_angle_table.loc[area_angle_table[' '] == 'FBHE', ang_label].iloc[0]

          recommendation_data[player][idx] = f"{top_angle} ({top_pct}, FBHE: {top_fbhe})"
          print(f"recommendation: {recommendation_data}")
        else:
          recommendation_data[player][idx] = 'N/A'

    recommendation_df = pd.DataFrame(recommendation_data)
    df_list[1] = recommendation_df.to_dict('records')
    df_desc_list[1] = f"Player Tendencies: {pair_a}"

    # Third Table: Serving Recommendations
    serving_data = {'Metric': ['Optimal Serve From', 'Serve From-To 1', 'Serve From-To 2', 'Serve From-To 3', 'Serve From-To 4', 'Serve From-To 5'], 
                    player1: ['', '', '', '', '', ''], 
                    player2: ['', '', '', '', '', '']}

    zone_labels = {1: 'Left', 3: 'Middle', 5: 'Right'}

    for i, player in enumerate([player1, player2]):
      serve_zones = [1, 3, 5]
      fbhe_scores = []
      for zone in serve_zones:
        zone_df = ppr_df[(ppr_df['pass_player'] == player) & (ppr_df['serve_src_zone_net'] == zone)]
        if not zone_df.empty:
          fbhe_result = fbhe_obj(zone_df, player, 'pass', False)
          fbhe = round(fbhe_result.fbhe, 2) if hasattr(fbhe_result, 'fbhe') else 0.0
          att = len(zone_df)
          fbhe_scores.append((zone, fbhe, att))

      if fbhe_scores:
        optimal_zone, optimal_fbhe, optimal_att = min(fbhe_scores, key=lambda x: x[1])
        serving_data[player][0] = f"{zone_labels[optimal_zone]} (FBHE: {optimal_fbhe}, att: {optimal_att})"
      else:
        serving_data[player][0] = 'N/A'

      serve_combinations = []
      for from_zone in serve_zones:
        for to_zone in [1, 2, 3, 4, 5]:
          for to_zone_depth in ['C','D','E']:
            zone_df = ppr_df[(ppr_df['pass_player'] == player) & 
              (ppr_df['serve_src_zone_net'] == from_zone) & 
              (ppr_df['serve_dest_zone_net'] == to_zone) & 
              (ppr_df['serve_dest_zone_depth'] == to_zone_depth) ]
            if not zone_df.empty:
              fbhe_result = fbhe_obj(zone_df, player, 'pass', False)
              fbhe = round(fbhe_result.fbhe, 2) if hasattr(fbhe_result, 'fbhe') else 0.0
              att = len(zone_df)
              serve_combinations.append((from_zone, str(to_zone)+str(to_zone_depth), fbhe, att))

      serve_combinations = sorted(serve_combinations, key=lambda x: x[2])[:5]
      for idx, (from_zone, to_zone, fbhe, att) in enumerate(serve_combinations, 1):
        serving_data[player][idx] = f"From {zone_labels.get(from_zone, f'Zone {from_zone}')} to Zone {to_zone} (FBHE: {fbhe}, att: {att})"

    serving_df = pd.DataFrame(serving_data)
    df_list[2] = serving_df.to_dict('records')
    df_desc_list[2] = f"Serving Recommendations: {pair_a}"

    '''
    # we don't do this here, these fields come from the report_list table
    if title_list and len(title_list) > 0:
      title_list[0] = f"Scouting Overview: {pair_a}"
    if label_list and len(label_list) > 0:
      label_list[0] = f"Individual Pair Analysis Report"
      if len(label_list) > 1:
        label_list[1] = f"League: {disp_league}, Gender: {disp_gender}, Year: {disp_year}"
      if len(label_list) > 2:
        label_list[2] = f"Team: {team}"
    '''

  except Exception as e:
    error_df = pd.DataFrame({'Error': [f"Error generating scouting report: {str(e)}"]})
    df_list[0] = error_df.to_dict('records')
    df_desc_list[0] = "Report Generation Error"

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



  

def get_pair_performance_analysis(disp_league: str, disp_gender: str, disp_year: str, pair_name: str, disp_team: str, ppr_df: pd.DataFrame) -> Union[pd.DataFrame, str]:
  """
    Generate individual player performance analysis for a single pair.
    
    This function creates a detailed breakdown of each player's statistics including
    serving percentages, receiving effectiveness, and overall performance metrics, calculated from the ppr_df.
    
    Args:
        disp_league (str): League identifier
        disp_gender (str): Gender category (e.g., 'M', 'F')
        disp_year (str): Year of competition
        pair_name (str): Pair identifier (format: unique pair ID)
        disp_team (str): Team identifier (not used for filtering in this version)
        ppr_df (pd.DataFrame): Point-by-point rally data
        
    Returns:
        dict: Player performance analysis as a dict of records with columns:
            - Player: Player name
            - Total Serves: Total number of serves
            - Serve %: Percentage of pair's serves
            - Ace %: Service ace percentage
            - Err %: Service error percentage
            - Knockout%: Knockout percentage
            - Total Receives: Total number of receives
            - FBHE: First Ball Hit Efficiency
            - FBSO: First Ball Side Out
            - Expected: Expected value
            - Receive %: Percentage of pair's receives
        dict: Error message as a DataFrame converted to dict if data cannot be found
    """
  # Input validation
  if not all(isinstance(x, str) for x in [disp_league, disp_gender, disp_year, pair_name, disp_team]):
    error_df = pd.DataFrame({'Error': ["All string parameters must be strings"]})
    return error_df.to_dict('records')

  if not isinstance(ppr_df, pd.DataFrame):
    error_df = pd.DataFrame({'Error': ["ppr_df must be a pandas DataFrame"]})
    return error_df.to_dict('records')

  if not all([disp_league.strip(), disp_gender.strip(), disp_year.strip(), pair_name.strip()]):
    error_df = pd.DataFrame({'Error': ["Missing required parameters for performance analysis"]})
    return error_df.to_dict('records')

  try:
    if ppr_df.empty:
      error_df = pd.DataFrame({'Error': [f"No data found for {disp_league}-{disp_gender}-{disp_year}"]})
      return error_df.to_dict('records')

    # Extract player names
    try:
      player1, player2 = pair_players(pair_name)
    except ValueError as e:
      error_df = pd.DataFrame({'Error': [f"Error retrieving players for pair {pair_name}: {str(e)}"]})
      return error_df.to_dict('records')

    players = [player1.strip(), player2.strip()]

    performance_data = []

    for player in players:
      # Serve stats
      serve_df = ppr_df[ppr_df['serve_player'] == player]
      total_serves = len(serve_df)
      aces = len(serve_df[serve_df['point_outcome'] == 'TSA'])
      errors = len(serve_df[serve_df['point_outcome'] == 'TSE'])
      ace_per = round(aces / total_serves * 100, 2) if total_serves > 0 else 0.0
      err_per = round(errors / total_serves * 100, 2) if total_serves > 0 else 0.0

      # Receive stats
      receive_df = ppr_df[ppr_df['pass_player'] == player]
      total_receives = len(receive_df)

      # Calculate Knockout%
      knockout_result = calc_knockout_obj(ppr_df, player)
      knockout = round(knockout_result.get('knock_out_rate', 0.0) * 100, 2)

      # Calculate FBHE and FBSO
      fbhe_result = fbhe_obj(ppr_df, player, 'both', False)
      fbhe = round(fbhe_result.fbhe, 2) if hasattr(fbhe_result, 'fbhe') else 0.0
      fbso = round(fbhe_result.fbso, 2) if hasattr(fbhe_result, 'fbso') else 0.0

      # Calculate Expected
      ev_result = calc_ev_obj(ppr_df, player)
      expected = round(ev_result.get('expected_value', 0.0), 2)

      player_stats = {
        'Player': player,
        'Total Serves': total_serves,
        'Serve %': ace_per,
        'Ace %': ace_per,
        'Err %': err_per,
        'Knockout%': knockout,
        'Total Receives': total_receives,
        'FBHE': fbhe,
        'FBSO': fbso,
        'Expected': expected,
        'Receive %': 0.0,  # Will be calculated later
      }

      performance_data.append(player_stats)

    # Calculate pair-level percentages
    total_serves_pair = sum(p['Total Serves'] for p in performance_data)
    total_receives_pair = sum(p['Total Receives'] for p in performance_data)

    for player_stats in performance_data:
      player_stats['Serve %'] = round(player_stats['Total Serves'] / total_serves_pair * 100, 1) if total_serves_pair > 0 else 0.0
      player_stats['Receive %'] = round(player_stats['Total Receives'] / total_receives_pair * 100, 1) if total_receives_pair > 0 else 0.0

    # Create DataFrame
    performance_df = pd.DataFrame(performance_data)

    # Add pair summary row
    if total_serves_pair > 0:
      ace_per_pair = round(sum((p['Ace %'] / 100 * p['Total Serves']) for p in performance_data) / total_serves_pair * 100, 2)
      err_per_pair = round(sum((p['Err %'] / 100 * p['Total Serves']) for p in performance_data) / total_serves_pair * 100, 2)
    else:
      ace_per_pair = 0.0
      err_per_pair = 0.0

    pair_summary = {
      'Player': f"{pair_name} (Pair Total)",
      'Total Serves': total_serves_pair,
      'Serve %': 100.0,
      'Ace %': ace_per_pair,
      'Err %': err_per_pair,
      'Knockout%': round(performance_df['Knockout%'].mean(), 2),
      'Total Receives': total_receives_pair,
      'FBHE': round(performance_df['FBHE'].mean(), 2),
      'FBSO': round(performance_df['FBSO'].mean(), 2),
      'Expected': round(performance_df['Expected'].mean(), 2),
      'Receive %': 100.0
    }

    # Add summary as the last row
    performance_df = pd.concat([performance_df, pd.DataFrame([pair_summary])], ignore_index=True)

    # Reorder columns
    columns = ['Player', 'Total Serves', 'Serve %', 'Ace %', 'Err %', 'Knockout%', 'Total Receives', 'FBHE', 'FBSO', 'Expected', 'Receive %']
    performance_df = performance_df[columns]

    return performance_df.to_dict('records')

  except Exception as e:
    error_df = pd.DataFrame({'Error': [f"Error generating pair performance analysis: {str(e)}"]})
    return error_df.to_dict('records')


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

  

def get_single_pair_net_analysis(disp_league, disp_gender, disp_year, pair_name, disp_team):
  """
    Generate net position analysis for a single pair across all zones.
    
    This function analyzes how each player in the pair performs in different
    net zones (1-5) for both serving and receiving scenarios.
    
    Args:
        disp_league (str): League identifier
        disp_gender (str): Gender category
        disp_year (str): Competition year
        pair_name (str): Pair identifier (format: "Player1/Player2")
        disp_team (str): Team identifier
        
    Returns:
        pd.DataFrame: Net analysis with columns:
            - Player: Player name
            - Zone: Net zone (1-5)
            - FBHE: First Ball Hitting Effectiveness
            - FBHE Percentile: Performance percentile rank
            - Opp FBHE: Opponent FBHE when this player serves
            - Opp FBHE Percentile: Opponent performance percentile
            - Net Advantage: Serving advantage in this zone
            
        str: Error message if data cannot be found
    """

  # Input validation
  if not all([disp_league, disp_gender, disp_year, pair_name]):
    return "Error: Missing required parameters for net analysis"

  try:
    # Fetch pair data and statistics
    pair_data_df, pair_stats_df = get_pair_data(disp_league, disp_gender, disp_year)

    if pair_data_df is None or pair_data_df.empty:
      return f"No pair data found for {disp_league}-{disp_gender}-{disp_year}"

    if pair_stats_df is None or pair_stats_df.empty:
      return f"No pair statistics found for {disp_league}-{disp_gender}-{disp_year}"

      # Extract player names
    player_a1, player_a2 = pair_players(pair_name)

    # Find data indices
    pair_indices = _get_player_indices(pair_data_df, pair_name, [player_a1, player_a2])
    if isinstance(pair_indices, str):
      return pair_indices

    player_a1_index, player_a2_index = pair_indices

    # Generate zone analysis for all zones and players
    NET_ZONES = [1, 2, 3, 4, 5]
    zone_analysis = []

    for player, index in [(player_a1, player_a1_index), (player_a2, player_a2_index)]:
      for zone in NET_ZONES:

        # Calculate zone metrics for this player
        zone_metrics = _calculate_single_player_zone_metrics(
          pair_data_df, pair_stats_df, index, zone
        )

        if isinstance(zone_metrics, str):  # Error occurred
          continue

        zone_record = {
          'Player': player,
          'Zone': zone,
          'FBHE': zone_metrics['fbhe'],
          'FBHE Percentile': zone_metrics['fbhe_per'],
          'Opp FBHE': zone_metrics['opp_fbhe'],
          'Opp FBHE Percentile': zone_metrics['opp_per'],
          'Net Advantage': zone_metrics['net_advantage']
        }

        zone_analysis.append(zone_record)

    if not zone_analysis:
      return "No valid zone analysis data could be generated"

      # Create DataFrame and sort by player, then zone
    zone_df = pd.DataFrame(zone_analysis)
    zone_df = zone_df.sort_values(by=['Player', 'Zone']).reset_index(drop=True)

    return zone_df

  except Exception as e:
    return f"Error generating single pair net analysis: {str(e)}"


def _calculate_single_player_zone_metrics(pair_data_df, pair_stats_df, player_index, zone):
  """
    Calculate zone-specific metrics for a single player.
    
    Args:
        pair_data_df (pd.DataFrame): Pair performance data
        pair_stats_df (pd.DataFrame): Statistical distributions
        player_index (int): Player's data index
        zone (int): Net zone (1-5)
        
    Returns:
        dict: Zone metrics including FBHE, opponent FBHE, and percentiles
    """

  try:
    # Generate column names for this zone
    opp_fbhe_col = f'opp_fbhe{zone}'
    fbhe_col = f'fbhe{zone}'
    opp_mean_col = f'{opp_fbhe_col}_mean'
    opp_stdev_col = f'{opp_fbhe_col}_stdev'
    fbhe_mean_col = f'{fbhe_col}_mean'
    fbhe_stdev_col = f'{fbhe_col}_stdev'

    # Validate columns exist
    required_data_cols = [opp_fbhe_col, fbhe_col]
    required_stats_cols = [opp_mean_col, opp_stdev_col, fbhe_mean_col, fbhe_stdev_col]

    missing_data_cols = [col for col in required_data_cols if col not in pair_data_df.columns]
    missing_stats_cols = [col for col in required_stats_cols if col not in pair_stats_df.columns]

    if missing_data_cols:
      return f"Missing data columns: {missing_data_cols}"
    if missing_stats_cols:
      return f"Missing statistics columns: {missing_stats_cols}"

      # Extract raw values
    opp_fbhe_value = pair_data_df.loc[player_index, opp_fbhe_col]
    fbhe_value = pair_data_df.loc[player_index, fbhe_col]

    # Get statistical parameters
    opp_mean = pair_stats_df.at[0, opp_mean_col]
    opp_stdev = pair_stats_df.at[0, opp_stdev_col]
    fbhe_mean = pair_stats_df.at[0, fbhe_mean_col]
    fbhe_stdev = pair_stats_df.at[0, fbhe_stdev_col]

    # Calculate percentiles
    opp_percentile = 1 - stats.norm.cdf((opp_fbhe_value - opp_mean) / opp_stdev)
    fbhe_percentile = stats.norm.cdf((fbhe_value - fbhe_mean) / fbhe_stdev)

    # Calculate net advantage (serving advantage - receiving strength)
    net_advantage = opp_percentile - fbhe_percentile

    return {
      'opp_fbhe': round(float(opp_fbhe_value), 4),
      'opp_per': round(float(opp_percentile), 4),
      'fbhe': round(float(fbhe_value), 4),
      'fbhe_per': round(float(fbhe_percentile), 4),
      'net_advantage': round(float(net_advantage), 4)
    }

  except Exception as e:
    return f"Error calculating zone {zone} metrics: {str(e)}"


def get_pair_serving_analysis(disp_league, disp_gender, disp_year, pair_name, disp_team):
  """
    Generate detailed serving analysis for a single pair.
    
    This is a placeholder function - implement based on available serving data.
    
    Args:
        disp_league (str): League identifier
        disp_gender (str): Gender category
        disp_year (str): Competition year
        pair_name (str): Pair identifier
        disp_team (str): Team identifier
        
    Returns:
        pd.DataFrame: Serving analysis data or error string
    """

  # This function would be implemented based on your specific serving data structure
  # For now, return a placeholder message
  return "Detailed serving analysis function not yet implemented"
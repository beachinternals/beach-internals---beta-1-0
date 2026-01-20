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
import numpy as np
from .logger_utils import log_debug, log_info, log_error, log_critical

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.


def clean_player_name(team, player_num):
  """
  Clean and normalize player names from BTD data
  
  Args:
    team: Team name (e.g., 'Stetson Beach VB Balltime', 'Opponent')
    player_num: Player number (e.g., '1', 'None 1', 'nan')
  
  Returns:
    Normalized player string or empty string if invalid
  """
  # Convert to string and handle NaN/None
  team_str = str(team) if pd.notna(team) else ''
  player_str = str(player_num) if pd.notna(player_num) else ''

  # Skip if either is missing or contains error markers
  if not team_str or not player_str or 'NOTEAM' in team_str or 'NOPLAYER' in player_str:
    return ''

  # Remove 'None ' prefix from player numbers (e.g., 'None 1' -> '1')
  # This handles the case where Balltime adds 'None' as a prefix
  if player_str.startswith('None '):
    player_str = player_str[5:].strip()

  # Skip if player is still empty or just 'nan'/'None'
  if not player_str or player_str.lower() in ['nan', 'none', '']:
    return ''

  # Return combined team + player
  # This ensures that "Team A 1" and "Team B 1" are different players
  return f"{team_str} {player_str}"


def identify_declared_team(btd_df):
  """
  Identify which team is the "declared" team (usually the home team or the team being tracked)
  
  Args:
    btd_df: DataFrame with 'team' column
    
  Returns:
    String: The most common non-opponent team name, or None if can't determine
  """
  if 'team' not in btd_df.columns:
    return None

  # Get team counts, excluding rows with no team or NOTEAM
  valid_teams = btd_df[
    (btd_df['team'].notna()) & 
    (~btd_df['team'].str.contains('NOTEAM', na=False))
    ]

  if valid_teams.empty:
    return None

  team_counts = valid_teams['team'].value_counts()

  # If there's only one team, return it
  if len(team_counts) == 1:
    return team_counts.index[0]

  # If there are exactly 2 teams and one is called 'Opponent', return the other
  if len(team_counts) == 2:
    teams = list(team_counts.index)
    if 'Opponent' in teams:
      return teams[0] if teams[0] != 'Opponent' else teams[1]

  # Otherwise, return the most common team
  return team_counts.index[0]


def detect_cross_team_players(players_df, btd_df):
  """
  Detect players who might be attributed to the wrong team
  
  Returns:
    List of dictionaries with misattribution info
  """
  if players_df.empty:
    return []

  # Get player counts
  player_counts = players_df['player'].value_counts()

  # Extract team and number from each player
  player_info = {}
  for player in player_counts.index:
    if not player or player.strip() == '':
      continue

    # Split from the right to get the last part (number)
    parts = player.rsplit(' ', 1)
    if len(parts) == 2:
      team = parts[0]
      number = parts[1]
      player_info[player] = {
        'team': team,
        'number': number,
        'count': player_counts[player]
      }

  # Look for same numbers on different teams
  misattributions = []

  # Group by number
  numbers = {}
  for player, info in player_info.items():
    number = info['number']
    if number not in numbers:
      numbers[number] = []
    numbers[number].append((player, info))

  # Find numbers that appear on multiple teams
  for number, players_with_number in numbers.items():
    if len(players_with_number) < 2:
      continue

    # Sort by count (frequency)
    players_with_number.sort(key=lambda x: x[1]['count'], reverse=True)

    # If the highest frequency is much higher than others, flag the low ones
    highest = players_with_number[0]
    highest_count = highest[1]['count']

    for player, info in players_with_number[1:]:
      # If this player appears less than 20% as often as the most common one
      if info['count'] < highest_count * 0.2:
        misattributions.append({
          'likely_wrong': player,
          'likely_correct': highest[0],
          'wrong_count': info['count'],
          'correct_count': highest_count,
          'number': number
        })

  return misattributions


def extract_four_players(players_df, btd_df):
  """
  Extract exactly 4 players from the BTD data, handling duplicates and errors
  
  Returns:
    Tuple: (list of 4 player strings, list of warnings)
  """

  warnings = []

  if players_df.empty:
    log_error("No player data found in BTD file", with_traceback=False)
    warnings.append("No player data found in BTD file")
    return ["Unknown", "Unknown", "Unknown", "Unknown"], warnings

  # Get unique players
  players_unique = players_df['player'].unique()
  players_unique = [p for p in players_unique if p and len(p.strip()) > 0]

  log_info("=== PLAYER EXTRACTION DEBUG ===")
  log_info(f"Initial unique players found: {len(players_unique)}")
  log_debug(f"Players: {players_unique}")

  # Detect cross-team misattributions
  misattributions = detect_cross_team_players(players_df, btd_df)
  if misattributions:
    log_error("WARNING: Detected potential cross-team player misattributions:", with_traceback=False)
    for m in misattributions:
      msg = f"  Player '{m['likely_wrong']}' ({m['wrong_count']} times) might actually be '{m['likely_correct']}' ({m['correct_count']} times)"
      log_error(msg, with_traceback=False)
      warnings.append(msg)

  # Identify the declared team
  declared_team = identify_declared_team(btd_df)
  log_info(f"Declared team identified as: '{declared_team}'")

  if not declared_team:
    log_error("WARNING: Could not identify declared team, using frequency-based approach", with_traceback=False)
    warnings.append("Could not identify declared team")
    return extract_by_frequency(players_df, players_unique), warnings
  
  # Count occurrences of each player
  player_counts = players_df['player'].value_counts().to_dict()
  log_debug("Player frequencies:")
  for player, count in sorted(player_counts.items(), key=lambda x: x[1], reverse=True):
    log_debug(f"  {player}: {count} occurrences")
  
  # Separate players by team
  team_players = []
  opponent_players = []
  
  for player in players_unique:
    if declared_team in player:
      team_players.append(player)
    else:
      opponent_players.append(player)
  
  log_debug(f"Team players ({declared_team}): {team_players}")
  log_debug(f"Opponent players: {opponent_players}")
  
  # Function to get top N players by frequency
  def get_top_players(player_list, n=2):
    if not player_list:
      return []
    sorted_players = sorted(
      player_list, 
      key=lambda p: (-player_counts.get(p, 0), p)
    )
    return sorted_players[:n]
  
  # Get top 2 from each group
  final_team_players = get_top_players(team_players, 2)
  final_opponent_players = get_top_players(opponent_players, 2)
  
  log_debug(f"Top 2 team players: {final_team_players}")
  log_debug(f"Top 2 opponent players: {final_opponent_players}")
  
  # Warn if we filtered out players
  if len(team_players) > 2:
    filtered = [p for p in team_players if p not in final_team_players]
    msg = f"Filtered out low-frequency team players: {filtered}"
    log_error(msg, with_traceback=False)
    warnings.append(msg)
  
  if len(opponent_players) > 2:
    filtered = [p for p in opponent_players if p not in final_opponent_players]
    msg = f"Filtered out low-frequency opponent players: {filtered}"
    log_error(msg, with_traceback=False)
    warnings.append(msg)
  
  # Combine and validate
  all_players = final_team_players + final_opponent_players
  
  # Handle edge cases
  if len(final_team_players) < 2:
    msg = f"WARNING: Only {len(final_team_players)} team players found (expected 2)"
    log_error(msg, with_traceback=False)
    warnings.append(msg)
  if len(final_opponent_players) < 2:
    msg = f"WARNING: Only {len(final_opponent_players)} opponent players found (expected 2)"
    log_error(msg, with_traceback=False)
    warnings.append(msg)
  
  # Pad with "Unknown" if we have less than 4
  while len(all_players) < 4:
    all_players.append("Unknown")
    msg = "Adding 'Unknown' player to reach 4 total"
    log_error(msg, with_traceback=False)
    warnings.append(msg)
  
  # Take only first 4 if somehow we have more
  all_players = all_players[:4]
  
  # Sort alphabetically for consistency
  all_players.sort()
  
  log_info(f"Final 4 players (sorted): {all_players}")
  log_info("=" * 50)
  
  return all_players, warnings


def extract_by_frequency(players_df, players_unique):
  """Fallback method: Extract players purely by frequency"""
  log_info("Using frequency-based extraction (fallback method)")
  
  player_counts = players_df['player'].value_counts().to_dict()
  sorted_players = sorted(
    players_unique,
    key=lambda p: (-player_counts.get(p, 0), p)
  )
  
  top_four = sorted_players[:4]
  while len(top_four) < 4:
    top_four.append("Unknown")
  
  top_four.sort()
  log_info(f"Extracted 4 players by frequency: {top_four}")
  return top_four


# ############ server function to calculate completeness for all btd file entries
@anvil.server.callable
def update_btd_characteristics(file):
  """
  Analyze BTD file and return statistics AND cleaned CSV file
  
  Returns:
    Tuple: (statistics_list, cleaned_csv_media_object)
  """

  # set the user qualities
  if anvil.users.get_user() is None:
    user_team = ""
    user_gender = ""
    user_email = ""
    user_league = ""
    user_year = ""
  else:
    user_team = anvil.users.get_user()['team']
    user_league = anvil.users.get_user()['def_league']
    user_gender = anvil.users.get_user()['def_gender']
    user_year = anvil.users.get_user()['def_year']
    user_email = anvil.users.get_user()['email']
    
  log_info(f"BTD Upload: Processing file for user={user_email}, team={user_team}")
  
  # Read the BTD CSV file
  file_obj = io.BytesIO(file.get_bytes())
  btd_df = pd.read_csv(file_obj)

  #----------
  # NORMALIZE THE DATA IN THE CSV FILE
  #----------

  # check if the dataframe has a field called team
  if 'team' in btd_df.columns:
    # Fill nan with '' in team and player
    btd_df = btd_df.fillna({'team': str('NOTEAM'), 'player': str('NOPLAYER')})
    
    # Rename 'player' to 'only_player' to preserve original
    btd_df = btd_df.rename(columns={'player': 'only_player'})
    
    # Clean and combine team + player using the new function
    # THIS NORMALIZES THE DATA - removes 'None ' prefixes, etc.
    btd_df['player'] = btd_df.apply(
      lambda row: clean_player_name(row['team'], row['only_player']), 
      axis=1
    )
    
    log_info(f"Normalized BTD data: Unique teams={btd_df['team'].nunique()}, Unique players={btd_df['player'].nunique()}")
    
  # Calculate statistics on the CLEANED data
  num_actions = int(btd_df.shape[0])
  serves_df = btd_df[btd_df['action_type'] == "serve"]
  num_serves = int(serves_df.shape[0])
  players_df = btd_df[btd_df['player'].notna() & (btd_df['player'] != '')]
  per_action_players = int(players_df.shape[0]) / num_actions if num_actions > 0 else 0
  srv_players = serves_df[serves_df['player'].notna() & (serves_df['player'] != '')]
  per_srv_players = int(srv_players.shape[0]) / num_serves if num_serves > 0 else 0
  src_coord = btd_df[btd_df['src_zone'].notna()]
  dest_coord = btd_df[btd_df['dest_zone'].notna()]
  num_src_coord = int(src_coord.shape[0]) + int(dest_coord.shape[0])
  per_coord = num_src_coord / (2 * num_actions) if num_actions > 0 else 0
  
  comp_score = (5 * per_srv_players + 3 * per_action_players + 2 * per_coord) / 10
  comp_score = str('{:.2%}'.format(comp_score))
  per_coord = str('{:.2%}'.format(per_coord))
  per_srv_players = str('{:.2%}'.format(per_srv_players))
  per_action_players = str('{:.2%}'.format(per_action_players))
  
  log_info(f"BTD Statistics: Actions={num_actions}, Serves={num_serves}, %SrvPlayers={per_srv_players}, %Coord={per_coord}, %ActionPlayers={per_action_players}, Score={comp_score}")

  # Extract the 4 players using improved logic (from CLEANED data)
  four_players, warnings = extract_four_players(players_df, btd_df)
  
  playera1 = four_players[0]
  playera2 = four_players[1]
  playerb1 = four_players[2]
  playerb2 = four_players[3]
  
  log_info(f"Final 4 players for mapping: {playera1}, {playera2}, {playerb1}, {playerb2}")
  
  if warnings:
    log_error(f"BTD Upload Warnings ({len(warnings)}):", with_traceback=False)
    for w in warnings:
      log_error(f"  - {w}", with_traceback=False)

  # Convert the CLEANED dataframe back to CSV
  cleaned_csv_string = btd_df.to_csv(index=False)
  cleaned_csv_media = anvil.BlobMedia(
    content_type="text/csv",
    content=cleaned_csv_string.encode('utf-8'),
    name=file.name  # Keep the same filename
  )
  
  log_info("Created cleaned CSV file ready for storage")

  # Return both the statistics AND the cleaned CSV file
  statistics = [playera1, playera2, playerb1, playerb2, num_serves, comp_score, per_action_players, per_coord, per_srv_players]
  
  return statistics, cleaned_csv_media

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
from logger_utils import log_info, log_error, log_critical, log_debug

# ============================================================================
#  AUTH HELPER
# ============================================================================

def _require_login():
  """Verify the caller is logged in. Returns user row or raises Exception."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  return user


# ============================================================================
#
#  INPUT VALIDATION HELPER
#
#  _validate_league_params() — validates league, gender, team against live data
#  _validate_lgy_string()    — parses and validates "NCAA | W | 2025" strings
#
#  Leagues are read dynamically from app_tables.league_list, so adding a new
#  league to that table is all that's needed — no code change required here.
#
#  Team is validated against app_tables.subscriptions. INTERNALS is always
#  allowed regardless of subscription records.
#
# ============================================================================

VALID_GENDERS = {'M', 'W'}

def _get_valid_leagues():
  """Return the set of valid league names from the league_list table."""
  try:
    return {row['league'] for row in app_tables.league_list.search()}
  except Exception:
    # If table is unreachable, fail open rather than block all requests
    return None

def _get_valid_teams():
  """Return the set of valid team names from the subscriptions table."""
  try:
    return {row['team'] for row in app_tables.subscriptions.search()}
  except Exception:
    return None

def _validate_league_params(league=None, gender=None, team=None):
  """
  Validate user-supplied league, gender, and/or team parameters.
  Year is intentionally not validated here.
  Raises ValueError with a clear message if any value is invalid.

  Usage:
    _validate_league_params(league=league, gender=gender, team=team)
  """
  if league is not None:
    if not isinstance(league, str) or not league.strip():
      raise ValueError("League must be a non-empty string.")
    valid_leagues = _get_valid_leagues()
    if valid_leagues is not None and league.strip() not in valid_leagues:
      raise ValueError(f"Invalid league: {repr(league)}. Must be one of {sorted(valid_leagues)}.")

  if gender is not None:
    if not isinstance(gender, str) or gender.strip().upper() not in VALID_GENDERS:
      raise ValueError(f"Invalid gender: {repr(gender)}. Must be one of {sorted(VALID_GENDERS)}.")

  if team is not None:
    if not isinstance(team, str) or not team.strip():
      raise ValueError("Team must be a non-empty string.")
    if re.search(r'[<>]', team):
      raise ValueError(f"Invalid team name: {repr(team)}.")
    # INTERNALS can access any team — skip subscription check
    if team.strip() != 'INTERNALS':
      valid_teams = _get_valid_teams()
      if valid_teams is not None and team.strip() not in valid_teams:
        raise ValueError(f"Invalid team: {repr(team)}. Team is not registered in the system.")


def _validate_lgy_string(lgy):
  """
  Parse and validate an lgy string in format "NCAA | W | 2025" or "NCAA|W|2025".
  Validates league and gender. Year is not validated.
  Raises ValueError if format or values are invalid.
  Returns (league, gender, year_str) as cleaned values.
  """
  if not isinstance(lgy, str) or '|' not in lgy:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected 'LEAGUE | GENDER | YEAR'."
    )
  parts = [p.strip() for p in lgy.split('|')]
  if len(parts) != 3:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected exactly 3 parts separated by '|'."
    )
  league, gender, year_str = parts
  _validate_league_params(league=league, gender=gender)
  return league, gender, year_str



# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#

# ############ server function to calculate completeness for all btd file entries
@anvil.server.callable
def update_btd_characteristics( file ):

  # Auth check — must be logged in and assigned to a team
  user = _require_login()
  user_team   = user['team']
  user_league = user['def_league']
  user_gender = user['def_gender']
  user_year   = user['def_year']
  user_email  = user['email']

  if not user_team:
    raise Exception("Your account is not assigned to a team. Please contact Beach Internals.")

  # Validate the uploaded file is a CSV
  if not file.name.lower().endswith('.csv'):
    return None, None, "Invalid file type: only .csv files are accepted."

  # Read the BTD CSV file
  file_obj = io.BytesIO( file.get_bytes() )
  try:
    btd_df = pd.read_csv(file_obj)
  except Exception as e:
    return None, None, f"Could not read CSV file: {str(e)}"

  #----------
  #
  #.    Make the updates here because balltime added the Team filed to work with scouted data (where neither team is the home team)
  #
  #----------

  # check if the dataframe has a field called team
  if 'team' in btd_df.columns:
    # fill nan with '' in team and player
    btd_df = btd_df.fillna({'team':str('NOTEAM'),'player':str('NOPLAYER')})
    # this must be a new actions file,, so we will rename 'player' to 'only_player', then merge team and player and store it in a new 'player' column
    btd_df = btd_df.rename(columns={'player':'only_player'})
    btd_df['player'] = btd_df['team'].astype(str)+' ' + btd_df['only_player'].astype(str)
    #btd_df['player'] = np.where( ('NOTEAM' in btd_df['player']) or ('NOPLAYER' in btd_df['player'] ), '', btd_df['player'] )
    # we should be good, let's check
    #print(f"BTD Fields of interest: {btd_df['team']}, {btd_df['only_player']}, {btd_df['player']}")

  # Calculate number of actions
  num_actions = int(btd_df.shape[0])

  # Caculate the number of serves (points)
  serves_df = btd_df[btd_df['action_type'] == "serve" ]
  num_serves = int(serves_df.shape[0])

  # Calculate number of actions with a player
  players_df = btd_df[btd_df['player'].notna() ]

  per_action_players = int(players_df.shape[0]) / num_actions

  # calcaulte the number of serves with players idendified
  srv_players = serves_df[serves_df['player'].notna()]
  per_srv_players = int(srv_players.shape[0]) / num_serves

  # calculate number of actions with coordinates   
  src_coord = btd_df[btd_df['src_zone'].notna()]
  dest_coord = btd_df[btd_df['dest_zone'].notna()]
  num_src_coord = int(src_coord.shape[0]) + int(dest_coord.shape[0])
  per_coord = num_src_coord / (2*num_actions)

  # create a 'score' for completeness
  comp_score = (5*per_srv_players + 3*per_action_players + 2*per_coord)/10
  comp_score = str('{:.2%}'.format(comp_score))
  per_coord = str('{:.2%}'.format(per_coord))
  per_srv_players = str('{:.2%}'.format(per_srv_players))
  per_action_players = str('{:.2%}'.format(per_action_players))

  #print(f"how are we doing?  Actions: {num_actions} Serves: {num_serves}, % Srv Players: {per_srv_players} % Coordiantes {per_coord} % Acion Players {per_action_players} Completion Score: {comp_score}")
  #print("  ")

  # now find the four or more) players in the file
  players_unique = players_df.player.unique()
  players_unique.sort()
  num_players = players_unique.shape[0]

  # we should check that we have 4 players
  if num_players != 4:
    print(f"Number of players found in the ball time data file is incorrect, number found is :{num_players} players are: {players_unique}")
    #print(f"player 0: {players_unique[0]}, player 1: {players_unique[1]}, player 2: {players_unique[2]}, player 3: {players_unique[3]}, player 4: {players_unique[4]}, ")

    # loop thru the player list and delete those that look bad
    tmp_players = players_unique
    num_deleted = 0
    for p in range(len(players_unique)):
      print(f"player : {players_unique[p]}")
      #if ("NOTEAM" in players_unique[p]) and ("NOPLAYER" in players_unique[p]):
      if ("NOPLAYER" in players_unique[p]):
        tmp_players = np.delete(tmp_players,p-num_deleted)
        num_deleted = num_deleted + 1
        print(f"Deleting : {players_unique[p]}")      

    players_unique = tmp_players
    print(f"Number of players reduced to:{players_unique.shape[0]} players are: {players_unique}")

    while len(players_unique) <4:
      # if we are less then 4, then add a row
      players_unique = np.append(players_unique, "Unknown")

  playera1 = players_unique[0]
  playera2 = players_unique[1]
  playerb1 = players_unique[2]
  playerb2 = players_unique[3]

  # print(f"number of players: {num_players} List of players")
  #print(players_unique, playera1, playera2, playerb1, playerb2 )

  # now let's try to set the self.item data bindings to display this new data

  # Convert dataframe back to CSV for storage
  cleaned_csv_string = btd_df.to_csv(index=False)
  cleaned_csv_media = anvil.BlobMedia(
    content_type="text/csv",
    content=cleaned_csv_string.encode("utf-8"),
    name=file.name
  )

  statistics = [playera1, playera2, playerb1, playerb2, num_serves, comp_score, per_action_players, per_coord, per_srv_players]
  return statistics, cleaned_csv_media, None
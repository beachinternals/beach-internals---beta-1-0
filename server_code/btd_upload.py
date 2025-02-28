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

  # set the user qualities
  if anvil.users.get_user() is None :
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
    
  # pass in the btd csv file, then return the characterestics
  #print("file:", file)
  
  # take file, and make the 
  file_obj = io.BytesIO( file.get_bytes() )
  btd_df = pd.read_csv(file_obj)

  #----------
  #
  #.    Make the updates here because balltime added the Team filed to work with scouted data (where neither team is the home team)
  #
  #----------

  # check if the dataframe has a field called team
  if 'team' in btd_df.columns:
    # this must be a new actions file,, so we will rename 'player' to 'only_player', then merge team and player and store it in a new 'player' column
    btd_df = btd_df.rename(columns={'player':'only_player'})
    btd_df['player'] = btd_df['team'].astype(str)+' ' + btd_df['only_player'].astype(str)
    # we should be good, let's check
    print(f"BTD Fields of interest: {btd_df['team']}, {btd_df['only_player']}, {btd_df['player']}")
    
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
    if num_players > 4:
      # maybe we can ignore the 5th plaeyr?
      players_unique.drop(4)
    elif num_players <4:
      # if we are less then 4, then add a row
      players_unique.loc[len(players_unique)]
    
  playera1 = players_unique[0]
  playera2 = players_unique[1]
  playerb1 = players_unique[2]
  playerb2 = players_unique[3]
  
  # print(f"number of players: {num_players} List of players")
  #print(players_unique, playera1, playera2, playerb1, playerb2 )

  # now let's try to set the self.item data bindings to display this new data
  
  return [playera1, playera2, playerb1, playerb2, num_serves, comp_score, per_action_players, per_coord, per_srv_players]




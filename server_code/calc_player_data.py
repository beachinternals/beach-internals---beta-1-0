import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import io
import math
import numpy as np
from tabulate import tabulate
from server_functions import fbhe

# ########## Calculate league summaries, stored as player data
#
#  player_data is only for league entries in the ppr_csv_tables table.  We also sotre the list of player data stats

@anvil.server.callable
def calcualte_player_data( c_league, c_gender, c_year):
  result_string = "Calcualte Player Data server module Called"

  c_team = "League"    # only updating hte league tables
  print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
      ) )

  if ppr_csv_row:
    ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
  else:
    print('No Rows Found')
    return ["No Rows"]

  print(f"shape of ppr_df :{ppr_df.shape}")
  
  # now, how many player do we have>?  Create  a list of just the players
  # first, four lists, one each for player 1a, 1b, 2a, 2b
  p_list1 = ppr_df['player_a1'].unique()
  print(f"player list 1:{p_list1}")
  p_list2 = ppr_df['player_a2'].unique()
  p_list3 = ppr_df['player_b1'].unique() 
  p_list4 = ppr_df['player_b2'].unique()

  p_list = p_list1
  p_list = np.append(p_list1, p_list2)
  p_list = np.append(p_list,p_list3)
  p_list = np.append(p_list,p_list4)
  print(f"Player List 2: {p_list}")
  p_list = np.unique(p_list)
  print(f"Player List 3: {p_list}")
  num_players = p_list.shape[0]
  print(f"player list{p_list}")

  print(f"number of players: {num_players}")

  # build the ppr_dataframe out tpo the proper number of rows, equal total points,
  # His should make a blank (except for flist_r values) ppr dataframe with the correct number of rows (maybe one extra due to a 0 start)

  player_dict = {'player':[str()],
                 'fbhe':[float()],'fbhe1':[float()],'fbhe2':[float()],'fbhe3':[float()],'fbhe4':[float()],'fbhe5':[float()]
                }
  print(f"Player Dict:{player_dict}")
  player_df = pd.DataFrame.from_records(player_dict)
  #player_df = pd.DataFrame(player_dict, columns=['player', 'fbhe', 'fbhe1','fbhe2','fbhe3','fbhe4','fbhe5'])
  
  for i in  range(1,num_players):
    player_df.loc[max(player_df.index)+1] = player_dict
  
  # create the player_data_stats dataframe
  player_stats_dict = {'fbhe_mean':[float()],'fbhe_stdv':[float()], 'fbhe_range_mean':[float()],'fbhe_range_stdev':[float()]
                      }
  player_stats_df =  pd.DataFrame.from_records(player_stats_dict)    # shoudl only need one row here
  print(f"player stats df:{player_stats_df}")
  
  for i in range(1,num_players):
    print(f"player: {p_list[i]}")
    fbhe_vector = fbhe(ppr_df, p_list[i])
    player_df[i,'fbhe'] = fbhe_vector[0]

  for j in range(1,5):
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net'] == i], p_list[i])
    field = "fbhe" + str(i)
    player_df[field] = fbhe_vector[0]

  print(f"Player Df when done:{player_df}")
  # now lets store our player_data file back as a csv file in the database
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  player_csv_file = pd.DataFrame.to_csv(player_df)
  player_media = anvil.BlobMedia(content_type="text/plain", content=player_csv_file.encode(), name="player_data.csv")
  ppr_csv_row.update( player_data = player_media )
  
  return result_string

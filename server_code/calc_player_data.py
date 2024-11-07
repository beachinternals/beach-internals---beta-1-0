import anvil.email
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
import statistics
import numpy as np
from tabulate import tabulate
from server_functions import *

# ########## Calculate league summaries, stored as player data
#
#  player_data is only for league entries in the ppr_csv_tables table.  We also sotre the list of player data stats

# begin with the server callable task, this then provides status and calls the background task
@anvil.server.callable
def calc_player_data_background( c_league, c_gender, c_year):
  # 
  # calculate the player data files for all teams in the league, gender, year given
  #

  print(f'Calling Background Task calculate_player_data for {c_league},{c_gender},{c_year}')
  task = anvil.server.launch_background_task('calculate_player_data', c_league, c_gender, c_year)

  # check return status
  print(f' Background Task, Task id:{task.get_id()} return Status:{task.get_termination_status()}')
  
  return task

#----------------------------
#
# Define the background task to calculate the player_data for a single league
#
#---------------------------------------------
@anvil.server.background_task
def calculate_player_data( c_league, c_gender, c_year):
  result_string = "Calculate Player Data server module Called"

  c_team = "League"    # only updating the league tables
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
    #print('No Rows Found')
    return ["No Rows"]

  #print(f"shape of ppr_df :{ppr_df.shape}")
  min_att = ppr_csv_row['min_att']
  
  # now, how many player do we have>?  Create  a list of just the players
  # first, four lists, one each for player 1a, 1b, 2a, 2b
  p_list1 = ppr_df['player_a1'].unique()
  #print(f"player list 1:{p_list1}")
  p_list2 = ppr_df['player_a2'].unique()
  p_list3 = ppr_df['player_b1'].unique() 
  p_list4 = ppr_df['player_b2'].unique()

  p_list = p_list1
  p_list = np.append(p_list1, p_list2)
  p_list = np.append(p_list,p_list3)
  p_list = np.append(p_list,p_list4)
  #print(f"Player List 2: {p_list}")
  p_list = np.unique(p_list)
  #print(f"Player List 3: {p_list}")
  num_players = p_list.shape[0]
  
  #print(f"player list{p_list}")
  #print(f"number of players: {num_players}")

  # build the ppr_dataframe out tpo the proper number of rows, equal total points,
  # His should make a blank (except for flist_r values) ppr dataframe with the correct number of rows (maybe one extra due to a 0 start)

  player_dict = {'player':[str()], 'team':[str()],
                 'fbhe':None,'fbhe1':None,'fbhe2':None,'fbhe3':None,'fbhe4':None,'fbhe5':None,'fbhe_range':None,
                 'err_den':None,'tcr':None,'tcr_r':None,'tcr_s':None,'expected':None,
                 'srv1_fbhe':None,'srv3_fbhe':None,'srv5_fbhe':None,
                 'poke_fbhe':None,'poke_per':None,'shoot_fbhe':None,'shoot_per':None,'bang_fbhe':None,'bang_per':None,
                 'fbhe_srv':None,'fbhe_srv1':None,'fbhe_srv3':None,'fbhe_srv5':None,
                 'fbhe_option':None, 'option_per':None,'fbhe_behind':None,'behind_per':None,'fbhe_tempo':None,'tempo_per':None
                }
  #print(f"Player Dict:{player_dict}")
  player_df = pd.DataFrame.from_records(player_dict)
  #player_df = pd.DataFrame(player_dict, columns=['player', 'fbhe', 'fbhe1','fbhe2','fbhe3','fbhe4','fbhe5'])
  
  for i in  range(1,num_players):
    player_df.loc[max(player_df.index)+1] = player_dict
  
  # create the player_data_stats dataframe
  player_stats_dict = {'fbhe_mean':[float()],'fbhe_stdev':[float()], 'fbhe_range_mean':[float()],'fbhe_range_stdev':[float()],
                       'tcr_mean':[float()],'tcr_stdev':[float()], 'tcr_r_mean':[float()],'tcr_r_stdev':[float()],'tcr_s_mean':[float()],'tcr_s_stdev':[float()], 
                       'expected_mean':[float()],'expected_stdev':[float()],'err_den_mean':[float()],'err_den_stdev':[float()]
                      }
  player_stats_df =  pd.DataFrame.from_records(player_stats_dict)    # shoudl only need one row here
  #print(f"player stats df:{player_stats_df}")
  
  for i in range(0,num_players):
    print(f"player: {p_list[i]}")
    player_df.at[i,'player'] = p_list[i]

    # unpack the player into the team, number, and short name
    # there is a space in between, built lie this:
    #  ppr_player_list.append( i['team']+" "+i['number']+" "+i['shortname'] )
    teama = player_df.at[i,'player']
    teama_loc = teama.index(" ")
    this_team = teama[:teama_loc].strip()
    player_df.at[i,'team'] = this_team

    # ----------- calculate FBHE, 1-5 ------------------
    fbhe_vector = fbhe(ppr_df, p_list[i], 'att')
    if fbhe_vector[3] >= min_att:
      player_df.at[i,'fbhe'] = fbhe_vector[0]

    #print(f"player_df after fbhe calc:{player_df}")

    fbhe_min = 1
    fbhe_max = 0
    for j in [1,2,3,4,5]:
      fbhe_vector = fbhe(ppr_df[ppr_df['att_src_zone_net']==j], p_list[i], 'att')
      field = "fbhe" + str(j)
      #print(f"Field:{field}, fbhe vector:{fbhe_vector}")
      if fbhe_vector[3] >= min_att:
        player_df.at[i,field] = fbhe_vector[0]
        fbhe_min = fbhe_vector[0] if fbhe_vector[0] < fbhe_min else fbhe_min
        fbhe_max = fbhe_vector[0] if fbhe_vector[0] > fbhe_max else fbhe_max
    if fbhe_max - fbhe_min != -1:
      player_df.at[i,'fbhe_range'] = float("{:.3f}".format(fbhe_max - fbhe_min))
    else:
      player_df.at[i,'fbhe_range'] = None

    # ------------calculate transition Conversion ------------------
    trans_vector = calc_trans( ppr_df, p_list[i], 'all' )
    player_df.at[i,'tcr'] = float(trans_vector[0][:-1])
    trans_vector = calc_trans( ppr_df, p_list[i], 'srv' )
    player_df.at[i,'tcr_s'] = float(trans_vector[0][:-1])
    trans_vector = calc_trans( ppr_df, p_list[i], 'rcv' )
    player_df.at[i,'tcr_r'] = float(trans_vector[0][:-1])

    # -------------- calculate expected value ---------------
    ev_vector = calc_ev(ppr_df, p_list[i])
    player_df.at[i,'expected'] = float(ev_vector[0][:-1])

    # ----------------- calculate error density ----------
    ed_vector = calc_error_den( ppr_df, p_list[i] )
    player_df.at[i,'err_den'] = float(ed_vector[0][:-1])

    #------------------- Calculate Poke, Shoot, and Bang fbhe and %
    fbhe_vector = fbhe( ppr_df, p_list[i], 'all')
    total_attempts = fbhe_vector[3]if fbhe_vector[3] != 0 else 1
    fbhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'poke')
    player_df.at[i,'poke_fbhe'] = fbhe_vector[0]
    player_df.at[i,'poke_per'] = fbhe_vector[3]/total_attempts
    fbhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'shoot')
    player_df.at[i,'shoot_fbhe'] = fbhe_vector[0]
    player_df.at[i,'shoot_per'] = fbhe_vector[3]/total_attempts
    bhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'bang')
    player_df.at[i,'bang_fbhe'] = fbhe_vector[0]
    player_df.at[i,'bang_per'] = fbhe_vector[3]/total_attempts

    #------------------- Behind, Option, and Tempo fbhe and %
    fbhe_vector = fbhe(ppr_df, p_list[i], 'pass')
    total_attempts = fbhe_vector[3] if fbhe_vector[3] != 0 else 1
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'option'],p_list[i],'pass')
    player_df.at[i,'fbhe_option'] = fbhe_vector[0]
    player_df.at[i,'option_per'] = int(fbhe_vector[3])/total_attempts
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'behind'],p_list[i],'pass')
    player_df.at[i,'fbhe_behind'] = fbhe_vector[0]
    player_df.at[i,'behind_per'] = int(fbhe_vector[3])/total_attempts
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'tempo'],p_list[i],'pass')
    player_df.at[i,'fbhe_tempo'] = fbhe_vector[0]
    player_df.at[i,'tempo_per'] = fbhe_vector[3]/total_attempts
    
    #-------------------- Serving Effectiviness, fbhe on all, zone 1, 3, 5
    fbhe_vector = fbhe(ppr_df, p_list[i], 'srv')
    player_df.at[i,'fbhe_srv'] = fbhe_vector[0]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==1],p_list[i],'srv')
    player_df.at[i,'fbhe_srv1'] = fbhe_vector[0]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==3],p_list[i],'srv')
    player_df.at[i,'fbhe_srv3'] = fbhe_vector[0]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==5],p_list[i],'srv')
    player_df.at[i,'fbhe_srv5'] = fbhe_vector[0]

    #------------------- FBHE when served from 1, 3, 5
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==1],p_list[i],'pass')
    player_df.at[i,'srv1_fbhe'] = fbhe_vector[0]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==3],p_list[i],'pass')
    player_df.at[i,'srv3_fbhe'] = fbhe_vector[0]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==5],p_list[i],'pass')
    player_df.at[i,'srv5_fbhe'] = fbhe_vector[0]


  ########## end of loop over players
  #print(f"Player Df when done:{player_df}")

  # Now store and calulate the statistical values
  #------------------------------------------------
  player_stats_df.at[0,"fbhe_mean"] = player_df['fbhe'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_stdev"] = player_df['fbhe'].std(skipna=True)
  player_stats_df.at[0,"fbhe_range_mean"] = player_df['fbhe_range'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_range_stdev"] = player_df['fbhe_range'].std(skipna=True)
  player_stats_df.at[0,"tcr_mean"] = player_df['tcr'].mean(skipna=True)
  player_stats_df.at[0,"tcr_stdev"] = player_df['tcr'].std(skipna=True)
  player_stats_df.at[0,"tcr_r_mean"] = player_df['tcr_r'].mean(skipna=True)
  player_stats_df.at[0,"tcr_r_stdev"] = player_df['tcr_r'].std(skipna=True)
  player_stats_df.at[0,"tcr_s_mean"] = player_df['tcr_s'].mean(skipna=True)
  player_stats_df.at[0,"tcr_s_stdev"] = player_df['tcr_s'].std(skipna=True)
  player_stats_df.at[0,"expected_mean"] = player_df['expected'].mean(skipna=True)
  player_stats_df.at[0,"expected_stdev"] = player_df['expected'].std(skipna=True)
  player_stats_df.at[0,"err_den_mean"] = player_df['err_den'].mean(skipna=True)
  player_stats_df.at[0,"err_den_stdev"] = player_df['err_den'].std(skipna=True)
  
  # now lets store our player_data file back as a csv file in the database
  #---------------------------------------------------------------------------
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  player_csv_file = pd.DataFrame.to_csv(player_df)
  player_media = anvil.BlobMedia(content_type="text/plain", content=player_csv_file.encode(), name="player_data.csv")

  player_stats_csv = pd.DataFrame.to_csv(player_stats_df)
  player_stats_media = anvil.BlobMedia(content_type="text/plain", content=player_stats_csv.encode(), name="player_sats.csv")
  
  ppr_csv_row.update( player_data = player_media, player_data_stats=player_stats_media )
  
  return result_string

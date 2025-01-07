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

@anvil.server.callable
def calc_all_player_data():
  # caluclate the plaeyr data for ALL leagues
  # so seach th eleague data base, then loop thru them

  # for each row:
  for l_row in app_tables.ppr_csv_tables.search(team='League'):
    task = calc_player_data_background(l_row['league'],l_row['gender'],l_row['year'])
        
  return task

# begin with the server callable task, this then provides status and calls the background task
@anvil.server.callable
def calc_player_data_background( c_league, c_gender, c_year):
  # 
  # calculate the player data files for all teams in the league, gender, year given
  #

  #print(f'Calling Background Task calculate_player_data for {c_league},{c_gender},{c_year}')
  task = anvil.server.launch_background_task('calculate_player_data', c_league, c_gender, c_year)

  # check return status
  #print(f' Background Task, Task id:{task.get_id()} return Status:{task.get_termination_status()}')
  
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
  #print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
      ) )

  if ppr_csv_row:
    ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
    if ppr_df.shape[0] == 0:
      return ["No Rows"]
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

  player_dict = {'pair':[str()],'player':[str()], 'team':[str()],
                 'fbhe':None,'fbhe1':None,'fbhe2':None,'fbhe3':None,'fbhe4':None,'fbhe5':None,'fbhe_range':None,
                 'fbhe_n':None,'fbhe1_n':None,'fbhe2_n':None,'fbhe3_n':None,'fbhe4_n':None,'fbhe5_n':None,
                 'fbhe_behind':None,'fbhe_behind_per':None,'fbhe_behind_n':None,
                 'fbhe_option':None, 'fbhe_option_per':None,'fbhe_option_n':None,
                 'fbhe_tempo':None,'fbhe_tempo_per':None,'fbhe_tempo_n':None,
                 'fbhe_poke':None,'fbhe_poke_per':None,'fbhe_poke_n':None,
                 'fbhe_shoot':None,'fbhe_shoot_per':None,'fbhe_shoot_n':None,
                 'fbhe_bang':None,'fbhe_bang_per':None,'fbhe_bang_n':None,
                 'fbhe_srv1':None,'fbhe_srv3':None,'fbhe_srv5':None,
                 'fbhe_srv1_n':None,'fbhe_srv3_n':None,'fbhe_srv5_n':None,
                 'err_den':None,'tcr':None,'tcr_r':None,'tcr_s':None,'expected':None,
                 'srv_n':None,'srv_fbhe':None,'srv_ace_per':None,'srv_err_per':None,
                 'srv1_n':None,'srv1_fbhe':None,'srv1_ace_per':None,'srv1_err_per':None,
                 'srv3_n':None,'srv3_fbhe':None,'srv3_ace_per':None,'srv3_err_per':None,
                 'srv5_n':None,'srv5_fbhe':None,'srv5_ace_per':None,'srv5_err_per':None          
                }
  #print(f"Player Dict:{player_dict}")
  player_df = pd.DataFrame.from_records(player_dict)
  #player_df = pd.DataFrame(player_dict, columns=['player', 'fbhe', 'fbhe1','fbhe2','fbhe3','fbhe4','fbhe5'])
  
  for i in  range(1,num_players):
    player_df.loc[max(player_df.index)+1] = player_dict
  
  # create the player_data_stats dataframe
  player_stats_dict = {'fbhe_mean':[float()],'fbhe_stdev':[float()], 'fbhe_range_mean':[float()],'fbhe_range_stdev':[float()],
                       'fbhe1_mean':[float()],'fbhe2_mean':[float()],'fbhe3_mean':[float()],'fbhe4_mean':[float()],'fbhe5_mean':[float()],
                       'fbhe1_stdev':[float()],'fbhe2_stdev':[float()],'fbhe3_stdev':[float()],'fbhe4_stdev':[float()],'fbhe5_stdev':[float()],
                       'fbhe_behind_mean':[float()],'fbhe_behind_per_mean':[float()],'fbhe_option_mean':[float()],'fbhe_option_per_mean':[float()],'fbhe_tempo_mean':[float()],'fbhe_tempo_per_mean':[float()],
                       'fbhe_behind_stdev':[float()],'fbhe_behind_per_stdev':[float()],'fbhe_option_stdev':[float()],'fbhe_option_per_stdev':[float()],'fbhe_tempo_stdev':[float()],'fbhe_tempo_per_stdev':[float()],                       
                       'fbhe_srv1_mean':[float()],'fbhe_srv3_mean':[float()],'fbhe_srv5_mean':[float()],
                       'fbhe_srv1_stdev':[float()],'fbhe_srv3_stdev':[float()],'fbhe_srv5_stdev':[float()],
                       'tcr_mean':[float()],'tcr_stdev':[float()], 'tcr_r_mean':[float()],'tcr_r_stdev':[float()],'tcr_s_mean':[float()],'tcr_s_stdev':[float()], 
                       'expected_mean':[float()],'expected_stdev':[float()],'err_den_mean':[float()],'err_den_stdev':[float()],
                       'srv_fbhe_mean':[float()],'srv1_fbhe_mean':[float()],'srv_3fbhe_mean':[float()],'srv5_fbhe_mean':[float()],
                       'srv_fbhe_stdev':[float()],'srv1_fbhe_stdev':[float()],'srv3_fbhe_stdev':[float()],'srv5_fbhe_stdev':[float()]
                      }
  player_stats_df =  pd.DataFrame.from_records(player_stats_dict)    # shoudl only need one row here
  #print(f"player stats df:{player_stats_df}")
  
  for i in range(0,num_players):
    #print(f"player: {p_list[i]}")
    player_df.at[i,'player'] = p_list[i]

    # unpack the player into the team, number, and short name
    # there is a space in between, built lie this:
    #  ppr_player_list.append( i['team']+" "+i['number']+" "+i['shortname'] )
    teama = player_df.at[i,'player']
    teama_loc = teama.index(" ")
    this_team = teama[:teama_loc].strip()
    player_df.at[i,'team'] = this_team

    # ----------- calculate FBHE, 1-5 ------------------
    fbhe_vector = fbhe(ppr_df, p_list[i], 'att', True )
    if fbhe_vector[3] >= min_att:
      player_df.at[i,'fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
      player_df.at[i,'fbhe_n'] = fbhe_vector[3]

    #print(f"player_df after fbhe calc:{player_df}")

    fbhe_min = 1
    fbhe_max = 0
    for j in [1,2,3,4,5]:
      fbhe_vector = fbhe(ppr_df[ppr_df['att_src_zone_net']==j], p_list[i], 'att', False)
      field = "fbhe" + str(j)
      field_n = field + str('_n')
      #print(f"Field:{field}, fbhe vector:{fbhe_vector}")
      if fbhe_vector[3] >= min_att:
        player_df.at[i,field] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
        player_df.at[i,field_n] = fbhe_vector[3]
        fbhe_min = fbhe_vector[0] if fbhe_vector[0] < fbhe_min else fbhe_min
        fbhe_max = fbhe_vector[0] if fbhe_vector[0] > fbhe_max else fbhe_max
    if fbhe_max - fbhe_min != -1:
      player_df.at[i,'fbhe_range'] = float("{:.3f}".format(fbhe_max - fbhe_min))
    else:
      player_df.at[i,'fbhe_range'] = None
      
    #------------------- Behind, Option, and Tempo fbhe and %
    fbhe_vector = fbhe(ppr_df, p_list[i], 'pass', True)
    total_attempts = fbhe_vector[3] if fbhe_vector[3] != 0 else 1
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'option'],p_list[i],'pass', False)
    player_df.at[i,'fbhe_option'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_option_n'] = fbhe_vector[3]
    player_df.at[i,'fbhe_option_per'] = int(fbhe_vector[3])/total_attempts
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'behind'],p_list[i],'pass', False)
    player_df.at[i,'fbhe_behind'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_behind_n'] = fbhe_vector[3]
    player_df.at[i,'fbhe_behind_per'] = int(fbhe_vector[3])/total_attempts
    fbhe_vector = fbhe(ppr_df[ppr_df['tactic'] == 'tempo'],p_list[i],'pass', False)
    player_df.at[i,'fbhe_tempo'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_tempo_n'] = fbhe_vector[3]
    player_df.at[i,'fbhe_tempo_per'] = fbhe_vector[3]/total_attempts

    #------------------- Calculate Poke, Shoot, and Bang fbhe and %
    fbhe_vector = fbhe( ppr_df, p_list[i], 'all', False)
    #print(f"player: {p_list[i]}, fbhe_vector: {fbhe_vector}")
    total_attempts = fbhe_vector[3] if fbhe_vector[3] != 0 else 1
    fbhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'poke', False)
    #print(f"player: {p_list[i]}, i: {i}, fbhe_vector line 188: {fbhe_vector}")
    player_df.at[i,'fbhe_poke'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_poke_n'] = fbhe_vector[3]    
    player_df.at[i,'poke_per_per'] = fbhe_vector[3]/total_attempts
    fbhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'shoot', False)
    player_df.at[i,'fbhe_shoot'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_shoot_n'] = fbhe_vector[3]
    player_df.at[i,'fbhe_shoot_per'] = fbhe_vector[3]/total_attempts
    bhe_vector = fbhe_attack_type( ppr_df, p_list[i], 'bang', False)
    player_df.at[i,'fbhe_bang'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_bang_n'] = fbhe_vector[3]
    player_df.at[i,'fbhe_bang_per'] = fbhe_vector[3]/total_attempts

    #--------------calculate in and out of system
    fbhe_vector = fbhe( ppr_df, p_list[i], 'pass', False)  
    tot_att = fbhe_vector[3]
    fbhe_vector = fbhe( ppr_df[ppr_df['pass_oos'] > 0], p_list[i], 'pass', False)
    player_df.at[i,'fbhe_oos'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_oos_n'] = fbhe_vector[3]
    fbhe_vector = fbhe( ppr_df[ppr_df['pass_oos'] == 0], p_list[i], 'pass', False)
    player_df.at[i,'fbhe_insys'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_insys_n'] = fbhe_vector[3]  
    if tot_att != 0:
      player_df.at[i,'fbhe_oos_per'] = player_df.at[i,'fbhe_oos_n']/tot_att 
      player_df.at[i,'fbhe_insys_per'] = player_df.at[i,'fbhe_insys_n']/tot_att 
    else:
      player_df.at[i,'fbhe_oos_per'] = None
      player_df.at[i,'fbhe_insys_per'] = None
      
    # ------------calculate transition Conversion ------------------
    trans_vector = calc_trans( ppr_df, p_list[i], 'all' )
    player_df.at[i,'tcr'] = float(trans_vector[0][:-1]) if trans_vector[0] else None
    trans_vector = calc_trans( ppr_df, p_list[i], 'srv' )
    player_df.at[i,'tcr_s'] = float(trans_vector[0][:-1]) if trans_vector[0] else None
    trans_vector = calc_trans( ppr_df, p_list[i], 'rcv' )
    player_df.at[i,'tcr_r'] = float(trans_vector[0][:-1]) if trans_vector[0] else None

    # -------------- calculate expected value ---------------
    ev_vector = calc_ev(ppr_df, p_list[i])
    player_df.at[i,'expected'] = float(ev_vector[0][:-1])

    # ----------------- calculate error density ----------
    ed_vector = calc_error_den( ppr_df, p_list[i] )
    player_df.at[i,'err_den'] = float(ed_vector[0][:-1])

    #-------------------- Serving Effectiviness, fbhe on all, zone 1, 3, 5
    #
    #       All Serves
    #
    fbhe_vector = fbhe(ppr_df, p_list[i], 'srv', False)
    player_df.at[i,'srv_fbhe'] = fbhe_vector[0]
    player_df.at[i,'srv_n'] = fbhe_vector[3]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSA" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    ace_n = tmp_df.shape[0]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSE" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    err_n = tmp_df.shape[0]
    player_df.at[i,'srv_ace_per'] = (ace_n / fbhe_vector[3]) if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv_err_per'] = err_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    #
    #        Serves from Zone 1
    #
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==1],p_list[i],'srv', False)
    player_df.at[i,'srv1_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv1_n'] = fbhe_vector[3]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSA" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 1 ]
    ace_n = tmp_df.shape[0]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSE" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 1 ]
    err_n = tmp_df.shape[0]
    player_df.at[i,'srv1_ace_per'] = ace_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv1_err_per'] = err_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    #
    #        Serves from Zone 3
    #
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==3],p_list[i],'srv', False)
    player_df.at[i,'srv3_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv3_n'] = fbhe_vector[3]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSA" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 3 ]
    ace_n = tmp_df.shape[0]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSE" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 3 ]
    err_n = tmp_df.shape[0]
    player_df.at[i,'srv3_ace_per'] = ace_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv3_err_per'] = err_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    #
    #        Serves from Zone 5
    #
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==5],p_list[i],'srv', False)
    player_df.at[i,'srv5_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv5_n'] = fbhe_vector[3]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSA" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 5 ]
    ace_n = tmp_df.shape[0]
    tmp_df = ppr_df[ ppr_df['point_outcome'] == "TSE" ]
    tmp_df = tmp_df[ tmp_df['serve_player'] == p_list[i]]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] == 5 ]
    err_n = tmp_df.shape[0]
    player_df.at[i,'srv5_ace_per'] = ace_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv5_err_per'] = err_n / fbhe_vector[3] if fbhe_vector[3] != 0 else None


    #------------------- FBHE when served from 1, 3, 5
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==1],p_list[i],'pass', False)
    player_df.at[i,'fbhe_srv1'] = fbhe_vector[0]  if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_srv1_n'] = fbhe_vector[3]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==3],p_list[i],'pass', False)
    player_df.at[i,'fbhe_srv3'] = fbhe_vector[0]  if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_srv3_n'] = fbhe_vector[3]
    fbhe_vector = fbhe(ppr_df[ppr_df['serve_src_zone_net']==5],p_list[i],'pass', False)
    player_df.at[i,'fbhe_srv5'] = fbhe_vector[0]  if fbhe_vector[3] != 0 else None
    player_df.at[i,'fbhe_srv5_n'] = fbhe_vector[3]

    #--------------------Opponent's FBHE when we are serving, all, then zones 1,3,5
    fbhe_vector = fbhe(ppr_df,p_list[i],'srv', False)
    player_df.at[i,'srv_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv_fbhe_n'] = fbhe_vector[3]
    srv_att = ppr_df[ (ppr_df['serve_player'] == p_list[i]) ].shape[0]
    if srv_att != 0:
      player_df.at[i,'srv_ace_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSA")].shape[0] / srv_att
      player_df.at[i,'srv_err_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSE")].shape[0] / srv_att
    else:
      player_df.at[i,'srv_ace_per'] = None
      player_df.at[i,'srv_err_per'] = None

    fbhe_vector = fbhe(ppr_df[ ppr_df['serve_src_zone_net'] == 1],p_list[i],'srv', False)
    player_df.at[i,'srv1_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv1_fbhe_n'] = fbhe_vector[3]
    srv_att = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['serve_src_zone_net'] == 1 ) ].shape[0]
    if srv_att != 0:
      player_df.at[i,'srv1_ace_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSA") & (ppr_df['serve_src_zone_net'] == 1) ].shape[0] / srv_att
      player_df.at[i,'srv1_err_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSE") & (ppr_df['serve_src_zone_net'] == 1) ].shape[0] / srv_att
    else:
      player_df.at[i,'srv1_ace_per'] = None
      player_df.at[i,'srv1_err_per'] = None

    fbhe_vector = fbhe(ppr_df[ ppr_df['serve_src_zone_net'] == 3],p_list[i],'srv', False)
    player_df.at[i,'srv3_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv3_fbhe_n'] = fbhe_vector[3]
    srv_att = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['serve_src_zone_net'] == 3 ) ].shape[0]
    if srv_att != 0:
      player_df.at[i,'srv3_ace_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSA") & (ppr_df['serve_src_zone_net'] == 3) ].shape[0] / srv_att
      player_df.at[i,'srv3_err_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSE") & (ppr_df['serve_src_zone_net'] == 3) ].shape[0] / srv_att
    else:
      player_df.at[i,'srv3_ace_per'] = None
      player_df.at[i,'srv3_err_per'] = None

    fbhe_vector = fbhe(ppr_df[ ppr_df['serve_src_zone_net'] == 5],p_list[i],'srv', False)
    player_df.at[i,'srv5_fbhe'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None
    player_df.at[i,'srv5_fbhe_n'] = fbhe_vector[3]
    srv_att = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['serve_src_zone_net'] == 5 ) ].shape[0]
    if srv_att != 0:
      player_df.at[i,'srv5_ace_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSA") & (ppr_df['serve_src_zone_net'] == 5) ].shape[0] / srv_att
      player_df.at[i,'srv5_err_per'] = ppr_df[ (ppr_df['serve_player'] == p_list[i]) & (ppr_df['point_outcome'] == "TSE") & (ppr_df['serve_src_zone_net'] == 5) ].shape[0] / srv_att
    else:
      player_df.at[i,'srv5_ace_per'] = None
      player_df.at[i,'srv5_err_per'] = None
      

  ########## end of loop over players
  #print(f"Player Df when done:{player_df}")

  # Now store and calulate the statistical values
  #------------------------------------------------
  player_stats_df.at[0,"fbhe_mean"] = player_df['fbhe'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_stdev"] = player_df['fbhe'].std(skipna=True)  
  player_stats_df.at[0,"fbhe_range_mean"] = player_df['fbhe_range'].mean(skipna=True) 
  player_stats_df.at[0,"fbhe_range_stdev"] = player_df['fbhe_range'].std(skipna=True)
  
  player_stats_df.at[0,"fbhe1_mean"] = player_df['fbhe1'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_stdev"] = player_df['fbhe1'].std(skipna=True)
  player_stats_df.at[0,"fbhe2_mean"] = player_df['fbhe2'].mean(skipna=True)
  player_stats_df.at[0,"fbhe2_stdev"] = player_df['fbhe2'].std(skipna=True)  
  player_stats_df.at[0,"fbhe3_mean"] = player_df['fbhe3'].mean(skipna=True)
  player_stats_df.at[0,"fbhe3_stdev"] = player_df['fbhe3'].std(skipna=True)  
  player_stats_df.at[0,"fbhe4_mean"] = player_df['fbhe4'].mean(skipna=True)
  player_stats_df.at[0,"fbhe4_stdev"] = player_df['fbhe4'].std(skipna=True)  
  player_stats_df.at[0,"fbhe5_mean"] = player_df['fbhe5'].mean(skipna=True)
  player_stats_df.at[0,"fbhe5_stdev"] = player_df['fbhe5'].std(skipna=True)  

  player_stats_df.at[0,"fbhe_behind_mean"] = player_df['fbhe_behind'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_behind_stdev"] = player_df['fbhe_behind'].std(skipna=True)
  player_stats_df.at[0,"fbhe_option_mean"] = player_df['fbhe_option'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_option_stdev"] = player_df['fbhe_option'].std(skipna=True)
  player_stats_df.at[0,"fbhe_tempo_mean"] = player_df['fbhe_tempo'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_tempo_stdev"] = player_df['fbhe_tempo'].std(skipna=True)

  player_stats_df.at[0,"fbhe_poke_mean"] = player_df['fbhe_poke'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_poke_stdev"] = player_df['fbhe_poke'].std(skipna=True)
  player_stats_df.at[0,"fbhe_shoot_mean"] = player_df['fbhe_shoot'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_shoot_stdev"] = player_df['fbhe_shoot'].std(skipna=True)
  player_stats_df.at[0,"fbhe_bang_mean"] = player_df['fbhe_bang'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_bang_stdev"] = player_df['fbhe_bang'].std(skipna=True)

  player_stats_df.at[0,"fbhe_oos_mean"] = player_df['fbhe_oos'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_oos_stdev"] = player_df['fbhe_oos'].std(skipna=True)
  player_stats_df.at[0,"fbhe_insys_mean"] = player_df['fbhe_insys'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_insys_stdev"] = player_df['fbhe_insys'].std(skipna=True)
  player_stats_df.at[0,"fbhe_oos_per_mean"] = player_df['fbhe_oos_per'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_oos_per_stdev"] = player_df['fbhe_oos_per'].std(skipna=True)
  player_stats_df.at[0,"fbhe_insys_per_mean"] = player_df['fbhe_insys_per'].mean(skipna=True)
  player_stats_df.at[0,"fbhe1_insys_per_stdev"] = player_df['fbhe_insys_per'].std(skipna=True)

  player_stats_df.at[0,"fbhe_srv1_mean"] = player_df['fbhe_srv1'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_srv1_stdev"] = player_df['fbhe_srv1'].std(skipna=True)
  player_stats_df.at[0,"fbhe_srv3_mean"] = player_df['fbhe_srv3'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_srv3_stdev"] = player_df['fbhe_srv3'].std(skipna=True)  
  player_stats_df.at[0,"fbhe_srv5_mean"] = player_df['fbhe_srv5'].mean(skipna=True)
  player_stats_df.at[0,"fbhe_srv5_stdev"] = player_df['fbhe_srv5'].std(skipna=True) 
  
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

  player_stats_df.at[0,"srv_fbhe_mean"] = player_df['srv_fbhe'].mean(skipna=True)
  player_stats_df.at[0,"srv_fbhe_stdev"] = player_df['srv_fbhe'].std(skipna=True)
  player_stats_df.at[0,"srv1_fbhe_mean"] = player_df['srv1_fbhe'].mean(skipna=True)
  player_stats_df.at[0,"srv1_fbhe_stdev"] = player_df['srv1_fbhe'].std(skipna=True)
  player_stats_df.at[0,"srv3_fbhe_mean"] = player_df['srv3_fbhe'].mean(skipna=True)
  player_stats_df.at[0,"srv3_fbhe_stdev"] = player_df['srv3_fbhe'].std(skipna=True)  
  player_stats_df.at[0,"srv5_fbhe_mean"] = player_df['srv5_fbhe'].mean(skipna=True)
  player_stats_df.at[0,"srv5_fbhe_stdev"] = player_df['srv5_fbhe'].std(skipna=True) 
  
  # now lets store our player_data file back as a csv file in the database
  #---------------------------------------------------------------------------
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  player_csv_file = pd.DataFrame.to_csv(player_df)
  player_media = anvil.BlobMedia(content_type="text/plain", content=player_csv_file.encode(), name="player_data.csv")

  player_stats_csv = pd.DataFrame.to_csv(player_stats_df)
  player_stats_media = anvil.BlobMedia(content_type="text/plain", content=player_stats_csv.encode(), name="player_sats.csv")
  
  ppr_csv_row.update( player_data = player_media, player_data_stats=player_stats_media )
  
  return result_string

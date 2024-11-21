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
def calc_triangle_scoring_background( c_league, c_gender, c_year):
  # 
  # calculate the player data files for all teams in the league, gender, year given
  #

  print(f'Calling Background Task calculate_triangle_scoring for {c_league},{c_gender},{c_year}')
  task = anvil.server.launch_background_task('calculate_triangle_scoring', c_league, c_gender, c_year)

  # check return status
  print(f' Background Task, Task id:{task.get_id()} return Status:{task.get_termination_status()}')
  
  return task

#----------------------------
#
# Define the background task to calculate the player_data for a single league
#
#---------------------------------------------
@anvil.server.background_task
def calculate_triangle_scoring( c_league, c_gender, c_year):
  result_string = "Calculate Triangle Scoring server module Called"

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

  # now, how many matches do we have, looking at video_id:
  m_list = ppr_df['video_id'].unique()
  num_matches = m_list.shape[0]
  #print(f"Match List: #{num_matches}")
  #print(m_list)

  # build the ppr_dataframe out tpo the proper number of rows, equal total points,
  # His should make a blank (except for flist_r values) ppr dataframe with the correct number of rows (maybe one extra due to a 0 start)

  tri_dict = {'video_id':[str()],'set':[int()],'filename':[str()],
              'teama':None, 'player_a1':None, 'player_a2':None, 'teamb':None, 'player_b1':None, 'player_b2':None,
              'total_pts':None, 'pts_teama':None, 'pts_teamb':None, 
              'tsa_a':None, 'tse_a':None, 'srv_num_a':None, 'tsa_b':None, 'tse_b':None, 'srv_num_b':None, 
              'tsrv_pts_a':None, 'tsrv_pts_b':None, 'tsrv_adv_a':None, 'tsrv_pts':None, 
              'fbk_a':None, 'fbe_a':None, 'fbk_b':None, 'fbe_b':None, 'fb_pts_a':None, 'fb_pts_b':None, 'fb_adv_a':None, 'fb_pts':None, 
              'tk_a':None, 'te_a':None, 'tk_b':None, 'te_b':None, 'tran_pts_a':None, 'tran_pts_b':None, 'tran_adv_a':None, 'tran_pts':None, 
              'ts_pts_per':None, 'fb_pts_per':None, 'tran_pts_per':None, 
              'fbhe_a_noace':None, 'fbhe_b_noace':None, 'fbhe_a_withace':None, 'fbhe_b_withace':None, 'tcr_a':None, 'tcr_b':None, 'err_den_a':None, 'err_den_b':None, 
              'err_den_criteria_met_a':None, 'err_den_criteria_met_b':None,'tcr_criteria_met_a':None,'tcr_criteria_met_b':None,
              'winning_team':None,'win_fbhe_noace':None,'win_fbhe_withace':None,'win_tcr':None, 'point_diff':None,
              'loser_fbhe_noace':None,'loser_fbhe_withace':None,
              'fbhe_diff_noace':None,'fbhe_diff_withace':None,'win_err_den_criteria_met':None, 'win_tcr_criteria_met':None,
              'assume_met_noace':None, 'assume_met_noace_per':None, 'assume_met_withace':None, 'assume_met_withace_per':None
             }
  #print(f"Player Dict:{player_dict}")
  tri_df = pd.DataFrame.from_records(tri_dict)
  #player_df = pd.DataFrame(player_dict, columns=['player', 'fbhe', 'fbhe1','fbhe2','fbhe3','fbhe4','fbhe5'])
  
  #for i in  range(1,num_matches):
  #  tri_df.loc[max(tri_df.index)+1] = tri_dict

  for i in range(0,num_matches):
    teama_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    teama_list = teama_list['teama'].unique()
    print(f'team a list {teama_list}')
    teama = teama_list[0]
    print(f'teama {teama}')
    teamb_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    teamb_list = teamb_list['teamb'].unique()
    print(f'team a list {teamb_list}')
    teamb = teamb_list[0]
    print(f'teamb {teamb}')

    player_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    player_a1 = player_list['player_a1'].unique()[0]
    player_a2 = player_list['player_a2'].unique()[0]
    player_b1 = player_list['player_b1'].unique()[0]
    player_b2 = player_list['player_b2'].unique()[0]

    for s in [1,2,3]:
      set_df = ppr_df[ppr_df['video_id'] == m_list[i]]  # limit to this video match
      set_df = set_df[set_df['set'] == s ] # limit to this set
      if (set_df.shape[0] != 0):
        # then we have a valid set to work with

        # add a row for this set
        tri_df.loc[max(tri_df.index)+1] = tri_dict
        tri_row = tri_df.shape[0]-1
        #print(tri_df)

        # save video id and set number
        tri_df.at[tri_row,'video_id'] = m_list[i]
        tri_df.at[tri_row,'set'] = s
        teama = set_df.at[1,'teama']
        player_a1 = set_df.at[1,'player_a1']
        player_a2 = set_df.at[1,'player_a2']
        tri_df.at[tri_row,'teama'] = teama
        tri_df.at[tri_row,'player_a1'] =  player_a1
        tri_df.at[tri_row,'player_a2'] =  player_a2
        teamb =  set_df.at[0,'teamb']
        player_b1 = set_df.at[0,'player_b1']
        player_b2 = set_df.at[0,'player_b2']
        tri_df.at[tri_row,'teamb'] =  teamb
        tri_df.at[tri_row,'player_b1'] =  player_b1
        tri_df.at[tri_row,'player_b2'] =  player_b2
        tri_df.at[tri_row,'total_pts'] = set_df.shape[0]
        print(f"I = {i}, Video id:{m_list[i]}, Set: {s}, Tri Row:{tri_row} Tri_df Shape:{tri_df.shape[0]}, max index:{max(tri_df.index)}, Set size:{set_df.shape[0]}, Team a/b:{teama,teamb}")

        # terminal Serving
        tmp_df = set_df[set_df['point_outcome'] == "TSA" ]
        tri_df.at[tri_row,'tsa_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'tsa_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tmp_df = set_df[set_df['point_outcome'] == "TSE" ]
        tri_df.at[tri_row,'tse_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'tse_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tri_df.at[tri_row,'srv_num_a'] = tmp_df[tmp_df['serve_player'] == player_a1 ].shape[0] + set_df[set_df['serve_player'] == player_a2 ].shape[0]
        tri_df.at[tri_row,'srv_num_b'] = tmp_df[tmp_df['serve_player'] == player_b1 ].shape[0] + set_df[set_df['serve_player'] == player_b2 ].shape[0]
        tri_df.at[tri_row,'tsrv_pts_a'] = tri_df.at[tri_row,'tsa_a'] + tri_df.at[tri_row,'tse_b']
        tri_df.at[tri_row,'tsrv_pts_b'] = tri_df.at[tri_row,'tsa_b'] + tri_df.at[tri_row,'tse_a']        
        tri_df.at[tri_row,'tsrv_adv_a'] = tri_df.at[tri_row,'tsrv_pts_a'] - tri_df.at[tri_row,'tsrv_pts_b']
        tri_df.at[tri_row,'tsrv_pts'] = tri_df.at[tri_row,'tsrv_pts_a'] + tri_df.at[tri_row,'tsrv_pts_b']
        print(f"Terminal Serve: Ace {tri_df.at[tri_row,'tsa_a'],tri_df.at[tri_row,'tsa_b']} Terminal Serve Error: {tri_df.at[tri_row,'tse_a'],tri_df.at[tri_row,'tse_b']}")
        
        # first ball
        tmp_df = set_df[set_df['point_outcome'] == "FBK" ]
        tri_df.at[tri_row,'fbk_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'fbk_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tmp_df = set_df[set_df['point_outcome'] == "FBE" ]
        tri_df.at[tri_row,'fbe_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'fbe_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0]  
        tri_df.at[tri_row,'fb_pts_a'] = tri_df.at[tri_row,'fbk_a'] + tri_df.at[tri_row,'fbe_b']
        tri_df.at[tri_row,'fb_pts_b'] = tri_df.at[tri_row,'fbk_b'] + tri_df.at[tri_row,'fbe_a']        
        tri_df.at[tri_row,'fb_adv_a'] = tri_df.at[tri_row,'fb_pts_a'] - tri_df.at[tri_row,'fb_pts_b']  
        tri_df.at[tri_row,'fb_pts'] = tri_df.at[tri_row,'fb_pts_a'] + tri_df.at[tri_row,'fb_pts_b']  
        print(f"First Ball: Kill {tri_df.at[tri_row,'fbk_a'],tri_df.at[tri_row,'fbk_b']} Error: {tri_df.at[tri_row,'fbe_a'],tri_df.at[tri_row,'fbe_b']}")

        # Transition
        tmp_df = set_df[set_df['point_outcome'] == "TK" ]
        tri_df.at[tri_row,'tk_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'tk_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tmp_df = set_df[set_df['point_outcome'] == "TE" ]
        tri_df.at[tri_row,'te_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'te_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0]  
        tri_df.at[tri_row,'tran_pts_a'] = tri_df.at[tri_row,'tk_a'] + tri_df.at[tri_row,'te_b']
        tri_df.at[tri_row,'tran_pts_b'] = tri_df.at[tri_row,'tk_b'] + tri_df.at[tri_row,'te_a']        
        tri_df.at[tri_row,'tran_adv_a'] = tri_df.at[tri_row,'tran_pts_a'] - tri_df.at[tri_row,'tran_pts_b']  
        tri_df.at[tri_row,'tran_pts'] = tri_df.at[tri_row,'tran_pts_a'] + tri_df.at[tri_row,'tran_pts_b'] 
        print(f"Transition: Kill {tri_df.at[tri_row,'tk_a'],tri_df.at[tri_row,'tk_b']} Error: {tri_df.at[tri_row,'te_a'],tri_df.at[tri_row,'te_b']}")
        
        # Total points and percenta
        tri_df.at[tri_row,'total_pts'] = tri_df.at[tri_row,'tsrv_pts'] + tri_df.at[tri_row,'fb_pts'] + tri_df.at[tri_row,'tran_pts']
        tri_df.at[tri_row,'tsrv_pts_per'] = tri_df.at[tri_row,'tsrv_pts'] / tri_df.at[tri_row,'total_pts']
        tri_df.at[tri_row,'fb_pts_per'] = tri_df.at[tri_row,'fb_pts'] / tri_df.at[tri_row,'total_pts']
        tri_df.at[tri_row,'tran_pts_per'] = tri_df.at[tri_row,'tran_pts'] / tri_df.at[tri_row,'total_pts']

        # count points and find a winner!
        teama_pts = tri_df.at[tri_row,'tran_pts_a'] + tri_df.at[tri_row,'tsrv_pts_a'] + tri_df.at[tri_row,'fb_pts_a']
        teamb_pts = tri_df.at[tri_row,'tran_pts_b'] + tri_df.at[tri_row,'tsrv_pts_b'] + tri_df.at[tri_row,'fb_pts_b']
        tri_df.at[tri_row,'winning_team'] = teama if teama_pts > teamb_pts else teamb
        tri_df.at[tri_row,'point_diff'] = teama_pts - teamb_pts if teama_pts > teamb_pts else teamb_pts - teama_pts
    

        # fbhe and tcr
        tri_df.at[tri_row,'fbhe_a_noace'] = ( tri_df.at[tri_row,'fbk_a'] - tri_df.at[tri_row,'fbe_a'] ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tsa_b'] - tri_df.at[tri_row,'tse_b'] )
        tri_df.at[tri_row,'fbhe_b_noace'] = ( tri_df.at[tri_row,'fbk_b'] - tri_df.at[tri_row,'fbe_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tsa_a'] - tri_df.at[tri_row,'tse_a'] )
        tri_df.at[tri_row,'fbhe_a_withace'] = ( tri_df.at[tri_row,'fbk_a'] - tri_df.at[tri_row,'fbe_a'] ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tse_b'] )
        tri_df.at[tri_row,'fbhe_b_withace'] = ( tri_df.at[tri_row,'fbk_b'] - tri_df.at[tri_row,'fbe_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tse_a'] )

        tri_df.at[tri_row,'tcr_a'] = ( tri_df.at[tri_row,'tk_a'] + tri_df.at[tri_row,'te_b'] ) / ( tri_df.at[tri_row,'tran_pts'] )   
        tri_df.at[tri_row,'tcr_b'] = ( tri_df.at[tri_row,'tk_b'] + tri_df.at[tri_row,'te_a'] ) / ( tri_df.at[tri_row,'tran_pts'] )   

        tri_df.at[tri_row,'err_den_a'] = ( tri_df.at[tri_row,'te_a'] + tri_df.at[tri_row,'fbe_a'] + tri_df.at[tri_row,'tse_a']) / ( tri_df.at[tri_row,'total_pts'] )   
        tri_df.at[tri_row,'err_den_b'] = ( tri_df.at[tri_row,'te_b'] + tri_df.at[tri_row,'fbe_b'] + tri_df.at[tri_row,'tse_b']) / ( tri_df.at[tri_row,'total_pts'] )   

        tri_df.at[tri_row,'win_fbhe_noace'] = tri_df.at[tri_row,'fbhe_a_noace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_b_noace']
        tri_df.at[tri_row,'loser_fbhe_noace'] = tri_df.at[tri_row,'fbhe_b_noace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_a_noace']
        tri_df.at[tri_row,'win_fbhe_withace'] = tri_df.at[tri_row,'fbhe_a_withace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_b_withace']
        tri_df.at[tri_row,'loser_fbhe_withace'] = tri_df.at[tri_row,'fbhe_b_withace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_b_withace']
        tri_df.at[tri_row,'win_tcr'] = tri_df.at[tri_row,'tcr_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'tcr_b']

        tri_df.at[tri_row,'fbhe_diff_noace'] = tri_df.at[tri_row,'win_fbhe_noace'] - tri_df.at[tri_row,'loser_fbhe_noace']
        tri_df.at[tri_row,'fbhe_diff_withace'] = tri_df.at[tri_row,'win_fbhe_withace'] - tri_df.at[tri_row,'loser_fbhe_withace']

        # need to find the 25th and 75th percentile for error density, and TCR to see if conditions are met.

        # End if statment for the set
      # end loop over sets
    # end loop over video id's, or match
    
  # now lets store our triangle stats data file back as a csv file in the database
  #---------------------------------------------------------------------------
  # first, I need to change the ppr_file dataframe to a csv file.
  tri_csv_file = pd.DataFrame.to_csv(tri_df)
  tri_media = anvil.BlobMedia(content_type="text/plain", content=tri_csv_file.encode(), name="triangle_data.csv")

  ppr_csv_row.update( tri_data = tri_media )
  
  return result_string

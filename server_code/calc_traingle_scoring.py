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
from datetime import datetime, timedelta, date

# ########## Calculate league summaries, stored as player data
#
#  player_data is only for league entries in the ppr_csv_tables table.  We also sotre the list of player data stats

# begin with the server callable task, this then provides status and calls the background task
@anvil.server.callable
def calc_triangle_scoring_background( c_league, c_gender, c_year):
  # 
  # calculate the player data files for all teams in the league, gender, year given
  #

  #print(f'Calling Background Task calculate_triangle_scoring for {c_league},{c_gender},{c_year}')
  task = anvil.server.launch_background_task('calculate_triangle_scoring', c_league, c_gender, c_year)

  # check return status
  #print(f' Background Task, Task id:{task.get_id()} return Status:{task.get_termination_status()}')
  
  return task

#----------------------------
#
# Define the background task to calculate the player_data for a single league
#
#---------------------------------------------
@anvil.server.background_task
def calculate_triangle_scoring( c_league, c_gender, c_year):
  return calculate_triangle_scoring_not_background( c_league, c_gender, c_year)

def calculate_triangle_scoring_not_background( c_league, c_gender, c_year):
  result_string = "Calculate Triangle Scoring server module Called"


  c_team = "League"    # only updating the league tables
  #print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")

  # open out data files, get the correct row, then also get ppd_df, and player_data_df
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
    ) )

  ppr_df = get_ppr_data( c_league, c_gender, c_year, c_team, True)  
  player_df, player_stats_df = get_player_data(c_league, c_gender, c_year)

  # calculate data from plalyer_data
  err_den_mean = player_df['err_den'].mean(skipna=True, numeric_only=True)
  err_den_sd = player_df['err_den'].std(skipna=True, numeric_only=True)
  err_den_25 = ((-0.674)*err_den_sd + err_den_mean)*2/100
  err_den_75 = ((0.674)*err_den_sd + err_den_mean)*2/100
  tcr_mean = player_df['tcr'].mean(skipna=True, numeric_only=True)
  tcr_sd = player_df['tcr'].std(skipna=True, numeric_only=True)
  tcr_25 = ((-0.674)*tcr_sd + tcr_mean)/100
  tcr_75 = ((0.674)*tcr_sd + tcr_mean)/100
  #print(f"Stats: Err Den: {err_den_25,err_den_75}, TCR: {tcr_25,tcr_75}")
  
  #print(f"shape of ppr_df :{ppr_df.shape}")
  min_att = ppr_csv_row['min_att']


  # now, how many matches do we have, looking at video_id:
  m_list = ppr_df['video_id'].unique()
  num_matches = m_list.shape[0]
  #print(f"Match List: #{num_matches}")
  #print(m_list)

  # build the ppr_dataframe out tpo the proper number of rows, equal total points,
  # His should make a blank (except for flist_r values) ppr dataframe with the correct number of rows (maybe one extra due to a 0 start)

  tri_dict = {'video_id':[str()],'set':[int()],'game_date':None,
              'teama':None, 'player_a1':None, 'player_a2':None, 
              'teamb':None, 'player_b1':None, 'player_b2':None,
              'total_pts':None, 'teama_pts':None, 'teamb_pts':None, 
              # Serving point totals for team A and B
              'tsa_a':None, 'tse_a':None, 'srv_num_a':None, 'tsa_b':None, 'tse_b':None, 'srv_num_b':None, 
              'tsrv_pts_a':None, 'tsrv_pts_b':None, 'tsrv_adv_a':None, 'tsrv_pts':None, 
              'ace_error_a':None,'ace_error_b':None,
              # first point totals for team A and B
              'fbk_a':None, 'fbe_a':None, 'fbk_b':None, 'fbe_b':None, 'fb_pts_a':None, 'fb_pts_b':None, 'fb_adv_a':None, 'fb_pts':None, 
              # Transition data for team A and B
              'tk_a':None, 'te_a':None, 'tk_b':None, 'te_b':None, 'tran_pts_a':None, 'tran_pts_b':None, 'tran_adv_a':None, 'tran_pts':None,
              # point totals
              'tran_pts_per':None, 'fb_pts_per':None, 'tsrv_pts_per':None,
              # FBHE and FBSO for A and b
              'fbhe_a_noace':None, 'fbhe_b_noace':None, 'fbhe_a_withace':None, 'fbhe_b_withace':None, 'fbso_a_noace':None, 'fbso_b_noace':None, 'fbso_a_withace':None, 'fbso_b_withace':None, 'eso_a':None, 'eso_b':None,
              # Transition and errors for A and B
              'tcr_a':None, 'tcr_b':None, 'err_den_a':None, 'err_den_b':None, 't_eff_a':None, 't_eff_b':None,
              # Winning team metrics
              'winning_team':None,'win_fbhe_noace':None,'win_fbhe_withace':None,'win_tcr':None, 'win_err_den':None, 'win_ace_error':None, 'win_fbso_noace':None, 
              'win_fbso_withace':None, 'win_t_eff':None, 'win_eso':None,'win_knockout':None,'win_goodpass':None,
              'point_diff':None, 
              # Losing team metrics
              'losing_team':None, 'loser_tcr':None, 'loser_err_den':None,'loser_fbhe_noace':None,'loser_fbhe_withace':None,'loser_ace_error':None, 
              'loser_fbso_noace':None, 'loser_fbso_withace':None, 'loser_eso':None, 'loser_t_eff':None, 'loser_knockout':None,'loser_goodpass':None,
              # differencees between winner and loser (winner - loser)
              'fbhe_diff_noace':None,'fbhe_diff_withace':None,'fbso_diff_noace':None,'fbso_diff_withace':None,'eso_diff':None,'ace_error_diff':None,'t_eff_diff':None,
              'knockout_diff':None,'goodpass_diff':None, 'tcr_diff':None,'err_den_diff':None,

              # kncokout and goodpass:-)
              'knockout_a':None, 'knockout_b':None, 'win_knockout':None, 'loser_knockout':None, 
              'goodpass_a':None,'goodpass_b':None,'win_goodpass':None,'loser_goodpass':None,
             }
  '''
  Dictionary Definitions  
              game_date, set : we have one row per set in the database
              'total_pts': Total Points Scored in the set
              'teama_pts': Total point by team A
              teamb_pts': Total Point by team B
            Serving
              'tsa_a': Aces by A
              'tse_a': Errors by A
              'srv_num_a': TOtal Serves by A
              'tsa_b': Aces by B
              'tse_b': Errors by B
              'srv_num_b': Total Serves by B
              'tsrv_pts_a': A's terminal serve poitns (A's aces + B's errors)
              'tsrv_pts_b': B's terminal serve points
              'tsrv_adv_a': A's advantage (A - B)
              'tsrv_pts': Total Terminal Serve Points
              'ace_error_a': ace to error ratio, A
              'ace_error_b': ace to error ratio, B
              
            First Ball
              'fbk_a':None, FBK by A
              fbe_a':None, FB Errors by A
              'fbk_b':None, FBK by B
              'fbe_b':None, First BAll Errors by B
              'fb_pts_a': First Ball Poitns for A (A kills + B Errors)
              'fb_pts_b': First BAll Poiints B
              'fb_adv_a': Adv A (A pts - B pts)
              'fb_pts': Total First Ball Points
              
            Transition
              'tk_a': Transition Kills A
              'te_a' Transition errors A
              'tk_b': Transition Kills B
              'te_b': Transition Errors B
              'tran_pts_a': Transition Poiints A (A kills + B errors)
              'tran_pts_b': Transition Points B
              'tran_adv_a': Advantage A (A - B)
              'tran_pts': total Transition Points
              
            KNockout and Good Pass
              'knockout_a'
              'knockout_b'
              'good_pass_a'
              'good_pass_b'
              
            Point Percentages
              'total_pts' : Total Poitns (TS + FB + TR)
              'terv_pts_per' : Percent of points taht are Terminal Serve
              'tran_pts_per': Percent of total points that are Transition
              'fb_pts_per': Percent of Total Points taht are Frist Ball
              
            FBHE, TCR, and ED
              'fbhe_a_noace':FBHE for A
              'fbhe_b_noace':FBHE for B
              'fbhe_a_withace': FBHE for A
              'fbhe_b_withace': FHE for B
              'tcr_a': TCR for Team A
              'tcr_b': TCR for Team B
              'err_den_a': Error Den A
              'err_den_b':Error Den B
              
            Winning and Losing teams
              'winning_team':Text of wining team
              'win_fbhe_noace': FBHE of Winning Team
              'win_fbhe_withace':FBHE of Winning Team
              'win_tcr': TCR of Winning Team
              'win_err_den': Err Den opf winning Team
              'win_knockout' : Winner's knockout rate
              'win_goodpass' : WInner's good pass rate
              'win_ace_error': Winner Ace to Error Ratio
              
              'point_diff': ABS of points difference (always Postive)

              'loser_fbhe_noace': :Loser's FBHE
              'loser_fbhe_withace':Loser FBHE
              'loser_tcr': Loser's TCR
              'loser_err_den': Loser's Err Den
              'loser_knockout': Loser's knockout rate
              'loser_goodpass' : Loser's good pass rate
              'loser_ace_error': Loser's Ace to Error Ratio
              
              'fbhe_diff_noace': FBHE, winner's - loser's
              'fbhe_diff_withace': FBHE, winners's - loser's
              
  '''
  #print(f"Player Dict:{player_dict}")
  tri_df = pd.DataFrame.from_records(tri_dict)
  #player_df = pd.DataFrame(player_dict, columns=['player', 'fbhe', 'fbhe1','fbhe2','fbhe3','fbhe4','fbhe5'])
  
  #for i in  range(1,num_matches):
  #  tri_df.loc[max(tri_df.index)+1] = tri_dict

  for i in range(0,num_matches):
    teama_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    teama_list = teama_list['teama'].unique()
    #print(f'team a list {teama_list}')
    teama = teama_list[0]
    #print(f'teama {teama}')
    teamb_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    teamb_list = teamb_list['teamb'].unique()
    #print(f'team a list {teamb_list}')
    teamb = teamb_list[0]
    #print(f'teamb {teamb}')

    player_list = ppr_df[ ppr_df['video_id'] == m_list[i] ]
    player_a1 = player_list['player_a1'].unique()[0]
    player_a2 = player_list['player_a2'].unique()[0]
    player_b1 = player_list['player_b1'].unique()[0]
    player_b2 = player_list['player_b2'].unique()[0]
    #print(f"Teams: {teama, teamb}, Players: {player_a1, player_a2, player_b1, player_b2}")

    for s in [1,2,3]:
      set_df = ppr_df[ppr_df['video_id'] == m_list[i]]  # limit to this video match
      set_df = set_df[set_df['set'] == s ] # limit to this set
      if (set_df.shape[0] > 8):
        # then we have a valid set to work with

        # add a row for this set
        tri_df.loc[max(tri_df.index)+1] = tri_dict
        tri_row = tri_df.shape[0]-2
        #print(tri_df)

        # save video id and set number
        tri_df.at[tri_row,'video_id'] = m_list[i]
        tri_df.at[tri_row,'set'] = s
        #print(f"tri_df: Saving the date, set_df date : {set_df['game_date']}")
        tri_df.at[tri_row,'game_date'] = set_df['game_date'].iloc[0]
        tri_df.at[tri_row,'teama'] = teama
        tri_df.at[tri_row,'player_a1'] =  player_a1
        tri_df.at[tri_row,'player_a2'] =  player_a2
        tri_df.at[tri_row,'teamb'] =  teamb
        tri_df.at[tri_row,'player_b1'] =  player_b1
        tri_df.at[tri_row,'player_b2'] =  player_b2
        tri_df.at[tri_row,'total_pts'] = set_df.shape[0]
        #print(f"I = {i}, Video id:{m_list[i]}, Set: {s}, Tri Row:{tri_row} Tri_df Shape:{tri_df.shape[0]}, max index:{max(tri_df.index)}, Set size:{set_df.shape[0]}, Team a/b:{teama,teamb}")

        # terminal Serving
        tmp_df = set_df[set_df['point_outcome'] == "TSA" ]
        tri_df.at[tri_row,'tsa_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'tsa_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tmp_df = set_df[set_df['point_outcome'] == "TSE" ]
        tri_df.at[tri_row,'tse_a'] = tmp_df[ tmp_df['point_outcome_team'] == teama ].shape[0]
        tri_df.at[tri_row,'tse_b'] = tmp_df[ tmp_df['point_outcome_team'] == teamb ].shape[0] 
        tri_df.at[tri_row,'srv_num_a'] = set_df[set_df['serve_player'] == player_a1 ].shape[0] + set_df[set_df['serve_player'] == player_a2 ].shape[0]
        tri_df.at[tri_row,'srv_num_b'] = set_df[set_df['serve_player'] == player_b1 ].shape[0] + set_df[set_df['serve_player'] == player_b2 ].shape[0]
        tri_df.at[tri_row,'tsrv_pts_a'] = tri_df.at[tri_row,'tsa_a'] + tri_df.at[tri_row,'tse_b']
        tri_df.at[tri_row,'tsrv_pts_b'] = tri_df.at[tri_row,'tsa_b'] + tri_df.at[tri_row,'tse_a']        
        tri_df.at[tri_row,'tsrv_adv_a'] = tri_df.at[tri_row,'tsrv_pts_a'] - tri_df.at[tri_row,'tsrv_pts_b']
        tri_df.at[tri_row,'tsrv_pts'] = tri_df.at[tri_row,'tsrv_pts_a'] + tri_df.at[tri_row,'tsrv_pts_b']
        #print(f"Terminal Serve: Ace {tri_df.at[tri_row,'tsa_a'],tri_df.at[tri_row,'tsa_b']} Terminal Serve Error: {tri_df.at[tri_row,'tse_a'],tri_df.at[tri_row,'tse_b']}")
        if tri_df.at[tri_row,'tse_a'] != 0:
          tri_df.at[tri_row,'ace_error_a'] = tri_df.at[tri_row,'tsa_a']/tri_df.at[tri_row,'tse_a']
        if tri_df.at[tri_row,'tse_b'] != 0:
          tri_df.at[tri_row,'ace_error_b'] = tri_df.at[tri_row,'tsa_b']/tri_df.at[tri_row,'tse_b']
        
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
        #print(f"First Ball: Kill {tri_df.at[tri_row,'fbk_a'],tri_df.at[tri_row,'fbk_b']} Error: {tri_df.at[tri_row,'fbe_a'],tri_df.at[tri_row,'fbe_b']}")

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
        #print(f"Transition: Kill {tri_df.at[tri_row,'tk_a'],tri_df.at[tri_row,'tk_b']} Error: {tri_df.at[tri_row,'te_a'],tri_df.at[tri_row,'te_b']}")
        
        # Total points and percenta
        tri_df.at[tri_row,'total_pts'] = tri_df.at[tri_row,'tsrv_pts'] + tri_df.at[tri_row,'fb_pts'] + tri_df.at[tri_row,'tran_pts']
        if ( tri_df.at[tri_row,'total_pts'] != 0 ):
          tri_df.at[tri_row,'tsrv_pts_per'] = tri_df.at[tri_row,'tsrv_pts'] / tri_df.at[tri_row,'total_pts']
          tri_df.at[tri_row,'fb_pts_per'] = tri_df.at[tri_row,'fb_pts'] / tri_df.at[tri_row,'total_pts']
          tri_df.at[tri_row,'tran_pts_per'] = tri_df.at[tri_row,'tran_pts'] / tri_df.at[tri_row,'total_pts']
        else:
          tri_df.at[tri_row,'tsrv_pts_per'] = None
          tri_df.at[tri_row,'fb_prs_per'] = None  
          tri_df.at[tri_row,'tran_pts_per'] = None

        # count points and find a winner!
        tri_df.at[tri_row,'teama_pts'] = tri_df.at[tri_row,'tran_pts_a'] + tri_df.at[tri_row,'tsrv_pts_a'] + tri_df.at[tri_row,'fb_pts_a']
        tri_df.at[tri_row,'teamb_pts'] = tri_df.at[tri_row,'tran_pts_b'] + tri_df.at[tri_row,'tsrv_pts_b'] + tri_df.at[tri_row,'fb_pts_b']
        tri_df.at[tri_row,'winning_team'] = teama if tri_df.at[tri_row,'teama_pts'] > tri_df.at[tri_row,'teamb_pts'] else teamb
        tri_df.at[tri_row,'losing_team'] = teama if tri_df.at[tri_row,'teama_pts'] <= tri_df.at[tri_row,'teamb_pts'] else teamb
        tri_df.at[tri_row,'point_diff'] = tri_df.at[tri_row,'teama_pts'] - tri_df.at[tri_row,'teamb_pts'] if tri_df.at[tri_row,'teama_pts'] > tri_df.at[tri_row,'teamb_pts'] else tri_df.at[tri_row,'teamb_pts'] - tri_df.at[tri_row,'teama_pts']
    

        # fbhe and tcr
        if ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tsa_b'] - tri_df.at[tri_row,'tse_b'] ) != 0:
          tri_df.at[tri_row,'fbhe_a_noace'] = ( tri_df.at[tri_row,'fbk_a'] - tri_df.at[tri_row,'fbe_a'] ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tsa_b'] - tri_df.at[tri_row,'tse_b'] )
          tri_df.at[tri_row,'fbso_a_noace'] = ( tri_df.at[tri_row,'fbk_a'] ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tsa_b'] - tri_df.at[tri_row,'tse_b'] )
        else:
          tri_df.at[tri_row,'fbhe_a_noace'] = None
          tri_df.at[tri_row,'fbso_a_noace'] = None
        if ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tsa_a'] - tri_df.at[tri_row,'tse_a'] ) != 0:
          tri_df.at[tri_row,'fbhe_b_noace'] = ( tri_df.at[tri_row,'fbk_b'] - tri_df.at[tri_row,'fbe_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tsa_a'] - tri_df.at[tri_row,'tse_a'] )
          tri_df.at[tri_row,'fbso_b_noace'] = ( tri_df.at[tri_row,'fbk_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tsa_a'] - tri_df.at[tri_row,'tse_a'] )
        else:
          tri_df.at[tri_row,'fbhe_b_noace'] = None
          tri_df.at[tri_row,'fbso_b_noace'] = None

        if ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tse_b'] ) != 0:
          tri_df.at[tri_row,'fbhe_a_withace'] = ( tri_df.at[tri_row,'fbk_a'] - tri_df.at[tri_row,'fbe_a'] ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tse_b'] )
          tri_df.at[tri_row,'fbso_a_withace'] = ( tri_df.at[tri_row,'fbk_a']  ) / ( tri_df.at[tri_row,'srv_num_b'] - tri_df.at[tri_row,'tse_b'] )
          eso_a_obj = calc_team_eso(ppr_df, teama)
          if eso_a_obj.get('status'):
            #print(f"saving eso_a {eso_a_obj.get('eso')}")
            tri_df.at[tri_row,'eso_a'] = eso_a_obj.get('eso')
        else:
          tri_df.at[tri_row,'fbhe_a_withace'] = None
          tri_df.at[tri_row,'fbso_a_withace'] = None
        if ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tse_a'] ) != 0:
          tri_df.at[tri_row,'fbhe_b_withace'] = ( tri_df.at[tri_row,'fbk_b'] - tri_df.at[tri_row,'fbe_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tse_a'] )
          tri_df.at[tri_row,'fbso_b_withace'] = ( tri_df.at[tri_row,'fbk_b'] ) / ( tri_df.at[tri_row,'srv_num_a'] - tri_df.at[tri_row,'tse_a'] )
          eso_b_obj = calc_team_eso(ppr_df, teamb)
          if eso_b_obj.get('status'):
            #print(f"saving eso_b {eso_b_obj.get('eso')}")
            tri_df.at[tri_row,'eso_b'] = eso_b_obj.get('eso')
        else:
          tri_df.at[tri_row,'fbhe_b_withace'] = None
          tri_df.at[tri_row,'fbso_b_withace'] = None
        
        if ( tri_df.at[tri_row,'tran_pts'] != 0 ):
          tri_df.at[tri_row,'tcr_a'] = ( tri_df.at[tri_row,'tk_a'] + tri_df.at[tri_row,'te_b'] ) / ( tri_df.at[tri_row,'tran_pts'] )   
          tri_df.at[tri_row,'t_eff_a'] = ( tri_df.at[tri_row,'tk_a'] - tri_df.at[tri_row,'te_a'] ) / ( tri_df.at[tri_row,'tran_pts'] )   
          tri_df.at[tri_row,'tcr_b'] = ( tri_df.at[tri_row,'tk_b'] + tri_df.at[tri_row,'te_a'] ) / ( tri_df.at[tri_row,'tran_pts'] )   
          tri_df.at[tri_row,'t_eff_b'] = ( tri_df.at[tri_row,'tk_b'] - tri_df.at[tri_row,'te_b'] ) / ( tri_df.at[tri_row,'tran_pts'] )  
        else:
          tri_df.at[tri_row,'tcr_a'] = None
          tri_df.at[tri_row,'tcr_b'] = None
          tri_df.at[tri_row,'t_eff_a'] = None
          tri_df.at[tri_row,'t_eff_b'] = None

        if ( tri_df.at[tri_row,'total_pts'] != 0 ):
          tri_df.at[tri_row,'err_den_a'] = ( tri_df.at[tri_row,'te_a'] + tri_df.at[tri_row,'fbe_a'] + tri_df.at[tri_row,'tse_a']) / ( tri_df.at[tri_row,'total_pts'] )   
          tri_df.at[tri_row,'err_den_b'] = ( tri_df.at[tri_row,'te_b'] + tri_df.at[tri_row,'fbe_b'] + tri_df.at[tri_row,'tse_b']) / ( tri_df.at[tri_row,'total_pts'] )   
        else:
          tri_df.at[tri_row,'err_den_a'] = None
          tri_df.at[tri_row,'err_den_b'] = None          

        # knockoout and goodpass
        tri_df.at[tri_row,'knockout_a'] = (calc_knock_out( set_df, player_a1) + calc_knock_out( set_df, player_a2))/2
        tri_df.at[tri_row,'knockout_b'] = (calc_knock_out(set_df, player_b1) + calc_knock_out( set_df, player_b2))/2
        tri_df.at[tri_row,'goodpass_a'] = ( calc_good_pass( set_df, player_a1) + calc_good_pass( set_df, player_a2))/2
        tri_df.at[tri_row,'goodpass_b'] = (calc_good_pass( set_df, player_b1) + calc_good_pass( set_df, player_b2))/2

        # calcualte winning and losing team variables
        tri_df.at[tri_row,'win_fbhe_noace'] = tri_df.at[tri_row,'fbhe_a_noace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_b_noace']
        tri_df.at[tri_row,'win_fbhe_withace'] = tri_df.at[tri_row,'fbhe_a_withace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbhe_b_withace']
        tri_df.at[tri_row,'win_fbso_noace'] = tri_df.at[tri_row,'fbso_a_noace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbso_b_noace']
        tri_df.at[tri_row,'win_fbso_withace'] = tri_df.at[tri_row,'fbso_a_withace'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'fbso_b_withace']
        tri_df.at[tri_row,'win_tcr'] = tri_df.at[tri_row,'tcr_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'tcr_b']
        tri_df.at[tri_row,'win_ace_error'] = tri_df.at[tri_row,'ace_error_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'ace_error_b']
        tri_df.at[tri_row,'win_err_den'] = tri_df.at[tri_row,'err_den_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'err_den_b']
        tri_df.at[tri_row,'win_t_eff'] = tri_df.at[tri_row,'t_eff_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'t_eff_b']
        tri_df.at[tri_row,'win_knockout'] = tri_df.at[tri_row,'knockout_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'knockout_b']
        tri_df.at[tri_row,'win_goodpass'] = tri_df.at[tri_row,'goodpass_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'goodpass_b']
        tri_df.at[tri_row,'win_eso'] = tri_df.at[tri_row,'eso_a'] if tri_df.at[tri_row,'winning_team'] == teama else tri_df.at[tri_row,'eso_b']

        tri_df.at[tri_row,'loser_fbhe_noace'] = tri_df.at[tri_row,'fbhe_a_noace'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'fbhe_b_noace']
        tri_df.at[tri_row,'loser_fbhe_withace'] = tri_df.at[tri_row,'fbhe_a_withace'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'fbhe_b_withace']
        tri_df.at[tri_row,'loser_fbso_noace'] = tri_df.at[tri_row,'fbso_a_noace'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'fbso_b_noace']
        tri_df.at[tri_row,'loser_fbso_withace'] = tri_df.at[tri_row,'fbso_a_withace'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'fbso_b_withace']
        tri_df.at[tri_row,'loser_tcr'] = tri_df.at[tri_row,'tcr_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'tcr_b']
        tri_df.at[tri_row,'loser_ace_error'] = tri_df.at[tri_row,'ace_error_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'ace_error_b']
        tri_df.at[tri_row,'loser_err_den'] = tri_df.at[tri_row,'err_den_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'err_den_b']
        tri_df.at[tri_row,'loser_t_eff'] = tri_df.at[tri_row,'t_eff_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'t_eff_b']
        tri_df.at[tri_row,'loser_knockout'] = tri_df.at[tri_row,'knockout_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'knockout_b']
        tri_df.at[tri_row,'loser_goodpass'] = tri_df.at[tri_row,'goodpass_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'goodpass_b']
        tri_df.at[tri_row,'loser_eso'] = tri_df.at[tri_row,'eso_a'] if tri_df.at[tri_row,'losing_team'] == teama else tri_df.at[tri_row,'eso_b']


        # difference calculations
        if isinstance(tri_df.at[tri_row,'win_fbhe_noace'],float) & isinstance(tri_df.at[tri_row,'loser_fbhe_noace'],float) :
          tri_df.at[tri_row,'fbhe_diff_noace'] = tri_df.at[tri_row,'win_fbhe_noace'] - tri_df.at[tri_row,'loser_fbhe_noace']
        else:
          tri_df.at[tri_row,'fbhe_diff_noace'] = None
        if isinstance(tri_df.at[tri_row,'win_fbhe_withace'],float) & isinstance(tri_df.at[tri_row,'loser_fbhe_withace'],float) :
          tri_df.at[tri_row,'fbhe_diff_withace'] = tri_df.at[tri_row,'win_fbhe_withace'] - tri_df.at[tri_row,'loser_fbhe_withace']
        else:
          tri_df.at[tri_row,'fbhe_diff_withace'] = None      
          
        if isinstance(tri_df.at[tri_row,'win_fbso_noace'],float) & isinstance(tri_df.at[tri_row,'loser_fbso_noace'],float) :
          tri_df.at[tri_row,'fbso_diff_noace'] = tri_df.at[tri_row,'win_fbso_noace'] - tri_df.at[tri_row,'loser_fbso_noace']
        else:
          tri_df.at[tri_row,'fbso_diff_noace'] = None
        if isinstance(tri_df.at[tri_row,'win_fbso_withace'],float) & isinstance(tri_df.at[tri_row,'loser_fbso_withace'],float) :
          tri_df.at[tri_row,'fbso_diff_withace'] = tri_df.at[tri_row,'win_fbso_withace'] - tri_df.at[tri_row,'loser_fbso_withace']
        else:
          tri_df.at[tri_row,'fbso_diff_withace'] = None 

        if isinstance(tri_df.at[tri_row,'win_eso'],float) & isinstance(tri_df.at[tri_row,'loser_eso'],float) :
          tri_df.at[tri_row,'eso_diff'] = tri_df.at[tri_row,'win_eso'] - tri_df.at[tri_row,'loser_eso']
        else:
          tri_df.at[tri_row,'eso_diff'] = None
          
        if isinstance(tri_df.at[tri_row,'win_ace_error'],float) & isinstance(tri_df.at[tri_row,'loser_ace_error'],float) :
          tri_df.at[tri_row,'ace_error_diff'] = tri_df.at[tri_row,'win_ace_error'] - tri_df.at[tri_row,'loser_ace_error']
        else:
          tri_df.at[tri_row,'ace_error_diff'] = None

        if isinstance(tri_df.at[tri_row,'win_tcr'],float) & isinstance(tri_df.at[tri_row,'loser_tcr'],float) :
          tri_df.at[tri_row,'tcr_diff'] = tri_df.at[tri_row,'win_tcr'] - tri_df.at[tri_row,'loser_tcr']
        else:
          tri_df.at[tri_row,'tcr_diff'] = None
          
        if isinstance(tri_df.at[tri_row,'win_t_eff'],float) & isinstance(tri_df.at[tri_row,'loser_t_eff'],float) :
          tri_df.at[tri_row,'t_eff_diff'] = tri_df.at[tri_row,'win_t_eff'] - tri_df.at[tri_row,'loser_t_eff']
        else:
          tri_df.at[tri_row,'t_eff_diff'] = None
          
        if isinstance(tri_df.at[tri_row,'win_err_den'],float) & isinstance(tri_df.at[tri_row,'loser_err_den'],float) :
          tri_df.at[tri_row,'err_den_diff'] = tri_df.at[tri_row,'win_err_den'] - tri_df.at[tri_row,'loser_err_den']
        else:
          tri_df.at[tri_row,'err_den_diff'] = None
          
        if isinstance(tri_df.at[tri_row,'win_knockout'],float) & isinstance(tri_df.at[tri_row,'loser_knockout'],float) :
          tri_df.at[tri_row,'knockout_diff'] = tri_df.at[tri_row,'win_knockout'] - tri_df.at[tri_row,'loser_knockout']
        else:
          tri_df.at[tri_row,'knockout_diff'] = None
          
        if isinstance(tri_df.at[tri_row,'win_goodpass'],float) & isinstance(tri_df.at[tri_row,'loser_goodpass'],float) :
          tri_df.at[tri_row,'goodpass_diff'] = tri_df.at[tri_row,'win_goodpass'] - tri_df.at[tri_row,'loser_goodpass']
        else:
          tri_df.at[tri_row,'goodpass_diff'] = None

        #print("End of Loop over the Set")
        #print(tri_df)
        # End if statment for the set
      # end loop over sets
    # end loop over video id's, or match
    
  # now lets store our triangle stats data file back as a csv file in the database
  #---------------------------------------------------------------------------
  # first, I need to change the ppr_file dataframe to a csv file.
  #print("Saving Tri_df back to database")

  
  tri_csv_file = pd.DataFrame.to_csv(tri_df)
  tri_media = anvil.BlobMedia(content_type="text/plain", content=tri_csv_file.encode(), name="triangle_data.csv")

  ppr_csv_row.update( tri_data = tri_media, tri_data_date=datetime.now() )
  
  return result_string

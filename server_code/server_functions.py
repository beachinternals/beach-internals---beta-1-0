import anvil.email
import anvil.google.auth, anvil.google.mail
from anvil.google.drive import app_files
from googleapiclient.discovery import build
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.media
import pandas as pd
import io
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
import math


# This is a server module. It runs on the Anvil server,

# Functions in this library
#
#   fbhe( ppr_df, disp_player):
#
#   get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout ): 

 
def fbhe( ppr_df, disp_player, play_type, video_yn ):
  # pass this a query of rows, figures the FBHE for the display player as the attacker
  # initialize the vector
  #
  # 0 = fbhe
  # 1 = kills
  # 2 = errors
  # 3 = attempts
  # 4 = first ball sideout (FBSO)
  # 5 = URL
  
  fbhe_list = [ 0.0, 0, 0, 0, 0, " " ]    # FBHE

  # limit to attacks by our player
  #print(f"fbhe funct: ppr_df shape:{ppr_df.shape}")
  if ppr_df.shape[0] == 0:      # Then no data passed!
    fbhe_list = [ 0.0, 0, 0, 0, 0, " No Data Available " ]
  else:
    if play_type == "att":
      ppr_df = ppr_df[ppr_df['att_player'].str.strip() == disp_player.strip()]
    elif play_type == "srv":
      ppr_df = ppr_df[ppr_df['serve_player'].str.strip() == disp_player.strip()]
    elif play_type == "pass":
      ppr_df = ppr_df[ppr_df['pass_player'].str.strip() == disp_player.strip()]

    #print(f"Size of DB in calc_fbhe:{ppr_df.shape[0]}, Disp Player:{disp_player}")
    # to build the video link, need a quick loop over rows:
    video_list = [*range(0,ppr_df.shape[0],1)]
    #print(f"video list: {video_list}")
    video_btd_id = ""
    video_link = ""
    i = 0
    if video_yn:
      for index,r in ppr_df.iterrows():
        if r['video_id'] != video_btd_id:
          # build a new link
          #print(f"start new link, video_btd_id:{video_btd_id}, Lenght: {len(video_btd_id)}")
          video_link = video_link + ")" if len(video_btd_id) != 0 else video_link
          video_list[i] = " [G"+str(i)+"](https://app.balltime.com/video/"+r['video_id']+"?actionIds="+str(r['serve_action_id'])
          video_list[i] = video_list[i] + ',' + str(r['pass_action_id']) if r['pass_action_id'] != 0 else video_list[i]
          video_list[i] = video_list[i] + ',' + str(r['set_action_id']) if r['set_action_id'] != 0 else video_list[i]
          video_list[i] = video_list[i] + ',' + str(r['att_action_id']) if r['att_action_id'] != 0 else video_list[i]
          video_list[i] = video_list[i] + ',' + str(r['dig_action_id']) if r['dig_action_id'] != 0 else video_list[i]
          video_list[i] = video_list[i] 
          video_link = video_link+ video_list[i]
          #print(f"New Link i: {i} Video Link: {video_link}")
          i += 1
        elif r['video_id'] == video_btd_id:
          # add on to the current video list
          video_list[i] = str(video_list[i]) + ',' + str(r['serve_action_id']) if r['serve_action_id'] != 0 else video_list[i]
          video_list[i] = str(video_list[i]) + ',' + str(r['pass_action_id']) if r['pass_action_id'] != 0 else video_list[i]
          video_list[i] = str(video_list[i]) + ',' + str(r['set_action_id']) if r['set_action_id'] != 0 else video_list[i]
          video_list[i] = str(video_list[i]) + ',' + str(r['att_action_id']) if r['att_action_id'] != 0 else video_list[i]
          video_list[i] = str(video_list[i]) + ',' + str(r['dig_action_id']) if r['dig_action_id'] != 0 else video_list[i]
          video_link = video_link+ video_list[i]
          #print(f"Add to existing Link i: {i}, Video Link: {video_link}")
      
        video_btd_id = r['video_id']

      video_link = video_link + ")" if len(video_link) != 0 else video_link
      if "No Video Id" in video_link:     # in case we have old data with no video id
        video_link = ""
    else:
      video_link = "N/A"
    
    #print(f"player :{disp_player}, ppr df size:{ppr_df.shape}")
    fbhe_list[3] = ppr_df.shape[0]  # number of attempts
    fbhe_list[1] = ppr_df[ppr_df.point_outcome == "FBK"].shape[0] # kills
    fbhe_list[2] = ppr_df[ppr_df.point_outcome == "FBE"].shape[0] # errors
    fbhe_list[0] = ( fbhe_list[1] - fbhe_list[2]) / fbhe_list[3] if fbhe_list[3] != 0 else 0  # fbhe
    fbhe_list[0] = float("{:.3f}".format(fbhe_list[0]))
    fbhe_list[4] = fbhe_list[1] / fbhe_list[3] if fbhe_list[3] != 0 else 0.0
    fbhe_list[4] = float("{:.3f}".format(fbhe_list[4]))    
    fbhe_list[5] = video_link
    #print(f"fbhe Funct: fbhe_list:{fbhe_list}")

  return fbhe_list
  
def get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout ):  
  #
  # fetch the appropriate ppr table(s) from the ppr_csv table given the league and team, and if scout data

  no_data = True
  #print(f"Searching Team Rows: L:{disp_league}, G:{disp_gender},Y:{disp_year},T:{disp_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  if ppr_csv_row:
    m_ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
    ppr_for_team_found = True
  else:
    m_ppr_df = [" "]
    print(f'get_ppr_data : No Team Rows Found {disp_league}, {disp_gender}, {disp_year}, {disp_team}')
    ppr_for_team_found = False

  #print(f"GET PPR DATA ppr team db: l/g/y: {disp_league},{disp_gender},{disp_year}, Team: {disp_team}, Rows:{m_ppr_df.shape[0]}")
  if scout:
    # now look for the scout data (league wide public data) and merge the two
    # print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
    ppr_scout_row = app_tables.ppr_csv_tables.get( 
      q.all_of(
        league = disp_league,
        gender = disp_gender,
        year = disp_year,
        team = "Scout"
      ) 
    )

    if ppr_scout_row:
      scout_ppr_df =  pd.read_csv(io.BytesIO( ppr_scout_row['ppr_csv'].get_bytes()))
      #print(f"GET PPR DATA ppr Scout db: l/g/y: {disp_league},{disp_gender},{disp_year}, Team: {disp_team}, Rows:{scout_ppr_df.shape[0]}")
      if ppr_for_team_found:
        m_ppr_df = pd.concat([m_ppr_df,scout_ppr_df])
      else:
        m_ppr_df = scout_ppr_df
      #print(f'Scout DB Found:{scout_ppr_df.shape}')
    else:
      #print('No Scout Rows Found')
      a=b

  #print(f"GET PPR DATA ppr db returned: l/g/y: {disp_league},{disp_gender},{disp_year}, Team: {disp_team}, Rows:{m_ppr_df.shape[0]}")


  return m_ppr_df

def ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         ):
  #
  # take an imput ppr_df, and limit it by competition level and date
  if comp_l1_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l1'] ==  disp_comp_l1 ]
    #print(f"Limitiing by Comp l1:{disp_comp_l1}, Size:{m_ppr_df.shape}")
  if comp_l2_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l2'] == disp_comp_l2 ]
    #print(f"Limitiing by Comp l2:{disp_comp_l2}, Size:{m_ppr_df.shape}")
  if comp_l3_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l3'] == disp_comp_l3 ]
    #print(f"Limitiing by Comp l3:{disp_comp_l3}, Size:{m_ppr_df.shape}")
  if date_checked:
    m_ppr_df['game_date'] = pd.to_datetime(m_ppr_df['game_date'])
    m_ppr_df['game_date'] = m_ppr_df['game_date'].dt.date
    m_ppr_df = m_ppr_df.loc[(m_ppr_df['game_date'] >= disp_start_date) & (m_ppr_df['game_date'] <= disp_end_date) ]
    #print(f"Limitiing by Dates:{disp_start_date},{disp_end_date}")
    
  return m_ppr_df

def calc_trans( ppr_df, disp_player, flag ):
  # calcaulte transition details
  trans_list = [0,0,0,0,0,0,0,0,0,0]
  # defiitions:
  #  0 = % of transition
  #  1 = Percentile
  #  2 = % of transition
  #  3 = Kills Earned
  #  4 = Errors Received
  #  5 = Kills Lost
  #  6 = Errors Given
  #  7 = Points Earned
  #  8 = Points Lost
  #  9 = Total Points

  tmp_df = ppr_df
  # filter for serve or receive, or all
  # first, make sue we have point relating to this player
  ppr_df = ppr_df[(( ppr_df['player_a1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_a2'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_b2'].str.strip() == disp_player.strip() ) ) &
                  (( ppr_df['point_outcome'] == 'TK') | 
                  ( ppr_df['point_outcome'] == 'TE' ))
    ]
  total_trans = ppr_df.shape[0]
  #print(f"All Point Ooutcome Teams:{ppr_df['point_outcome_team']}")
  if flag == 'srv':
    ppr_df = ppr_df[ ppr_df['serve_player'].str.strip() == disp_player.strip()]
  elif flag == 'rcv':
    ppr_df = ppr_df[ ppr_df['pass_player'].str.strip() == disp_player.strip()]

  # first, kilsl and errors for this team
  #print(f"Total Transition points:{ppr_df.shape[0]}")
  tmp_df = ppr_df[ (ppr_df['point_outcome_team'].str.contains(disp_player[:-1]) )]
  #print(f"Transition points earned by team with {disp_player} = {tmp_df.shape[0]}")
  trans_list[3] = tmp_df[ (tmp_df['point_outcome'] == 'TK')].shape[0]    # kills earned
  trans_list[6] = tmp_df[ (tmp_df['point_outcome'] == 'TE')].shape[0]      # errors given

  # second, calculate the opponent's kills and errors
  tmp_df = ppr_df[ (~ppr_df['point_outcome_team'].str.contains(disp_player[:-1]) ) ]
  #print(f"Transition points earned by team without {disp_player} = {tmp_df.shape[0]}")
  trans_list[5] = tmp_df[ (tmp_df['point_outcome'] == 'TK')].shape[0]    # kills earned
  trans_list[4] = tmp_df[ (tmp_df['point_outcome'] == 'TE')].shape[0]      # errors given

  trans_list[7] = trans_list[3] + trans_list[4]
  trans_list[8] = trans_list[5] + trans_list[6]
  trans_list[9] = trans_list[7] + trans_list[8]
  if trans_list[9] != 0:
    trans_list[0] = trans_list[7] / trans_list[9]
    trans_list[0] = str('{:.2%}').format(trans_list[0])
    trans_list[2] = trans_list[9] / total_trans 
    trans_list[2] = str('{:.2%}').format(trans_list[2])
  else :
    trans_list[0] = None
    trans_list[2] = None
    
  trans_list[1] = 0  # to get the percentile, we need to look up the league mean and stdev


  return trans_list

def calc_ev(ppr_df, disp_player):
  # calculate expected value
  #
  # 0 = expected value
  # 1 = total points
  # 2 = points won
  # 3 = fbk earned
  # 4 = tk earned
  # 5 = te received
  # 6 = tse received
  # 7 = point lost
  # 8 = FBE given
  # 9 = te given
  # 10 = tk lost
  # 11 = tsa lost  
  ev_vector = [0,0,0,0,0,0,0,0,0,0,0,0]

  # now filter my ppr file to just those wher ethe disp_player receives serve
  ppr_df = ppr_df[ ppr_df['pass_player'].str.strip() == disp_player.strip()]

  ev_vector[3] = ppr_df[ppr_df['point_outcome'] == "FBK"].shape[0]
  ev_vector[8] = ppr_df[ppr_df['point_outcome'] == 'FBE'].shape[0]

  tmp_df = ppr_df[ppr_df['point_outcome_team'].str.contains(disp_player[:-1])]
  ev_vector[4] = tmp_df[tmp_df['point_outcome'] == "TK"].shape[0]
  ev_vector[9] = tmp_df[tmp_df['point_outcome'] == "TE"].shape[0]

  tmp_df = ppr_df[~ppr_df['point_outcome_team'].str.contains(disp_player[:-1])]
  ev_vector[10] = tmp_df[tmp_df['point_outcome'] == "TK"].shape[0]
  ev_vector[5] = tmp_df[tmp_df['point_outcome'] == "TE"].shape[0]
  ev_vector[11] = tmp_df[tmp_df['point_outcome'] == "TSA"].shape[0]
  ev_vector[6] = tmp_df[tmp_df['point_outcome'] == "TSE"].shape[0]

  # points Earned
  ev_vector[2] = ev_vector[2] + ev_vector[3] + ev_vector[4] + ev_vector[5] 
  # points lost
  ev_vector[7] = ev_vector[8] + ev_vector[9] + ev_vector[10] + ev_vector[11]
  # total points
  ev_vector[1] = ev_vector[2] + ev_vector[7]
  # percent:
  ev_vector[0] = ev_vector[2]/ev_vector[1] if ev_vector[1] != 0 else 0
  ev_vector[0] = str('{:.2%}').format(ev_vector[0])
  
  return ev_vector
  
def calc_error_den( ppr_df, disp_player):

  # calculate the error density vector
  # 0 = Error Density
  # 1 = Percentile
  # 2 = First Ball Errors
  # 3 = Service Errors
  # 4 = Transition Errors
  # 5 = Total Errors
  # 6 = total Points
  error_vector = [0,0,0,0,0,0,0]

  # make sure our player is involved in every point
  ppr_df = ppr_df[(( ppr_df['player_a1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_a2'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_b2'].str.strip() == disp_player.strip() ) ) ]
  error_vector[6] = ppr_df.shape[0]
  if error_vector[6] != 0:
    error_vector[2] = ppr_df[ ( ppr_df['point_outcome'] == 'FBE') & (ppr_df['att_player'].str.strip() == disp_player.strip() ) ].shape[0]
    error_vector[4] = ppr_df[ ( ppr_df['point_outcome'] == 'TE') & (ppr_df['point_outcome_team'].str.contains(disp_player)) ].shape[0]*0.5
    error_vector[3] = ppr_df[ ( ppr_df['point_outcome'] == 'TSE') & (ppr_df['serve_player'].str.strip() == disp_player.strip() ) ].shape[0]
    error_vector[5] = error_vector[2] + error_vector[3] + error_vector[4] 
    error_vector[0] = error_vector[5] / error_vector[6]
    error_vector[0] = str('{:.2%}').format(error_vector[0])
    error_vector[1] = 0
  else:
    error_vector[0] = 0
    error_vector[1] = 0
    error_vector[2] = 0
    error_vector[3] = 0
    error_vector[4] = 0
    error_vector[5] = 0

    
  return error_vector


def fbhe_attack_type( m_ppr_df, disp_player, att_type, video_yn ):
  # calcualte the fbhe byt he attack type:
  # att_type:
  #    'poke' - use the limit equation by poke
  #    'shoot'
  #    'bang'

  if (att_type == 'poke'):
    #print(f"fbhe_attack_type, Poke, ppr_df size:{m_ppr_df.shape[0]}")
    fbhe_vector = fbhe( m_ppr_df[ (m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist']) ], disp_player, 'att' , video_yn)
    #print(f"fbhe vector: {fbhe_vector}")
  elif (att_type == 'shoot'):
    fbhe_vector = fbhe( m_ppr_df[ (~(m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist']) & ( m_ppr_df['att_speed'] <= 6 )) ], disp_player, 'att', video_yn )
  elif (att_type == 'bang'):
    fbhe_vector = fbhe( m_ppr_df[ (~(m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist']) &  ( m_ppr_df['att_speed'] > 6 )) ], disp_player, 'att', video_yn )

  return fbhe_vector


def get_tri_data( disp_league, disp_gender, disp_year, date_checked, disp_start_date, disp_end_date ):  
  #
  # fetch the appropriate trianble scroing csv table(s) from the ppr_csv table given the league 

  #print(f"Searching Team Rows: L:{disp_league}, G:{disp_gender},Y:{disp_year},T:{disp_team}")
  disp_team = 'League'
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  if ppr_csv_row:
    tri_df =  pd.read_csv(io.BytesIO( ppr_csv_row['tri_data'].get_bytes()))
    #print(f"tri data found!, records:{tri_df.shape[0]}")
    tri_data_found = True
  else:
    tri_df = [" "]
    #print(f"No Tri data Found, {disp_league, disp_gender, disp_year, disp_team}")
    print(f'get_ppr_data :No Team Rows Found {disp_league}, {disp_gender}, {disp_year}, {disp_team}')
    tri_data_found = False

  if date_checked:
    tri_df['game_date'] = pd.to_datetime(tri_df['game_date'])
    tri_df['game_date'] = tri_df['game_date'].dt.date
    tri_df = tri_df.loc[(tri_df['game_date'] >= disp_start_date) & (tri_df['game_date'] <= disp_end_date) ]
    
  return tri_df, tri_data_found


def point_totals(ppr_df, disp_player):
  # count the total number of points in each category

  # return datafarme with the details
  pts_dict = {'p_tsa':[0],
              'p_tse':[0],
              'p_fbk':[0],
              'p_fbe':[0],
              'p_tk':[0],
              'p_te':[0],
              'p_serves':[0],
              'o_tsa':[0],
              'o_tse':[0],
              'o_fbk':[0],
              'o_fbe':[0],
              'o_tk':[0],
              'o_te':[0],
              'o_serves':[0],
              'pts_total':[0]
             }

  pts_df = pd.DataFrame.from_dict( pts_dict )

  # filter whoiel dataframe to this player:
  ppr_df = ppr_df[(( ppr_df['player_a1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_a2'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                  ( ppr_df['player_b2'].str.strip() == disp_player.strip() ) ) 
  ]

  # let's start counting!
  pts_df.at[0,'p_tsa'] = ppr_df[ ppr_df['point_outcome'] == 'TSA' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_tse'] = ppr_df[ ppr_df['point_outcome'] == 'TSE' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_fbk'] = ppr_df[ ppr_df['point_outcome'] == 'FBK' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_fbe'] = ppr_df[ ppr_df['point_outcome'] == 'FBE' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_tk'] = ppr_df[ ppr_df['point_outcome'] == 'TK' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_te'] = ppr_df[ ppr_df['point_outcome'] == 'TE' & ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'p_serves'] = ppr_df[ ppr_df['serve_player']]
  
  pts_df.at[0,'o_tsa'] = ppr_df[ ppr_df['point_outcome'] == 'TSA' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'o_tse'] = ppr_df[ ppr_df['point_outcome'] == 'TSE' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'o_fbk'] = ppr_df[ ppr_df['point_outcome'] == 'FBK' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'o_fbe'] = ppr_df[ ppr_df['point_outcome'] == 'FBE' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'o_tk'] = ppr_df[ ppr_df['point_outcome'] == 'TK' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'o_te'] = ppr_df[ ppr_df['point_outcome'] == 'TE' & ~ppr_df['point_outcome_team'].str.contains(disp_player)].shape[0]
  pts_df.at[0,'pts_total'] = ppr_df.shape[0]

  return pts_df

@anvil.server.callable
def unpack_league( league_string):
  # unpack 'FIVB | M | 2024' into 'FIVB' "M" '2024'
  # unpack the league data
  str_loc = league_string.index('|')
  disp_league = league_string[:str_loc-1].strip()
  league_string = league_string[str_loc+1:]
  str_loc = league_string.index('|')
  disp_gender = league_string[:str_loc-1].strip()
  disp_year = league_string[str_loc+1:].strip()

  return disp_league, disp_gender,disp_year

def get_player_data( disp_league, disp_gender, disp_year):
  # return the player_data dataframe
  
  # find the play_data table
  # pull out the player_data csv file
  #print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = "League"
      ) )

  if ppr_csv_row:
    player_data_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data'].get_bytes()))
    player_stats_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
  else:
    #print('No Rows Found')
    return ["No Player Data Found"], ["No Player Stats Found"]

  # somehow, we are getting a column called unamed: 0, so drop taht
  #print(player_data_df.to_dict())
  player_data_df = player_data_df.drop(['Unnamed: 0'], axis = 1 )
  player_stats_df = player_stats_df.drop(['Unnamed: 0'], axis = 1 )
  #print(player_data_df.to_dict())

  # need to replace a space with NaN 
  player_data_df = player_data_df.replace( " " , None )

  return player_data_df, player_stats_df

@anvil.server.callable
def get_team_num(disp_player):
  # when we want just "FSU 12" when given 'FSU 12 Alexis'

  # break disp_player down into its components
  first_space = disp_player.find(' ')
  if first_space == -1:
    return "No Space found in "+disp_player
  team = disp_player[:first_space]
  rest_of_string = disp_player[first_space+1:]
  second_space = rest_of_string.find(' ')
  if second_space == -1:
    return "No Space Found in "+rest_of_string
  number = rest_of_string[:second_space]

  return team+' '+number

@anvil.server.callable
def count_out_of_system(ppr_df,disp_player,action):
  #
  # out of system vector:
  #  0 = number out of system int()
  #  1 = percent out of system float()
  #  2 = number of attempts
  #
  #  action = 'pass', 'att', 'srv'

  oos_vector = [0,0,0]
  if action == 'att':
    action_filter = 'att_player'
  elif action == 'pass':
    action_filter = 'pass_player'
  elif action == 'srv':
    action_filter = 'serve_player'
  else:
    print(f'Invalid action passed to out_of_sytem, Action:{action}, using Pass')
    action_filter = 'pass_player'

  # first check, did we get passed a df with no elements
  if ppr_df.shape[0] == 0:
    oos_vector[0] = 0
    oos_vector[1] = 0
    oos_vector[2] = 0
  else:
    # let's count!!
    oos_vector[2] = ppr_df[ ppr_df[action_filter] == disp_player].shape[0]
    oos_vector[0] = ppr_df[ (ppr_df[action_filter] == disp_player) & (ppr_df['pass_oos'] > 0)].shape[0]
    if not oos_vector[0]:
      oos_vector[0] = 0
    if oos_vector[2] != 0:
      oos_vector[1] = oos_vector[0]/oos_vector[2]
    else:
      oos_vector[1] = 0
      
  #print(f"count_out_of_system: action filter: {action_filter}, Rows in ppr_df: {ppr_df.shape[0]} Display Player: {disp_player}")
  #print(f"count_out_of_system: Number OOS: {oos_vector[0]}, Percent OOS: {oos_vector[1]}, Total Attempts: {oos_vector[2]}")
  return oos_vector

#-----------------------------------------------------------------------------------------------
#
#          Functions for saving files to the server
#
#-----------------------------------------------------------------------------------------------

# a simple routine to write. afile to the drive
@anvil.server.callable
def create_google_drive_file( folder, filename, file ):
    return folder.create_file(filename, file)
  
#.     Get the report folder, and/or create it
@anvil.server.callable
def get_report_folder( root_folder, r_league, r_gender, r_year, r_team, r_date):
  #
  folder_name = r_league.upper() + '/' + r_gender.upper() + '/' + r_year.upper() + '/' + r_team.upper() + '/' + r_date.upper()
  # does the folder exist?
  if does_folder_exist( folder_name ):
    # then get the
    rpt_folder = getattr(app_files, folder_name)
  else:
    # create the folder
    rpt_folder = root_folder.create_folder( folder_name )

  return rpt_folder


@anvil.server.callable
def does_folder_exist(folder_name):
    try:
        # Attempt to access the folder by name under app_files
        folder = getattr(app_files, folder_name)
        # Check if it's a folder (not a file)
        if folder.is_folder():
            return True
        else:
            return False  # Exists but is not a folder
    except AttributeError:
        # Folder doesn't exist under app_files
        return False


# Example usage
@anvil.server.callable
def create_report_folder(folder_name, parent_id):
    #folder_name = "MyNewFolder"  # Replace with your desired folder name
    #parent_id = None  # Optional: Replace with a parent folder ID if needed
    result = create_folder(folder_name, parent_id)
    return f"Created folder '{result['name']}' with ID: {result['id']}"

def write_pdf_to_google_drive( folder, filename, pdf_file):
  new_pdf = folder.create_file(filename, pdf_file)
  return new_pdf

@anvil.server.callable
def write_to_drive(filename, directory, content):
    # Access the app_files folder (replace 'my_folder' with your folder name from the Google API Service)
    folder = app_files.reports  # e.g., app_files.my_folder if you added a folder named "my_folder"
    
    # Check if the file exists, if not create it
    file = folder.get(filename)
    if file is None:
        file = folder.create_file(filename, content)  # Create new file with content
    else:
        file.set_bytes(content)  # Update existing file with new content (as bytes)
    
    return f"File {filename} written to Google Drive"

# Example usage from client code
# anvil.server.call('write_to_drive', 'example.txt', b'Hello, World!')

@anvil.server.callable
def write_to_nested_folder(folder_path, filename, content):
  from anvil.google.drive import app_files
  current_folder = app_files.reports  # Replace with your folder name
    
  for subfolder_name in folder_path:
    next_folder = current_folder.get(subfolder_name)
    if next_folder is None:
      next_folder = current_folder.create_folder(subfolder_name)
    current_folder = next_folder
    
  file = current_folder.get(filename)
  if isinstance(content, str):
    content_bytes = content.encode()
    if file is None:
      file = current_folder.create_file(filename, content_bytes)
    else:
      file.set_bytes(content_bytes)
  elif hasattr(content, 'get_bytes'):
    if file is None:
      file = current_folder.create_file(filename, content)
    else:
      file.set_media(content)
  else:
    raise Exception(f"Unsupported content type: {type(content)}")
    
  return f"File {filename} written to {'/'.join(folder_path)}"


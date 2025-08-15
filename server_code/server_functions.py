import anvil
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
#from matplotlib.patches import Ellipse
import matplotlib.patches as patches
import matplotlib.colors as mcolors
import math
from pair_functions import *
from datetime import datetime, timedelta, date
import logging
import re
import scipy.stats as stats
from matplotlib.colors import LinearSegmentedColormap
from sklearn.cluster import DBSCAN
from plot_functions import *
from dataclasses import dataclass

# Set up logging
#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

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
  # ppr_df - the data frame (ppr format)
  # disp_player - player striing
  # play_type: 'att', 'pass', 'srv'
  # video_yn : True of False is desire the url at item 5.  
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

    #if 'STANFORD' in disp_player:
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
    print(f"fbhe Funct: fbhe_list:{fbhe_list}")

  
  return fbhe_list

#-------------------------------------------------------------------------------
#
# New FBHE FUnction and video link function
#
#-------------------------------------------------------------------------------
@dataclass
class FBHEResult:
  """
    Data class to hold the results of the FBHE calculation.
    
    Attributes:
    - fbhe (float): First Ball Hitting Efficiency, rounded to 3 decimal places.
    - kills (int): Number of kills.
    - errors (int): Number of errors.
    - attempts (int): Number of attempts.
    - fbso (float): First Ball Sideout, rounded to 3 decimal places.
    - video_link (str): Video links string, "N/A", or " No Data Available ".
    """
  fbhe: float
  kills: int
  errors: int
  attempts: int
  fbso: float
  video_link: str

def build_video_links(ppr_df: pd.DataFrame) -> str:
  """
    Builds a concatenated string of HTML links for video playback based on the provided DataFrame.
    
    This function groups the rows by 'video_id', collects unique non-zero action IDs from specified columns 
    in the order they first appear, and constructs an HTML link for each group that opens in a new window/tab.
    Links are joined with spaces for readability.
    
    Parameters:
    - ppr_df (pd.DataFrame): The DataFrame containing play-by-play data with columns like 'video_id', 
                             'serve_action_id', 'pass_action_id', 'set_action_id', 'att_action_id', 
                             and 'dig_action_id'.
    
    Returns:
    - str: A string of concatenated HTML links, e.g., '<a href="url1" target="_blank">G0</a> <a href="url2" target="_blank">G1</a>'.
           Returns an empty string if no valid links can be built.
    """
  if ppr_df.empty:
    return ""

  groups = ppr_df.groupby('video_id')
  links = []
  action_columns = ['serve_action_id', 'pass_action_id', 'set_action_id', 'att_action_id', 'dig_action_id']

  for idx, (video_id, group) in enumerate(groups):
    if pd.isna(video_id) or video_id == "No Video Id":
      continue

    actions = []
    seen = set()
    for _, row in group.iterrows():
      for col in action_columns:
        val = row[col]
        str_val = str(val)
        if val != 0 and str_val not in seen:
          actions.append(str_val)
          seen.add(str_val)

    if not actions:
      continue

    action_ids = ','.join(actions)
    url = f"https://app.balltime.com/video/{video_id}?actionIds={action_ids}"
    link = '[G]('+url+')'
    #link = f'<a href="{url}" target="_blank">G{idx}</a>'
    links.append(link)

  return ' '.join(links)

def fbhe_obj(ppr_df: pd.DataFrame | pd.Series, disp_player: str, play_type: str, video_yn: bool) -> FBHEResult:
  """
    Calculates the First Ball Hitting Efficiency (FBHE) and related statistics for a given player based on 
    the provided play-by-play data.
    
    The function filters the data to include only rows where the specified player performed the action 
    corresponding to the play_type. It then computes aggregate statistics such as FBHE, kills, errors, 
    attempts, and First Ball Sideout (FBSO). If video_yn is True, it generates video links using the 
    build_video_links function.
    
    Supports handling both a DataFrame (multiple plays) and a Series (single play) by converting Series to DataFrame.
    
    Parameters:
    - ppr_df (pd.DataFrame | pd.Series): The play-by-play data. Can be a DataFrame for multiple plays or a Series for a single play.
    - disp_player (str): The name of the player to calculate statistics for.
    - play_type (str): The type of play ('att' for attack, 'srv' for serve, 'pass' for pass).
    - video_yn (bool): Whether to generate video links (True) or not (False).
    
    Returns:
    - FBHEResult: An instance of FBHEResult containing the calculated values.
    """
  # Convert Series to DataFrame if necessary
  if isinstance(ppr_df, pd.Series):
    ppr_df = ppr_df.to_frame().T

  if ppr_df.empty:
    return FBHEResult(0.0, 0, 0, 0, 0.0, " No Data Available ")

    # Filter based on play_type
  disp_player = disp_player.strip()
  if play_type == "att":
    ppr_df = ppr_df[ppr_df['att_player'].str.strip() == disp_player]
  elif play_type == "srv":
    ppr_df = ppr_df[ppr_df['serve_player'].str.strip() == disp_player]
  elif play_type == "pass":
    ppr_df = ppr_df[ppr_df['pass_player'].str.strip() == disp_player]
  else:
    raise ValueError("Invalid play_type. Must be 'att', 'srv', or 'pass'.")

  if ppr_df.empty:
    return FBHEResult(0.0, 0, 0, 0, 0.0, " No Data Available ")

    # Calculate statistics
  attempts = len(ppr_df)
  kills = (ppr_df['point_outcome'] == "FBK").sum()
  errors = (ppr_df['point_outcome'] == "FBE").sum()
  fbhe_val = (kills - errors) / attempts if attempts > 0 else 0.0
  fbso = kills / attempts if attempts > 0 else 0.0

  # Generate video links if requested
  if video_yn:
    video_link = build_video_links(ppr_df)
    video_link = video_link if video_link else " No Data Available "
  else:
    video_link = "N/A"

  return FBHEResult(
    fbhe=round(fbhe_val, 3),
    kills=int(kills),
    errors=int(errors),
    attempts=attempts,
    fbso=round(fbso, 3),
    video_link=video_link
  )


'''
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

calcualte earned side out

eso = fbk + my TK / serves - tse

Return as an object
    
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
'''
def calc_player_eso( ppr_df, disp_player ):
  # pass this a query of rows, figures the FBHE for the display player as the attacker
  # initialize the vector
  #
  # ppr_df - the data frame (ppr format)
  # disp_player - player striing
  # play_type: 'att', 'pass', 'srv'
  # video_yn : True of False is desire the url at item 5.  

  # limit to attacks by our player
  #print(f"fbhe funct: ppr_df shape:{ppr_df.shape}")
  if ppr_df.shape[0] == 0:      # Then no data passed!
    return {
      'status':False,
      'error_msg':'No Rows in Dataframe'
    }
  else:
    # limit to only serve receive
    ppr_df = filter_serve_receive_only_player(ppr_df, disp_player)
    #print(f"eso : size of serve receive db: {ppr_df.shape[0]}")
    
    if ppr_df.shape[0] == 0:      # Then no data passed!
      return {
        'status':False,
        'error_msg':'No Rows in Dataframe after filter for serve receive only'
      }

    # now take out any service errors
    ppr_df = ppr_df[ ppr_df['point_outcome'] != 'TSE']

    #print(f"eso : size of serve receive db without service errors: {ppr_df.shape[0]}")

    # PPR_DF HOW HAS ALL SERVES WE ARE INTERESTED IN.
    eso_attempts = ppr_df.shape[0]
    eso_fbk = ppr_df[ ppr_df['point_outcome'] == 'FBK'].shape[0]
    eso_tk = ppr_df[ (ppr_df['point_outcome'] == 'TK') & ( ppr_df['point_outcome_team'].str.contains(disp_player) ) ].shape[0]
    if eso_attempts != 0:
      eso = float((eso_fbk+eso_tk)/eso_attempts)
      eso = round(eso,3)
    else:
      eso = None
      
    #print(f"eos: results, eso={eso}, fbk = {eso_fbk}, tk={eso_tk}, attemtps={eso_attempts}")

    return {
    'status':True,
    'video_url':'No Video in Code Yet',
    'player': disp_player,
    'eso': eso,
    'attempts': eso_attempts,
    'fbk': eso_fbk,
    'tk' : eso_tk,
    'eso_string': '' if eso is None else f'{eso:.1%}' if isinstance(eso, (int, float)) else str(eso)
  }


def calc_team_eso( ppr_df, disp_team ):
  # pass this a query of rows, figures the FBHE for the display player as the attacker
  # initialize the vector
  #
  # ppr_df - the data frame (ppr format)
  # disp_player - player striing
  # play_type: 'att', 'pass', 'srv'
  # video_yn : True of False is desire the url at item 5.  

  # limit to attacks by our player
  #print(f"fbhe funct: ppr_df shape:{ppr_df.shape}")
  if ppr_df.shape[0] == 0:      # Then no data passed!
    return {
      'status':False,
      'error_msg':'No Rows in Dataframe'
    }
  else:
    # limit to only serve receive
    ppr_df = filter_serve_receive_only_team(ppr_df, disp_team)

    #print(f"eso : size of serve receive db: {ppr_df.shape[0]}")

    # now take out any service errors
    ppr_df = ppr_df[ ppr_df['point_outcome'] != 'TSE']

    #print(f"eso : size of serve receive db without service errors: {ppr_df.shape[0]}")

    # PPR_DF HOW HAS ALL SERVES WE ARE INTERESTED IN.
    eso_attempts = ppr_df.shape[0]
    eso_fbk = ppr_df[ ppr_df['point_outcome'] == 'FBK'].shape[0]
    eso_tk = ppr_df[ (ppr_df['point_outcome'] == 'TK') & (ppr_df['point_outcome_team'] == disp_team ) ].shape[0]
    eso = (eso_fbk+eso_tk)/eso_attempts

    print(f"eos: results, eso={eso}, fbk = {eso_fbk}, tk={eso_tk}, attemtps={eso_attempts}")

  return {
    'status':True,
    'video_url':'No Video in Code Yet',
    'team':disp_team,
    'eso': eso,
    'attempts': eso_attempts,
    'fbk': eso_fbk,
    'tk' : eso_tk,
    'eso_string': str('{:.3%}'.format(eso))
  }

'''
Get a quick list of all the partners of disp_player
'''
def get_partners(df, disp_player):
  # Check each row to find disp_player and return their partner
  partners = []
  for index, row in df.iterrows():
    if row['player_a1'] == disp_player:
      partners.append(row['player_a2'])
    elif row['player_a2'] == disp_player:
      partners.append(row['player_a1'])
    elif row['player_b1'] == disp_player:
      partners.append(row['player_b2'])
    elif row['player_b2'] == disp_player:
      partners.append(row['player_b1'])
  return partners
'''
quick roytine to limit to serve receive plays

'''
def filter_serve_receive_only_player(df, disp_player):
  # Get partners of disp_player
  partners = get_partners(df, disp_player)
  # Combine disp_player and their partners
  exclude_players = [disp_player] + partners
  # Filter dataframe to exclude rows where serve_player is disp_player or their partners
  filtered_df = df[~df['serve_player'].isin(exclude_players)]
  return filtered_df

def filter_serve_receive_only_team(df, disp_team):
  # Get partners of disp_team
  player1, player2 = pair_players(disp_team)
  # Combine disp_player and their partners
  exclude_players = [player1] + [player2]
  # Filter dataframe to exclude rows where serve_player is disp_player or their partners
  filtered_df = df[~df['serve_player'].isin(exclude_players)]
  return filtered_df

  
  
def get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout ):  
  #
  # fetch the appropriate ppr table(s) from the ppr_csv table given the league and team, and if scout data

  no_data = True
  # modify so that team INTERNALS gets all data, using the League as team
  if disp_team == 'INTERNALS':
    disp_team = 'League'
    scout = False
    #print(f"Fetching Data for INTERNALS: {disp_team}")
    
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
    #print(f"Fectched the ppr file for : {disp_league}, {disp_gender}, {disp_year}, {disp_team}, number of rows = {m_ppr_df.shape[0]}")
    ppr_for_team_found = True
  else:
    m_ppr_df = [" "]
    #print(f'get_ppr_data : No Team Rows Found {disp_league}, {disp_gender}, {disp_year}, {disp_team}')
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


def calc_trans_obj( ppr_df, disp_player, flag ):
  # return the different parameters as an object
  # calcaulte transition details

  # defiitions:
  #  tcr_per = % of transition
  #  tcr_percentile = Percentile
  #   = % of transition
  #  tran_kills_won = Kills Earned
  #  tran_errors_won = Errors Received
  #  tran_kills_lost = Kills Lost
  #  tran_errors_lost = Errors Given
  #  tran_pts_earned = Points Earned
  #  tran_pts_lost = Points Lost
  #  Tran_total_pts = Total Points

  return_status = True
  error_msg = ''
  
  # first, make sue we have point relating to this player
  ppr_df = ppr_df[(( ppr_df['player_a1'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_a2'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                   ( ppr_df['player_b2'].str.strip() == disp_player.strip() ) ) ]

  # if the flag is passes at srv or receive, limit the data
  if flag == 'srv':
    ppr_df = ppr_df[ ppr_df['serve_player'].str.strip() == disp_player.strip()]
  elif flag == 'rcv':
    ppr_df = ppr_df[ ppr_df['pass_player'].str.strip() == disp_player.strip()]

  # need to record the total points
  all_total_pts = ppr_df.shape[0]
  if all_total_pts == 0:
    error_msg = 'In Transition Calcaultions, All Total Points is 0'


  # now filter for just transition points
  ppr_df = ppr_df[  (( ppr_df['point_outcome'] == 'TK') | ( ppr_df['point_outcome'] == 'TE' ))]
  
  # caculate total transsition points
  tran_total_pts = ppr_df.shape[0]
  print(f"Calc_tran_obj called: disp_player: {disp_player}, {flag}, all total points = {all_total_pts}, transition points {tran_total_pts}")
  
  # set return status if we have no data left
  if tran_total_pts == 0:
    return_status = False
    error_msg = 'Total Transition Points is 0'
    
  # kills earned and errors lost
  tmp_df = ppr_df[ (ppr_df['point_outcome_team'].str.contains(disp_player[:-1]) )]
  #print(f"Transition points earned by team with {disp_player} = {tmp_df.shape[0]}")
  tran_kills_won = tmp_df[ (tmp_df['point_outcome'] == 'TK')].shape[0]    # kills earned
  tran_errors_lost = tmp_df[ (tmp_df['point_outcome'] == 'TE')].shape[0]      # errors given

  # second, calculate the opponent's kills and errors
  tmp_df = ppr_df[ (~ppr_df['point_outcome_team'].str.contains(disp_player[:-1]) ) ]
  #print(f"Transition points earned by team without {disp_player} = {tmp_df.shape[0]}")
  tran_kills_lost = tmp_df[ (tmp_df['point_outcome'] == 'TK')].shape[0]    # kills earned
  tran_errors_won = tmp_df[ (tmp_df['point_outcome'] == 'TE')].shape[0]      # errors given

  # total points earned
  tran_pts_won = tran_errors_won + tran_errors_won
  tran_pts_lost = tran_kills_lost + tran_errors_lost

  # transition conversion rate, float and string
  if tran_total_pts != 0:
    tcr = tran_pts_won / tran_total_pts
    tcr_str = str('{:.1%}').format(tcr)
  else:
    tcr = None
    tcr_str = None
    error_msg = 'Transition Total Points is 0'

  # transition effeiciency
  if tran_total_pts != 0:
    t_eff = (tran_kills_won - tran_errors_lost) / tran_total_pts
    t_eff_str = str('{:.1%}').format(t_eff)
  else:
    t_eff = None
    t_eff_str = None
    error_msg = 'Transition Total Points is 0'
    
  # transition creates
  if tran_total_pts != 0:
    t_create = (tran_total_pts) / all_total_pts
    t_create_str = str('{:.1%}').format(t_create)
  else:
    t_create = None
    t_create_str= None
    error_msg = 'Transition Total Points is 0'
    
  return {
    'status':return_status,
    'error_msg':error_msg,
    'tran_kills_won':tran_kills_won,
    'tran_errors_lost':tran_errors_lost,
    'tran_kills_lost':tran_kills_lost,
    'tran_errors_won':tran_errors_won,
    'tran_pts_won':tran_pts_won,
    'tran_pts_lost':tran_pts_lost,
    'tran_total_pts':tran_total_pts,
    'tcr':tcr,
    'tcr_str':tcr_str,
    't_eff':t_eff,
    't_eff_str':t_eff_str,
    't_create':t_create,
    't_create_str':t_create_str
  }


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

def calc_ev_obj(ppr_df, disp_player):
  # Calculate expected value and return as a dictionary
  ev_dict = {
    'expected_value': 0,
    'total_points': 0,
    'points_won': 0,
    'fbk_earned': 0,
    'tk_earned': 0,
    'te_received': 0,
    'tse_received': 0,
    'points_lost': 0,
    'fbe_given': 0,
    'te_given': 0,
    'tk_lost': 0,
    'tsa_lost': 0
  }

  # Filter ppr_df to where disp_player receives serve
  ppr_df = ppr_df[ppr_df['pass_player'].str.strip() == disp_player.strip()]

  ev_dict['fbk_earned'] = ppr_df[ppr_df['point_outcome'] == "FBK"].shape[0]
  ev_dict['fbe_given'] = ppr_df[ppr_df['point_outcome'] == 'FBE'].shape[0]

  tmp_df = ppr_df[ppr_df['point_outcome_team'].str.contains(disp_player[:-1])]
  ev_dict['tk_earned'] = tmp_df[tmp_df['point_outcome'] == "TK"].shape[0]
  ev_dict['te_given'] = tmp_df[tmp_df['point_outcome'] == "TE"].shape[0]

  tmp_df = ppr_df[~ppr_df['point_outcome_team'].str.contains(disp_player[:-1])]
  ev_dict['tk_lost'] = tmp_df[tmp_df['point_outcome'] == "TK"].shape[0]
  ev_dict['te_received'] = tmp_df[tmp_df['point_outcome'] == "TE"].shape[0]
  ev_dict['tsa_lost'] = tmp_df[tmp_df['point_outcome'] == "TSA"].shape[0]
  ev_dict['tse_received'] = tmp_df[tmp_df['point_outcome'] == "TSE"].shape[0]

  # Points earned
  ev_dict['points_won'] = ev_dict['fbk_earned'] + ev_dict['tk_earned'] + ev_dict['te_received']

  # Points lost
  ev_dict['points_lost'] = ev_dict['fbe_given'] + ev_dict['te_given'] + ev_dict['tk_lost'] + ev_dict['tsa_lost']

  # Total points
  ev_dict['total_points'] = ev_dict['points_won'] + ev_dict['points_lost']

  # Expected value percentage
  ev_dict['expected_value'] = (ev_dict['points_won'] / ev_dict['total_points'] 
                               if ev_dict['total_points'] != 0 else 0)
  ev_dict['expected_value'] = f"{ev_dict['expected_value']:.0%}"

  return ev_dict

  
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
    error_vector[0] = str('{:.2%}').format(error_vector[0])

    
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
    #print(f'get_ppr_data :No Team Rows Found {disp_league}, {disp_gender}, {disp_year}, {disp_team}')
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


'''

A qiuck routine to calculate the knock percent given a ppr_df and the serving players

'''
def calc_knock_out( ppr_df, disp_player):
  player_point_totals = player_pt_total(ppr_df, disp_player)
  if player_point_totals.at[0,'p_serves'] == 0:
    knock_out = 0
  else:
   knock_out = (player_point_totals.at[0,'p_tsa']+player_point_totals.at[0,'o_bad_pass'] )/player_point_totals.at[0,'p_serves']
  return knock_out


def calc_point_diff( ppr_df, disp_player):
  #player_point_totals = player_pt_total(ppr_df, disp_player)
  #print(f"calc_point_diff: player: {disp_player}, rows in ppr: {ppr_df.shape[0]}")
  kills = ['FBK','TSA','TK']
  errors = ['FBE','TSE','TE']
  # Lets make sure disp_player is in the data somewhere (involved in these points)
  tmp_df = ppr_df[  (ppr_df['player_a1'] == disp_player) | 
                    (ppr_df['player_a2'] == disp_player) |
                    (ppr_df['player_b1'] == disp_player) |
                    (ppr_df['player_b2'] == disp_player) 
                     ]
  pts_earned = tmp_df[ (tmp_df['point_outcome_team'].str.contains(disp_player)) & (tmp_df['point_outcome'].isin(kills)) ].shape[0]
  pts_lost = tmp_df[ (tmp_df['point_outcome_team'].str.contains(disp_player)) & (tmp_df['point_outcome'].isin(errors)) ].shape[0]
  opp_pts_earned = tmp_df[ ~(tmp_df['point_outcome_team'].str.contains(disp_player)) & (tmp_df['point_outcome'].isin(errors)) ].shape[0]
  opp_pts_lost = tmp_df[ ~(tmp_df['point_outcome_team'].str.contains(disp_player)) & (tmp_df['point_outcome'].isin(kills)) ].shape[0]
  if (pts_earned+pts_lost+opp_pts_earned+opp_pts_lost) == 0:
    pts_earned_ratio = None
  else:
    pts_earned_ratio = (pts_earned+ opp_pts_lost)/(pts_earned+pts_lost+opp_pts_earned+opp_pts_lost)
  #print(f"calc_point_diff: player: {disp_player}, ratio:{pts_earned_ratio}, pts earned:{pts_earned}, pts_lost: {pts_lost}, opp pts earned:{opp_pts_earned}, opp pts lost {opp_pts_lost}")
  return pts_earned_ratio
  

@anvil.server.callable
def calc_consistency_match_table( m_ppr_df, disp_player ):
  # calculate hte consistency table for the during match parameters
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['All','1a','1b','2a','2b','3a','3b','Mean','St Dev','Percentile'],
             'Points':[0,0,0,0,0,0,0,0,0,0],
             'FBHE':[0,0,0,0,0,0,0,0,0,0],
             'Att':[0,0,0,0,0,0,0,0,0,0],
             'Tran Conv':[0,0,0,0,0,0,0,0,0,0],
             "Error Den":[0,0,0,0,0,0,0,0,0,0],
             'Knockout %':[0,0,0,0,0,0,0,0,0,0],
             'Good Passes':[0,0,0,0,0,0,0,0,0,0],
             'Points Earned':[0,0,0,0,0,0,0,0,0,0]
            }
  # Define desired column order
  column_order = [' ', 'Points', 'Att', 'FBHE', 'Tran Conv', 'Error Den', 'Knockout %', 'Good Passes', 'Points Earned']

  # Create DataFrame with specified column order
  cons_table = pd.DataFrame(df_dict, columns=column_order)
  
  ko_vector = [0,0,0,0,0,0,0]
  fb_vector = [0,0,0,0,0,0,0]
  tcr_vector = [0,0,0,0,0,0,0]
  ed_vector = [0,0,0,0,0,0,0]
  pass_vector = [0,0,0,0,0,0,0]
  pts_vector = [0,0,0,0,0,0,0]
  
  ############### Third Populate the dataframe, assuming we have data returned
  '''
  Loop indexes:
  0 = All
  1 = 1a, set 1, 0 - 21 points
  2 = 1b, set 1, 22+ points
  3 = 2a, set 2, 0 - 21 points
  4 = 2b, set 2, >21 points
  5 = 3a, set 3 <= 15 poitns (if total points > 10)
  6 = 3b, set 3, > 15 points (if total points in set > 10)
  
  '''
  # first, make sure we limit the df to plays with the display player involved.
  m_ppr_df = m_ppr_df[ (m_ppr_df['player_a1'] == disp_player) | (m_ppr_df['player_a2'] == disp_player) | (m_ppr_df['player_b1'] == disp_player) | (m_ppr_df['player_b2'] == disp_player) ]

  if m_ppr_df.shape[0] > 0:
    no_data = False
    #
    # we'll do a loop to calculate all columns
    for index in [0,1,2,3,4,5,6]:
      tmp_df = m_ppr_df
      if index == 0:
        tmp_df = tmp_df ## looking at all data
      elif index == 1:
        tmp_df = tmp_df[ (tmp_df['set'] == 1) & ((tmp_df['a_score'] + tmp_df['b_score'])<=21)]
      elif index == 2:
        tmp_df = tmp_df[ (tmp_df['set'] == 1) & ((tmp_df['a_score'] + tmp_df['b_score']) >21)]
      elif index == 3:
        tmp_df = tmp_df[ (tmp_df['set'] == 2) & ((tmp_df['a_score'] + tmp_df['b_score']) <=21)]
      elif index == 4:
        tmp_df = tmp_df[ (tmp_df['set'] == 2) & ((tmp_df['a_score'] + tmp_df['b_score'])  >21)]
      elif index == 5:
        tmp_df = tmp_df[ (tmp_df['set'] == 3) & ((tmp_df['a_score'] + tmp_df['b_score']) <=15) ]
      elif index == 6:
        tmp_df = tmp_df[ (tmp_df['set'] == 3) & ((tmp_df['a_score'] + tmp_df['b_score']) >15)] 

      # calculate fbhe 
      #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}, index ")
      fbhe_vector = fbhe( tmp_df, disp_player, 'att', True )
      cons_table.at[index,'FBHE'] = fbhe_vector[0] if fbhe_vector[3] != 0 else None # fbhe
      fb_vector[index] = fbhe_vector[0]
      cons_table.at[index,'Att'] = fbhe_vector[3]  # attack attempts
      cons_table.at[index,'Points'] = tmp_df.shape[0]

      # calcualte tcr
      trans_list = calc_trans( tmp_df, disp_player, 'all')
      #print(f" during match: Trans List: {trans_list}")
      if trans_list[0] is None:
        tcr_vector[index] = None
        cons_table.at[index,'Tran Conv'] = None
      else:
        tcr_vector[index] = float(trans_list[0][:-1])
        cons_table.at[index,'Tran Conv'] = float(trans_list[0][:-1]) 

      # calculate Error Density
      error_vector = calc_error_den(tmp_df, disp_player)
      cons_table.at[index,"Error Den"] = float(error_vector[0][:-1]) if error_vector[6] != 0.0 else None 
      ed_vector[index] = float(error_vector[0][:-1])

      # calcualte Knock Out
      cons_table.at[index,'Knockout %'] = calc_knock_out(tmp_df,disp_player)
      ko_vector[index] = cons_table.at[index,'Knockout %']
      #cons_table.at[index,'Knockout %'] = str('{:.1%}').format(cons_table.at[index,'Knockout %'])
    
      # Calculate good passing percent
      oos_vector = count_out_of_system(tmp_df,disp_player,'pass')
      if oos_vector[1] is not None:
        cons_table.at[index,'Good Passes'] = 1 - oos_vector[1]
        pass_vector[index] = 1 - oos_vector[1]
      else:
        cons_table.at[index,'Good Passes'] = None
        pass_vector[index] = None       

      # calculate point differential (as a percent of total points)
      pt_diff = calc_point_diff( tmp_df, disp_player)
      cons_table.at[index,'Points Earned'] = pt_diff
      pts_vector[index] = pt_diff

      # now I want to format all these to 3 decimals
      cons_table.at[index,'FBHE'] = float('{:.3}'.format(float(cons_table.at[index,'FBHE'])))
      cons_table.at[index,'Error Den'] = float('{:.3}'.format(float(cons_table.at[index,'Error Den'])))
      cons_table.at[index,'Tran Conv'] = float('{:.3}'.format(float(cons_table.at[index,'Tran Conv'])))
      cons_table.at[index,'Knockout %'] = float('{:.3}'.format(float(cons_table.at[index,'Knockout %'])))
      cons_table.at[index,'Good Passes'] = float('{:.3}'.format(float(cons_table.at[index,'Good Passes'])))
      cons_table.at[index,'Points Earned'] = float('{:.3}'.format(float(cons_table.at[index,'Points Earned'])))

    # now the last two rows, mean and stdev
    #print(f"Error Vector: {ed_vector}")
    # first, clean the data lists
    fb_vector = [x for x in fb_vector if x is not None]
    ed_vector = [x for x in ed_vector if x is not None]
    tcr_vector = [x for x in tcr_vector if x is not None]
    ko_vector = [x for x in ko_vector if x is not None]
    pass_vector = [x for x in pass_vector if x is not None]
    pts_vector = [x for x in pts_vector if x is not None]
    cons_table.at[index+1,'Points'] = ' '
    cons_table.at[index+1,'Att'] = ' '
    cons_table.at[index+1,' '] = 'Mean'
    cons_table.at[index+1,'FBHE'] = float('{:.3}'.format(np.nanmean(fb_vector)))
    cons_table.at[index+1,'Error Den'] = float('{:.3}'.format(np.nanmean(ed_vector)))
    #print(f"cons table: player: {disp_player}, tcr_vector {tcr_vector}, mean: {np.nanmean(tcr_vector)}")
    cons_table.at[index+1,'Tran Conv'] = float('{:.3}'.format(np.nanmean(tcr_vector)))
    cons_table.at[index+1,'Knockout %'] = float('{:.3}'.format(np.nanmean(ko_vector)))
    cons_table.at[index+1,'Good Passes'] = float('{:.3}'.format(np.nanmean(pass_vector)))
    cons_table.at[index+1,'Points Earned'] = float('{:.3}'.format(np.nanmean(pts_vector)))

    index = index + 1
    cons_table.at[index+1,' '] = 'St Dev'
    cons_table.at[index+1,'Points'] = ' '
    cons_table.at[index+1,'Att'] = ' '
    cons_table.at[index+1,'FBHE'] = float('{:.3}'.format(np.nanstd(fb_vector)))
    cons_table.at[index+1,'Error Den'] = float('{:.3}'.format(np.nanstd(ed_vector)))
    #print(f"cons table: player: {disp_player}, tcr_vector {tcr_vector}, stdev: {np.nanstd(tcr_vector)}")
    cons_table.at[index+1,'Tran Conv'] = float('{:.3}'.format(np.nanstd(tcr_vector)))
    cons_table.at[index+1,'Knockout %'] = float('{:.3}'.format(np.nanstd(ko_vector)))
    cons_table.at[index+1,'Good Passes'] = float('{:.3}'.format(np.nanstd(pass_vector)))
    cons_table.at[index+1,'Points Earned'] = float('{:.3}'.format(np.nanstd(pts_vector)))
  else:
    cons_table = 'No Data Found'
    no_data = True

  #cons_table = cons_table.set_index(' ')
    
  return cons_table, no_data

@anvil.server.callable
def calc_consistency_s2s_table( m_ppr_df, disp_player ):
  # now move on to consistency by set
  '''
    - Create a list of video'id's for this player
    - then loop over sets 1, 2,3 for each video id
    - to create a dataframe for each set with:
      - FBHE
      - Tcr
      - Ed
      - Knock out
      - Good Passes
      - Point differential
    '''

  # create the dataframe with one row
  df_dict = {'Set':[' '],
             'Points':[0],
             'FBHE':[0],
             'Att':[0],
             'Tran Conv':[0],
             "Error Den":[0],
             'Knockout %':[0],
             'Good Passes':[0],
             'Points Earned':[0]
            }
  cons2_table = pd.DataFrame.from_dict( df_dict )
  stat_table = pd.DataFrame.from_dict( df_dict)

  # make sure the dataframe only has points with this display player
  m_ppr_df = m_ppr_df[ (m_ppr_df['player_a1'] == disp_player) | (m_ppr_df['player_a2'] == disp_player) | (m_ppr_df['player_b1'] == disp_player) | (m_ppr_df['player_b2'] == disp_player) ]

  # get alist of unique video_id numbers plus set number
  set_list = m_ppr_df[ ['video_id','set','game_date','teama','teamb']]
  set_list['vid_set'] = set_list['video_id'] + str(set_list['set'])
  set_list = set_list.drop_duplicates(subset=['video_id','set'])

  # now loop thru this calculating for each set
  index = -1
  for set_index,set_row in set_list.iterrows():
    # make a tmp df with the points for tis set
    tmp_df = m_ppr_df[ (m_ppr_df['video_id'] == set_row['video_id'] ) & (m_ppr_df['set'] == set_row['set']) ]
    
    # we only record tis set if it has over 10 points
    if tmp_df.shape[0] >= 20:
      index = index + 1

      # record the set information
      opponent = set_row['teama'] if disp_player in set_row['teamb'] else set_row['teamb']
      cons2_table.at[index,'Set'] = str(set_row['game_date']) + ' ' + opponent + ' ' + str(set_row['set'])
        
      # calculate fbhe 
      #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}, index ")
      fbhe_vector = fbhe( tmp_df, disp_player, 'att', True )
      cons2_table.at[index,'Att'] = float(fbhe_vector[3])  # attack attempts
      cons2_table.at[index,'FBHE'] = float(fbhe_vector[0]) if fbhe_vector[3] != 0 else None # fbhe
      cons2_table.at[index,'FBHE'] = float('{:.3}'.format(float(cons2_table.at[index,'FBHE'])))
      cons2_table.at[index,'Points'] = tmp_df.shape[0]
      stat_table.at[index,'FBHE'] = cons2_table.at[index,'FBHE']

      # calcualte tcr
      trans_list = calc_trans( tmp_df, disp_player, 'all')
      #print(f"set 2 set trans list: Player {disp_player}, Trans list {trans_list}")
      if trans_list[0] is None:
        cons2_table.at[index,'Tran Conv'] = None
        stat_table.at[index,'Tran Conv'] = None
      else:
        cons2_table.at[index,'Tran Conv'] = float(trans_list[0][:-1])
        cons2_table.at[index,'Tran Conv'] = float('{:.3}'.format(float(cons2_table.at[index,'Tran Conv'])))
        stat_table.at[index,'Tran Conv'] = float(trans_list[0][:-1])

      # calculate Error Density
      error_vector = calc_error_den(tmp_df, disp_player)
      #print(f"error vector: Player {disp_player}, Ed Vector {error_vector}")
      cons2_table.at[index,"Error Den"] = float(error_vector[0][:-1])  if error_vector[6] != 0 else None 
      cons2_table.at[index,"Error Den"] = float('{:.3}'.format(float(cons2_table.at[index,"Error Den"])))
      stat_table.at[index,'Error Den'] = float(error_vector[0][:-1])  if error_vector[6] != 0 else None 

      # calcualte Knock Out
      cons2_table.at[index,'Knockout %'] = float('{:.3}'.format(float(calc_knock_out(tmp_df,disp_player))))
      stat_table.at[index,'Knockout %'] = cons2_table.at[index,'Knockout %']
      
      # Calculate good passing percent
      oos_vector = count_out_of_system(tmp_df,disp_player,'pass')
      if oos_vector[1] is None:
        cons2_table.at[index,'Good Passes'] = None
        stat_table.at[index,'Good Passes'] = None
      else:
        cons2_table.at[index, 'Good Passes'] = float(1 - oos_vector[1])  # Ensure float
        cons2_table.at[index, 'Good Passes'] = float('{:.3f}'.format(float(cons2_table.at[index, 'Good Passes'])))  # Format to 3 decimal places
        stat_table.at[index,'Good Passes'] = 1-oos_vector[1]

      # calculate point differential (as a percent of total points)
      pt_diff = float(calc_point_diff( tmp_df, disp_player))
      cons2_table.at[index,'Points Earned'] = float('{:.3}'.format(float(pt_diff)))
      stat_table.at[index,'Points Earned'] = pt_diff

  no_data = False if index > -1 else False

  # need to add the mean and standard devaition rows to this table
  index = index + 1
  cons2_table.at[index,'Set'] = "Mean"
  cons2_table.at[index,'Points'] = ''
  cons2_table.at[index,'Att'] = ''
  cons2_table.at[index,'FBHE'] = float('{:.3}'.format(stat_table['FBHE'].mean(skipna=True)))
  #print(f"set 2 set: stat table {stat_table}")
  cons2_table.at[index,'Tran Conv'] = float('{:.3}'.format(stat_table['Tran Conv'].mean(skipna=True)))
  cons2_table.at[index,'Error Den'] = float('{:.3}'.format(stat_table['Error Den'].mean(skipna=True)))
  cons2_table.at[index,'Knockout %'] = float('{:.3}'.format(stat_table['Knockout %'].mean(skipna=True)))
  cons2_table.at[index,'Good Passes'] = float('{:.3}'.format(stat_table['Good Passes'].mean(skipna=True)))
  cons2_table.at[index,'Points Earned'] = float('{:.3}'.format(stat_table['Points Earned'].mean(skipna=True)))
  index = index + 1
  cons2_table.at[index,'Set'] = "St Dev"
  cons2_table.at[index,'Points'] = ''
  cons2_table.at[index,'Att'] = ''
  cons2_table.at[index,'FBHE'] = float('{:.3}'.format(stat_table['FBHE'].std(skipna=True)))
  cons2_table.at[index,'Tran Conv'] = float('{:.3}'.format(stat_table['Tran Conv'].std(skipna=True)))
  cons2_table.at[index,'Error Den'] = float('{:.3}'.format(stat_table['Error Den'].std(skipna=True)))
  cons2_table.at[index,'Knockout %'] = float('{:.3}'.format(stat_table['Knockout %'].std(skipna=True)))
  cons2_table.at[index,'Good Passes'] = float('{:.3}'.format(stat_table['Good Passes'].std(skipna=True)))
  cons2_table.at[index,'Points Earned'] = float('{:.3}'.format(stat_table['Points Earned'].std(skipna=True)))

  #cons2_table = cons2_table.set_index('Set')
  return cons2_table, no_data


# Server-side code
@anvil.server.callable
def get_form_url(form_name, params):
  # Server-side alternative (if needed)
  print(f" get form url, form name {form_name}")
  target_url  = anvil.get_url(form_name, query=params)
  print(f"Target URL: {target_url}")
  
  return target_url  # Note: This may still require client-side context



def unpack_lgy(lgy):
  # unpacks the league/year/gender string into three
  # extract league, gender, year from league selected value
  str_loc = lgy.index("|")
  disp_league = lgy[: str_loc - 1].strip()
  lgy = lgy[str_loc + 1 :]
  str_loc = lgy.index("|")
  disp_gender = lgy[: str_loc - 1].strip()
  disp_year = lgy[str_loc + 1 :].strip()

  return disp_league, disp_gender, disp_year


def initialize_report_lists( calling_function_name, **rpt_filters) :
  # intialize all the lists for the new report format

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=calling_function_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_row['lgy']
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9]= rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  return title_list, label_list, image_list, df_list


@anvil.server.callable
def filter_ppr_df( dataframe, **kwargs):
  # given the dataframe, filter it by rpt_filters
  """
    Generate a report by filtering the DataFrame based on kwargs.
    Args:
        dataframe: pandas DataFrame to filter
        **kwargs: Keyword arguments where key=column_name, value=filter_value
    Returns:
        Filtered DataFrame

    This function DOES NOT filter on a given pair or player.  This needs to be done by the report function

    This function filters on:
      comp_l1
      comp_l2
      comp_l3
      opp_pair
      start_date, end_date
      set 1,2,3
      srv_fr [1,3,5]
      srv_to [1e ... 5c]
      serve speed low & high
      pass out of system
      pass height low & high
      set height low & high
      set touch type (bump, hand, unknown)
      att_height low & high
      att_speed low & high
    """
  result = dataframe.copy()  # Avoid modifying the original DataFrame
  for column, value in kwargs.items():
    #if column in dataframe.columns:
    # we'll check for ones not set up as equal to
    # this includes: Date Range, srv_to, srv_fr, srv speed, pass height, set height, att speed, att height

    if column == 'comp_l1':
      result = result[ result['comp_l1'] == value ]
    if column == 'comp_l2':
      result = result[ result['comp_l2'] == value ]
    if column == 'comp_l3':
      result = result[ result['comp_l3'] == value ]

    if column == 'start_date':
      result = apply_date_filters(result, column, value )
    if column == 'end_date':
      result = apply_date_filters(result, column, value )

    # opponent pair for matchu0ps
    if column == 'opp_pair':
      result = result[ ( result['teama'] == value | result['teamb'] == value ) ]

    # set, these are setup as a radio button, so only one can be chceked.  We pass the 'set' as either 1,2,3, or it is not in the list
    if column == 'set':
      result = result[ result[column] == value ]

    # serving to, this is a list of up to 15 zones
    if column == 'srv_fr':
      result = result[ result['serve_src_zone_net'].isin(value) ]

    # serving from, this is a list of 3 zones
    if column == 'srv_to':
      result['srv_to'] = str(result['serve_dest_zone_net'])+result['serve_dest_zone_depth']
      result = result[ result['srv_to'].isin(value) ]

    # serve speed
    if column == 'srv_speed_low':
      result = result[ result['srv_speed'] >= value ]
    if column == 'srv_speed_high':
      result = result[ result['srv_speed'] <= value ]

    # pass out of system
    if column == 'pass_oos':
      if value == 0:
        result = result[ result['pass_oos'] == 0 ]
      elif value == 1:
        result = result[ result['pass_oos'] > 0 ]

    # pass height
    if column == 'pass_ht_low':
      result = result[ result['pass_height'] >= value ]
    if column == 'pass_ht_high':
      result = result[ result['pass_height'] <= value ]

    # set height
    if column == 'set_ht_low':
      result = result[ result['set_height'] >= value ]
    if column == 'set_ht_high':
      result = result[ result['set_height'] <= value ]

    # set type : bump, hand, unknownn, where only one can be selected
    if column == 'set_touch_type':
      if value == 'unkown':
        result = result[ ( result[column] == value | result[column] == 'empty' ) ]
      else:
        result = result[ result[column] == value ]

    # att height
    if column == 'att_ht_low':
      result = result[ result['att_height'] >= value ]
    if column == 'att_ht_high':
      result = result[ result['att_height'] <= value ]

    # att speed
    if column == 'att_speed_low':
      result = result[ result['att_speed'] >= value ]
    if column == 'att_speed_high':
      result = result[ result['att_speed'] <= value ]


    #result = result[result[column] == value]
    #print(f"Number of Rows: {result.shape[0]}")

    #else:
      #print(f"Warning: Column '{column}' not found in DataFrame")

  return result


def apply_date_filters(df, column, value ):
  date_column = 'game_date'
  if date_column not in df.columns:
    raise KeyError(f"Column '{date_column}' not found")
  if isinstance(value, str):
    parsed_value = pd.to_datetime(value, errors='coerce').date()
    if pd.isna(parsed_value):
      raise ValueError(f"Invalid date format: {value}")
  elif isinstance(value, (datetime, date)):
    parsed_value = value if isinstance(value, date) else value.date()
  else:
    raise ValueError(f"Invalid date value: {value}")
  if df[date_column].dtype == object or isinstance(df[date_column].iloc[0], str):
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.date
    if df[date_column].isna().any():
      raise ValueError("Some dates could not be parsed")
  if column == 'start_date':
    df = df[df[date_column] >= parsed_value]
  elif column == 'end_date':
    df = df[df[date_column] <= parsed_value]
  if df.empty:
    raise ValueError(f"No data after applying {column} filter")
  return df



def find_clusters(ppr_df, disp_player, category):
  """
    Find clusters of kills or errors using DBSCAN and return cluster labels and densities metrics.
    """
  #print("Entered find kill error clusters")
  #print(f"size of ppr_df passed:{ppr_df.shape[0]}")
  try:
    logger.info(f"Finding {category} clusters for player: {disp_player}")
    # Fetch data from ppr_df, limit to only pass by disp_player
    ppr_df = ppr_df[ ppr_df['pass_player'] == disp_player]
    #print(f"ppr df size {ppr_df.shape[0]}")
    #print(f" ppr df pass src x:\n{ppr_df['pass_dest_x']}")
    #print(f" ppr df pass src y:\n{ppr_df['pass_dest_y']}")
    #print(f" ppr df point outcome:\n{ppr_df['point_outcome']}")
    
    # Select three columns and rename them
    df = ppr_df[['pass_dest_x', 'pass_dest_y', 'point_outcome']]
    #print(f"DF with old columns: {df}")
    df = df.rename(columns={
                'pass_dest_x': 'x', 'pass_dest_y': 'y', 'point_outcome': 'value'
          })

    #print(f" DF with new names: /n:{df}")
    
    # Ensure x, y are numeric
    df['x'] = pd.to_numeric(df['x'], errors='coerce')
    df['y'] = pd.to_numeric(df['y'], errors='coerce')
    df = df.dropna(subset=['x', 'y'])

    # Logical limits on x values ( along the net)
    lower = -1
    upper = 9
    ppr_df = ppr_df.loc[(ppr_df['pass_dest_x'] >= lower) & (ppr_df['pass_dest_x'] <= upper)]
    lower = 0
    upper = 9
    ppr_df = ppr_df.loc[(ppr_df['pass_dest_y'] >= lower) & (ppr_df['pass_dest_y'] <= upper)]

    # Filter for the specified category (kill or error)
    df_category = df[df['value'] == category]
    #print(f" category total: {df_category.shape[0]}")
    if df_category.empty:
      logger.warning(f"No {category} data found for report_id: {report_id}")
      return {'error': f'No {category} data found'}

      # Extract coordinates
    X = df_category[['x', 'y']].values

    #print(f" X, whatever that is: {X.shape[0]},{X}")
    # Apply DBSCAN
    eps = .85  # Adjust based on your data's scale (e.g., distance threshold)
    min_samples = int(df_category.shape[0]/5)
    if min_samples < 5:
      min_samples = 5 # Minimum points to form a cluster
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
    labels = db.labels_  # Cluster labels (-1 for noise)

    # Calculate density (points per cluster)
    cluster_counts = pd.Series(labels).value_counts()
    density_info = {
      f'cluster_{label}': count for label, count in cluster_counts.items() if label != -1
    }

    # Add cluster labels to DataFrame
    df_category['cluster'] = labels

    return {
      'status': 'success',
      'data': df_category.to_dict('records'),
      'density': density_info,
      'n_clusters': len(set(labels)) - (1 if -1 in labels else 0)
    }
  except Exception as e:
    logger.error(f"Error in find_kill_error_clusters: {str(e)}", exc_info=True)
    return {'error': str(e)}



def calculate_standard_deviation_ellipse(points, confidence=1.0):
  """
   Calculate and plot the standard deviation ellipse for a set of 2D points.

   Args:
       points (numpy.ndarray): A 2D array of shape (n, 2) where each row is a point [x, y].
       confidence (float): Scaling factor for the ellipse size (1.0 for 1 standard deviation).

   Returns:
       center (tuple): The center of the ellipse (mean of the points).
       width (float): The width of the ellipse (major axis length).
       height (float): The height of the ellipse (minor axis length).
       angle (float): The rotation angle of the ellipse in degrees.
   """
  #print(f"calculate_standard_deviation_ellipse: points passed: {points}")

  # Compute the mean of the points
  mean = np.mean(points, axis=0)

  # Calculate the covariance matrix
  cov_matrix = np.cov(points, rowvar=False)

  #print(f"calculate_standard_devciation_ellipse: covariance matrix: {cov_matrix}")

  # Eigen decomposition
  eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)

  # Sort eigenvalues and eigenvectors
  order = eigenvalues.argsort()[::-1]
  eigenvalues = eigenvalues[order]
  eigenvectors = eigenvectors[:, order]

  # Compute the ellipse parameters
  width = 2 * confidence * np.sqrt(eigenvalues[0])  # Major axis
  height = 2 * confidence * np.sqrt(eigenvalues[1])  # Minor axis
  angle = np.degrees(np.arctan2(*eigenvectors[:, 0][::-1]))

  return mean, width, height, angle



def calculate_ellipse_area(width, height):
  """
   Calculate the area of an ellipse.

   Args:
       width (float): The full width (major axis) of the ellipse.
       height (float): The full height (minor axis) of the ellipse.

   Returns:
       float: The area of the ellipse.
   """
  return math.pi * (width / 2) * (height / 2)

def calculate_percentile(metric, mean, std_dev):
  '''
    Calculate the percentile for a given metric based on mean and standard deviation.
    
    INPUT Parameters:
        - metric: The value to calculate the percentile for
        - mean: The mean of the distribution
        - std_dev: The standard deviation of the distribution
    
    OUTPUT Return Parameters:
        - percentile: Float value of the percentile
        - percentile_str: String representation of the percentile (e.g., '88.9%')
    '''
  if std_dev == 0 or metric is None or mean is None or std_dev is None:
    return None, None

    # Calculate the z-score
  z_score = (metric - mean) / std_dev

  # Calculate the percentile using the cumulative distribution function
  percentile = stats.norm.cdf(z_score)

  # Convert to percentage string
  percentile_str = f"{percentile * 100:.0f}%"

  return percentile, percentile_str
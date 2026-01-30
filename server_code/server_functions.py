import anvil.secrets
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
import anvil.secrets
import anvil.http
import pandas as pd
import io
from io import BytesIO
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.colors as mcolors
import math
from pair_functions import *
from datetime import datetime, timedelta, date
import re
import scipy.stats as stats
from matplotlib.colors import LinearSegmentedColormap
from sklearn.cluster import DBSCAN
from plot_functions import *
from dataclasses import dataclass
import inspect
import copy
import time          # ← Keep only ONE set of imports here
import functools     # ← Keep only ONE set of imports here

# Create logger with formatting
from logger_utils import log_info, log_error, log_critical, log_debug

import json
import requests
from faker import Faker
import fitz  # pymupdf
from anvil.pdf import PDFRenderer
from PyPDF2 import PdfMerger
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

fake = Faker()

# ============================================================================
# TIERED PERFORMANCE MONITORING
# Added: November 15, 2025
# Purpose: Control monitoring granularity with levels
# ============================================================================

# Monitoring levels (module-level constants)
MONITORING_LEVEL_OFF = 0       # No monitoring
MONITORING_LEVEL_CRITICAL = 1  # Only high-level orchestrators
MONITORING_LEVEL_IMPORTANT = 2 # Add report generation
MONITORING_LEVEL_DETAILED = 3  # Add data processing
MONITORING_LEVEL_VERBOSE = 4   # Everything including helpers

# Current monitoring level
CURRENT_MONITORING_LEVEL = MONITORING_LEVEL_DETAILED  # ← Set to IMPORTANT for now

def monitor_performance(func=None, level=MONITORING_LEVEL_IMPORTANT):
  """
    Decorator to track function execution time with configurable monitoring levels.
    
    Usage:
        @monitor_performance                           # Default level (IMPORTANT)
        @monitor_performance(level=MONITORING_LEVEL_CRITICAL)  # Specific level
        
    Args:
        func: The function to decorate (provided automatically)
        level: Minimum monitoring level required for this function to be monitored
    """
  def decorator(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
      # Check if this function should be monitored based on current level
      if CURRENT_MONITORING_LEVEL < level:
        # Current level is lower than required, skip monitoring
        return f(*args, **kwargs)

        # Monitoring is enabled for this level, track performance
      start_time = time.time()
      error_occurred = None

      try:
        result = f(*args, **kwargs)
        success = True
        return result

      except Exception as e:
        success = False
        error_occurred = type(e).__name__
        raise  # Re-raise the error

      finally:
        # Always log if monitoring is enabled for this level
        elapsed = time.time() - start_time

        try:
          app_tables.performance_log.add_row(
            timestamp=datetime.now(),
            function_name=f.__name__,
            elapsed_seconds=round(elapsed, 3),
            success=success,
            error_type=error_occurred
          )

          # Print to console for immediate feedback (only if >1 second)
          if elapsed > 1.0:
            print(f"⏱️  {f.__name__}: {elapsed:.2f}s")

        except Exception as log_error:
          # Don't let logging break the function
          print(f"⚠️  Failed to log performance for {f.__name__}: {log_error}")

    return wrapper

    # Handle being called with or without arguments
  if func is None:
    # Called with arguments: @monitor_performance(level=X)
    return decorator
  else:
    # Called without arguments: @monitor_performance
    return decorator(func)


@anvil.server.callable
def set_monitoring_level(level: int):
  """
    Change the current monitoring level.
    
    Args:
        level: 0=OFF, 1=CRITICAL, 2=IMPORTANT, 3=DETAILED, 4=VERBOSE
    
    Returns:
        String describing the new level
    """
  global CURRENT_MONITORING_LEVEL

  level_names = {
    0: "OFF (no monitoring)",
    1: "CRITICAL (orchestrators only)",
    2: "IMPORTANT (+ report generation)",
    3: "DETAILED (+ data processing)",
    4: "VERBOSE (everything)"
  }

  if level not in level_names:
    return f"Invalid level {level}. Use 0-4."

  CURRENT_MONITORING_LEVEL = level
  return f"Monitoring level set to {level}: {level_names[level]}"


@anvil.server.callable
def get_monitoring_level():
  """Get current monitoring level and description"""
  level_names = {
    0: "OFF", 1: "CRITICAL", 2: "IMPORTANT", 3: "DETAILED", 4: "VERBOSE"
  }
  return f"Level {CURRENT_MONITORING_LEVEL}: {level_names.get(CURRENT_MONITORING_LEVEL, 'UNKNOWN')}"

# ============================================================================
# END OF PERFORMANCE MONITORING CODE
# Now your regular functions start below
# ============================================================================


@monitor_performance(level=MONITORING_LEVEL_VERBOSE)
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
    #print(f"fbhe Funct: fbhe_list:{fbhe_list}")
  
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

@monitor_performance(level=MONITORING_LEVEL_VERBOSE)
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
  elif play_type == "both":
    ppr_df = ppr_df[ (ppr_df['pass_player'].str.strip() == disp_player) & (ppr_df['att_player'].str.strip() == disp_player) & (ppr_df['att_yn'] == 'Y')]
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
  return calc_player_eso_obj(ppr_df,disp_player)
  
def calc_player_eso_obj( ppr_df, disp_player ):
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

    #print(f"eos: results, eso={eso}, fbk = {eso_fbk}, tk={eso_tk}, attemtps={eso_attempts}")

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
  # modify so that team INTERNALS gets all data, using the League as team ... nor NOT
  #if disp_team == 'INTERNALS':
  #  disp_team = 'League'
  #  scout = False
  #  #print(f"Fetching Data for INTERNALS: {disp_team}")
    
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
      a = 1

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

  # Flag = 'all', 'att', 'srv', 'rcv'

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
  #print(f"ppr_df passed with {ppr_df.shape[0]} rows")
  # first, make sue we have point relating to this player
  #ppr_df = ppr_df[(( ppr_df['player_a1'].str.strip() == disp_player.strip() ) |
  #                 ( ppr_df['player_a2'].str.strip() == disp_player.strip() ) |
  #                 ( ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
  #                 ( ppr_df['player_b2'].str.strip() == disp_player.strip() ) ) ]

  # if the flag is passes at srv or receive, limit the data
  # for transition calculations, we need to take out the service errors and aces
  ppr_df = ppr_df[ (ppr_df['point_outcome'] != 'TSA')]
  ppr_df = ppr_df[ (ppr_df['point_outcome'] != 'TSE') ]
  #print(f"ppr_df without service aces and errors {ppr_df.shape[0]} rows")
  if flag == 'srv':
    ppr_df = ppr_df[ ppr_df['serve_player'].str.strip() == disp_player.strip()]
  elif flag == 'rcv':
    ppr_df = ppr_df[ ppr_df['pass_player'].str.strip() == disp_player.strip()]
  elif flag == 'att':
    ppr_df = ppr_df[ ppr_df['att_player'].str.strip() == disp_player.strip()]

  # need to record the total points
  all_total_pts = ppr_df.shape[0]
  if all_total_pts == 0:
    error_msg = 'In Transition Calcaultions, All Total Points is 0'


  # now filter for just transition points
  ppr_df = ppr_df[  (( ppr_df['point_outcome'] == 'TK') | ( ppr_df['point_outcome'] == 'TE' ))]
  
  # caculate total transsition points
  tran_total_pts = ppr_df.shape[0]
  #print(f"Calc_tran_obj called: disp_player: {disp_player}, {flag}, all total points = {all_total_pts}, transition points {tran_total_pts}")
  
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
  tran_pts_won = tran_kills_won + tran_errors_won
  tran_pts_lost = tran_kills_lost + tran_errors_lost

  # transition conversion rate, float and string
  if tran_total_pts != 0:
    tcr = tran_pts_won / tran_total_pts
    tcr_str = str('{:.0%}').format(tcr)
  else:
    tcr = None
    tcr_str = None
    error_msg = 'Transition Total Points is 0'

  # transition effeiciency
  if tran_total_pts != 0:
    t_eff = (tran_kills_won - tran_errors_lost) / tran_total_pts
    t_eff_str = str('{:.0%}').format(t_eff)
  else:
    t_eff = None
    t_eff_str = None
    error_msg = 'Transition Total Points is 0'
    
  # transition creates
  if tran_total_pts != 0:
    t_create = (tran_total_pts) / all_total_pts
    t_create_str = str('{:.0%}').format(t_create)
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
    'expected_value_per': '',
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
  ev_dict['points_won'] = ev_dict['fbk_earned'] + ev_dict['tk_earned'] + ev_dict['te_received'] + ev_dict['tse_received']

  # Points lost
  ev_dict['points_lost'] = ev_dict['fbe_given'] + ev_dict['te_given'] + ev_dict['tk_lost'] + ev_dict['tsa_lost']

  # Total points
  ev_dict['total_points'] = ev_dict['points_won'] + ev_dict['points_lost']

  # Expected value percentage
  ev_dict['expected_value'] = (ev_dict['points_won'] / ev_dict['total_points'] 
                               if ev_dict['total_points'] != 0 else 0)
  ev_dict['expected_value_per'] = f"{ev_dict['expected_value']:.0%}"

  return ev_dict



#========-----------=============----------===========-------------===============-------------===========
def calc_error_density_obj(ppr_df, disp_player):
  """
    Calculate error density metrics for a specific player.
    
    Args:
        ppr_df (pandas.DataFrame): Point-by-point results dataframe containing match data
        disp_player (str): Name of the player to analyze (will be stripped of whitespace)
    
    Returns:
        dict: Dictionary containing error density metrics with the following keys:
            - 'error_density': Error density as a percentage string (e.g., '15.25%')
            - 'percentile': Percentile ranking (placeholder, currently always 0)
            - 'first_ball_errors': Count of first ball errors by the player
            - 'service_errors': Count of service errors by the player
            - 'transition_errors': Count of transition errors by the player (weighted by 0.5)
            - 'total_errors': Sum of all error types
            - 'total_points': Total number of points the player was involved in
            - 'error_density_raw': Raw error density as a float (for calculations)
    
    Note:
        - Only includes points where the player was actively involved (present in any player position)
        - Transition errors are weighted by 0.5 (assumes team responsibility)
        - Returns all zeros if player was not involved in any points
    """

  # Input validation
  if ppr_df is None or ppr_df.empty:
    return _create_empty_error_dict()

  if not disp_player or not isinstance(disp_player, str):
    return _create_empty_error_dict()

    # Clean player name
  clean_player_name = disp_player.strip()

  # Filter dataframe to only include points where the player was involved
  # Check all four possible player positions (a1, a2, b1, b2)
  player_involved_mask = (
    (ppr_df['player_a1'].str.strip() == clean_player_name) |
    (ppr_df['player_a2'].str.strip() == clean_player_name) |
    (ppr_df['player_b1'].str.strip() == clean_player_name) |
    (ppr_df['player_b2'].str.strip() == clean_player_name)
  )

  filtered_df = ppr_df[player_involved_mask].copy()
  total_points = len(filtered_df)

  # If player wasn't involved in any points, return zeros
  if total_points == 0:
    return _create_empty_error_dict()

    # Calculate different types of errors

    # First Ball Errors (FBE) - where this player made the attacking error
  first_ball_errors = len(filtered_df[
    (filtered_df['point_outcome'] == 'FBE') & 
    (filtered_df['att_player'].str.strip() == clean_player_name)
    ])

  # Service Errors (TSE) - where this player made the service error
  service_errors = len(filtered_df[
    (filtered_df['point_outcome'] == 'TSE') & 
    (filtered_df['serve_player'].str.strip() == clean_player_name)
    ])

  # Transition Errors (TE) - weighted by 0.5 (team responsibility)
  # Only count if player's name appears in the point_outcome_team field
  transition_errors = len(filtered_df[
    (filtered_df['point_outcome'] == 'TE') & 
    (filtered_df['point_outcome_team'].str.contains(clean_player_name, na=False))
    ]) * 0.5

  # Calculate service attempts (serves by this player)
  service_attempts = len(filtered_df[
    filtered_df['serve_player'].str.strip() == clean_player_name
    ])

  # Calculate service error percentage
  percent_service_error = service_errors / service_attempts if service_attempts > 0 else 0.0
  percent_service_error_str = f"{percent_service_error:.0%}"

  # Calculate totals
  total_errors = first_ball_errors + service_errors + transition_errors
  error_density_raw = total_errors / total_points if total_points > 0 else 0.0

  # Format error density as percentage
  error_density_formatted = f"{error_density_raw:.2%}"

  # TODO: Implement percentile calculation based on league/tournament averages
  percentile = 0  # Placeholder for future implementation

  return {
    'error_density': error_density_formatted,
    'error_density_raw': error_density_raw,
    'percentile': percentile,
    'first_ball_errors': first_ball_errors,
    'service_errors': service_errors,
    'service_attempts': service_attempts,
    'percent_service_error': percent_service_error,
    'percent_service_error_str': percent_service_error_str,
    'transition_errors': transition_errors,
    'total_errors': total_errors,
    'total_points': total_points
  }


def _create_empty_error_dict():
  """
    Create a dictionary with all error metrics set to zero/empty values.
    
    Returns:
        dict: Dictionary with all error metrics initialized to zero
    """
  return {
    'error_density': '0.00%',
    'error_density_raw': 0.0,
    'percentile': 0,
    'first_ball_errors': 0,
    'service_errors': 0,
    'service_attempts': 0,
    'percent_service_error': 0.0,
    'percent_service_error_str': '0%',
    'transition_errors': 0.0,
    'total_errors': 0.0,
    'total_points': 0
  }

  
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

@monitor_performance(level=MONITORING_LEVEL_VERBOSE)
def fbhe_attack_type(m_ppr_df: pd.DataFrame, disp_player: str, att_type: str, video_yn: bool) -> FBHEResult:
  # calcualte the fbhe byt he attack type:
  # att_type:
  #    'poke' - use the limit equation by poke
  #    'shoot'
  #    'bang'
  if att_type == 'poke':
    fbhe_result = fbhe_obj(m_ppr_df[(m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist'])], disp_player, 'att', video_yn)
  elif att_type == 'shoot':
    fbhe_result = fbhe_obj(m_ppr_df[(~(m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist']) & (m_ppr_df['att_speed'] <= 6))], disp_player, 'att', video_yn)
  elif att_type == 'bang':
    fbhe_result = fbhe_obj(m_ppr_df[(~(m_ppr_df['att_speed'] <= (2.5/15)*m_ppr_df['att_dist']) & (m_ppr_df['att_speed'] > 6))], disp_player, 'att', video_yn)
  else:
    raise ValueError(f"Invalid att_type: {att_type}. Must be 'poke', 'shoot', or 'bang'.")
  return fbhe_result
  

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


  
#=============----------------===============---------------==============----------
def count_oos_obj(ppr_df, disp_player, action, video_yn=False):
  """
    Count out-of-system statistics for a player's actions.
    
    Args:
        ppr_df: DataFrame containing player performance data
        disp_player: Player name to analyze
        action: Action type ('pass', 'att', 'srv')
    
    Returns:
        dict: Dictionary with keys 'count', 'percent', 'percent_str', 'attempts'
              Use .get() method to safely access values with defaults
        'count' - number out of system
        'percent' - decimal percentage out of system (e.g., 0.15 for 15%)
        'percent_str' - formatted percentage string with no decimals (e.g., '15%')
        'attempts' - total attempts for the action
    """
  # Map actions to column names
  action_mapping = {
    'att': 'att_player',
    'pass': 'pass_player', 
    'srv': 'serve_player'
  }

  # Get action filter or default to pass
  action_filter = action_mapping.get(action.lower())
  if action_filter is None:
    print(f'Invalid action: {action}. Valid options: {list(action_mapping.keys())}. Using "pass".')
    action_filter = 'pass_player'

    # Initialize result dictionary
  result = {
    'count': 0,           # number out of system
    'percent': 0.0,       # percent out of system (decimal)
    'percent_str': '0%',  # percent out of system (formatted string)
    'attempts': 0 ,       # total attempts
    'URL':''              # Video Links
  }

  # Check if DataFrame is empty
  if ppr_df.empty:
    return result

    # Filter data for the specific player
  player_data = ppr_df[ppr_df[action_filter] == disp_player]

  # Calculate statistics
  result['attempts'] = len(player_data)

  if result['attempts'] > 0:
    # Count out-of-system occurrences
    result['count'] = len(player_data[player_data['pass_oos'] > 0])

    # Calculate percentage
    result['percent'] = result['count'] / result['attempts']

    # Format percentage string with no decimals
    result['percent_str'] = f"{result['percent']:.0%}"

    # Get video links if video_yn is True, else return empty string
    #print(f"In count_oos_obj, calling bulid video, number = {len(player_data[player_data['pass_oos'] > 0])}")
    result['URL'] = build_video_links(player_data[player_data['pass_oos'] > 0]) if video_yn else ''
  
  return result

#=============----------------===============---------------==============----------
def count_good_passes_obj(ppr_df, disp_player, action='pass', video_yn=False):
  """
    Count good passes (in-system) statistics for a player's actions.
    This is the inverse of out-of-system passes.
    
    Args:
        ppr_df: DataFrame containing player performance data
        disp_player: Player name to analyze
        action: Action type ('pass', 'att', 'srv'), defaults to 'pass'
        video_yn: Boolean to include video links, defaults to False
    
    Returns:
        dict: Dictionary with keys 'count', 'percent', 'percent_str', 'attempts', 'URL'
              Use .get() method to safely access values with defaults
        'count' - number of good passes (in-system)
        'percent' - decimal percentage of good passes (e.g., 0.85 for 85%)
        'percent_str' - formatted percentage string with no decimals (e.g., '85%')
        'attempts' - total attempts for the action
        'URL' - string of video links from build_video_links if video_yn is True, else empty string
    """
  # Get out-of-system statistics first
  oos_stats = count_oos_obj(ppr_df, disp_player, action, video_yn)

  # Calculate good passes as the inverse of out-of-system
  total_attempts = oos_stats.get('attempts', 0)
  oos_count = oos_stats.get('count', 0)

  # Calculate good passes
  good_count = total_attempts - oos_count
  good_percent = 1.0 - oos_stats.get('percent', 0.0) if total_attempts > 0 else 0.0

  # Format percentage string with no decimals
  good_percent_str = f"{good_percent:.0%}"

  # Get video links if video_yn is True, else return empty string
  video_links = oos_stats.get('URL')

  return {
    'count': good_count,           # number of good passes
    'percent': good_percent,       # percent of good passes (decimal)
    'percent_str': good_percent_str,  # percent of good passes (formatted string)
    'attempts': total_attempts,    # total attempts
    'URL': video_links            # video links or empty string
  }

  


 
#-----------------------------------------------------------------------------------------------
#
#          Functions for saving files to the server
#
#-----------------------------------------------------------------------------------------------

# a simple routine to write. afile to the drive

def create_google_drive_file( folder, filename, file ):
    return folder.create_file(filename, file)
  
#.     Get the report folder, and/or create it
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

def create_report_folder(folder_name, parent_id):
    #folder_name = "MyNewFolder"  # Replace with your desired folder name
    #parent_id = None  # Optional: Replace with a parent folder ID if needed
    result = create_folder(folder_name, parent_id)
    return f"Created folder '{result['name']}' with ID: {result['id']}"

def write_pdf_to_google_drive( folder, filename, pdf_file):
  new_pdf = folder.create_file(filename, pdf_file)
  return new_pdf


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
def write_to_nested_folder(folder_path, filename, content):
  """
    Handles PDFs, JSON, and Markdown:
    - PDFs: Saved as raw PDFs
    - Markdown: Converted to native Google Docs for NotebookLM (removes .md extension)
    - JSON: Kept as JSON files (for API use) but shared for visibility
    
    Args:
        folder_path: List of folder names for nested structure
        filename: Name of file to create
        content: Can be str, bytes, or anvil.BlobMedia object
    """
  current_folder = app_files.reports  

  # 1. Navigate/Create folders
  for subfolder_name in folder_path:
    next_folder = current_folder.get(subfolder_name)
    if next_folder is None:
      next_folder = current_folder.create_folder(subfolder_name)
    current_folder = next_folder

  if content is None:
    return "Skipped: Content is None"

    # 2. Identify File Type
  filename_lower = filename.lower()
  is_pdf = filename_lower.endswith('.pdf')
  is_json = filename_lower.endswith('.json')
  is_markdown = filename_lower.endswith('.md')

  # 3. Set mime type and title
  if is_pdf:
    target_mime = 'application/pdf'
    clean_title = filename 
    file_type_desc = "PDF"
  elif is_json:
    target_mime = 'application/json'
    clean_title = filename
    file_type_desc = "JSON"
  elif is_markdown:
    target_mime = 'application/vnd.google-apps.document'
    clean_title = filename.replace('.md', '')
    file_type_desc = "Google Doc"
  else:
    target_mime = 'application/octet-stream'
    clean_title = filename
    file_type_desc = "File"

    # 4. Convert content to BlobMedia if needed
  try:
    if isinstance(content, str):
      # String: encode to bytes and wrap in BlobMedia
      content_media = anvil.BlobMedia(
        target_mime,
        content.encode('utf-8'),
        name=clean_title
      )
    elif isinstance(content, bytes):
      # Raw bytes: wrap in BlobMedia
      content_media = anvil.BlobMedia(
        target_mime,
        content,
        name=clean_title
      )
    elif hasattr(content, 'get_bytes'):
      # Already a BlobMedia or Media object
      content_media = content
    else:
      return f"Error: Unsupported content type: {type(content)}"
  except Exception as e:
    return f"Error preparing content: {str(e)}"

    # 5. Create or Update file
  file = current_folder.get(clean_title)

  try:
    if file is None:
      # Create new file
      file = current_folder.create_file(clean_title, content_media)
    else:
      # Update existing file
      file.set_media(content_media)

      # 6. Share for NotebookLM visibility
    try:
      file.share("spccoach@gmail.com", type='user', role='writer', send_notification=False)
    except (AttributeError, Exception) as e:
      # Fallback if share method doesn't work
      try:
        file.add_permission(email="spccoach@gmail.com", role="writer")
      except Exception as e2:
        print(f"Warning: Could not share file: {e2}")

        # Touch the title to trigger metadata sync
    file.title = clean_title

    return f"✓ {clean_title} written as {file_type_desc} and shared"

  except Exception as e:
    error_msg = f"Error writing {filename}: {str(e)}"
    print(error_msg)
    return error_msg
    
    


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

  

def calc_knock_out_obj(ppr_df, disp_player):
  """
    Calculate knock-out statistics for a player.
    
    Knock-out rate is calculated as:
    (Player Total Service Aces + Opponent Bad Passes) / Player Total Serves
    
    Args:
        ppr_df: DataFrame containing player performance data
        disp_player: Player name to analyze
    
    Returns:
        dict: Dictionary with keys for knock-out statistics
              Use .get() method to safely access values with defaults
    """

  try:
    # Get player point totals
    player_point_totals = player_pt_total(ppr_df, disp_player)

    # Initialize result dictionary
    result = {
      'knock_out_rate': 0.0,     # Main knock-out rate
      'total_serves': 0,         # Total serves by player
      'service_aces': 0,         # Player's service aces
      'opponent_bad_passes': 0,  # Opponent bad passes caused
      'knock_out_points': 0,     # Total knock-out points (aces + bad passes)
      'has_serves': False        # Whether player has any serves
    }

    # Check if player_point_totals has data
    if player_point_totals.empty:
      return result

      # Extract values from the DataFrame
    total_serves = player_point_totals.at[0, 'p_serves']
    service_aces = player_point_totals.at[0, 'p_tsa']
    bad_passes = player_point_totals.at[0, 'o_bad_pass']

    # Populate result dictionary
    result['total_serves'] = total_serves
    result['service_aces'] = service_aces
    result['opponent_bad_passes'] = bad_passes
    result['knock_out_points'] = service_aces + bad_passes
    result['has_serves'] = total_serves > 0

    # Calculate knock-out rate
    if total_serves > 0:
      result['knock_out_rate'] = (service_aces + bad_passes) / total_serves
    else:
      result['knock_out_rate'] = 0.0

  except (KeyError, IndexError) as e:
    print(f"Error calculating knock-out stats for {disp_player}: {e}")
    # Return default values on error
    result = {
      'knock_out_rate': 0.0,
      'total_serves': 0,
      'service_aces': 0,
      'opponent_bad_passes': 0,
      'knock_out_points': 0,
      'has_serves': False,
      'error': str(e)
    }

  return result


def get_knock_out_summary(ppr_df, disp_player):
  """
    Get a formatted summary of knock-out statistics.
    
    Args:
        ppr_df: DataFrame containing player performance data
        disp_player: Player name to analyze
    
    Returns:
        dict: Summary statistics with formatted strings
    """

  stats = calc_knock_out(ppr_df, disp_player)

  return {
    'player': disp_player,
    'knock_out_rate': stats.get('knock_out_rate', 0.0),
    'knock_out_percentage': f"{stats.get('knock_out_rate', 0.0):.1%}",
    'summary': f"{stats.get('knock_out_points', 0)} knock-out points on {stats.get('total_serves', 0)} serves",
    'breakdown': f"Aces: {stats.get('service_aces', 0)}, Bad passes caused: {stats.get('opponent_bad_passes', 0)}",
    'has_data': stats.get('has_serves', False)
  }


  


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

    # set, these are setup as a radio button, so only one can be chceked.  We pass the 'set' as either 1,2,3, or it is not in the list
    if column == 'set':
      result = result[ result[column] == value ]

    if column == 'srv_fr':
      #print(f"Value list: {value}, type of first item: {type(value[0])}")
      #print(f"Column values: {result['serve_src_zone_net'].head()}")
      #print(f"Column dtype: {result['serve_src_zone_net'].dtype}")

      result = result[result['serve_src_zone_net'].astype(str).isin(value)]
      #print(f"Srv_fr filter, {value}, result : {result.shape[0]}")

      # serving to filter...
    if column == 'srv_to':
      # Create the srv_to column by concatenating (convert each value to string)
      result['srv_to'] = result['serve_dest_zone_net'].astype(str) + result['serve_dest_zone_depth'].astype(str)
      result = result[result['srv_to'].isin(value)]
      #print(f"Srv_to filter, {value}, result : {result.shape[0]}")


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
      result = result[ result['att_touch_height'] >= value ]
    if column == 'att_ht_high':
      result = result[ result['att_touch_height'] <= value ]

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


  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Setup Report Basics
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
def setup_report_basics(lgy, team, function_name=None):
  """
  Helper function to set up basic title and label lists from report_list table.
  This can be reused by all report functions.
  
  Args:
    function_name: Name of the function in report_list table
    lgy: League identifier
    team: Team identifier
    
  Returns:
    tuple: (title_list, label_list) pre-populated from database
  """

  if function_name is None:
    function_name = inspect.stack()[1].function

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=function_name)
  if rpt_row:
    title_list[0] = rpt_row['rpt_title']
    title_list[1] = rpt_row['rpt_sub_title']
    title_list[2] = rpt_row['rpt_section_title1']
    title_list[3] = lgy  # Override with actual lgy parameter
    title_list[4] = team  # Override with actual team parameter
    title_list[5] = rpt_row['rpt_type']
    title_list[6] = rpt_row['filter_text']
    title_list[7] = rpt_row['explain_text']

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

    df_desc_list[0] = rpt_row['df_desc_1']
    df_desc_list[1] = rpt_row['df_desc_2']
    df_desc_list[2] = rpt_row['df_desc_3']
    df_desc_list[3] = rpt_row['df_desc_4']
    df_desc_list[4] = rpt_row['df_desc_5']
    df_desc_list[5] = rpt_row['df_desc_6']
    df_desc_list[6] = rpt_row['df_desc_7']
    df_desc_list[7] = rpt_row['df_desc_8']
    df_desc_list[8] = rpt_row['df_desc_9']
    df_desc_list[9] = rpt_row['df_desc_10']

    image_desc_list[0] = rpt_row['image_desc_1']
    image_desc_list[1] = rpt_row['image_desc_2']
    image_desc_list[2] = rpt_row['image_desc_3']
    image_desc_list[3] = rpt_row['image_desc_4']
    image_desc_list[4] = rpt_row['image_desc_5']
    image_desc_list[5] = rpt_row['image_desc_6']
    image_desc_list[6] = rpt_row['image_desc_7']
    image_desc_list[7] = rpt_row['image_desc_8']
    image_desc_list[8] = rpt_row['image_desc_9']
    image_desc_list[9] = rpt_row['image_desc_10']


  return title_list, label_list, df_desc_list, image_desc_list

#------------------------------------------------------------------------------------------
#
#.   Calculate Ellipse Area
#       This is a bit differenct, calc_player_data uses similar code, but not this function (yet)
#
#--------------------------------------------------------------------------------------------------
def find_ellipse_area(tmp1_df, disp_player, type, min_att=5, video_yn=False):
  '''
  tmp1_df is the ppr dataframe with the data in one of the touch dest x,y to calculate ellipse for
  type can be:
    - 'srv'
    - 'pass'
    - 'set'
    - 'att'
    - 'dig'

    This always uses the destination coordinate, defaults to pass if it does not recognize type

    min_att, pass the minimum number of attempts, default is 5

    This calculates for all points in tmp1_df, so limit it to the point desired before calling

    Args:
        tmp1_df: DataFrame containing player performance data
        disp_player: Player name to analyze
        type: Action type ('srv', 'pass', 'set', 'att', 'dig')
        min_att: Minimum number of attempts, defaults to 5
        video_yn: Boolean to include video links, defaults to False

    Returns:
        dict: Dictionary with keys 'area', 'type', 'message', 'success', 'attempts', 'URL'
        'area' - calculated ellipse area or None
        'type' - action type
        'message' - status message
        'success' - boolean indicating if ellipse calculation was successful
        'attempts' - number of data points used
        'URL' - string of video links from build_video_links if video_yn is True, else empty string
  '''

  # default 
  el_area = None
  el_message = 'find_ellipse_area: '
  el_success = False
  el_url = ''

  if type == 'srv':
    var_x = 'serve_dest_x'
    var_y = 'serve_dest_y'
    filtered_df = tmp1_df[tmp1_df['serve_player'] == disp_player]

    # and a type, srv-pass shows the pass area for all serves bu the disp_player.  Used when looking for the opponenet's pass area
  if type == 'srv-pass':
    var_x = 'pass_dest_x'
    var_y = 'pass_dest_y'
    filtered_df = tmp1_df[tmp1_df['serve_player'] == disp_player]
  elif type == 'pass':
    var_x = 'pass_dest_x'
    var_y = 'pass_dest_y'
    filtered_df = tmp1_df[tmp1_df['pass_player'] == disp_player]
  elif type == 'set':
    var_x = 'set_dest_x'
    var_y = 'set_dest_y'
    filtered_df = tmp1_df[tmp1_df['set_player'] == disp_player]
  elif type == 'att':
    var_x = 'att_dest_x'
    var_y = 'att_dest_y'
    filtered_df = tmp1_df[tmp1_df['att_player'] == disp_player]
  elif type == 'dig':
    var_x = 'dig_dest_x'
    var_y = 'dig_dest_y'
    filtered_df = tmp1_df[tmp1_df['dig_player'] == disp_player]
  else:
    # default to pass
    el_message = el_message + 'type mismatch, used pass.  type='+type
    var_x = 'pass_dest_x'
    var_y = 'pass_dest_y'
    filtered_df = tmp1_df[tmp1_df['pass_player'] == disp_player]

  el_points = pd.concat([filtered_df[var_x], filtered_df[var_y]], axis=1)
  el_points = el_points.dropna().values
  el_att = len(el_points)
  if el_att >= min_att:  # must have at least 5 points to calculate the ellipse
    el_message = el_message + ' Ellipse calculated, number of points ='+str(el_att)
    el_mean, el_width, el_height, el_angle = calculate_standard_deviation_ellipse(el_points, confidence=1.0)
    el_area = math.pi*(el_width/2)*(el_height/2)
    el_success = True

  # Get video links if video_yn is True, else return empty string
  el_url = build_video_links(filtered_df) if video_yn else ''

  return {
    'area': round(el_area, 1) if el_area is not None else None,
    'type': type,
    'message': el_message,
    'success': el_success,
    'attempts': el_att,
    'URL': el_url
  }
  


def calc_ace_error_ratio(ppr_df, disp_player):
  # Filter rows where the player is serving
  player_serves = ppr_df[ppr_df['serve_player'] == disp_player]

  # Count aces (TSA) and errors (TSE)
  aces = len(player_serves[player_serves['point_outcome'] == 'TSA'])
  errors = len(player_serves[player_serves['point_outcome'] == 'TSE'])

  # Calculate ratio (handle division by zero)
  if errors == 0:
    return float('inf') if aces > 0 else 0
  return aces / errors if aces > 0 or errors > 0 else 0

def get_player_row(disp_player):
  from anvil.tables import app_tables

  # Split disp_player into team, number, and shortname
  parts = disp_player.split(' ', 2)  # Still split on first two spaces
  if len(parts) < 3:  # Ensure we have at least team, number, and some shortname
    return None

  team, number, shortname = parts[0], parts[1], parts[2]

  # Query the master_player table
  player_row = app_tables.master_player.get(
    team=team,
    number=number,
    shortname=shortname
  )

  return player_row

def get_player_angular_attack_table(new_df, player_data_stats_df, disp_player):
  '''
  Generates the dataframe for the player angular attack table
  INPUTS:
    - new_df: DataFrame, limited as desired before calling
    - player_data_stats_df: DataFrame containing player statistics
    - disp_player: String, player name for the dataframe
  RETURNS
    - DataFrame containing the player angular attack statistics
  '''
  # Define the structure of the DataFrame
  df_dict = {
    ' ': ['FBHE', 'FBSO', 'Kills', 'Errors', 'Attempts', '% of Attempts', '% In System', 'URL'],
    'Cut-Left': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A1
    'Angle-Left': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A2
    'Over-Middle': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A3
    'Angle-Right': [0, 0, 0, 0, 0, 0, 0, ' '],   # Zone A4
    'Cut-Right': [0, 0, 0, 0, 0, 0, 0, ' ']      # Zone A5
  }

  # Create DataFrame without setting an index
  angle_table = pd.DataFrame.from_dict(df_dict)

  #print(f"get player angular attack table: df passed in: {new_df.shape[0]}, player: {disp_player}")
  #print(f"angle table (initial):\n{angle_table}")

  angles = ['A1', 'A2', 'A3', 'A4', 'A5']
  ang_labels = ['Cut-Left', 'Angle-Left', 'Over-Middle', 'Angle-Right', 'Cut-Right']
  attempts = 0

  for i in range(5):
    # Filter the DataFrame for the current angular zone
    tmp_df = new_df[new_df['att_angular_zone'] == angles[i]]
    #print(f"in Loop for i:{i}, ang_label: {ang_labels[i]}, angles: {angles[i]}, # of rows: {tmp_df.shape[0]}")

    # Compute metrics
    fbhe_result = fbhe_obj(tmp_df, disp_player, 'both', True)
    oos_vector = count_out_of_system(tmp_df, disp_player, 'att')

    # Update the DataFrame using row index (integer) and column (ang_labels[i])
    angle_table.loc[angle_table[' '] == 'FBHE', ang_labels[i]] = fbhe_result.fbhe
    angle_table.loc[angle_table[' '] == 'FBSO', ang_labels[i]] = fbhe_result.fbso
    angle_table.loc[angle_table[' '] == 'Kills', ang_labels[i]] = fbhe_result.kills
    angle_table.loc[angle_table[' '] == 'Errors', ang_labels[i]] = fbhe_result.errors
    angle_table.loc[angle_table[' '] == 'Attempts', ang_labels[i]] = fbhe_result.attempts
    angle_table.loc[angle_table[' '] == '% In System', ang_labels[i]] = f"{(1 - oos_vector[1]):.1%}"
    angle_table.loc[angle_table[' '] == 'URL', ang_labels[i]] = fbhe_result.video_link
    attempts = fbhe_result.attempts + attempts
    #print(f"Attempts: {attempts}")

  # Calculate percent of attempts
  for i in range(5):
    value = angle_table.loc[angle_table[' '] == 'Attempts', ang_labels[i]]
    #print(f"Value: {value}")
    value = float(value)
    if attempts != 0:
      angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]] = str('{:.0%}').format(value / attempts)
    else:
      angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]] = '0%'
    #print(f"number of attempts: {value}, attempts: {attempts}, percent of attempts: {angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]]}")

  #print(f"returning angular table for {disp_player} (formatted for display):\n{angle_table}")

  return angle_table



#----------------------------------------------------------------------------------

def anonymize_json(json_data, player_replacement="Player A", pair_replacement="Pair A"):
  """
    Anonymize JSON data, preserving structure while anonymizing player/pair names in title_9, title_10, and strings.
    """
  log_info("---------- Anonymize Json --------------")
  try:
    if json_data is None:
      log_info("json_data is None in anonymize_json")
      return {}

      # Parse JSON string if necessary
    if isinstance(json_data, str):
      log_info("Parsing JSON string")
      try:
        parsed_data = json.loads(json_data)
      except json.JSONDecodeError as e:
        log_error(f"Failed to parse JSON string: {e}")
        return {"error": f"Invalid JSON string: {str(e)}"}
    else:
      parsed_data = json_data

      # Deep copy to avoid modifying original
    anon_data = copy.deepcopy(parsed_data)

    # Extract original player and pair names from titles for string replacement
    player_name = None
    pair_name = None
    if isinstance(anon_data, dict) and 'titles' in anon_data and isinstance(anon_data['titles'], dict):
      # Get and clean player name (strip whitespace)
      raw_player_name = anon_data['titles'].get('title_9')
      if raw_player_name and isinstance(raw_player_name, str):
        player_name = raw_player_name.strip()
        # Get pair name (handle None case)
      raw_pair_name = anon_data['titles'].get('title_10')
      if raw_pair_name and isinstance(raw_pair_name, str):
        pair_name = raw_pair_name.strip()

    log_info(f"Anonymizing - Player: '{player_name}' -> '{player_replacement}', Pair: '{pair_name}' -> '{pair_replacement}'")

    def recursive_anonymize(data):
      if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
          if key in ['title_9', 'title_10']:
            # Anonymize title_9 and title_10
            if key == 'title_9':
              new_data[key] = player_replacement
            elif key == 'title_10':
              new_data[key] = pair_replacement if value is not None else None
          elif isinstance(value, (dict, list)):
            new_data[key] = recursive_anonymize(value)
          elif isinstance(value, str) and value:  # Only process non-empty strings
            # Replace player/pair names in strings
            anonymized_value = value
            if player_name and player_name in anonymized_value:
              anonymized_value = anonymized_value.replace(player_name, player_replacement)
            if pair_name and pair_name in anonymized_value:
              anonymized_value = anonymized_value.replace(pair_name, pair_replacement)
            new_data[key] = anonymized_value
          else:
            new_data[key] = value
        return new_data
      elif isinstance(data, list):
        return [recursive_anonymize(item) for item in data]
      else:
        return data

    anon_data = recursive_anonymize(anon_data)

    # Validate that anon_data contains some data
    if not anon_data or (isinstance(anon_data, dict) and not any(anon_data.values())):
      log_info(f"Anonymized JSON is empty: {anon_data}")
      return {"error": "No valid data after anonymization"}

    log_info("Anonymization completed successfully")
    return anon_data

  except Exception as e:
    log_error(f"Error in anonymize_json: {str(e)}")
    return {"error": f"Anonymization failed: {str(e)}"}



def remove_null_fields(data):
  """
    Recursively clean JSON data by:
    - Removing null fields
    - Removing empty dictionaries/lists
    - Replacing URL-like strings with ''
    """
  try:
    if isinstance(data, dict):
      cleaned_dict = {}
      for k, v in data.items():
        cleaned_v = remove_null_fields(v)

        # Skip null or empty values
        if cleaned_v is None:
          continue
        if isinstance(cleaned_v, (dict, list)) and not cleaned_v:
          continue

        cleaned_dict[k] = cleaned_v
      return cleaned_dict if cleaned_dict else None

    elif isinstance(data, list):
      cleaned_list = [remove_null_fields(item) for item in data]
      cleaned_list = [item for item in cleaned_list if item is not None and (not isinstance(item, (dict, list)) or item)]
      return cleaned_list if cleaned_list else None

    elif isinstance(data, str):
      # Detect and replace URLs
      if re.search(r"https?://", data):
        return ''
      if data.strip() == "URL":
        return None
      return data

    else:
      return data if data is not None else None

  except Exception as e:
    print(f"Error in remove_null_fields: {str(e)}")
    return data

def generate_ai_summary(json_data, prompt_template, coach_id=None, human_summary=None, images=None, include_images=False):
  """
  Call Gemini API to generate a summary.
  
  Args:
    json_data: Performance data (dict or JSON string)
    prompt_template: Instructions for AI (from ai_prompt_templates table)
    coach_id: Coach email
    human_summary: Optional additional context
    images: List of BlobMedia image objects (optional)
    include_images: Boolean - send images to API (controlled by rpt_mgr.send_ai_images)
  
  Returns:
    str: AI-generated markdown summary
  """
  try:
    log_debug(f"Generate AI Summary: coach_id={coach_id}, include_images={include_images}, num_images={len(images) if images else 0}")

    # --- Get API key ---
    api_key = anvil.secrets.get_secret('GEMINI_API_KEY')
    if not api_key:
      log_error("Gemini API key not found in Secrets")
      return "Error: Gemini API key not found"

    # --- Ensure JSON is parsed ---
    if not json_data:
      return "Error: No input data provided"
    if isinstance(json_data, str):
      try:
        json_data = json.loads(json_data)
      except json.JSONDecodeError as e:
        return f"Error: Invalid JSON input - {str(e)}"

    # --- Remove Null fields and URLs ---
    json_data = remove_null_fields(json_data)

    # --- Anonymize sensitive fields ---
    anon_json = anonymize_json(json_data)
    if isinstance(anon_json, dict) and "error" in anon_json:
      return f"Error: {anon_json['error']}"

    # --- Default prompt if missing ---
    if not prompt_template:
      prompt_template = "Please summarize the following data:"

    # --- Build the combined prompt string ---
    anon_json_string = json.dumps(anon_json, indent=2)

    prompt_text = (
      f"{prompt_template}\n\n"
      f"The following data is provided in JSON format:\n"
      f"```json\n{anon_json_string}\n```"
    )

    if human_summary:
      prompt_text += f"\n\nAdditional context: {human_summary}"

    # --- Build payload parts ---
    parts = [{"text": prompt_text}]

    # --- ONLY add images if include_images=True (set by rpt_mgr.send_ai_images) ---
    if include_images and images and len(images) > 0:
      import base64
      log_info(f"Including {len(images)} images in API request (rpt_mgr.send_ai_images=True)")

      # ... image instruction code ...

      # Add images to payload
      for idx, img in enumerate(images):
        try:
          # Handle LazyMedia - convert to BlobMedia first
          if hasattr(img, 'get_bytes'):
            img_bytes = img.get_bytes()
          else:
            # For LazyMedia, read the content
            img_bytes = img.get_bytes() if callable(getattr(img, 'get_bytes', None)) else bytes(img)

            # Convert to base64
          img_base64 = base64.b64encode(img_bytes).decode('utf-8')

          # Get mime type - LazyMedia may not have content_type
          if hasattr(img, 'content_type') and img.content_type:
            mime_type = img.content_type
          elif hasattr(img, 'mime_type') and img.mime_type:
            mime_type = img.mime_type
          else:
            mime_type = 'image/png'  # Default assumption

          log_debug(f"Processing image {idx+1}: type={type(img).__name__}, mime={mime_type}, size={len(img_bytes)} bytes")
    

          
          parts.append({
            "inline_data": {
              "mime_type": mime_type,
              "data": img_base64
            }
          })
          log_debug(f"Added image {idx+1} to API request")
        except Exception as e:
          log_error(f"Failed to add image {idx+1}: {e}")
    else:
      if include_images:
        log_info("include_images=True but no images provided")
      else:
        log_info("include_images=False, not sending images (cost savings)")

    # --- Build payload ---
    payload = {
      "contents": [{
        "role": "user",
        "parts": parts
      }],
      "generationConfig": {
        "temperature": 0.7,
        "topP": 0.9,
        "maxOutputTokens": 8192
      }
    }

    # --- Send request ---
    json_payload = json.dumps(payload)

    # Use Gemini 2.5 Flash
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={api_key}"

    log_info(f"Sending request to Gemini API with {len(parts)} parts (1 text + {len(parts)-1} images)")

    response = anvil.http.request(
      url=gemini_url,
      method="POST",
      headers={"Content-Type": "application/json"},
      data=json_payload
    )

    # --- Parse response ---
    json_response = json.loads(response._content)
    summary = json_response['candidates'][0]['content']['parts'][0]['text']

    log_info(f"AI Summary Returned ({len(summary)} chars)")

    return summary.strip()

  except anvil.http.HttpError as e:
    log_error(f"HTTP error: {e.content}")
    if e.status == 403:
      return "Error: 403 Forbidden - API key blocked or API not enabled"
    return f"Error generating summary: HTTP {e.status} - {e.content}"
  except KeyError as e:
    log_error(f"Response parsing error: {str(e)}")
    return f"Error: Invalid response format from Gemini API - {str(e)}"
  except Exception as e:
    log_critical(f"Unexpected error in generate_ai_summary: {str(e)}")
    import traceback
    log_critical(traceback.format_exc())
    return f"Error generating summary: {str(e)}"
    



#----------------------------------------------------------------------------------
@anvil.server.callable
def test_gemini_api():
  """
    Test function to verify Gemini API connectivity.
    """
  test_data = {
    "dataframes": {
      "df_1": [
        {"": "FBHE", "All": "0.292", "Right Slot": "0.6", "Left Pin": "-0.5"},
        {"": "Kills", "All": "23", "Right Slot": "10", "Left Pin": "0"},
        {"": "Attempts", "All": "48", "Right Slot": "15", "Left Pin": "2"},
        {"": "Errors", "All": "9", "Right Slot": "1", "Left Pin": "1"},
        {"": "Percentile", "All": "67%", "Right Slot": "95%", "Left Pin": "0%"},
        {"": "FBSO", "All": "0.479", "Right Slot": "0.667", "Left Pin": "0.0"}
      ]
    }
  }
  test_prompt = "Summarize: {json_data}"
  log_info(f"Testing Gemini API with test_data={test_data}, prompt={test_prompt}")
  return generate_ai_summary(test_data, test_prompt, coach_id="test_coach")


#----------------------------------------------------------------------------------
def insert_summary_into_pdf(pdf_media, summary_text):
  """Insert AI summary into PDF below subtitle."""
  try:
    doc = fitz.open(stream=pdf_media.get_bytes(), filetype="pdf")
    page = doc[0]  # First page
    # Assume subtitle is at y=100 (adjust based on form layout)
    insert_point = fitz.Point(50, 150)  # Below subtitle, left margin
    page.insert_text(
      insert_point,
      summary_text,
      fontsize=10,
      fontname="helv",
      color=(0,0,0)
    )
    updated_bytes = doc.tobytes()
    doc.close()
    return anvil.BlobMedia('application/pdf', updated_bytes, name=pdf_media.name)
  except Exception as e:
    log_error(f"Error inserting summary into PDF: {str(e)}")
    return pdf_media

#----------------------------------------------------------------------------------
def create_summary_pdf(summary_text, pdf_name):
  """Create a new PDF with the AI summary."""
  try:
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    content = [Paragraph(summary_text, styles['Normal'])]
    doc.build(content)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return anvil.BlobMedia('application/pdf', pdf_bytes, name=pdf_name)
  except Exception as e:
    log_error(f"Error creating summary PDF: {str(e)}")
    return None

def generate_ai_pdf_summary(report_id, summary, ai_form='player_ai_summary'):
  """
    Generate a one-page PDF file containing an AI summary in markdown format.
    
    Args:
        report_id (str): The unique identifier for the report
        summary (str): The AI-generated summary content in markdown
        ai_form (str): The form template to use for PDF generation
        
    Returns:
        dict: Contains 'pdf', 'json_file_name', and 'error' keys
              - pdf: The generated PDF file or None if error
              - json_file_name': Always None in current implementation
              - error: Error message string or None if successful
    """
  try:
    # Validate inputs
    if not report_id:
      error_msg = "Report ID cannot be empty"
      log_error(error_msg)
      return {'pdf': None, 'json_file_name': None, 'error': error_msg}

    if not summary:
      error_msg = "Summary cannot be empty"
      log_error(error_msg)
      return {'pdf': None, 'json_file_name': None, 'error': error_msg}

    # Convert markdown to ReportLab-compatible HTML ( no need to convert to HTML when displaying using Anvil PDf Rendered)
    formatted_summary = summary
    #formatted_summary = markdown_to_reportlab_html(summary)
    
    # Clean the summary - remove markdown code fence tags
    cleaned_summary = formatted_summary.strip()

    # Remove opening ```markdown or ``` tag
    if cleaned_summary.startswith('```markdown'):
      cleaned_summary = cleaned_summary[11:].strip()
    elif cleaned_summary.startswith('```'):
      cleaned_summary = cleaned_summary[3:].strip()

    # Remove closing ``` tag
    if cleaned_summary.endswith('```'):
      cleaned_summary = cleaned_summary[:-3].strip()

    log_info("Cleaned summary, removed code fences")

    # Store in report_data table for form rendering
    report_data_row = app_tables.report_data.get(report_id=report_id)
    
    # Convert string to media object
    summary_media = anvil.BlobMedia(
      'text/markdown',
      cleaned_summary.encode('utf-8'),  # ← Use cleaned_summary
      name=f'ai_summary_{report_id}.md'
    )


    if report_data_row:
      report_data_row['df_1'] = summary_media
      log_info(f"Stored AI summary in report_data for report_id: {report_id}")
    else:
      log_error(f"Report data row not found for report_id: {report_id}")
      return {'pdf': None, 'json_file_name': None, 'error': 'Report data not found'}

    # Generate PDF using the specified form
    try:
      pdf_result = PDFRenderer().render_form(ai_form, report_id)

      # Convert StreamingMedia to BlobMedia if necessary
      if isinstance(pdf_result, anvil._serialise.StreamingMedia):
        pdf_result = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), 
                                     name=f"ai_summary_{report_id}.pdf")

      log_info(f"Successfully generated AI summary PDF for report_id: {report_id}")
      return {'pdf': pdf_result, 'json_file_name': None, 'error': None}

    except Exception as e:
      log_error(f"Error rendering PDF form: {str(e)}")
      return {'pdf': None, 'json_file_name': None, 'error': f'PDF rendering failed: {str(e)}'}

  except Exception as e:
    log_critical(f"Unexpected error in generate_ai_pdf_summary: {str(e)}")
    return {'pdf': None, 'json_file_name': None, 'error': str(e)}



@anvil.server.callable
def test_ai_images_flag():
  """Test the rpt_mgr.send_ai_images flag with real report data."""

  try:
    log_info("=== Starting AI Images Flag Test ===")

    # Find a report that actually HAS images
    all_reports = app_tables.report_data.search(
      tables.order_by("created_at", ascending=False)
    )

    report_with_images = None
    for report in all_reports:
      # Check if this report has at least one image
      if report['image_1']:
        report_with_images = report
        log_info(f"Found report with images: {report['report_id']}")
        break

    if not report_with_images:
      log_error("No reports found with images!")
      return {"error": "No reports with images found. Generate a scouting report first."}

      # Collect images from this report - FIXED VERSION
    images = []
    for i in range(1, 11):
      img_field = f'image_{i}'
      try:
        img = report_with_images[img_field]  # Dictionary-style access
        if img:
          images.append(img)
          log_info(f"✓ Collected {img_field}: {type(img).__name__}, size: {len(img.get_bytes())} bytes")
      except Exception as e:
        log_debug(f"No image at {img_field} or error: {e}")

    log_info(f"Total images collected: {len(images)}")

    if len(images) == 0:
      log_error("Report found but no images could be extracted!")
      # Debug: print what fields the report HAS
      log_info(f"Report fields: {list(report_with_images.keys())}")
      return {"error": "Report found but no images could be extracted"}

      # Get some real data from the report
    test_data = {
      "report_id": report_with_images['report_id'],
      "title": report_with_images['title_1'] if report_with_images['title_1'] else 'Test Report',
      "player": "Test Player",
      "fbhe": 0.450,
      "attacks": 100,
      "kills": 45,
      "errors": 10
    }

    prompt = """Analyze this beach volleyball player's attacking performance. 
        
Key metrics:
- FBHE (First Ball Hitting Efficiency): 0.450
- Total Attacks: 100
- Kills: 45
- Errors: 10

You have access to shot charts showing attack distribution across the court.
When discussing attack patterns, reference these visual charts using "as shown in Figure [number]".
Provide insights based on both the statistics and the visual patterns you observe.
"""

    # Test 1: WITH images
    log_info("=== Test 1: WITH IMAGES ===")
    result_with_images = generate_ai_summary(
      test_data,
      prompt,
      coach_id="test@test.com",
      include_images=True,
      images=images
    )

    log_info(f"WITH IMAGES Result: {result_with_images[:300]}...")

    # Test 2: WITHOUT images (same data, just flag=False)
    log_info("=== Test 2: WITHOUT IMAGES ===")
    result_without_images = generate_ai_summary(
      test_data,
      prompt,
      coach_id="test@test.com",
      include_images=False,
      images=images  # Images available but not sent
    )

    log_info(f"WITHOUT IMAGES Result: {result_without_images[:300]}...")

    # Compare results
    with_images_mentions_figures = "Figure" in result_with_images or "figure" in result_with_images or "chart" in result_with_images.lower()
    without_images_mentions_figures = "Figure" in result_without_images or "figure" in result_without_images or "chart" in result_without_images.lower()

    return {
      "success": True,
      "report_id": report_with_images['report_id'],
      "images_collected": len(images),
      "with_images": {
        "length": len(result_with_images),
        "preview": result_with_images[:500],
        "mentions_figures": with_images_mentions_figures,
        "full": result_with_images
      },
      "without_images": {
        "length": len(result_without_images),
        "preview": result_without_images[:500],
        "mentions_figures": without_images_mentions_figures,
        "full": result_without_images
      },
      "comparison": {
        "with_images_longer": len(result_with_images) > len(result_without_images),
        "difference_chars": len(result_with_images) - len(result_without_images),
        "both_mention_figures": with_images_mentions_figures and without_images_mentions_figures,
        "only_with_images_mentions": with_images_mentions_figures and not without_images_mentions_figures
      }
    }

  except Exception as e:
    log_critical(f"Test failed: {e}")
    import traceback
    log_critical(traceback.format_exc())
    return {"error": str(e), "traceback": traceback.format_exc()}

def markdown_to_reportlab_html(markdown_text):
  """Convert markdown to ReportLab-compatible HTML"""
  if not markdown_text:
    return ""

  html = markdown_text

  # Headers
  html = re.sub(r'^### (.+)$', r'<b>\1</b>', html, flags=re.MULTILINE)
  html = re.sub(r'^## (.+)$', r'<b><font size="12">\1</font></b>', html, flags=re.MULTILINE)
  html = re.sub(r'^# (.+)$', r'<b><font size="14">\1</font></b>', html, flags=re.MULTILINE)

  # Bold
  html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', html)
  html = re.sub(r'__(.+?)__', r'<b>\1</b>', html)

  # Italic
  html = re.sub(r'\*(.+?)\*', r'<i>\1</i>', html)
  html = re.sub(r'_(.+?)_', r'<i>\1</i>', html)

  # Bullets
  html = re.sub(r'^[\*\-] (.+)$', r'• \1', html, flags=re.MULTILINE)

  # Line breaks
  html = html.replace('\n\n', '<br/><br/>')
  html = html.replace('\n', '<br/>')

  # Code
  html = re.sub(r'`(.+?)`', r'<font face="Courier">\1</font>', html)

  return html
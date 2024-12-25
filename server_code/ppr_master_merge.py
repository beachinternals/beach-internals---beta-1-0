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
import math
import datetime

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#

@anvil.server.callable
def create_master_ppr( user_league, user_gender, user_year, user_team, data_set ):
  # main idea here is to go thru the datafiles table, limited by league, gender, year, and team.
  # then just append all the different ppr csv files together into a master. THen write this to a csv file
  # then put this into a data table  
  # sample code on how to do this from a file system

  # data_set = 
  #    All - do all three options below - NOT IMPLEMENTED 
  #    Private - only the team private file
  #    Scouting - the league based scouting file
  #    League - the league wide file that includes all files (team and private) for a league
  #
  #     Therefore, the ppr_csv_tables will have a 'Team' designation for each team (i.e. FSU, LSU, Internals), Scouting, and League

  # pass this off to a background task!
  task = anvil.server.launch_background_task('make_master_ppr',user_league, user_gender, user_year, user_team, data_set )
  return task
  
@anvil.server.background_task
def make_master_ppr( user_league, user_gender, user_year, user_team, data_set ):
  task = make_master_ppr_not_background(user_league, user_gender, user_year, user_team, data_set)
  return task

@anvil.server.callable
def make_master_ppr_not_background(user_league, user_gender, user_year, user_team, data_set):
  # initialize a dataframe to hold hte master ppr data
  master_ppr_df = pd.DataFrame()
  
  # open btd file list, filer for the desired group
  #if data_set == "All":
  #  btd_rows = app_tables.btd_files.search(
  #    league = user_league,
  #    gender = user_gender,
  #    year = user_year,
  #    team = user_team,
  #    private = True
  #    )
  #print(f"Data Set:{data_set}")
  if data_set == "Private":
    btd_rows = app_tables.btd_files.search(
      league = user_league,
      gender = user_gender,
      year = user_year,
      team = user_team,
      private = True
      )
  elif data_set == "Scouting":
    btd_rows = app_tables.btd_files.search(
      league = user_league,
      gender = user_gender,
      year = user_year,
      private = False
      )
    user_team = "Scout"
  elif data_set == "League":
    btd_rows = app_tables.btd_files.search(
      league = user_league,
      gender = user_gender,
      year = user_year
      )
    user_team = "League"
  else:
    #print(f"We failed with data set:{data_set}")
    return False

  # now,, start a loop of the rows
  for flist_r in btd_rows:
    # call the function to return the ppr file given the btd file
    # check if there is a data object in the ppr_data field:
    if  flist_r['ppr_data']:
      ppr_df = pd.read_csv(io.BytesIO( flist_r['ppr_data'].get_bytes()))
      #print(f"reading ppr_file, size: {ppr_df.size}")
      master_ppr_df = pd.concat([master_ppr_df,ppr_df])
      #print(f"master ppr file sie: {master_ppr_df.size}")
  
  #print(f"size of master_ppr_df: {master_ppr_df.size}")
  #print(master_ppr_df)
  # so we should now have te master ppr_df for the given league, gender, year, and team
  # write this to a data file

  create_master_ppr_table(master_ppr_df, user_league, user_gender, user_year, user_team )
  
  return True

def create_master_ppr_table( master_ppr_df, user_league, user_gender, user_year, user_team ):
  # append all the records in the master_ppr_df file into the master_ppr datable

  for d in master_ppr_df.to_dict(orient="records"): ## this loop just tries to clean up the data.  may be able to discard?
    # d is now a dict of {columnname -> value} for this row
    # We use Python's **kwargs syntax to pass the whole dict as
    # keyword arguments
    #print(f"inspecting row d: {d['point_no']}")

    # So I gues we will look at each variable, get it right
    d['league'] = d['league'] if isinstance(d['league'],str) else ""
    d['gender'] = d['gender'] if isinstance(d['gender'],str) else ""
    d['year'] = d['year'] if isinstance(d['year'],(float,int)) else 0
    d['comp_l1'] = d['comp_l1'] if isinstance(d['comp_l1'],str) else ""
    d['comp_l2'] = d['comp_l2'] if isinstance(d['comp_l2'],str) else ""
    d['comp_l3'] = d['comp_l3'] if isinstance(d['comp_l3'],str) else ""
    d['team'] = d['point_no'] if isinstance(d['point_no'],(float,int)) else 0
    #d['game_date'] = d['game_date'] if isinstance(d['game_date'],[float(),int()]) else 0
    d['filename'] = d['filename'] if isinstance(d['filename'],str) else ""
    d['video_id'] = d['video_id'] if isinstance(d['video_id'],str) else ""
    d['rally_id'] = d['rally_id'] if isinstance(d['rally_id'],(float,int)) else 0
    d['point_no'] = d['point_no'] if isinstance(d['point_no'],(float,int)) else 0
    d['a_set_diff'] = d['a_set_diff'] if isinstance(d['a_set_diff'],(float,int)) else 0
    d['a_score_diff'] = d['a_score_diff'] if isinstance(d['a_score_diff'],(float,int)) else 0
    d['set'] = d['set'] if isinstance(d['set'],(float,int)) else 0
    d['a_score'] = d['a_score'] if isinstance(d['a_score'],(float,int)) else 0
    d['teama'] = d['teama'] if isinstance(d['teama'],str) else ""
    d['player_a1'] = d['player_a1'] if isinstance(d['player_a1'],str) else ""
    d['player_a2'] = d['player_a2'] if isinstance(d['player_a2'],str) else ""
    d['b_score'] = d['b_score'] if isinstance(d['b_score'],(float,int)) else 0
    d['teamb'] = d['teamb'] if isinstance(d['teamb'],str) else ""
    d['player_b1'] = d['player_b1'] if isinstance(d['player_b1'],str) else ""
    d['player_b2'] = d['player_b2'] if isinstance(d['player_b2'],str) else ""

    d['serve_player'] = d['serve_player'] if isinstance(d['serve_player'],str) else ""
    d['serve_src_x'] = d['serve_src_x'] if isinstance(d['serve_src_x'],(float,int)) else 0
    d['serve_src_y'] = d['serve_src_y'] if isinstance(d['serve_src_y'],(float,int)) else 0
    d['serve_src_t'] = d['serve_src_t'] if isinstance(d['serve_src_t'],(float,int)) else 0
    d['serve_src_zone_depth'] = d['serve_src_zone_depth'] if isinstance(d['serve_src_zone_depth'],str) else ""
    d['serve_src_zone_net'] = d['serve_src_zone_net'] if isinstance(d['serve_src_zone_net'],(float,int)) else 0
    d['serve_dest_x'] = d['serve_dest_x'] if isinstance(d['serve_dest_x'],(float,int)) else 0
    d['serve_dest_x'] = d['serve_dest_x'] if isinstance(d['serve_dest_x'],(float,int)) else 0
    d['serve_dest_y'] = d['serve_dest_y'] if isinstance(d['serve_dest_y'],(float,int)) else 0
    d['serve_dest_zone_depth'] = d['serve_dest_zone_depth'] if isinstance(d['serve_dest_zone_depth'],str) else ""
    d['serve_dest_zone_net'] = d['serve_dest_zone_net'] if isinstance(d['serve_dest_zone_net'],(float,int)) else 0    
    d['serve_dist'] = d['serve_dist'] if isinstance(d['serve_dist'],(float,int)) else 0
    d['serve_dur'] = d['serve_dur'] if isinstance(d['serve_dur'],(float,int)) else 0
    d['serve_speed'] = d['serve_speed'] if isinstance(d['serve_speed'],(float,int)) else 0    
    d['serve_angle'] = d['serve_angle'] if isinstance(d['serve_angle'],(float,int)) else 0
    d['serve_action_id'] = d['serve_action_id'] if isinstance(d['serve_action_id'],(float,int)) else 0
    d['serve_height'] = d['serve_height'] if isinstance(d['serve_height'],(float,int)) else 0

    d['pass_player'] = d['pass_player'] if isinstance(d['pass_player'],str) else ""
    d['pass_src_x'] = d['pass_src_x'] if isinstance(d['pass_src_x'],(float,int)) else 0
    d['pass_src_y'] = d['pass_src_y'] if isinstance(d['pass_src_y'],(float,int)) else 0
    d['pass_src_t'] = d['pass_src_t'] if isinstance(d['pass_src_t'],(float,int)) else 0
    d['pass_src_zone_depth'] = d['pass_src_zone_depth'] if isinstance(d['pass_src_zone_depth'],str) else ""
    d['pass_src_zone_net'] = d['pass_src_zone_net'] if isinstance(d['pass_src_zone_net'],(float,int)) else 0
    d['pass_dest_x'] = d['pass_dest_x'] if isinstance(d['pass_dest_x'],(float,int)) else 0
    d['pass_dest_x'] = d['pass_dest_x'] if isinstance(d['pass_dest_x'],(float,int)) else 0
    d['pass_dest_y'] = d['pass_dest_y'] if isinstance(d['pass_dest_y'],(float,int)) else 0
    d['pass_dest_zone_depth'] = d['pass_dest_zone_depth'] if isinstance(d['pass_dest_zone_depth'],str) else ""
    d['pass_dest_zone_net'] = d['pass_dest_zone_net'] if isinstance(d['pass_dest_zone_net'],(float,int)) else 0    
    d['pass_dist'] = d['pass_dist'] if isinstance(d['pass_dist'],(float,int)) else 0
    d['pass_dur'] = d['pass_dur'] if isinstance(d['pass_dur'],(float,int)) else 0
    d['pass_speed'] = d['pass_speed'] if isinstance(d['pass_speed'],(float,int)) else 0    
    d['pass_angle'] = d['pass_angle'] if isinstance(d['pass_angle'],(float,int)) else 0
    d['pass_action_id'] = d['pass_action_id'] if isinstance(d['pass_action_id'],(float,int)) else 0
    d['pass_height'] = d['pass_height'] if isinstance(d['pass_height'],(float,int)) else 0    

    d['set_player'] = d['set_player'] if isinstance(d['set_player'],str) else ""
    d['set_src_x'] = d['set_src_x'] if isinstance(d['set_src_x'],(float,int)) else 0
    d['set_src_y'] = d['set_src_y'] if isinstance(d['set_src_y'],(float,int)) else 0
    d['set_src_t'] = d['set_src_t'] if isinstance(d['set_src_t'],(float,int)) else 0
    d['set_src_zone_depth'] = d['set_src_zone_depth'] if isinstance(d['set_src_zone_depth'],str) else ""
    d['set_src_zone_net'] = d['set_src_zone_net'] if isinstance(d['set_src_zone_net'],(float,int)) else 0
    d['set_dest_x'] = d['set_dest_x'] if isinstance(d['set_dest_x'],(float,int)) else 0
    d['set_dest_x'] = d['set_dest_x'] if isinstance(d['set_dest_x'],(float,int)) else 0
    d['set_dest_y'] = d['set_dest_y'] if isinstance(d['set_dest_y'],(float,int)) else 0
    d['set_dest_zone_depth'] = d['set_dest_zone_depth'] if isinstance(d['set_dest_zone_depth'],str) else ""
    d['set_dest_zone_net'] = d['set_dest_zone_net'] if isinstance(d['set_dest_zone_net'],(float,int)) else 0    
    d['set_dist'] = d['set_dist'] if isinstance(d['set_dist'],(float,int)) else 0
    d['set_dur'] = d['set_dur'] if isinstance(d['set_dur'],(float,int)) else 0
    d['set_speed'] = d['set_speed'] if isinstance(d['set_speed'],(float,int)) else 0    
    d['set_angle'] = d['set_angle'] if isinstance(d['set_angle'],(float,int)) else 0
    d['set_action_id'] = d['set_action_id'] if isinstance(d['set_action_id'],(float,int)) else 0
    d['set_height'] = d['set_height'] if isinstance(d['set_height'],(float,int)) else 0  

    d['att_player'] = d['att_player'] if isinstance(d['att_player'],str) else ""
    d['att_src_x'] = d['att_src_x'] if isinstance(d['att_src_x'],(float,int)) else 0
    d['att_src_y'] = d['att_src_y'] if isinstance(d['att_src_y'],(float,int)) else 0
    d['att_src_t'] = d['att_src_t'] if isinstance(d['att_src_t'],(float,int)) else 0
    d['att_src_zone_depth'] = d['att_src_zone_depth'] if isinstance(d['att_src_zone_depth'],str) else ""
    d['att_src_zone_net'] = d['att_src_zone_net'] if isinstance(d['att_src_zone_net'],(float,int)) else 0
    d['att_dest_x'] = d['att_dest_x'] if isinstance(d['att_dest_x'],(float,int)) else 0
    d['att_dest_x'] = d['att_dest_x'] if isinstance(d['att_dest_x'],(float,int)) else 0
    d['att_dest_y'] = d['att_dest_y'] if isinstance(d['att_dest_y'],(float,int)) else 0
    d['att_dest_zone_depth'] = d['att_dest_zone_depth'] if isinstance(d['att_dest_zone_depth'],str) else ""
    d['att_dest_zone_net'] = d['att_dest_zone_net'] if isinstance(d['att_dest_zone_net'],(float,int)) else 0    
    d['att_dist'] = d['att_dist'] if isinstance(d['att_dist'],(float,int)) else 0
    d['att_dur'] = d['att_dur'] if isinstance(d['att_dur'],(float,int)) else 0
    d['att_speed'] = d['att_speed'] if isinstance(d['att_speed'],(float,int)) else 0    
    d['att_angle'] = d['att_angle'] if isinstance(d['att_angle'],(float,int)) else 0
    d['att_action_id'] = d['att_action_id'] if isinstance(d['att_action_id'],(float,int)) else 0
    d['att_height'] = d['att_height'] if isinstance(d['att_height'],(float,int)) else 0  

    d['dig_player'] = d['dig_player'] if isinstance(d['dig_player'],str) else ""
    d['dig_player'] = d['dig_player'] if not d['dig_player'] else " "
    d['dig_src_x'] = d['dig_src_x'] if isinstance(d['dig_src_x'],(float,int)) else 0
    d['dig_src_y'] = d['dig_src_y'] if isinstance(d['dig_src_y'],(float,int)) else 0
    d['dig_src_t'] = d['dig_src_t'] if isinstance(d['dig_src_t'],(float,int)) else 0
    d['dig_src_zone_depth'] = d['dig_src_zone_depth'] if isinstance(d['dig_src_zone_depth'],str) else ""
    d['dig_src_zone_net'] = d['dig_src_zone_net'] if isinstance(d['dig_src_zone_net'],(float,int)) else 0
    d['dig_dest_x'] = d['dig_dest_x'] if isinstance(d['dig_dest_x'],(float,int)) else 0
    d['dig_dest_x'] = d['dig_dest_x'] if isinstance(d['dig_dest_x'],(float,int)) else 0
    d['dig_dest_y'] = d['dig_dest_y'] if isinstance(d['dig_dest_y'],(float,int)) else 0
    d['dig_dest_zone_depth'] = d['dig_dest_zone_depth'] if isinstance(d['dig_dest_zone_depth'],str) else ""
    d['dig_dest_zone_net'] = d['dig_dest_zone_net'] if isinstance(d['dig_dest_zone_net'],(float,int)) else 0    
    d['dig_dist'] = d['dig_dist'] if isinstance(d['dig_dist'],(float,int)) else 0
    d['dig_dur'] = d['dig_dur'] if isinstance(d['dig_dur'],(float,int)) else 0
    d['dig_speed'] = d['dig_speed'] if isinstance(d['dig_speed'],(float,int)) else 0    
    d['dig_angle'] = d['dig_angle'] if isinstance(d['dig_angle'],(float,int)) else 0
    d['dig_action_id'] = d['dig_action_id'] if isinstance(d['dig_action_id'],(float,int)) else 0
    d['dig_height'] = d['dig_height'] if isinstance(d['dig_height'],(float,int)) else 0  

    d['point_outcome'] = d['point_outcome'] if isinstance(d['point_outcome'],str) else ""
    d['point_outcome_team'] = d['point_outcome_team'] if isinstance(d['point_outcome_team'],str) else ""
    d['tactic'] = d['tactic'] if isinstance(d['tactic'],str) else ""
    d['last_action_id'] = d['last_action_id'] if isinstance(d['last_action_id'],(float,int)) else 0

  # fill all the NaN in the df

  # last thing, place this mppr_df (master ppr dataframe) into the database as a csv file
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  #print(f"saving ppr df, size:{master_ppr_df.size}")
  master_ppr_csv_file = pd.DataFrame.to_csv(master_ppr_df)
  ppr_media = anvil.BlobMedia(content_type="text/plain", content=master_ppr_csv_file.encode(), name="ppr_csv.csv")
    
  # now I can store it in the btd files database
  # find the correct row
  #print(f"looking in ppr csv tables: League:{user_league}, Gender:{user_gender}, Year:{user_year}, Team:{user_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = user_league,
      gender = user_gender,
      year = user_year,
      team = user_team
    ) )

  if ppr_csv_row:
    print(f"Updating a row to the csv table {user_league}, {user_gender}, {user_year}, {user_team}")
    ppr_csv_row.update(
      league = user_league,
      gender = user_gender,
      year = user_year,
      team = user_team,
      ppr_csv = ppr_media,
      date = datetime.datetime.now()
    )
  else:
    #print("adding a row to the csv table")
    app_tables.ppr_csv_tables.add_row(
      league = user_league,
      gender = user_gender,
      year = user_year,
      team = user_team,
      ppr_csv = ppr_media,
      date = datetime.datetime.now(),
      min_att= 5  # curently set at this number, can be changed in the table
    )

  pass
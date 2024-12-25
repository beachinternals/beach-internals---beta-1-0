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

@anvil.server.callable
def make_all_ppr_files():
  # make a quick loop to make all the ppr files

  leagues = [
    {
      r['league'],
      r['gender'],
      r['year'],
    }
    for r in app_tables.btd_files.search()]

  for i in leagues:
    task = anvil.server.call('make_ppr_files', leagues.at[i,'league'], r['gender'],r['year'],)
  

  return task
  

# rather than in the user's browser.
@anvil.server.callable
def make_ppr_files( u_league, u_gender, u_year, u_team, rebuild):
  # call the background task
  task = anvil.server.launch_background_task('generate_ppr_files',u_league, u_gender, u_year, u_team, rebuild)
  
  return task

# ############# server function to loop thru btd_file database and create and store the corresponding ppr file
@anvil.server.background_task
def generate_ppr_files( user_league, user_gender, user_year, user_team, rebuild ):
  task = generate_ppr_files_not_background(user_league, user_gender, user_year, user_team, rebuild )
  return task

@anvil.server.callable
def generate_ppr_files_not_background(user_league, user_gender, user_year, user_team, rebuild  ): 
  # select rows from the btd_files database and limit it to league, gender, and year
  btd_row = app_tables.btd_files.search(
    league = user_league,
    gender = user_gender,
    year = user_year,
    team = user_team,
  )

  for flist_r in btd_row:
    #print(f"In loop over rows, number of points in btd row: {flist_r['points']}")
    calc_ppr = False
    
    if rebuild:
      # call the function to return the ppr file given the btd file
      ppr_df = btd_to_ppr_file( io.BytesIO( flist_r['csv_data'].get_bytes()), flist_r ) 
      calc_ppr = True
    else:
      if not flist_r['ppr_file_date']: # no date for the ppr file
        ppr_df = btd_to_ppr_file( io.BytesIO( flist_r['csv_data'].get_bytes()), flist_r )         
        calc_ppr = True
      elif flist_r['btd_file_date'] > flist_r['ppr_file_date']: # btd file is newer then the ppr file
        ppr_df = btd_to_ppr_file( io.BytesIO( flist_r['csv_data'].get_bytes()), flist_r ) 
        calc_ppr = True

    if calc_ppr:
      # clean up the ppr_df databale, just in case.
      ppr_df = ppr_df.replace({float('nan'): None})
    
      # We now have a complete ppr datafile.  THree more steps:
      # 1) Transpose some points (so we always serve from close, first ball attack from the far court) Transpose first, makes zone's much easier
      ppr_df = transpose_ppr_coord(ppr_df)
    
      # 2) Cacluate the data (speed, distance, etc...)
      ppr_df = calc_ppr_data(ppr_df)

      # 3) Calculate offensive tactic
      ppr_df = calc_tactic(ppr_df)
    
      # 4) Error check the ppr file for consistency, maybe raise errors into an email/text message??
      ppr_df, no_errors, error_string = error_check_ppr(ppr_df)
      #print(f"Error String: {error_string}")
    
      # 5) Lastly, save the ppr csv file back into the btd_files database
      # first, I need to cahnge the ppr_file dataframe to a csv file.
      ppr_csv_file = pd.DataFrame.to_csv(ppr_df)
      ppr_media = anvil.BlobMedia(content_type="text/plain", content=ppr_csv_file.encode(), name="ppr.csv")
    
      # now I can store it in the btd files database
      flist_r.update( ppr_data = ppr_media, error_str = error_string, no_errors = no_errors, ppr_file_date=datetime.datetime.now() )
    else:
      True
      #print(f"Not processing file:{flist_r['filename']}")
      
  return True

# ############ server function to convert a btd file to a ppr file
def btd_to_ppr_file(btd_file_bytes, flist_r):
  #print(f"btd to ppr process of :{flist_r['filename']}")
  # convert the btd file to a dataframe
  btd_df = pd.read_csv(btd_file_bytes)

  # Clean up the data, all numbers to None, all players to ""
  btd_df = btd_df.replace({float('nan'): None})
  #values = {"player": " ", "src_x": None, 'src_y': None, 'dest_x': None, 'dest_y': None}
  btd_df.fillna( value={'player': ''} )
  btd_df.fillna( value={'src_x': 0} )
  

  #print(f"read the btd csv file into a dataframe: {btd_df['dest_x']}")

  # to be backwards compatible, check if there is a video_id column, if not, add one, ok to leave it blank
  if 'video_id' not in btd_df.columns:
    # then lets add it
    #print(f"BTD DF Shape:{btd_df.shape}")
    new_column = ["No Video Id"] * btd_df.shape[0]
    #print(f"column size:{len(new_column)}")
    btd_df.insert(1,'video_id',new_column)
    #print(f"dataframe shape:{btd_df.shape}")
    
  # call function to make the convesion
  ppr_df = btd_to_ppr_df(btd_df, flist_r)

  #print(f"ppr dataframe returned: {ppr_df}")
  return ppr_df

# ############ server function to convert a btd dataframe to a ppr dataframe
def btd_to_ppr_df(btd_df, flist_r):

  # define the two teams and the four players in this file:
  # we need to sor the players alphs
  player_a1 = flist_r['ppr_playera1']
  player_a2 = flist_r['ppr_playera2']
  if player_a1 > player_a2:
    tmp = player_a1 
    player_a1 = player_a2
    player_a2 = tmp
  teama = player_a1 + " " + player_a2
  player_b1 = flist_r['ppr_playerb1']
  player_b2 = flist_r['ppr_playerb2']
  if player_b1 > player_b2:
    tmp = player_b1 
    player_b1 = player_b2
    player_b2 = tmp
  teamb =  player_b1 + " " + player_b2 
  zero = 0
  yn = "N"
  blank = 'empty'
  
    # create the ppr datafram
  ppr_dict = {
    "league":flist_r["league"],"gender":flist_r['gender'],"year":flist_r['year'],"comp_l1":flist_r['comp_l1'],"comp_l2":flist_r['comp_l2'],"comp_l3":flist_r['comp_l3'],'team':flist_r['team'],'game_date':flist_r['date'],
    "filename":flist_r['csv_data'].name,'video_id':blank,'rally_id':zero,
    'point_no':zero,'set':zero,'a_set_diff':zero,'a_score_diff':zero,
    'a_score':zero,'teama':teama,'player_a1':player_a1,'player_a2':player_a2,
    'b_score':zero,'teamb':teamb,'player_b1':player_b1,'player_b2':player_b2,
    'serve_player':[''],'serve_src_x':zero,'serve_src_y':zero,'serve_src_t':zero,'serve_src_zone_depth':blank,'serve_src_zone_net':zero,
                  'serve_dest_x':zero,'serve_dest_y':zero,'serve_dest_t':zero,'serve_dest_zone_depth':blank,'serve_dest_zone_net':zero,
                  'serve_dist':zero,'serve_dur':zero,'serve_speed':zero,'serve_angle':zero,'serve_action_id':zero,'serve_height':zero,
    'pass_player':blank,'pass_yn':yn,'pass_src_x':zero,'pass_src_y':zero,'pass_src_t':zero,'pass_src_zone_depth':blank,'pass_src_zone_net':zero,
                  'pass_dest_x':zero,'pass_dest_y':zero,'pass_dest_t':zero,'pass_dest_zone_depth':blank,'pass_dest_zone_net':zero,
                  'pass_dist':zero,'pass_dur':zero,'pass_speed':zero,'pass_angle':zero,'pass_action_id':zero,'pass_height':zero,
                  'pass_rtg_btd':zero,'pass_oos':zero,
    'set_player':blank,'set_yn':yn,'set_src_x':zero,'set_src_y':zero,'set_src_t':zero,'set_src_zone_depth':blank,'set_src_zone_net':zero,
                  'set_dest_x':zero,'set_dest_y':zero,'set_dest_t':zero,'set_dest_zone_depth':blank,'set_dest_zone_net':zero,
                  'set_dist':zero,'set_dur':zero,'set_speed':zero,'set_angle':zero,'set_action_id':zero,'set_height':zero,
    'att_player':blank,'att_yn':yn,'att_src_x':zero,'att_src_y':zero,'att_src_t':zero,'att_src_zone_depth':blank,'att_src_zone_net':zero,
                  'att_dest_x':zero,'att_dest_y':zero,'att_dest_t':zero,'att_dest_zone_depth':blank,'att_dest_zone_net':zero,
                  'att_dist':zero,'att_dur':zero,'att_speed':zero,'att_angle':zero,'att_action_id':zero,'att_height':zero, 'att_touch_height':zero,
    'dig_player':blank,'dig_yn':yn,'dig_src_x':zero,'dig_src_y':zero,'dig_src_t':zero,'dig_src_zone_depth':blank,'dig_src_zone_net':zero,
                  'dig_dest_x':zero,'dig_dest_y':zero,'dig_dest_t':zero,'dig_dest_zone_depth':blank,'dig_dest_zone_net':zero,
                  'dig_dist':zero,'dig_dur':zero,'dig_speed':zero,'dig_angle':zero,'dig_action_id':zero,'dig_height':zero,
    'point_outcome':blank,'point_outcome_team':blank,'tactic':blank,'last_action_id':zero
  }

  #print(f" ppr dictionary: {ppr_dict}")
  ppr_df = pd.DataFrame.from_dict(ppr_dict)
  #print(f" ppr_df dataframe: {ppr_df}")

  # build the ppr_dataframe out tpo the proper number of rows, equal total points,
  # His should make a blank (except for flist_r values) ppr dataframe with the correct number of rows (maybe one extra due to a 0 start)
  for i in  range(1,flist_r['points']):
    ppr_df.loc[max(ppr_df.index)+1] = ppr_dict

  #print(f" size of the ppr_df: {ppr_df.shape}")
  
  # now, let's loop over the btd dataframe, row by row, creating the ppr file as we go.
  ppr_row = -1  # set this to -1 as we increment on the first point, so the first index is 0
  in_trans = False    # a flag to tell when we have left first ball and are in transition
  touch_since_serve = 0
  serve_team = ""     # initialize these
  last_player = ""
  last_quality = ""
  last_action_type = ""
  last_action_id = 0
  
  # ################# loop over the rows in the balltime data file
  for index, btd_r in btd_df.iterrows():
    #print(f"loop over rows, index = {index}, Transition? {in_trans}, Action Type?{btd_r['action_type']}, Player: {btd_r['player']}")
    
    # replace the btd players with the master player reference
    if btd_r['player'] == flist_r['player1']:
      btd_r['player'] = player_a1
    elif btd_r['player'] == flist_r['player2']:
      btd_r['player'] = player_a2
    elif btd_r['player'] == flist_r['player3']:
      btd_r['player'] = player_b1
    elif btd_r['player'] == flist_r['player4']:
      btd_r['player'] = player_b2
    else:
      # ######## print this to a file to display as this is an error in the data #####################
      #print(f"Could not find the player!! {btd_r['player']}, Row {index} in these four: {flist_r['player1']}, {flist_r['player2']}, {flist_r['player3']}, {flist_r['player4']}") 
      btd_r['player'] = " "
    
    # if this is a serve, then start a new point
    if btd_r['action_type'] == "serve":
      # first thing, check if we have an outcome posted for the last point
      # when we call this, ppr_row is looking at the previous, just completed point, but btd_r is now looking at the next poiint, the serve
      if ppr_row > -1:  # first time is at -1, second at 0, don't need these two
        ppr_df = check_last_point( ppr_df, ppr_row, btd_r, last_player, last_quality, last_action_id, last_action_type, in_trans, teama, teamb, False )
        ppr_df = update_score(ppr_df, ppr_row, teama)

      # initialize variables for the beginning of abnew point
      ppr_row += 1
      touch_since_serve = 1
      in_trans = False
      serve_team = teama if btd_r['player'] in teama  else teamb
      
      #print(f" ################ New Serve ################### ------- Raly Id: {btd_r['rally_id']}, ppr_row: {ppr_row}, Quality: {btd_r['quality']}")
      
      ppr_df = save_serve_info( ppr_df, btd_r, ppr_row )
        
    if btd_r['action_type'] == "receive" and not in_trans:
      # process the serve receive data
      touch_since_serve +=  1
      ppr_df = save_pass_info(ppr_df, btd_r, ppr_row)    
      
    if btd_r['action_type'] == "set" and not in_trans:
      touch_since_serve += 1
      ppr_df = save_set_info(ppr_df, btd_r, ppr_row)

    if btd_r['action_type'] == "attack" and not in_trans:
      touch_since_serve += 1
      ppr_df = save_att_info(ppr_df, btd_r, ppr_row)

    if btd_r['action_type'] == "dig" and not in_trans:
      touch_since_serve += 1
      ppr_df = save_dig_info(ppr_df, btd_r, ppr_row)

    #print(f"TOuches since serve:{touch_since_serve}, Player:{btd_r['player']}, action type:{btd_r['action_type']}")
    if touch_since_serve == 5 or ( btd_r['player'] in serve_team and btd_r['action_type'] != "serve" ):
      in_trans = True

    # save the previous row
    last_player = btd_r['player']
    last_quality = btd_r['quality']
    last_action_id = int(btd_r['action_id'])
    last_action_type = btd_r['action_type']

  # we should be out of the loop over btd rows, so check the last point outcome and score
  ppr_df = check_last_point( ppr_df, ppr_row, btd_r, last_player, last_quality, last_action_id, last_action_type, in_trans, teama, teamb, True  )
  ppr_df = update_score(ppr_df, ppr_row, teama)
  return ppr_df

def save_serve_info( ppr_df, btd_r, ppr_row ):
  ppr_df.at[ppr_row,'video_id'] = btd_r['video_id']
  ppr_df.at[ppr_row,'serve_action_id'] = int(btd_r['action_id'])
  ppr_df.at[ppr_row,'serve_src_t'] = btd_r['action_time']
  ppr_df.at[ppr_row,'serve_dest_x'] = btd_r['dest_x']
  ppr_df.at[ppr_row,'serve_dest_y'] = btd_r['dest_y']
  ppr_df.at[ppr_row,'serve_player'] = btd_r['player']
  ppr_df.at[ppr_row,'rally_id'] = btd_r['rally_id']
  ppr_df.at[ppr_row,'serve_src_x'] = btd_r['src_x']
  ppr_df.at[ppr_row,'serve_src_y'] = btd_r['src_y']
  #print(f"Saving Serve INfo ppr_row {ppr_row}, rally number {btd_r['rally_id']}, Server:{ppr_df.at[ppr_row,'serve_player']}")  
  return ppr_df

def save_pass_info( ppr_df, btd_r, ppr_row):
  ppr_df.at[ppr_row,'pass_action_id'] = int(btd_r['action_id'])
  ppr_df.at[ppr_row,'pass_src_t'] = btd_r['action_time']
  ppr_df.at[ppr_row,'pass_dest_x'] = btd_r['dest_x']
  ppr_df.at[ppr_row,'pass_dest_y'] = btd_r['dest_y']
  ppr_df.at[ppr_row,'pass_player'] = btd_r['player']
  ppr_df.at[ppr_row,'pass_src_x'] = btd_r['src_x']
  ppr_df.at[ppr_row,'pass_src_y'] = btd_r['src_y']
  ppr_df.at[ppr_row,'pass_yn'] = "Y"  
  ppr_df.at[ppr_row,'serve_dest_t'] = ppr_df.at[ppr_row,'pass_src_t']
  ppr_df.at[ppr_row,'pass_rtg_btd'] = btd_r['quality']
  #print(f"Saving pass info Action Id: {ppr_df.at[ppr_row,'pass_action_id']}, ppr_row: {ppr_row}, Pass Player: {ppr_df.at[ppr_row,'pass_player']}")
  return ppr_df

def save_set_info( ppr_df, btd_r, ppr_row):
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_action_id'))] = int(btd_r['action_id'])
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_src_t'))] = btd_r['action_time']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_dest_x'))] = btd_r['dest_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_dest_y'))] = btd_r['dest_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_player'))] = btd_r['player']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_src_x'))] = btd_r['src_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_src_y'))] = btd_r['src_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_yn'))] = "Y"  
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('pass_dest_t'))] = ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_src_t'))]
  #print(f"saving SET info Action Id: {btd_r['action_id']}, ppr_row: {ppr_row}")

  return ppr_df

def save_att_info( ppr_df, btd_r, ppr_row):
  #print(f"saving ATT info Action Id: {btd_r['action_id']}, ppr_row: {ppr_row}")
  
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_action_id'))] = int(btd_r['action_id'])
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_src_t'))] = btd_r['action_time']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_dest_x'))] = btd_r['dest_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_dest_y'))] = btd_r['dest_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_player'))] = btd_r['player']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_src_x'))] = btd_r['src_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_src_y'))] = btd_r['src_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_yn'))] = "Y"  
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('set_dest_t'))] = ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_src_t'))]
  if 'vertical_touch_height' in btd_r:
    ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_touch_height'))] = btd_r['vertical_touch_height']
  if 'speed_mph' in btd_r:
    if isinstance(btd_r['speed_mph'],(float,int)):
      #print(f"Saving Att Speed:{btd_r['speed_mph']}")
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_speed'))] = float(btd_r['speed_mph'])*0.44704  # convert MPH to M/S  

  return ppr_df

def save_dig_info( ppr_df, btd_r, ppr_row):
  
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_action_id'))] = int(btd_r['action_id'])
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_src_t'))] = btd_r['action_time']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_dest_x'))] = btd_r['dest_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_dest_y'))] = btd_r['dest_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_player'))] = btd_r['player']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_src_x'))] = btd_r['src_x']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_src_y'))] = btd_r['src_y']
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_yn'))] = "Y"  
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('att_dest_t'))] = ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('dig_src_t'))]
  #print(f"saving Dig info Action Id: {btd_r['action_id']}, ppr_row: {ppr_row}")
  return ppr_df

def save_point(ppr_df, ppr_row, point_code, out_team, action ):
  ppr_df.at[ppr_row,'point_outcome'] = point_code
  ppr_df.at[ppr_row,'point_outcome_team'] = out_team
  ppr_df.at[ppr_row,'last_action_id'] = action
  #print(f"SAVED Point Outcome: Outcome: {ppr_df.at[ppr_row,'point_outcome']}, Point Team:{ppr_df.at[ppr_row,'point_outcome_team']}, Point #: {ppr_row}:  Action ID:{ppr_df.at[ppr_row,'last_action_id']}")
  return ppr_df
  
def check_last_point( ppr_df, ppr_row, btd_r, last_player, last_quality, last_action_id, last_action_type, in_trans, teama, teamb, final_pt ):
  # this is called when we get a new serve, before we imove on to that point, and also on the final point
  # this is setting hte point outcome for the previous point (last_point)
  # logic is that whoever is serving won the point, so then award it into one of the six categories (TSA, TSE, FBK, FBE, TE, TK )
  
  last_team = teama if last_player in teama else teamb  # last team to touch the ball
  serve_team = teama if btd_r['player'] in teama else teamb   # team that serves the next ball
  if final_pt:
    serve_team = teama if ppr_df.at[ppr_row,'a_score_diff'] > 0 else teamb  # if final point ,like the next server is the one in the lead
    
  #print(f"### CHECKING LAST POINT ###, rally id:{btd_r['rally_id']}, Serve Team next point):{serve_team}, last team: {last_team}, Last Player: {last_player}  Transition? {in_trans}, Last Action Id:{last_action_id},Last Action Type:{last_action_type}, Last Quality:{last_quality}")
  
  if in_trans:
    # do I have a terminal quality?
    if last_team == serve_team:
      ppr_df = save_point( ppr_df, ppr_row, "TK", last_team, last_action_id )
    else:
      ppr_df = save_point( ppr_df, ppr_row, "TE", last_team, last_action_id )
      
  else:
    if last_quality in ["ace","error","kill"]:
      # ace or error?
      if last_action_type == "serve":
        if last_quality == "ace" or last_team == serve_team:
          ppr_df = save_point( ppr_df, ppr_row, "TSA", last_team, last_action_id )
        elif last_quality == "error" or last_team == serve_team:
          ppr_df = save_point( ppr_df, ppr_row, "TSE", last_team, last_action_id )
      else:
        # first ball kill
        if last_quality == "kill" or last_team == serve_team:
          ppr_df = save_point( ppr_df, ppr_row, "FBK", last_team, last_action_id )
        elif last_quality == "error" or last_team == serve_team:
          ppr_df = save_point( ppr_df, ppr_row, "FBE", last_team, last_action_id )
    elif last_team == serve_team:  # kill or ace
      if last_action_type == "serve":
        ppr_df = save_point( ppr_df, ppr_row, "TSA", last_team, last_action_id )
      else:
        ppr_df = save_point( ppr_df, ppr_row, "FBK", last_team, last_action_id )
    else:  # error, serve or first ball
      if last_action_type == "serve":
        ppr_df = save_point( ppr_df, ppr_row, "TSE", last_team, last_action_id )
      else:
        ppr_df = save_point( ppr_df, ppr_row, "FBE", last_team, last_action_id )
      
  return ppr_df

def update_score(ppr_df, ppr_row, teama):
  # pull out the outcome and team a score, team b score, and a set difference
  # first, record the old score
  if ppr_row == 0:
    old_a_score = 0
    old_b_score = 0
    old_set_diff = 0
    old_set = 1
  else:
    old_a_score = ppr_df.iloc[(ppr_row-1,ppr_df.columns.get_loc("a_score"))]
    old_b_score = ppr_df.iloc[(ppr_row-1,ppr_df.columns.get_loc("b_score"))]
    old_set_diff = ppr_df.iloc[(ppr_row-1,ppr_df.columns.get_loc("a_set_diff"))]
    old_set = ppr_df.iloc[(ppr_row-1,ppr_df.columns.get_loc("set"))]

  # if the old point was th elast of the set, need to reset the points to 0
  if ( old_a_score > 20 and old_a_score > old_b_score + 1):
    old_a_score = 0
    old_b_score = 0
    old_set = old_set + 1
    old_set_diff = old_set_diff +1
  elif ( old_b_score > 20 and old_b_score > old_a_score + 1):
    old_a_score = 0
    old_b_score = 0
    old_set = old_set + 1
    old_set_diff = old_set_diff -1
    
  kills = ["TSA","FBK","TK"]
  errors = ["TSE","FBE","TE"]
  
  #print("Update Score")
  #print(f"a score:{old_a_score}, b score: {old_b_score} Outcome: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))]} Team: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome_team'))]}, Team A: {teama}")
  
  # update the sets and scores
  ppr_df.at[ppr_row,'set'] = old_set
  ppr_df.at[ppr_row,'a_set_diff'] = old_set_diff
  if teama == ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("point_outcome_team"))]:
    if ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))] in kills:
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score"))] = old_a_score +1
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("b_score"))] = old_b_score
    elif ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))] in errors:
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("b_score"))] = old_b_score +1
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score"))] = old_a_score
  else:
    if ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))] in kills:
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("b_score"))] = old_b_score +1
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score"))] = old_a_score
    elif ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))] in errors:
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score"))] = old_a_score +1
      ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("b_score"))] = old_b_score

  # print(f"a score:{ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('a_score'))]}, b score: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('b_score'))]} Outcome: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))]} Team: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome_team'))]}, Team A: {teama}")
  # set the score difference
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score_diff"))] = ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("a_score"))] - ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("b_score"))]

  # set the other info, point number, sets
  ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc("point_no"))] = ppr_row


  
 # print(f"a score:{ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('a_score'))]}, b score: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('b_score'))]} Outcome: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome'))]} Team: {ppr_df.iloc[(ppr_row,ppr_df.columns.get_loc('point_outcome_team'))]}, Team A: {teama}")

  return ppr_df
  
def calc_ppr_data(ppr_df):
  #print("Calculate ppr data routing")
  # set a loop over rows in the ppr_df  
  for index, ppr_r in ppr_df.iterrows():
    # calculate the data for the serve
    ppr_df.at[index,'serve_dist'] = calc_dist(ppr_r['serve_src_x'],ppr_r['serve_src_y'],ppr_r['serve_dest_x'],ppr_r['serve_dest_y'])
    ppr_df.at[index,'serve_src_zone_depth'] = zone_depth(ppr_r['serve_src_y'])
    ppr_df.at[index,'serve_src_zone_net'] = srv_zone_net(ppr_r['serve_src_x'])
    ppr_df.at[index,'serve_dest_zone_depth'] = zone_depth(ppr_r['serve_dest_y'])
    ppr_df.at[index,'serve_dest_zone_net'] = zone_net(ppr_r['serve_src_x'])
    ppr_df.at[index,"serve_angle"] = calc_angle(ppr_r['serve_src_x'],ppr_r['serve_dest_x'],ppr_r['serve_src_y'],ppr_r['serve_dest_y'])
   # print(f"serve dest time:{ppr_r['serve_dest_t']}, rally id: {ppr_r['rally_id']}")
    if isinstance(ppr_r['serve_dest_t'],(float,int)):
      ppr_df.at[index,'serve_dur'] = calc_dur(ppr_r['serve_src_t'],ppr_r['serve_dest_t'])
      ppr_df.at[index,'serve_speed'] = calc_speed(ppr_df.at[index,'serve_dist'],ppr_df.at[index,'serve_dur'])
      ppr_df.at[index,'serve_height'] = calc_height(ppr_r['serve_src_t'],ppr_r['serve_dest_t'])
    
    if ppr_r['pass_yn'] == "Y":
      ppr_df.at[index,'pass_dist'] = calc_dist(ppr_r['pass_src_x'],ppr_r['pass_src_y'],ppr_r['pass_dest_x'],ppr_r['pass_dest_y'])
      ppr_df.at[index,'pass_src_zone_depth'] = zone_depth(ppr_r['pass_src_y'])
      ppr_df.at[index,'pass_src_zone_net'] = zone_net(ppr_r['pass_src_x'])
      ppr_df.at[index,'pass_dest_zone_depth'] = zone_depth(ppr_r['pass_dest_y'])
      ppr_df.at[index,'pass_dest_zone_net'] = zone_net(ppr_r['pass_dest_x'])
      ppr_df.at[index,'pass_angle'] = calc_angle(ppr_r['pass_src_x'],ppr_r['pass_dest_x'],ppr_r['pass_src_y'],ppr_r['pass_dest_y'])
      if isinstance(ppr_r['pass_dest_t'],(float,int)):
        ppr_df.at[index,'pass_dur'] = calc_dur(ppr_r['pass_src_t'],ppr_r['pass_dest_t'])
        ppr_df.at[index,'pass_speed'] = calc_speed(ppr_df.at[index,'pass_dist'],ppr_df.at[index,'pass_dur'])
        ppr_df.at[index,'pass_height'] = calc_height(ppr_r['pass_src_t'],ppr_r['pass_dest_t']) 
        ppr_df.at[index,'pass_oos'] = calc_out_of_system(ppr_df.at[index,'pass_dest_zone_net'],
                                                        ppr_df.at[index,'pass_dest_zone_depth'],
                                                        ppr_df.at[index,'pass_height'],
                                                        ppr_df.at[index,'pass_src_zone_net'],
                                                        ppr_df.at[index,'pass_angle']
                                                        )

    if ppr_r['set_yn'] == "Y":
      ppr_df.at[index,'set_dist'] = calc_dist(ppr_r['set_src_x'],ppr_r['set_src_y'],ppr_r['set_dest_x'],ppr_r['set_dest_y'])
      ppr_df.at[index,'set_src_zone_depth'] = zone_depth(ppr_r['set_src_y'])
      ppr_df.at[index,'set_src_zone_net'] = zone_net(ppr_r['set_src_x'])
      ppr_df.at[index,'set_dest_zone_depth'] = zone_depth(ppr_r['set_dest_y'])
      ppr_df.at[index,'set_dest_zone_net'] = zone_net(ppr_r['set_src_x'])
      ppr_df.at[index,'set_angle'] = calc_angle(ppr_r['set_src_x'],ppr_r['set_dest_x'],ppr_r['set_src_y'],ppr_r['set_dest_y'])
      if isinstance(ppr_r['set_dest_t'],(float,int)):
        ppr_df.at[index,'set_dur'] = calc_dur(ppr_r['set_src_t'],ppr_r['set_dest_t'])
        ppr_df.at[index,'set_speed'] = calc_speed(ppr_df.at[index,'set_dist'],ppr_df.at[index,'set_dur'])
        ppr_df.at[index,'set_height'] = calc_height(ppr_r['set_src_t'],ppr_r['set_dest_t'])

    if ppr_r['att_yn'] == "Y":
      ppr_df.at[index,'att_dist'] = calc_dist(ppr_r['att_src_x'],ppr_r['att_src_y'],ppr_r['att_dest_x'],ppr_r['att_dest_y'])
      ppr_df.at[index,'att_src_zone_depth'] = zone_depth(ppr_r['att_src_y'])
      ppr_df.at[index,'att_src_zone_net'] = zone_net(ppr_r['att_src_x'])
      ppr_df.at[index,'att_dest_zone_depth'] = zone_depth(ppr_r['att_dest_y'])
      ppr_df.at[index,'att_dest_zone_net'] = zone_net(ppr_r['att_src_x'])
      ppr_df.at[index,'att_angle'] = calc_angle(ppr_r['att_src_x'],ppr_r['att_dest_x'],ppr_r['att_src_y'],ppr_r['att_dest_y'])
      if isinstance(ppr_r['att_dest_t'],(float,int)):
        ppr_df.at[index,'att_dur'] = calc_dur(ppr_r['att_src_t'],ppr_r['att_dest_t'])
        if not ppr_df.at[index,'att_speed']:  # in case the speed was imported directly from the dataset
          ppr_df.at[index,'att_speed'] = calc_speed(ppr_df.at[index,'att_dist'],ppr_df.at[index,'att_dur'])
        ppr_df.at[index,'att_height'] = calc_height(ppr_r['att_src_t'],ppr_r['att_dest_t'])

    if ppr_r['dig_yn'] == "Y":
      ppr_df.at[index,'dig_dist'] = calc_dist(ppr_r['dig_src_x'],ppr_r['dig_src_y'],ppr_r['dig_dest_x'],ppr_r['dig_dest_y'])
      ppr_df.at[index,'dig_src_zone_depth'] = zone_depth(ppr_r['dig_src_y'])
      ppr_df.at[index,'dig_src_zone_net'] = zone_net(ppr_r['dig_src_x'])
      ppr_df.at[index,'dig_dest_zone_depth'] = zone_depth(ppr_r['dig_dest_y'])
      ppr_df.at[index,'dig_dest_zone_net'] = zone_net(ppr_r['dig_src_x'])
      ppr_df.at[index,'dig_angle'] = calc_angle(ppr_r['dig_src_x'],ppr_r['dig_dest_x'],ppr_r['dig_src_y'],ppr_r['dig_dest_y'])
      if isinstance(ppr_r['dig_dest_t'],(float,int)):
        ppr_df.at[index,'dig_dur'] = calc_dur(ppr_r['dig_src_t'],ppr_r['dig_dest_t'])
        ppr_df.at[index,'dig_speed'] = calc_speed(ppr_df.at[index,'dig_dist'],ppr_df.at[index,'dig_dur'])
        ppr_df.at[index,'dig_height'] = calc_height(ppr_r['dig_src_t'],ppr_r['dig_dest_t'])
    
  return ppr_df

def transpose_ppr_coord(ppr_df):
  #print("Transpose ppr coordiantes routing")

  for index, ppr_r in ppr_df.iterrows():
    # transpose from 0-1 cube ot +/-8m and 0-8m
    # serving from near court? or far?
    # print(f" serve_src: {ppr_r['serve_src_x']}, {ppr_r['serve_src_y']}, rally id : {ppr_r['rally_id']}")

    # what do we do if we don't have serve_src?
    # I added the not 0 and not 1 because balltime seems to be giving serve of 1 and 0 whent they really do not know, so then go to the pass.
    if (ppr_r['serve_src_y'] is None):
      if ppr_r['pass_src_y'] is None:
        if ppr_r['set_src_y'] is not None:
          near_court = False if ppr_r['set_src_y'] > 0.5 else True 
          #print(f"Near Court Calc: {near_court}, Rally ID: {ppr_r['rally_id']}, set_src_y: {ppr_r['set_src_y']}")
        else:
          # if give up!!
          near_court = True
          #print(f"Near Court Calc: gave up! {near_court}, Rally ID: {ppr_r['rally_id']}, serve Src Y: {ppr_r['serve_src_y']}, pass soruce y: {ppr_r['pass_src_y']}, set_src_y: {ppr_r['set_src_y']}")
      else:
        near_court = False if ppr_r['pass_src_y'] > 0.5 else True 
        #print(f"Near Court Calc: {near_court}, Rally ID: {ppr_r['rally_id']}, pass_src_y: {ppr_r['pass_src_y']}")
    else:
      if (ppr_r['serve_src_y'] == 0) or (ppr_r['serve_src_y'] == 1):
        # have a bit of doubt with teh 0 and 1 coordiante, sometimes they are wrong, so check pass src
        # check the pass src, is not none, use that, else use this
        if (ppr_r['pass_src_y'] is not None):
          near_court = False if ppr_r['pass_src_y'] > 0.5 else True 
        else:
          near_court = True if ppr_r['serve_src_y'] > 0.5 else False 
      else:
        near_court = True if ppr_r['serve_src_y'] > 0.5 else False  
        
      #print(f"Near Court Calc: {near_court}, Rally ID: {ppr_r['rally_id']}, serve_src_y: {ppr_r['serve_src_y']}")

    # Serve Coordinates
    ppr_df.at[index,'serve_src_x'] = ppr_transpose_x(near_court, ppr_r['serve_src_x'])
    ppr_df.at[index,'serve_src_y'] = ppr_transpose_y(near_court, ppr_r['serve_src_y'])
    ppr_df.at[index,'serve_dest_x'] = ppr_transpose_x(near_court, ppr_r['serve_dest_x'])
    ppr_df.at[index,'serve_dest_y'] = ppr_transpose_y(near_court, ppr_r['serve_dest_y'])

    # Pass Coordinates
    if ppr_r['pass_yn'] == "Y":
      ppr_df.at[index,'pass_src_x'] = ppr_transpose_x(near_court, ppr_r['pass_src_x'])
      ppr_df.at[index,'pass_src_y'] = ppr_transpose_y(near_court, ppr_r['pass_src_y'])
      ppr_df.at[index,'pass_dest_x'] = ppr_transpose_x(near_court, ppr_r['pass_dest_x'])
      ppr_df.at[index,'pass_dest_y'] = ppr_transpose_y(near_court, ppr_r['pass_dest_y'])
      
    # Set Coordinates
    if ppr_r['set_yn'] == "Y":
      ppr_df.at[index,'set_src_x'] = ppr_transpose_x(near_court, ppr_r['set_src_x'])
      ppr_df.at[index,'set_src_y'] = ppr_transpose_y(near_court, ppr_r['set_src_y'])
      ppr_df.at[index,'set_dest_x'] = ppr_transpose_x(near_court, ppr_r['set_dest_x'])
      ppr_df.at[index,'set_dest_y'] = ppr_transpose_y(near_court, ppr_r['set_dest_y'])

    # Attack Coordinates
    if ppr_r['att_yn'] == "Y":
      ppr_df.at[index,'att_src_x'] = ppr_transpose_x(near_court, ppr_r['att_src_x'])
      ppr_df.at[index,'att_src_y'] = ppr_transpose_y(near_court, ppr_r['att_src_y'])
      ppr_df.at[index,'att_dest_x'] = ppr_transpose_x(near_court, ppr_r['att_dest_x'])
      ppr_df.at[index,'att_dest_y'] = ppr_transpose_y(near_court, ppr_r['att_dest_y'])

    # Dig Coordinates
    if ppr_r['dig_yn'] == "Y":
      ppr_df.at[index,'dig_src_x'] = ppr_transpose_x(near_court, ppr_r['dig_src_x'])
      ppr_df.at[index,'dig_src_y'] = ppr_transpose_y(near_court, ppr_r['dig_src_y'])
      ppr_df.at[index,'dig_dest_x'] = ppr_transpose_x(near_court, ppr_r['dig_dest_x'])
      ppr_df.at[index,'dig_dest_y'] = ppr_transpose_y(near_court, ppr_r['dig_dest_y'])
      
  return ppr_df

def error_check_ppr(ppr_df):
  #print("#############  Error Checking ppr DataFrame ######################")
  all3 = False
  no_errors = 0

  # build a string to store this data
  error_string = str()
  # loop tyhru the datafarame looking for clear errors that can (or can not) be corrected
  for index,ppr_r in ppr_df.iterrows():
    # pass, set, attack all by same player
    if ppr_r['att_yn'] == "Y" and (ppr_r['pass_player'] == ppr_r['set_player'] ) and (ppr_r['set_player'] == ppr_r['att_player'] ):
      #print(f"|- Pass, Set, & Attack Same Player -| {ppr_r['pass_player']}, {ppr_r['set_player']}, {ppr_r['att_player']}, Point Number:{ppr_r['point_no']}")
      all3 = True
      error_string = error_string + print_to_string(f"|- Pass, Set, & Attack Same Player -| {ppr_r['pass_player']}, {ppr_r['set_player']}, {ppr_r['att_player']}, Point Number:{ppr_r['point_no']}")
      no_errors += 1
      #print(f"Error String in all 3:{error_string}")
      
    if ppr_r['set_yn'] == "Y" and (ppr_r['pass_player'] == ppr_r['set_player'] ) and not all3:
      #print(f"|- Pass and  Set Same Player       -| {ppr_r['pass_player']},{ppr_r['set_player']} Point Number:{ppr_r['point_no']}")
      error_string = error_string + print_to_string(f"|- Pass and  Set Same Player       -| {ppr_r['pass_player']},{ppr_r['set_player']} Point Number:{ppr_r['point_no']}")
      no_errors += 1

    if ppr_r['att_yn'] == "Y" and (ppr_r['att_player'] == ppr_r['set_player'] ) and not all3:  
      #print(f"|- Set and  Attack Same Player     -| {ppr_r['set_player']},{ppr_r['att_player']}Point Number:{ppr_r['point_no']}")  
      error_string = error_string + print_to_string(f"|- Set and  Attack Same Player     -| {ppr_r['set_player']},{ppr_r['att_player']}Point Number:{ppr_r['point_no']}")  
      no_errors += 1
      
    # serve and attack player are the same team
    if ppr_r['pass_yn'] == "Y":
      if (ppr_r['serve_player'] in ppr_r['teama']) and (ppr_r['pass_player'] in ppr_r['teama'] ) or (ppr_r['serve_player'] in ppr_r['teamb']) and (ppr_r['pass_player'] in ppr_r['teamb'] ):
        #print(f"|- Serve and Pass Same Team          -| {ppr_r['serve_player']}, {ppr_r['pass_player']}, Point Number:{ppr_r['point_no']}")
        error_string = error_string + print_to_string(f"|- Serve and Pass Same Team          -| {ppr_r['serve_player']}, {ppr_r['pass_player']}, Point Number:{ppr_r['point_no']}")
        no_errors += 1
      
    # can I check the service order?
    if not ppr_r['serve_player']:
      #print(f"|- No Serve Player                   -|{ppr_r['serve_player']}, Point Number:{ppr_r['point_no']}")
      error_string = error_string + print_to_string(f"|- No Serve Player                   -|{ppr_r['serve_player']}, Point Number:{ppr_r['point_no']}")
      no_errors += 1
      
    # can I check if the right team got the point?

    # someday, I'll have to deal with missing players

    #print(f"Total Errors Found:{no_errors}")
  error_string = error_string + print_to_string(f"Total Errors Found:{no_errors}")
  return ppr_df, no_errors, error_string

def calc_dist(x1,x2,y1,y2):
  # simple routine to calculate distance
  if x1 is not None and x2 is not None and y1 is not None and y2 is not None:
    return math.sqrt( (x2-x1)**2 + (y2-y1)**2 )
  else:
    return None

def calc_dur(t1, t2):
  return 0 if not ( isinstance(t1,(float,int)) and isinstance(t2,(float,int)) ) else (t2 - t1)

def calc_speed(dist, dur):
  # converting to km/hr from m/s
  #print(f"speed, Dist:{dist}, duration: {dur}")
  return 0 if not (isinstance(dur,(float,int)) and isinstance(dist,(float,int)) ) else dist/dur*(60/1000)

def calc_angle(x1,x2,y1,y2):
  if x1 is not None and x2 is not None and y1 is not None and y2 is not None:
    if (y2-y1) == 0: 
      return None
    else:
      angle = math.atan2( (x2-x1), -(y2-y1) ) # 180 degree phase shift with negative y
      return angle*57.29578
  else:
    return None

def calc_height(t1, t2):
  return 0 if not ( isinstance(t1,(float,int)) and isinstance(t2,(float,int)) ) else 1.225*(t2-t1)**2

def ppr_transpose_x(near_court, x1 ):
  # so x coordiantes are 0 - 1, where 0 is 0, and 1 is +8
  if x1 is not None:
    x2 = x1*8
    x2 = 8 - x2 if not near_court else x2
  else:
    x2 = None
  return x2

def ppr_transpose_y(near_court, y1 ):
  # so y coordiantes are 0 - 1, where 0 is -8, and 1 is +8
  if y1 is not None:
    y2 = y1*16 - 8
    y2 = -y2 if near_court else y2
  else:
    y2 = None
  return y2

def zone_depth(x1):
  if x1 is not None:
    if math.isnan(x1):
      zone = " "
    elif isinstance(x1,(float,int)):
      #print(f"zone depth x1:{x1}, Type:{type(x1)}")
      zone = "E"
      if x1 < 4*1.6:
        zone = "D"
      if x1 <3*1.6:
        zone = "C"
      if x1 <2*1.6:
        zone = "B"
      if x1 <1*1.6:
        zone = "A"
    else:
      zone = " "
  else:
      zone = " "
    
  return zone
  
def zone_net(x1):
  if x1 is not None:
    if math.isnan(x1):
      zone = 0
    elif isinstance(x1,(float,int)):
      zone = "1"
      if x1 < 4*1.6:
        zone = "2"
      if x1 < 3*1.6:
        zone = "3"
      if x1 < 2*1.6:
        zone = "4"
      if x1 < 1*1.6:
        zone = "5"
    else:
      zone = 0
  else:
    zone = 0
    
  return zone

def srv_zone_net(x1):
  if x1 is not None:
    if math.isnan(x1):
      zone = 0
    elif isinstance(x1,(float,int)):
      zone = "5"
      if x1 > 2.667:
        zone = "3"
      if x1 > 5.333:
        zone = "1"
    else:
      zone = 0
  else:
    zone = 0
    
  return zone
  
def calc_tactic( ppr_df ):
  for index, ppr_r in ppr_df.iterrows():
    # calculate the following tactics, put them into the 

    # option
    if ppr_r['pass_yn'] == "Y" and ppr_r['set_yn'] == "Y" and ppr_r['att_yn'] == "N":
      ppr_df.at[index,'tactic'] = "option" 
    if ppr_r['pass_yn'] == "Y" and ppr_r['set_yn'] == "N" and ppr_r['att_yn'] == "Y":
      ppr_df.at[index,'tactic'] = "option" 
      
    # set behind
    if ppr_r['pass_yn'] == "Y" and ppr_r['set_yn'] == "Y" and ppr_r['att_yn'] == "Y":
      if ppr_r['pass_src_x'] is not None and ppr_r['set_src_x'] is not None and ppr_r['att_src_x'] is not None:
        if ppr_r['pass_src_x'] < (4 - 0.1):          
          # now we must be apssing on the right
          ppr_df.at[index,'tactic'] = 'behind' if ( ppr_r['set_src_x'] >= ppr_r['pass_src_x'] and ppr_r['att_src_x'] >= ppr_r['set_src_x'] ) else ' '          
        if ppr_r['pass_src_x'] > ( 4 + 0.1):
          # we must be passing n the left
          ppr_df.at[index,'tactic'] = 'behind' if ( ppr_r['set_src_x'] <= ppr_r['pass_src_x'] and ppr_r['att_src_x'] <= ppr_r['set_src_x']) else ' '
 
  return (ppr_df)

def print_to_string(*args, **kwargs):
  output = io.StringIO()
  print(*args, file=output, **kwargs)
  contents = output.getvalue()
  output.close()
  return contents

def calc_out_of_system(dest_zone_net, dest_zone_depth, pass_height, src_zone_net, pass_angle):
  # calcualte the out of system flag
  oos_flag = 0
  #print(f'Out of System Called : {dest_zone_net,dest_zone_depth,pass_height,src_zone_net,pass_angle}')
  
  # Angle
  if (src_zone_net == '1') and ( float(pass_angle) > 10):
    oos_flag = oos_flag + 1
    #print(f'2a oos flag: {oos_flag}')
  if (src_zone_net == '2') & (pass_angle > 15 ):
    oos_flag = oos_flag + 1
    #print(f'2b oos flag: {oos_flag}')
  if (src_zone_net == '4') & (pass_angle < -15 ):
    oos_flag = oos_flag + 1
    #print(f'2c oos flag: {oos_flag}')
  if (src_zone_net == '5') & (pass_angle < -10 ):
    oos_flag = oos_flag + 1
    #print(f'2d oos flag: {oos_flag}')
    
  # zone
  if ( dest_zone_depth == 'E'):
    oos_flag = oos_flag + 10
    #print(f'3 oos flag: {oos_flag}')
  if (dest_zone_depth == 'D') & ( (dest_zone_net == '1' ) | (dest_zone_net == '5') ):
    oos_flag = oos_flag + 10
    #print(f'4 oos flag: {oos_flag}')

  # height
  if (pass_height > 0) & (pass_height < 1.00):
    oos_flag = oos_flag + 100
    #print(f'5 oos flag: {oos_flag}')
    
  #if ( oos_flag != 0 ):
   # print(f'6 oos flag: {oos_flag}, Dest net, depth, height, src net, angle: {dest_zone_net,dest_zone_depth,pass_height,src_zone_net,pass_angle}')
    
  return oos_flag
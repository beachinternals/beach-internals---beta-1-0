import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import io
import scipy.stats as stats
import pandas as pd
from server_functions import *

# This is a server module. It 
# runs on the Anvil server,
# rather than in the user's browser.
#

#-----------------------------------------------------------------------------#
#
#             Calculate the strenghts and weaknesses report
#
#
#-----------------------------------------------------------------------------#

@anvil.server.callable
def calc_sw_player_clientcallable(c_league, c_gender, c_year ):
  task = anvil.server.launch_background_task('calc_s_w_player_background',c_league, c_gender, c_year )
  return task
  
@anvil.server.background_task
def calc_s_w_player_background( c_league, c_gender, c_year ):
  return_value = calc_s_w_player(c_league, c_gender, c_year )
  return return_value
  
@anvil.server.callable
def calc_s_w_player( c_league, c_gender, c_year ):
  #
  # calculate the strengths and weaknesses dataframe based on:
  # - player_data 
  # - s_w_crit_player table
  #
  # Look for any values that meet the criteria, write a line to the dataframe
  #
  # then store the dataframe in the master_player file
  # open player data, create a data frame from it
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = 'League'
      ))

  if ppr_csv_row:
    if ppr_csv_row['player_data']:
      pdata_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data'].get_bytes()))
      pstat_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
      player_data_found= True
    else:
      pdata_df = ['']
      pstat_df = ['']
      print(f'calc_s_w_player : No Team Rows Found {c_league}, {c_gender}, {c_year}')
      player_data_found = False
  else:
    pdata_df = ['']
    pstat_df = ['']
    print(f'calc_s_w_player : No Team Rows Found {c_league}, {c_gender}, {c_year}, {c_team}')
    player_data_found = False

  if not player_data_found:
    return 'No Player Data Found'
    
  # create the dataframe for the s_w to be stored
  sw_dict = {'Player':[' '],
             'Category':[' '], # Category - Strength, Weakness, Of Note
             'Section':[' '], # Offense, Defense, Other ...
             'Description':[' '], # Description of the event
             'Var Name':[' '], # Name of Variable
             'Var Desc':[' '], # Description of the variable
             'Var Value':[0], # Value of the variable
             'Var Percentile':[0], # Percentile of this variable
             'Criteria':[0], # creitria, +/-1 stdev?
             'Criteria Value':[0]
            }

  # loop thru the master player file for the league 
  for mplayer_row in app_tables.master_player.search(league=c_league,gender=c_gender,year=c_year):

    # now find this row in the dataframe
    # pull out the player ID:
    mplayer = make_player_string( mplayer_row['team'], mplayer_row['number'], mplayer_row['shortname'])
    #print(f"s_w_player: Starting loop over master player for : {mplayer}")
    p = pdata_df.loc[pdata_df['player'] == mplayer ].index
    if len(p) > 0:
      print(f"s_w_player: Found {mplayer} in player data at index : {p[0]}")
    else:
      print(f"s_w_player: player not found in pair data file: player {mplayer}")
      
    sw_df = pd.DataFrame.from_dict(sw_dict)
    sw_df_new = pd.DataFrame.from_dict(sw_dict)

    #print(f"sw df: {sw_df}")
    #print(f"sw df new : {sw_df_new}")
    
    #print(f"Finding the player p=:{p}, player = {mplayer} p[0]: {p[0]}")
    #print(f"player data, plaeyer : {pdata_df['player']}")
    
    #print(f"player: {pdata_df.at[p,'player']}")

    # if we dd not find this player in the player data file, then skip this loop, and write an empty sw_df
    if len(p) != 0 :
      # loop thru the criteria file
      for c_row in app_tables.strength_weakness_criteria.search( q.all_of(active=True,type='player')):
        # start making comparisons
        variable = c_row['var']
        var_mean = variable + '_mean'
        var_sd = variable + '_stdev'
  
        #print(f"In the loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}, p:{p}")
        
        crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]
  
        print(f"variable: {variable}, critical value = {crit_value}, mean = {pstat_df.at[0,var_mean]}, StDev = {pstat_df.at[0,var_sd]}, Criteria = {c_row['criteria']}, p:{p}")
  
        if (((c_row['criteria'] > 0) & (pdata_df.at[p[0],variable] >= crit_value)) | ((c_row['criteria'] < 0) & (pdata_df.at[p[0],variable] <= crit_value )) | (c_row['criteria'] == 0)): 
          # then add a row to the sw_df dataframe
          #print("adding a row to new sw df, p:{p}")
          sw_df_new.at[0,'Player'] = pdata_df.at[p[0],'player']
          sw_df_new.at[0,'Category'] = c_row['category']
          sw_df_new.at[0,'Section'] = c_row['section']
          sw_df_new.at[0,'Description'] = c_row['description']
          sw_df_new.at[0,'Var Name'] = c_row['var']
          sw_df_new.at[0,'Var Desc'] = c_row['var_desc']
          sw_df_new.at[0,'Var Value'] = "{:.2f}".format(pdata_df.at[p[0],variable])
          #print(f"Calc Percentile: variable: {variable}, value:{pdata_df.at[p,variable]}, Mean: {pstat_df.at[0,var_mean]}, Stdev {pstat_df.at[0,var_sd]} Percentile:{stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )}")
          sw_df_new.at[0,'Var Percentile'] =  stats.norm.cdf( (pdata_df.at[p[0],variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )
          if not c_row['high_is_good']:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
          sw_df_new.at[0,'Criteria'] = c_row['criteria']
          sw_df_new.at[0,'Criteria Value'] = "{:.2f}".format(crit_value)

          sw_df_new.at[0,'Var Percentile'] = "{:.0%}".format(sw_df_new.at[0,'Var Percentile'])
          #print(sw_df_new)

          # Now append this to the df
          #print(f"sw_df: {sw_df.shape[0]}, {sw_df}")
          #print(f"sw_df_new: {sw_df_new.shape[0]},{sw_df_new}")
          #print(f"sw_df Player:{sw_df.at[0,'Player']}")
          #print(f"sw_df_new Player: {sw_df_new.at[0,'Player']}")
          #print(f"sw_df Category:{sw_df.at[0,'Category']}, sw_df_new Category: {sw_df_new.at[0,'Category']}")
          #print(f"sw_df Section:{sw_df.at[0,'Section']}, sw_df_new Section: {sw_df_new.at[0,'Section']}")
          #print(f"sw_df Description:{sw_df.at[0,'Description']}, sw_df_new Description: {sw_df_new.at[0,'Description']}")
          #print(f"sw_df Var Name:{sw_df.at[0,'Var Name']}, sw_df_new Var Name: {sw_df_new.at[0,'Var Name']}")
          #print(f"sw_df Var Desc:{sw_df.at[0,'Var Desc']}, sw_df_new Var Desc: {sw_df_new.at[0,'Var Desc']}")
          #print(f"sw_df Var Value:{sw_df.at[0,'Var Value']}, sw_df_new Var Value: {sw_df_new.at[0,'Var Value']}")
          #print(f"sw_df Var Percentile:{sw_df.at[0,'Var Percentile']}, sw_df_new Var Percentile: {sw_df_new.at[0,'Var Percentile']}")
          #print(f"sw_df Criteria:{sw_df.at[0,'Criteria']}, sw_df_new Criteria: {sw_df_new.at[0,'Criteria']}")
          #print(f"sw_df Criteria Value:{sw_df.at[0,'Criteria Value']}, sw_df_new Criteria Value: {sw_df_new.at[0,'Criteria Value']}")

          #print(f"Len of sw_df: {len(sw_df)}")
          sw_df = pd.concat([sw_df,sw_df_new])
          #print(f"updated sw df:{sw_df}, p:{p}")

        # unpack the team, number, and short name from our player defiition
        p_player = pdata_df.at[p[0],'player']
        str_loc = p_player.index(' ')
        p_team = p_player[:str_loc].strip()
        p_player = p_player[str_loc+1:]
        str_loc = p_player.index(' ')
        p_num = p_player[:str_loc].strip()
        p_sname = p_player[str_loc+1:].strip()
        #print(f"Updating the sw_df into the master player for: {c_league}, {c_gender}, {c_year}, {p_team}, {p_num}, {p_sname}, p:{p}")
    
    # save the dataframe into s_w in master_player
    #for mplayer_row in app_tables.master_player.search(
    #    q.all_of(
    #      league = c_league,
    #      gender = c_gender,
    #      year = c_year,
    #      team = p_team,
    #      number = p_num,
    #      shortname = p_sname
    #    )
    #  ):

    # convert DF to a media object
    #print(f"Saving SW DF for this player: {p_team}, {p_num},{p_sname}")
    sw_csv_file = pd.DataFrame.to_csv(sw_df[['Category','Section','Description','Var Name','Var Desc','Var Value','Var Percentile','Criteria','Criteria Value']])
    sw_media = anvil.BlobMedia(content_type="text/plain", content=sw_csv_file.encode(), name="sw.csv")
    save_result = mplayer_row.update( s_w = sw_media )
    print(f"Updated row in master player, result is: {save_result}, p:{p}")
  
    # next player

  # i think we are all done!

  return True

#-----------------------------------------------------------------------------
#
#         Now Calculate the 
#
#-----------------------------------------------------------------------------
@anvil.server.callable
def calc_sw_pair_clientcallable(c_league, c_gender, c_year ):
  task = anvil.server.launch_background_task('calc_s_w_pair_background',c_league, c_gender, c_year )
  return task
  
@anvil.server.background_task
def calc_s_w_pair_background( c_league, c_gender, c_year ):
  return_value = calc_s_w_pair(c_league, c_gender, c_year )
  return return_value
  
@anvil.server.callable
def calc_s_w_pair( c_league, c_gender, c_year ):
  #
  # calculate the strengths and weaknesses dataframe based on:
  # - player_data 
  # - s_w_crit_player table
  #
  # Look for any values that meet the criteria, write a line to the dataframe
  #
  # then store the dataframe in the master_player file
  # open player data, create a data frame from it
  c_team = 'League'
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
      ))

  if ppr_csv_row:
    if ppr_csv_row['pair_data']:
      pdata_df =  pd.read_csv(io.BytesIO( ppr_csv_row['pair_data'].get_bytes()))
      pstat_df =  pd.read_csv(io.BytesIO( ppr_csv_row['pair_data_stats'].get_bytes()))
      pair_data_found= True
    else:
      pdata_df = ['']
      pstat_df = ['']
      print(f'calc_s_w_pair : No Pair Data FOund on Team Row  {c_league}, {c_gender}, {c_year}, {c_team}')
      pair_data_found = False
  else:
    pdata_df = ['']
    pstat_df = ['']
    print(f'calc_s_w_pair : No Team Rows Found {c_league}, {c_gender}, {c_year}, {c_team}')
    pair_data_found = False

  if not pair_data_found:
    return 'No Pair Data Found'
    
  # create the dataframe for the s_w to be stored
  sw_dict = {'Pair':[' '],
             'Player':[' '],
             'Category':[' '], # Category - Strength, Weakness, Of Note
             'Section':[' '], # Offense, Defense, Other ...
             'Description':[' '], # Description of the event
             'Var Name':[' '], # Name of Variable
             'Var Desc':[' '], # Description of the variable
             'Var Value':[0], # Value of the variable
             'Var Percentile':[0], # Percentile of this variable
             'Criteria':[0], # creitria, +/-1 stdev?
             'Criteria Value':[0]
            }

  # loop thru the player file
  #print(f"Length of pdata df: {pdata_df.shape[0]}")
  # instead of looping thru pair data, we want to loop thru the master pair table to make sure we get everyone
  for pair_row in app_tables.master_pair.search(league=c_league,gender=c_gender,year=c_year):
    # now find this pair in the pair_data file, should return two records
    pdata_pair_df = pdata_df[pdata_df['pair'] == pair_row['pair']]
    print(f"sw_pair: looking for pair from the master table {pair_row['pair']} in the pair data file ")
    print(f"resulting dataframe: length {pdata_pair_df.shape[0]}")
    for p,p_row in pdata_pair_df.iterrows(): # this should be two loops
      #for p,p_row in pdata_df.iterrows():
      # put the dataframe definition inside the loop overr players to reset the dataframe for each player
    
      sw_df = pd.DataFrame.from_dict(sw_dict)
      sw_df_new = pd.DataFrame.from_dict(sw_dict)

      #print(f"sw df: {sw_df}")
      #print(f"sw df new : {sw_df_new}")
    
      #print(f"in loop over player data, p=:{p}")
      #print(f"player: {pdata_df.at[p,'player']}")
    
      # loop thru the criteria file
      for c_row in app_tables.strength_weakness_criteria.search( q.all_of(active=True,type='pair')):
        # start making comparisons
        variable = c_row['var']
        var_mean = variable + '_mean'
        var_sd = variable + '_stdev'

        #print(f"In the loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}, p:{p}")
      
        crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]
  
        #print(f"critical value = {crit_value}, mean = {pstat_df.at[0,var_mean]}, StDev = {pstat_df.at[0,var_sd]}, Criteria = {c_row['criteria']}, p:{p}")
        # added an 'or' for a 0 criteria, implying that this line should be included. This is used to have the actual value of a variable show up in the s_w table
        if (((c_row['criteria'] > 0) & (pdata_df.at[p,variable] >= crit_value)) | ((c_row['criteria'] < 0) & (pdata_df.at[p,variable] <= crit_value )) | (c_row['criteria'] == 0)): 
          # then add a row to the sw_df dataframe
          #print(f"adding a row to new sw df, p:{p}, {c_row['var']}, {c_row['criteria']}")
          sw_df_new.at[0,'Pair'] = pdata_df.at[p,'pair']
          sw_df_new.at[0,'Player'] = pdata_df.at[p,'player']
          sw_df_new.at[0,'Category'] = c_row['category']
          sw_df_new.at[0,'Section'] = c_row['section']
          sw_df_new.at[0,'Description'] = c_row['description']
          sw_df_new.at[0,'Var Name'] = c_row['var']
          sw_df_new.at[0,'Var Desc'] = c_row['var_desc']
          sw_df_new.at[0,'Var Value'] = "{:.2f}".format(pdata_df.at[p,variable])
          #print(f"Calc Percentile: value:{pdata_df.at[p,variable]}, Mean: {pstat_df.at[0,var_mean]}, Stdev {pstat_df.at[0,var_sd]} Percentile:{stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )}")
          sw_df_new.at[0,'Var Percentile'] =  stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )
          if not c_row['high_is_good']:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
          sw_df_new.at[0,'Criteria'] = c_row['criteria']
          sw_df_new.at[0,'Criteria Value'] = "{:.2f}".format(crit_value)
          sw_df_new.at[0,'Var Percentile'] = "{:.0%}".format(sw_df_new.at[0,'Var Percentile'])
          #print(sw_df_new)

          # Now append this to the df
          #print(f"sw_df: {sw_df.shape[0]}, {sw_df}")
          #print(f"sw_df_new: {sw_df_new.shape[0]},{sw_df_new}")
          #print(f"sw_df Player:{sw_df.at[0,'Player']}")
          #print(f"sw_df_new Player: {sw_df_new.at[0,'Player']}")
          #print(f"sw_df Category:{sw_df.at[0,'Category']}, sw_df_new Category: {sw_df_new.at[0,'Category']}")
          #print(f"sw_df Section:{sw_df.at[0,'Section']}, sw_df_new Section: {sw_df_new.at[0,'Section']}")
          #print(f"sw_df Description:{sw_df.at[0,'Description']}, sw_df_new Description: {sw_df_new.at[0,'Description']}")
          #print(f"sw_df Var Name:{sw_df.at[0,'Var Name']}, sw_df_new Var Name: {sw_df_new.at[0,'Var Name']}")
          #print(f"sw_df Var Desc:{sw_df.at[0,'Var Desc']}, sw_df_new Var Desc: {sw_df_new.at[0,'Var Desc']}")
          #print(f"sw_df Var Value:{sw_df.at[0,'Var Value']}, sw_df_new Var Value: {sw_df_new.at[0,'Var Value']}")
          #print(f"sw_df Var Percentile:{sw_df.at[0,'Var Percentile']}, sw_df_new Var Percentile: {sw_df_new.at[0,'Var Percentile']}")
          #print(f"sw_df Criteria:{sw_df.at[0,'Criteria']}, sw_df_new Criteria: {sw_df_new.at[0,'Criteria']}")
          #print(f"sw_df Criteria Value:{sw_df.at[0,'Criteria Value']}, sw_df_new Criteria Value: {sw_df_new.at[0,'Criteria Value']}")
  
          #print(f"Len of sw_df: {len(sw_df)}")
          sw_df = pd.concat([sw_df,sw_df_new])
          #print(f"updated sw df:{sw_df}, p:{p}")
    
     # save the dataframe into s_w in master_player
      for mpair_row in app_tables.master_pair.search(
          q.all_of(
            league = c_league,
            gender = c_gender,
            year = c_year,
            pair = pdata_df.at[p,'pair']
          )
        ):

        # convert DF to a media object
        #print(f"Saving SW DF for this player: {p_team}, {p_num},{p_sname}")
        sw_csv_file = pd.DataFrame.to_csv(sw_df[['Category','Section','Description','Var Name','Var Desc','Var Value','Var Percentile','Criteria','Criteria Value']])
        if p_row['player'] == mpair_row['player1']:
          sw_media = anvil.BlobMedia(content_type="text/plain", content=sw_csv_file.encode(), name="sw_player1.csv")
          save_result = mpair_row.update( s_w_player1 = sw_media )
          #print(f"calc_s_w_pair : UPdated row in master player, result is: {save_result}, p:{p}")
        elif p_row['player'] == mpair_row['player2']:
          sw_media = anvil.BlobMedia(content_type="text/plain", content=sw_csv_file.encode(), name="sw_player2.csv")
          save_result = mpair_row.update( s_w_player2 = sw_media )
          #print(f"calc_s_w_pair : UPdated row in master player, result is: {save_result}, p:{p}")
        else:
          print(f"calc_s_w_pair : Could not find the player in the pair: {p_row['pair']}, {p_row['player']}, Master Pair: {mpair_row['pair']}, {mpair_row['player1']}, {mpair_row['player2']}")
  
    # next player

  # i think we are all done!

  return True

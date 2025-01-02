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


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#

#-----------------------------------------------------------------------------#
#
#             Calculate the strenghts and weaknesses report
#
#
#-----------------------------------------------------------------------------#

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
    pdata_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data'].get_bytes()))
    pstat_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
    player_data_found= True
  else:
    pdata_df = ['']
    pstat_df = ['']
    #print('No Team Rows Found')
    player_data_found = False

  if not player_data_found:
    return 'No Player Data Found'
    
  # create the dataframe for the s_w to be stored
  sw_dict = {'Player':[''],
             'Category':[''], # Category - Strength, Weakness, Of Note
             'Section':[''], # Offense, Defense, Other ...
             'Descripton':[''], # Description of the event
             'Var Name':[''], # Name of Variable
             'Var Desc':[''], # Description of the variable
             'Var Value':[0], # Value of the variable
              'Var Percentile':[0], # Percentile of this variable
              'Criteria':[0], # creitria, +/-1 stdev?
              'Criteria Value':[0]
            }

  sw_df = pd.DataFrame.from_dict(sw_dict)
  sw_df_new = pd.DataFrame.from_dict(sw_dict)

  print(f"sw df: {sw_df}")
  print(f"sw df new : {sw_df_new}")

  # loop thru the player file
  print(f"Length of pdata df: {pdata_df.shape[0]}")
  for p in (0,pdata_df.shape[0]-1):

    print(f"in loop over player data, p=:{p}")
    print(f"player: {pdata_df.at[p,'player']}")
    
    # loop thru the criteria file
    for c_row in app_tables.strength_weakness_criteria.search( q.all_of(active=True,type='player')):
      # start making comparisons
      variable = c_row['var']
      var_mean = variable + '_mean'
      var_sd = variable + '_stdev'

      print(f"In teh loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}")
      
      crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]

      print(f"critical value = {crit_value}, mean = {pstat_df.at[0,var_mean]}, StDev = {pstat_df.at[0,var_sd]}, Criteria = {c_row['criteria']}")

      if ((c_row['criteria'] > 0) & (pdata_df.at[p,variable] >= crit_value)): 
        # then add a row to the sw_df dataframe
        print("adding a row to new sw df")
        sw_df_new.at[0,'Player'] = pdata_df.at[p,'player']
        sw_df_new.at[0,'Category'] = c_row['category']
        sw_df_new.at[0,'Section'] = c_row['section']
        sw_df_new.at[0,'Description'] = c_row['description']
        sw_df_new.at[0,'Var Name'] = c_row['var']
        sw_df_new.at[0,'Var Description'] = c_row['var_desc']
        sw_df_new.at[0,'Var Value'] = pdata_df.at[p,variable]
        sw_df_new.at[0,'Var Percentile'] = stats.norm.ppf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean] )/ pstat_df.at[0,var_sd])
        sw_df_new.at[0,'Criteria'] = c_row['criteria']
        sw_df_new.at[0,'Criteria Value'] = crit_value
        print(sw_df_new)

        # Now append this to the df
        sw_df = sw_df.append(sw_df_new)
        print(f"updated sw df:{sw_df}")

    # unpack the team, number, and short name from our player defiition
    p_player = pdata_df.at[p,'player']
    str_loc = p_player.index(' ')
    p_team = p_player[:str_loc].strip()
    p_player = p_player[str_loc+1:]
    str_loc = p_player.index(' ')
    p_num = p_player[:str_loc].strip()
    p_sname = p_player[str_loc+1:].strip()
    print(f"Updating the sw_df into the master player for: {c_league}, {c_gender}, {c_year}, {p_team}, {p_num}, {p_sname}")
    
    # save the dataframe into s_w in master_player
    for mplayer_row in app_tables.master_player.search(
      q.all_of(
        league = c_league,
        gender = c_gender,
        year = c_year,
        team = p_team,
        number = p_num,
        shortname = p_sname
      )
    ):

      # convert DF to a media object
      print(f"Saving SW DF for this player: {p_team}, {p_num},{p_sname}, SW DF: {sw_df}")
      sw_csv_file = pd.DataFrame.to_csv(sw_df)
      sw_media = anvil.BlobMedia(content_type="text/plain", content=sw_csv_file.encode(), name="sw.csv")
      save_result = mplayer_row.update( s_w = sw_media )
      print(f"UPdated row in master player, result is: {save_result}")
  
    # next player

  # i think we are all done!

  return True
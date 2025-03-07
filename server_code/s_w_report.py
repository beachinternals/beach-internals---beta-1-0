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

    # first off, create the two tables.  Then, if not foound, we will write an empty table.
    sw_df = pd.DataFrame.from_dict(sw_dict)
    sw_df_new = pd.DataFrame.from_dict(sw_dict)

    # now let's find the row in the pdata (player data) file
    current_player = mplayer_row['team'].strip()+' '+mplayer_row['number'].strip()+' '+mplayer_row['shortname'].strip()
    print(f"current Player: {current_player}")
    if current_player in pdata_df.values:
      # search for the value, test 
      p = pdata_df[pdata_df['player'] == current_player ].index[0]
      print(f"Length of pdata df: {pdata_df.shape[0]}, index of player: {p}")
      
      # loop thru the criteria file
      for c_row in app_tables.strength_weakness_criteria.search( q.all_of(active=True,type='player')):
        # start making comparisons
        variable = c_row['var']
        var_mean = variable + '_mean'
        var_sd = variable + '_stdev'
  
        #print(f"In the loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}, p:{p}")
      
        crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]

        #print(f"critical value = {crit_value}, mean = {pstat_df.at[0,var_mean]}, StDev = {pstat_df.at[0,var_sd]}, Criteria = {c_row['criteria']}, p:{p}")

        if (((c_row['criteria'] > 0) & (pdata_df.at[p,variable] >= crit_value)) | ((c_row['criteria'] < 0) & (pdata_df.at[p,variable] <= crit_value )) | (c_row['criteria'] == 0)): 
          # then add a row to the sw_df dataframe
          #print("adding a row to new sw df, p:{p}")
          sw_df_new.at[0,'Player'] = pdata_df.at[p,'player']
          sw_df_new.at[0,'Category'] = c_row['category']
          sw_df_new.at[0,'Section'] = c_row['section']
          sw_df_new.at[0,'Description'] = c_row['description']
          sw_df_new.at[0,'Var Name'] = c_row['var']
          sw_df_new.at[0,'Var Desc'] = c_row['var_desc']
          sw_df_new.at[0,'Var Value'] = "{:.2f}".format(pdata_df.at[p,variable])
          #print(f"Calc Percentile: value:{pdata_df.at[p,variable]}, Mean: {pstat_df.at[0,var_mean]}, Stdev {pstat_df.at[0,var_sd]} Percentile:{stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )}")
          sw_df_new.at[0,'Var Percentile'] =  stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )
          sw_df_new.at[0,'Criteria'] = c_row['criteria']
          sw_df_new.at[0,'Criteria Value'] = "{:.2f}".format(crit_value)
  
          # calibrate percentile to criteria and category
          if sw_df_new.at[0,'Category'] == 'Strength' and sw_df_new.at[0,'Criteria'] < 0:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
          if sw_df_new.at[0,'Category'] == 'Weakness' and sw_df_new.at[0,'Criteria'] > 0:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
  
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
        p_player = pdata_df.at[p,'player']
        str_loc = p_player.index(' ')
        p_team = p_player[:str_loc].strip()
        p_player = p_player[str_loc+1:]
        str_loc = p_player.index(' ')
        p_num = p_player[:str_loc].strip()
        p_sname = p_player[str_loc+1:].strip()
        #print(f"Updating the sw_df into the master player for: {c_league}, {c_gender}, {c_year}, {p_team}, {p_num}, {p_sname}, p:{p}")
    
    #else:
      # IN THIS CASE, the player was not found in the stats file, so we will just leave the dsw_datqaframe blank

      
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
    #print(f"UPdated row in master player, result is: {save_result}, p:{p}")
  
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
  #
  # start a loop over each player in the pair datatable
  for p,p_row in pdata_df.iterrows():
    # put the dataframe definition inside the loop over players to reset the dataframe for each player
    
    sw_df = pd.DataFrame.from_dict(sw_dict)
    sw_df_new = pd.DataFrame.from_dict(sw_dict)

    #print(f"sw df: {sw_df}")
    #print(f"sw df new : {sw_df_new}")
    
    #print(f"in loop over player data, p=:{p}")
    #print(f"player: {pdata_df.at[p,'player']}")
    
    # loop thru the criteria file
    for c_row in app_tables.strength_weakness_criteria.search( q.all_of(active=True,type='pair')):

      # now, we are adding two (maybe more) type of criteria: simple, serve strategies
      #
      # serve strategy type sof criteria are looking at variables acorss all 45 possbile serves
      if c_row['criteria_type'] == 's':
        # these are criteria for each of hte 45 different serve strategies, fbhe_#_&$ where:
        #.   '#' is the serve from [1,3,5]
        #.   '&' is the serve destinatiojn net, [1,2,3,4,5]
        #.   '$' is hte serve destination depth, [C,D,E]

        var_base = c_row['var']
        for i in [1,3,5]:
          var1 = var_base.replace('#',str(i))
          #print(f"Variable, in I loop over serve source {var1}")
          for j in [1,2,3,4,5]:
            var2 = var1.replace('&',str(j))
            #print(f"Variable, in J loop over serve dest net {var2}")
            for k in ['c', 'd', 'e']:
              variable = var2.replace('$',k)
              #print(f"Variable, in K loop over serve dest depth {variable}")
              var_mean = variable + '_mean'
              var_sd = variable + '_stdev'

              crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]
              print(f"In the loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}, p:{p}, Critical Value : {crit_value}, player value: {pdata_df.at[p,variable]}")
              
              # now, make the comparison and add the line to the new sw_df
              if (((c_row['criteria'] > 0) & (pdata_df.at[p,variable] >= crit_value)) | ((c_row['criteria'] < 0) & (pdata_df.at[p,variable] <= crit_value )) | (c_row['criteria'] == 0)): 
                # then add a row to the sw_df dataframe
                print(f"adding a row to new sw df, p:{p}, {c_row['var']}, {c_row['criteria']}, Variable: {variable}")
                sw_df_new.at[0,'Pair'] = pdata_df.at[p,'pair']
                sw_df_new.at[0,'Player'] = pdata_df.at[p,'player']
                sw_df_new.at[0,'Category'] = c_row['category']
                sw_df_new.at[0,'Section'] = c_row['section']
                sw_df_new.at[0,'Description'] = c_row['description'] + ' Serves from '+str(i)+" to "+str(j)+str(k)
                sw_df_new.at[0,'Var Name'] = c_row['var']
                sw_df_new.at[0,'Var Desc'] = c_row['var_desc']+ ' Serves from '+str(i)+" to "+str(j)+str(k)
                sw_df_new.at[0,'Var Value'] = "{:.2f}".format(pdata_df.at[p,variable])
                #print(f"Calc Percentile: value:{pdata_df.at[p,variable]}, Mean: {pstat_df.at[0,var_mean]}, Stdev {pstat_df.at[0,var_sd]} Percentile:{stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )}")
                sw_df_new.at[0,'Var Percentile'] =  stats.norm.cdf( (pdata_df.at[p,variable] - pstat_df.at[0,var_mean])/ pstat_df.at[0,var_sd] )
                sw_df_new.at[0,'Criteria'] = c_row['criteria']
                sw_df_new.at[0,'Criteria Value'] = "{:.2f}".format(crit_value)

                # calibrate percentile to criteria and category
                if sw_df_new.at[0,'Category'] == 'Strength' and sw_df_new.at[0,'Criteria'] < 0:
                  sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
                if sw_df_new.at[0,'Category'] == 'Weakness' and sw_df_new.at[0,'Criteria'] > 0:
                  sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']

                sw_df_new.at[0,'Var Percentile'] = "{:.0%}".format(sw_df_new.at[0,'Var Percentile'])
        
                # now add this to the sw dataframe
                sw_df = pd.concat([sw_df,sw_df_new])
              
      else:  # these are the 'normal' variable comparisons
        # start making comparisons
        variable = c_row['var']
        var_mean = variable + '_mean'
        var_sd = variable + '_stdev'

        print(f"In the loop over Criteria, variable = {variable}, var mean = {var_mean}, var sd = {var_sd}, p:{p}")
        crit_value = pstat_df.at[0,var_mean] + c_row['criteria']*pstat_df.at[0,var_sd]

        #print(f"critical value = {crit_value}, mean = {pstat_df.at[0,var_mean]}, StDev = {pstat_df.at[0,var_sd]}, Criteria = {c_row['criteria']}, p:{p}")
        # added an 'or' for a 0 criteria, implying that this line should be included. This is used to have the actual value of a variable show up in the s_w table
        if (((c_row['criteria'] > 0) & (pdata_df.at[p,variable] >= crit_value)) | ((c_row['criteria'] < 0) & (pdata_df.at[p,variable] <= crit_value )) | (c_row['criteria'] == 0)): 
          # then add a row to the sw_df dataframe
          print(f"adding a row to new sw df, p:{p}, {c_row['var']}, {c_row['criteria']}")
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
          sw_df_new.at[0,'Criteria'] = c_row['criteria']
          sw_df_new.at[0,'Criteria Value'] = "{:.2f}".format(crit_value)

          # calibrate percentile to criteria and category
          if sw_df_new.at[0,'Category'] == 'Strength' and sw_df_new.at[0,'Criteria'] < 0:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']
          if sw_df_new.at[0,'Category'] == 'Weakness' and sw_df_new.at[0,'Criteria'] > 0:
            sw_df_new.at[0,'Var Percentile'] = 1 - sw_df_new.at[0,'Var Percentile']

          sw_df_new.at[0,'Var Percentile'] = "{:.0%}".format(sw_df_new.at[0,'Var Percentile'])
          #print(sw_df_new)

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

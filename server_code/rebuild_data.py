import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta
import pandas as pd
import btd_ppr_conversion
import ppr_master_merge
import io

# functions to update / rebuild all the data files

#-------------------------------------------------------------------------
#
#        Set this up as three to call like a half hour apart
#
#.       1) btd > ppr                        : call_build_ppr > build_ppr
#        2) Merge ppr files                  : call_merge_ppr > merge_ppr
#        3) Calculate data and tri files     : call_calc_player_data > calc_player_data
#        4) create reports
#
#-------------------------------------------------------------------------

#--------------------------------------------------------------------
#
#        Build PPR data
#        Convert btd to ppr and save in ppr_csv table
#
#-------------------------------------------------------------------
@anvil.server.callable
def call_build_ppr_data():
  task = anvil.server.launch_background_task('build_ppr_data1')
  return task

@anvil.server.callable
def call_build_all_ppr_data():
  task = anvil.server.launch_background_task('build_ppr_data2')
  return task


@anvil.server.background_task
def build_ppr_data1():
  # this just call the background task with the 
  task = calculate_ppr_data(False)

  return task

@anvil.server.background_task
def build_ppr_data2():
  task = calculate_ppr_data(True)
  return task

@anvil.server.callable
def calculate_ppr_data(rebuild):
  now = datetime.now()
  email_text = "Calculate PPR Data \n Called at:" + str(now) + "\n"
  
  # build data for all new files, for all leagues

  # do the btd -> ppr conversion for all btf files
  dict = {'league':[str()],
          'gender':[str()],
          'year':[str()],
          'team':[str()]
         }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    # make a quick df of the values needed
    btd_df.at[i,'league'] = btd_file_r['league']
    btd_df.at[i,'gender'] = btd_file_r['gender']
    btd_df.at[i,'year'] = btd_file_r['year']
    btd_df.at[i,'team'] = btd_file_r['team']
    i = i + 1

  # now we need to make this unique  
  print(btd_df)
  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  team_list = pd.unique(btd_df['team'])

  # loop over league
  for c_league in league_list:
    # loop ober gender
    #print(f"processing for league: {c_league}")
    for c_gender in gender_list:
      #print(f"processing for gender: {c_gender}")
      # loop over year
      for c_year in year_list:
        # loop over team
        #print(f"processing for year: {c_year}")
        for c_team in team_list:
          # CALL BTD > PPR 
          email_text = email_text + ' Generating PPR Files for ' + c_league + " "+ c_gender +" "+ c_year +" "+ c_team +"\n"
          #print(email_text)
          r_val = anvil.server.launch_background_task('generate_ppr_files', c_league, c_gender, c_year, c_team, rebuild )

    #now, send an email with the updates
  internals_email = 'spccoach@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Rebuild Data',text=email_text)
  

  return r_val, email_status

@anvil.server.callable
def call_merge_ppr_data():
  task = anvil.server.launch_background_task('merge_ppr_data1')
  return task

@anvil.server.background_task
def merge_ppr_data1():
  # this just call the background task with the 
  task = merge_ppr_data()
  return task
  
@anvil.server.callable
def merge_ppr_data():
  now = datetime.now()
  email_text = "Merging PPR Data \n Called at:" + str(now) + "\n"
  
  # build data for all new files, for all leagues

  # do the btd -> ppr conversion for all btf files
  dict = {'league':[str()],
          'gender':[str()],
          'year':[str()],
          'team':[str()]
         }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    # make a quick df of the values needed
    btd_df.at[i,'league'] = btd_file_r['league']
    btd_df.at[i,'gender'] = btd_file_r['gender']
    btd_df.at[i,'year'] = btd_file_r['year']
    btd_df.at[i,'team'] = btd_file_r['team']
    i = i + 1

  # now we need to make this unique  
  print(btd_df)
  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  team_list = pd.unique(btd_df['team'])

  
  for c_league in league_list:
    for c_gender in gender_list:  
      for c_year in year_list:
        for c_team in team_list:
          # now loop and call the merge function
          email_text = email_text + ' Merging PPR Files for' + c_league +" "+ c_gender +" "+ c_year +" "+ c_team + "\n"
          #print(email_text)
          r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Private' )
          r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Scouting' )
          if c_team == 'INTERNALS':
            email_text = email_text + ' Merging PPR Files for' + ' '+c_league + " "+c_gender +" "+ c_year +" "+ c_team + ' League'+"\n"
            #print(email_text)
            r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'League' )

  #now, send an email with the updates
  internals_email = 'spccoach@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Merge PPR Data',text=email_text)
  
  return r_val, email_status

@anvil.server.callable
def call_calculate_data():
  task = anvil.server.launch_background_task('calculate_data')
  return task

@anvil.server.background_task
def calculate_data():
  # this just call the background task with the 
  task = calculate_data1()
  return task
  
@anvil.server.callable
def calculate_data1():
  now = datetime.now()
  email_text = "Calculate PPR Data \n Called at:" + str(now) + "\n"
  
  # build data for all new files, for all leagues

  # do the btd -> ppr conversion for all btf files
  dict = {'league':[str()],
          'gender':[str()],
          'year':[str()],
          'team':[str()]
         }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    # make a quick df of the values needed
    btd_df.at[i,'league'] = btd_file_r['league']
    btd_df.at[i,'gender'] = btd_file_r['gender']
    btd_df.at[i,'year'] = btd_file_r['year']
    i = i + 1

  # now we need to make this unique  
  print(btd_df)
  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  
  for c_league in league_list:
    for c_gender in gender_list:  
      for c_year in year_list:
        # now make the player data and triange data
        email_text = email_text + ' Calculating Player Data for ' + c_league + ' '+ c_gender + ' '+ c_year+"\n"
        r_val = anvil.server.launch_background_task('calculate_player_data', c_league, c_gender, c_year)
        email_text = email_text + ' Calculating Triangle Data for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
        r_val = anvil.server.launch_background_task('calculate_triangle_scoring', c_league, c_gender, c_year)
        email_text = email_text + ' Building Pair Table for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
        r_val = anvil.server.launch_background_task('build_pair_table_background', c_league, c_gender, c_year)

  #now, send an email with the updates
  internals_email = 'spccoach@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Calculate player and Triangle Data',text=email_text)
  

  return r_val, email_status

@anvil.server.background_task
def build_pair_table_background(c_league, c_gender, c_year):
  # call the background taks
  # build the pair table for each league based on the ppr.league table

  # get the ppr file
  c_team = "League"    # only updating the league tables
  #print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
      ) )

  if ppr_csv_row:
    ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
    if ppr_df.shape[0] == 0:
      return ["No Rows"]
  else:
    #print('No Rows Found')
    return ["No Rows"]

  # extract team a and team b lists
  team_list_a = ppr_df['teama']
  #print(f"Team List A: {team_list_a}")
  team_list_a = team_list_a.rename( {'teama':'team'} )
  team_list_b = ppr_df['teamb']
  #print(f"Team List B: {team_list_b}")
  team_list_b = team_list_b.rename( {'teamb':'team'} )
  team_list = pd.concat([team_list_a,team_list_b])
  print(f"Pair List:{team_list}")
  team_list = team_list.unique()
  print(f"Pair List:{team_list}")
  team_list = team_list.sort()

  # save it back to the ppr_csv table
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  tmp = pd.DataFrame(team_list)
  pair_csv_file = pd.DataFrame.to_csv(tmp)
  pair_media = anvil.BlobMedia(content_type="text/plain", content=pair_csv_file.encode(), name="pair_table.csv")
  ppr_csv_row.update(pair_list = pair_media)

  return True
  
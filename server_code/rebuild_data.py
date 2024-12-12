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

  #now, send an email with the updates
  internals_email = 'spccoach@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Calculate player and Triangle Data',text=email_text)
  

  return r_val, email_status


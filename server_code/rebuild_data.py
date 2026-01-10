import anvil.secrets
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
import io
import rebuild_data
import numpy as np

# ============================================================================
# PERFORMANCE MONITORING IMPORTS
# ============================================================================
from server_functions import (
monitor_performance,
MONITORING_LEVEL_OFF,
MONITORING_LEVEL_CRITICAL,
MONITORING_LEVEL_IMPORTANT,
MONITORING_LEVEL_DETAILED,
MONITORING_LEVEL_VERBOSE
)

# import error logging funcitons
from logger_utils import log_info, log_error, log_critical, log_debug

# Import other modules
from btd_ppr_conversion import *
from ppr_master_merge import *
from calc_player_data import *
from calc_pair_data import *
from calc_traingle_scoring import *
from s_w_report import *


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

#--------------------------------------------------------------------------------------
#
#         Night Processing
#
#  A single night processing routine that runs everything in sequence
#  This limits processor time (as opposed to multiple background jobs)
#
#--------------------------------------------------------------------------------------
@anvil.server.callable
def night_processing_callable(c_league,c_gender,c_year,rebuild_all, all_leagues):
  # this one we can call from the browser for testing
  # allows to call for just one league or all
  # all_leagues  = True for all leagues
  # rebuild_all = True to rebuild the ppr files for all btd file
  return anvil.server.launch_background_task('night_processing_backgound',c_league,c_gender,c_year,rebuild_all, all_leagues)

@anvil.server.background_task
def night_processing_cron():
  # this stub to put into the cron with the parameters desired
  return anvil.server.launch_background_task('night_processing_backgound','','','',False,True)


@anvil.server.background_task
@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def night_processing_backgound(d_league,d_gender,d_year,rebuild_all, all_leagues):
  # call all the different night processing tasks in sequence, league by leaguye

  # set up email text
  now = datetime.now()
  email_message = 'Night Processing Started at :' + str( now ) + "\n"
  email_message = email_message +'All Leagues:'+str(all_leagues)+'.  Rebuild All:'+str(rebuild_all)+'   League:'+d_league+'   Gender:'+d_gender+'.  Year:'+d_year+'\n'

  # do the btd -> ppr conversion for all btd files
  dict = {'league':[str()],
          'gender':[str()],
          'year':[str()],
          'team':[str()],
          'update':[str()]
         }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    # make a quick df of the values needed
    btd_df.at[i,'league'] = btd_file_r['league']
    btd_df.at[i,'gender'] = btd_file_r['gender']
    btd_df.at[i,'year'] = btd_file_r['year']
    btd_df.at[i,'team'] = btd_file_r['team']
    btd_df.at[i,'update'] = 'No'   # use this to mark an update so that we need to rebuild for this league
    i = i + 1

  # now we need to make this unique  
  #print(btd_df)
  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  team_list = pd.unique(btd_df['team'])
  
  for c_league in league_list:
    # loop ober gender
    #print(f"processing for league: {c_league}")
    for c_gender in gender_list:
      #print(f"processing for gender: {c_gender}")
      # loop over year
      for c_year in year_list:
        # loop over team
        #print(f"processing for year: {c_year}")
        #print(f"Checking all league iff statement, All League: {str(all_leagues)}")
        #print(f" c :{c_league}, d:{d_league} c :{c_gender}, d:{d_gender}, c:{c_year}, d:{d_year}")
        new_league_data = False
        new_team_data = False
        if ( all_leagues) or ((c_league == d_league) and (c_gender == d_gender) and (c_year == d_year)):
          for c_team in team_list:
            email_message = email_message + 'Generating PPR files for: '+c_league+' '+c_gender+' '+c_year+' '+c_team+'\n'
            #print(email_message)
            r_value, new_team_data = generate_ppr_files_not_background(c_league, c_gender, c_year, c_team, rebuild_all  ) # modify this to return true if there are update, false if not
            email_message = email_message + '        '+str(r_value) + ' New Data Found =' + str(new_team_data) + "\n"
    
            # now merge the data for this league
            #-------------------------------------
            if new_team_data:
              new_league_data = True
              email_message = email_message + ' Merging PPR Files for ' + c_league +" "+ c_gender +" "+ c_year +" "+ c_team + "\n"
              #print(email_message)
              r_val =  make_master_ppr_not_background( c_league, c_gender, c_year, c_team, 'Private' )
              r_val =  make_master_ppr_not_background( c_league, c_gender, c_year, c_team, 'Scouting' )   
              # anytime a team has new data, need to build the league file
              email_message = email_message + ' Merging PPR Files for ' + c_league + ' '+ c_gender + ' '+ c_year+' League'+ "\n"
              r_val =  make_master_ppr_not_background( c_league, c_gender, c_year, c_team, 'League' )

        if new_league_data:
          # now calculate player data
          #-----------------------------
          email_message = email_message + ' Calculating Player Data for ' + c_league + ' '+ c_gender + ' '+ c_year+"\n"
          r_val = calculate_player_data_not_background(c_league, c_gender, c_year)
          email_message = email_message + '        '+str(r_val)+"\n"

          # Calculate Triangle Data
          #------------------------
          email_message = email_message + ' Calculating Triangle Data for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          r_val = calculate_triangle_scoring_not_background( c_league, c_gender, c_year)
          email_message = email_message + '        '+str(r_val) + "\n"

          # Calculate Pair Table
          #-----------------------
          email_message = email_message + ' Building Pair Table for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          r_val = build_pair_df( c_league, c_gender, c_year)
          email_message = email_message + ' Building Pair Data for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          r_val = calculate_pair_data_not_background( c_league, c_gender, c_year)
          email_message = email_message + '        '+str(r_val) + "\n"

          # calculate pair data
          email_message = email_message + ' Building Pair Data & Stats for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          r_val = calc_s_w_player( c_league, c_gender, c_year )
          email_message = email_message + '        '+str(r_val) + "\n"
          
          # calculate the strength and weaknesses
          email_message = email_message + ' Building Strengths & Weaknesses for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          r_val = calc_s_w_player( c_league, c_gender, c_year )
          email_message = email_message + '        '+str(r_val) + "\n"
        else:
          email_message = email_message + 'No New data Found for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
          
  # the very last thing, load the pair's data table
  email_message = email_message + ' Loading Pair data Table ' + "\n"
  r_val = load_pair_data_table()
  email_message = email_message + '        '+str(r_val) + "\n"
  
  #now, send an email with the updates
  internals_email = 'beachinternals@gmail.com'
  now1 = datetime.now()
  email_message = email_message + "Night Processing Completed at:" + str(now1) + ' Compute time: '+str(now1-now)+ "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Night Processing',text=email_message)

  return True

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
  #print(btd_df)
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
  internals_email = 'beachinternals@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Rebuild Data',text=email_text)
  

  return r_val, email_status

@anvil.server.callable
def call_merge_ppr_data():
  task = anvil.server.launch_background_task('merge_ppr_data1')
  return task

@anvil.server.background_task
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def merge_ppr_data1():
  # this just call the background task with the 
  task = merge_ppr_data()
  return task
  
@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
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
  #print(btd_df)
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
          print(email_text)
          r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Private' )
          r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Scouting' )
          if c_team == 'INTERNALS':
            email_text = email_text + ' Merging PPR Files for' + ' '+c_league + " "+c_gender +" "+ c_year +" "+ c_team + ' League'+"\n"
            #print(email_text)
            r_val =  anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'League' )

  #now, send an email with the updates
  internals_email = 'beachinternals@gmail.com'
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
  #print(btd_df)
  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  
  for c_league in league_list:
    for c_gender in gender_list:  
      for c_year in year_list:
        # now make the player data and triange data
        email_text = email_text + ' Calculating Player Data for ' + c_league + ' '+ c_gender + ' '+ c_year+"\n"
        r_val = calculate_player_data( c_league, c_gender, c_year)
        email_text = email_text + ' Calculating Triangle Data for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
        r_val = calculate_triangle_scoring( c_league, c_gender, c_year)
        email_text = email_text + ' Building Pair Table for ' + c_league + ' '+ c_gender + ' '+ c_year +"\n"
        r_val = build_pair_table_background( c_league, c_gender, c_year)

  #now, send an email with the updates
  internals_email = 'beachinternals@gmail.com'
  email_text = email_text + "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Calculate player and Triangle Data',text=email_text)
  

  return r_val, email_status

@anvil.server.callable
def build_pair_table(c_league,c_gender,c_year):
  return anvil.server.launch_background_task('build_pair_table_background',c_league,c_gender,c_year)

@anvil.server.callable
def build_pair_data_table():
  return anvil.server.launch_background_task('build_pair_data_background')

  
@anvil.server.background_task
def build_pair_table_background(c_league, c_gender, c_year):
  return build_pair_df(c_league,c_gender,c_year)

def build_pair_df(c_league,c_gender,c_year):
  # call the background taks
  # build the pair table for each league based on the ppr.league table

  # get the ppr file
  c_team = "League"    # only updating the league tables
  print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")
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
  team_list_a = ppr_df[['teama','player_a1','player_a2']]
  team_list_a = team_list_a.rename( columns={'teama':'team', 'player_a1':'player1', 'player_a2':'player2'} )
  #print(f"Team List A: {team_list_a}")
  
  team_list_b = ppr_df[['teamb','player_b1','player_b2']]
  team_list_b = team_list_b.rename( columns={'teamb':'team', 'player_b1':'player1','player_b2':'player2'} )
  #print(f"Team List B: {team_list_b}")
  
  team_list = pd.concat([team_list_a,team_list_b])
  #print(f"Pair List Concat:{team_list}")
  
  team_list = team_list.drop_duplicates()
  #print(f"Pair List Unique:{team_list}")
  
  team_list = team_list.sort_values(by=['team'])
  #print(f"Pair List Sort:{team_list}")

  # save it back to the ppr_csv table
  # first, I need to cahnge the ppr_file dataframe to a csv file.
  tmp = pd.DataFrame(team_list)
  #print(f"TMP: pair list in a dataframe:{tmp}")
  pair_csv_file = pd.DataFrame.to_csv(tmp)
  print(f"Pair list being updated for: {c_league}, {c_gender}, {c_year}")
  pair_media = anvil.BlobMedia(content_type="text/plain", content=pair_csv_file.encode(), name="pair_table.csv")
  ppr_csv_row.update(pair_list = pair_media, pair_list_date=datetime.now())

  return True

#----------------------------------------------------------------------------------
@anvil.server.callable
def load_pair_data_table_not_background():
  return anvil.server.launch_background_task('build_pair_data_background')
  
@anvil.server.background_task
def build_pair_data_background():
  return load_pair_data_table()

def load_pair_data_table():
  # here we put the pair dataframe into the pairs table in Anvil, using the pair_list in the 'league' entries

  # dump the contents of the master_pairs table in anvil
  #app_tables.master_pair.delete_all_rows()
  
  # get a set of rows from ppr_ccv table for team = league, loop thru the rows

  for lrow in app_tables.ppr_csv_tables.search( team=q.like("League") ):
    show_print = True if (lrow['league'] == 'AVP') else False
    print(f"Processing : {lrow['league']}, {lrow['gender']},{lrow['year']}")
    if show_print:
      print(f"pair list : {type(lrow['pair_list'])}")
      print(f"pair list : {lrow['pair_list']}")
    if lrow['pair_list']:
      pair_df =  pd.read_csv(io.BytesIO( lrow['pair_list'].get_bytes()))
      if pair_df.shape[0] == 0:
        if show_print:
          print(f"load_pair_data_table: Pair List Df Empty : {lrow['league']}, {lrow['gender']},{lrow['year']}")
        return ["No Pair List Found"]
      
      # loop thru the rows in in teh pair-list
      #print(pair_df)
      for index, p in pair_df.iterrows():
        # create a new row in the master_pair table
        #print(f"Row; {p}")
        team_delim = p[1].find(' ')
        if team_delim == -1:
          if show_print:
            print(f"Load Pair Table, no space found looking for Team: {p[1]}")
          a=b
        pair_team = p[1][:team_delim].strip()
        if show_print:
          print(f"load_pair_data_table: Looking for:{lrow['league']}, {lrow['gender']},{lrow['year']} p0 Index: {p[0]}, Pair: {p[1]}, Player1: {p[2]}, Player2: {p[3]}, Team: {pair_team}")
        #print(f"Adding to master pair list: {lrow['league']}, {lrow['gender']},{lrow['year']} p0 Index: {p[0]}, Pair: {p[1]}, Player1: {p[2]}, Player2: {p[3]}") 
        if not app_tables.master_pair.get( league = lrow['league'],
                                        gender = lrow['gender'],
                                        year = lrow['year'],
                                        player1 = p[2],
                                        player2 = p[3],
                                        pair = p[1],
                                        team = pair_team
                                        ):
          # add a row for this pair
          if show_print:
            print(f"load_pair_data_table: Adding to master pair list: {lrow['league']}, {lrow['gender']},{lrow['year']} p0 Index: {p[0]}, Team: {p[1]}, Player1: {p[2]}, Player2: {p[3]}") 
          app_tables.master_pair.add_row( league = lrow['league'],
                                        gender = lrow['gender'],
                                        year = lrow['year'],
                                        player1 = p[2],
                                        player2 = p[3],
                                        pair = p[1],
                                        team = pair_team
                                        )

    else:
      print(f"No Pair List Data Frame Found : {lrow['league']}, {lrow['gender']},{lrow['year']}")
      #return False

  return True



@anvil.server.background_task
def check_inconsistent_data():
  """
    Nightly task to check for inconsistent data in btd_files and master_player tables,
    including duplicate video_id in btd_files and near-duplicate numbers in master_player
    only for league='NCAA', and send an email report to beachinternals@gmail.com.
    """
  try:
    log_info("Starting nightly data consistency check at %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    issues = []

    # Check btd_files for per_xy == 0, points < 25, and duplicate video_id
    btd_issues = []
    btd_rows = list(app_tables.btd_files.search())  # Convert to list for multiple passes

    # Check per_xy and points
    for row in btd_rows:
      row_issues = []
      if row['per_xy'] == 0:
        row_issues.append("per_xy is 0")
      if row['points'] is not None and row['points'] < 25:
        row_issues.append(f"points ({row['points']}) < 25")
      if row_issues:
        league = str(row['league'] or 'None')
        gender = str(row['gender'] or 'None')
        year = str(row['year'] if row['year'] is not None else 'None')
        team = str(row['team'] or 'None')
        filename = str(row['filename'] or 'None')
        btd_issues.append(
          f"Row ID {row.get_id()}: {', '.join(row_issues)} "
          f"(league={league}, gender={gender}, year={year}, team={team}, filename={filename})"
        )

        # Check for duplicate video_id
    video_id_counts = {}
    for row in btd_rows:
      video_id = row['video_id']
      if video_id is not None:
        video_id = str(video_id)  # Ensure string for consistency
        video_id_counts[video_id] = video_id_counts.get(video_id, []) + [row]

    for video_id, rows in video_id_counts.items():
      if len(rows) > 1:  # Duplicates found
        row_ids = [row.get_id() for row in rows]
        for row in rows:
          league = str(row['league'] or 'None')
          gender = str(row['gender'] or 'None')
          year = str(row['year'] if row['year'] is not None else 'None')
          team = str(row['team'] or 'None')
          filename = str(row['filename'] or 'None')
          btd_issues.append(
            f"Row ID {row.get_id()}: Duplicate video_id '{video_id}' (found in Row IDs {', '.join(row_ids)}) "
            f"(league={league}, gender={gender}, year={year}, team={team}, filename={filename})"
          )

    if btd_issues:
      issues.append("<h3>btd_files Issues</h3><ul>" + "".join(f"<li>{issue}</li>" for issue in btd_issues) + "</ul>")

      # Check master_player for single-digit number and near-duplicates (NCAA only)
    mp_issues = []
    mp_rows = list(app_tables.master_player.search())
    groups = {}
    for row in mp_rows:
      key = (
        str(row['league'] or 'None'),
        str(row['gender'] or 'None'),
        row['year'] if row['year'] is not None else -1,
        str(row['team'] or 'None')
      )
      groups.setdefault(key, []).append(row)

    for key, rows in groups.items():
      league, gender, year, team = key
      for row in rows:
        number = row['number']
        row_id = row.get_id()
        # Single-digit number check (all leagues)
        if number and number.isdigit() and len(number) == 1:
          mp_issues.append(
            f"Row ID {row_id}: Single-digit number '{number}' without leading zero "
            f"(league={league}, gender={gender}, year={year}, team={team})"
          )
          # Near-duplicate number check (league='NCAA' only)
        if league == 'NCAA':
          for other_row in rows:
            if row is other_row:
              continue
            other_number = other_row['number']
            if number and other_number and number.lstrip('0') == other_number.lstrip('0'):
              mp_issues.append(
                f"Row ID {row_id} and {other_row.get_id()}: Near-duplicate numbers '{number}' and '{other_number}' "
                f"(league={league}, gender={gender}, year={year}, team={team})"
              )

    if mp_issues:
      issues.append("<h3>master_player Issues</h3><ul>" + "".join(f"<li>{issue}</li>" for issue in mp_issues) + "</ul>")

      # Prepare email content
    email_body = (
      f"<h2>Nightly Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')}</h2>"
      f"<p>Found the following inconsistencies in the database:</p>{''.join(issues)}"
      f"<p>Please review these issues and update the data as needed.</p>"
    ) if issues else (
      f"<h2>Nightly Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')}</h2>"
      f"<p>No inconsistencies found in btd_files or master_player tables.</p>"
    )

    # Send email
    anvil.email.send(
      to="beachinternals@gmail.com",
      subject=f"Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')}",
      html=email_body
    )
    log_info("Data consistency check completed and email sent")

  except Exception as e:
    log_error(f"Error in check_inconsistent_data: {str(e)}")
    anvil.email.send(
      to="beachinternals@gmail.com",
      subject=f"Data Consistency Check Error - {datetime.now().strftime('%Y-%m-%d')}",
      html=f"<h2>Error in Nightly Data Consistency Check</h2><p>An error occurred: {str(e)}</p>"
    )

@anvil.server.callable
def trigger_nightly_check():
  """
    Callable function to launch the background task (for testing or manual triggering).
    """
  anvil.server.launch_background_task('check_inconsistent_data')
  return {"status": "Nightly check triggered"}
  
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import datetime from datetime

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

# functions to update / rebuild all the data files

@anvil.server.callable
def call_build_data():
  task = anvil.server.launch_background_task('build_data1')
  return task

@anvil.server.callable
def call_rebuild_all_data():
  task = anvil.server.launch_background_task('rebuild_all_data1')
  return task


@anvil.server.background_task
def build_data1():
  # this just call the background task with the 
  task = calculate_all_data(False)

  return task

@anvil.server.background_task
def rebuild_all_data1():
  task = calculate_all_data(True)
  return task

@anvil.server.callable
def calculate_all_data(rebuild):
  email_text = "Calculate All Data /n Called at:" + str(datetime.now()) + "/n"
  
  # build data for all new files, for all leagues

  # do the btd -> ppr conversion for all btf files
  btd_df = pd.dataframe(c('league'),c('gender'),c('year'),c('team'))
  i = 0
  for btd_file_r in app_tables.btd_files():
    # make a quick df of the values needed
    btd_df.at[i,'league'] = btd_file_r['league']
    btd_df.at[i,'gender'] = btd_file_r['gender']
    btd_df.at[i,'year'] = btd_file_r['year']
    btd_df.at[i,'team'] = btd_file_r['team']
    i = i + 1

  # now we need to make this unique  
  print(btd_df)
  file_list = pd.unique(btd_df['league']+btd_df['gender']+btd_df['year']+btd_df['team'])
  print(file_list)
    
  
  # get a list of all leagues, unique sets of league, gender, year, and league in the btd file db, then do the merge to store in ppr_csv db

  # for each in the league, call the ppr merge fucntion

  # get a list of all ppr_csv with team of league, then call to calcualte player data, pair data, and tri data
  

  return True




import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

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
def calc_exec_summary( disp_league, disp_gender, disp_year ):

  # launch this as a background task
  task = anvil.server.launch_background_task('make_exec_summary',disp_league, disp_gender, disp_year)
  return task


@anvil.server.background_task
def make_exec_summary(disp_league, disp_gender, disp_year):

  # this routine is designed to create the executive summary, adn store it somewhere :=)

    
  # open the player_data file for this criteria
  c_team = "League"
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = c_team
      ) )

  if ppr_csv_row:
    ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
  else:
    #print('No Rows Found')
    return ["No Rows"]
    
  # open the exec summary criteria file

  # loop thru each row in player data

    # initialize the text string to hold the report
  
    # loop thru each row in the criteria file

      # write the comment, if there is one

    # store the player's executive summary
  
  
  return True
s_wimport anvil.email
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

#-----------------------------------------------------------------------------#
#
#             Calculate the strenghts and weaknesses report
#
#
#-----------------------------------------------------------------------------#

@anvil.server.callable
def calc_s_w_player( c_league, c_gender, c_year, c_player ):
  #
  # calculate the strengths and weaknesses dataframe based on:
  # - player_data 
  # - s_w_crit_player table
  #
  # Look for any values that meet the criteria, write a line to the dataframe
  #
  # then store the dataframe in the master_player file

  # open player data, create a data frame from it
  
  
  # open the player stats file for the criteria numbers

  # create the dataframe for the s_w to be stored
  s_w_dict = {'Player':[''],
             'Category':[''], # Category - Strength, Weakness, Of Note
             'Section':[''], # Offense, Defense, Other ...
             'Descripton':[''], # Description of the event
             'Var Name':[''], # Name of Variable
             'Var Desc':[''], # Description of the variable
             'Var Value':[0], # Value of the variable
              'Var Percentile':[0] # Percentile of this variable
              'Criteria':[0], # creitria, +/-1 stdev?
              'Criteria Value':[0]
            }

  s_w_df = pd_DataFrame.from_dict(s_w_dict)
  
  # loop thru the player file

    # loop thru the criteria file

    # save the dataframe into s_w in master_player

    # next player

  # i think we are all done!

  return True
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

#------------------------------------------------------------
#
#        Pair Functions 
#
#-------------------------------------------------------------
def pair_filter(ppr_df, player1, player2):
  # filter a ppr dataframe down to all plays with the pair palyer 1 and 2

  # two possible 'teams' for this pair
  team1 = player1 + player2
  team2 = player2 + player1
  
  tmp_df = ppr_df[ (ppr_df['teama'] == team1) |
                   (ppr_df['teama'] == team2) |
                   (ppr_df['teamb'] == team1) |
                   (ppr_df['teamb'] == team2 ) ]

  return tmp_df


  
  
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
def pair_filter(ppr_df, disp_pair):
  # filter a ppr dataframe down to all plays with the pair palyer 1 and 2
  disp_pair = disp_pair.strip()
  #print(f" Pair: {disp_pair}")
  #print("ppr df team a column, then team b")
  #print(ppr_df['teama'])
  #print(ppr_df['teamb'])
  
  tmp_df = ppr_df[ (ppr_df['teama'].str.strip() == disp_pair) |
                   (ppr_df['teamb'].str.strip() == disp_pair) 
                  ]
  return tmp_df

def pair_players(disp_pair):
  # search the master pair table to find the two players
  disp_pair = disp_pair.strip()
  for pair_row in app_tables.master_pair.search(pair=disp_pair):
    disp_player1 = pair_row['player1']
    disp_player2 = pair_row['player2']
  return disp_player1, disp_player2


  
  
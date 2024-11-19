import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import io

# This is a server module. It runs on the Anvil server,

@anvil.server.callable
def fbhe_histogram(league_value ):
  # given all this, look into the player_data csv stored in the csv_list datafile, then graph the FBHE Histogram

  # unpack the league data
  str_loc = league_value.index('|')
  disp_league = league_value[:str_loc-1].strip()
  league_value = league_value[str_loc+1:]
  str_loc = league_value.index('|')
  disp_gender = league_value[:str_loc-1].strip()
  disp_year = league_value[str_loc+1:].strip()
  
  # pull out the player_data csv file
  disp_team = "League"    # only updating hte league tables
  #print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  if ppr_csv_row:
    player_data_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data'].get_bytes()))
    #player_stats_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
  else:
    #print('No Rows Found')
    return ["No Rows"]
  hist_array = player_data_df['fbhe']
  #print(f"fhbe_hist: Histogram Array: {hist_array}")
  hist_array = hist_array.replace(np.nan, 0) 
  #print(f"fhbe_hist: Histogram Array, after notnull: {hist_array}")
  
  # plot it
  histogram = np.histogram(hist_array)
  #print("returned from np.histogram")
  #print(histogram)
  
  # return it
  return histogram

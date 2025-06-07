import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import io
import math
import statistics
import anvil.mpl_util
from server_functions import *

# This is a server module. It runs on the Anvil server,

@anvil.server.callable
def plot_histogram(league_value, plot_var, var_name ):
  # given all this, look into the player_data csv stored in the csv_list datafile, then graph the FBHE Histogram

  # unpack the league data, and fetch player data
  disp_league, disp_gender, disp_year = unpack_lgy( league_value)
  player_data_df, player_stats_df = get_player_data( disp_league, disp_gender, disp_year)

  # calculate percentiles
  plot_var_10 = player_data_df[plot_var].quantile(0.10)
  plot_var_25 = player_data_df[plot_var].quantile(0.25)
  plot_var_50 = player_data_df[plot_var].quantile(0.50)
  plot_var_75 = player_data_df[plot_var].quantile(0.75)
  plot_var_90 = player_data_df[plot_var].quantile(0.90)

  # build a markdown text of this to return
  stat_text = f"""


  ## {var_name} ##
  10th Percentile : {float("{:.3f}".format(plot_var_10))}
  25th Percentile : {float("{:.3f}".format(plot_var_25))}
  50th Percentile : {float("{:.3f}".format(plot_var_50))}
  75th Percentile : {float("{:.3f}".format(plot_var_75))}
  90th Percentile : {float("{:.3f}".format(plot_var_90))}
  
  """

  #now, let's create the plot using pandas.plot
  player_data_df[plot_var].plot( kind = 'hist', title = 'Histogram of '+var_name, xlabel = plot_var, bins = 25, figsize=(10,6))

  return stat_text, anvil.mpl_util.plot_image()
  
  


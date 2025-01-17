import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.mpl_util
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#import plotly.graph_objects as go

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-----------------------------------------------
#
#.   Plot Fucntions 
#
#----------------------------------------------

@anvil.server.callable
def plot_serve_src_dest( ppr_df):
  #
  # line drawing of the serve from source to destination
  #

  # we want a line drawing with on line for each serve, or each instance.
  print(f"plot_serve_src_dest: ppr_df size: {ppr_df.shape[0]}")
  #print(f"plot_serve_src_dest: ppr_df: {ppr_df}")

  # Plot it in the normal Matplotlib way
  plt.figure(1, figsize=(10,10))
  
  for index, ppr_r in ppr_df.iterrows():
    plt.plot( [ppr_r['serve_src_x'], ppr_r['serve_dest_x']], [ ppr_r['serve_src_y'], ppr_r['serve_dest_y'] ], 'blue') 
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()


@anvil.server.callable
def plot_pass_dest( ppr_df ):
  #
  # line drawing of the pass location(s) as dot
  #

  # Plot it in the normal Matplotlib way
  plt.figure(1, figsize=(10,5))
  x = ppr_df['pass_dest_x'].dropna().values
  y = ppr_df['pass_dest_y'].dropna().values
  print(f"plot_pass_dest: {len(x)}, {len(y)} x and y: {x}, {y}")
  plt.scatter( x, y )  
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()


@anvil.server.callable
def plot_set_dest( ppr_df ):
  #
  # line drawing of the set location(s) as dot
  #
  plt.figure(1, figsize=(10,5))
  x = ppr_df['set_dest_x'].dropna().values
  y = ppr_df['set_dest_y'].dropna().values
  print(f"plot_set_dest: {len(x)}, {len(y)} x and y: {x}, {y}")
  plt.scatter( x, y )  
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()
  

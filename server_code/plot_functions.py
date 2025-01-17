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
def plot_lines_on_court( ppr_src_x, ppr_dest_x, ppr_src_y, ppr_dest_y, plt_num):
  #
  # line drawing of the serve from source to destination
  #

  # we want a line drawing with on line for each serve, or each instance.

  # Plot it in the normal Matplotlib way
  plt.figure(plt_num, figsize=(10,20))
  
  for index, ppr_r in ppr_src_x.iterrows():
    plt.plot( ppr_src_x.at[index], ppr_dest_x.at[index], ppr_src_y.at[index], ppr_dest_y.at[index], 'blue') 

  plot_court_background()
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()

@anvil.server.callable
def plot_points_on_the_court( ppr_x,ppr_y, plt_num ):
  #
  # line drawing of the set location(s) as dot
  #
  plt.figure(plt_num, figsize=(10,20))
  x = ppr_x.dropna().values
  y = ppr_y.dropna().values
  #print(f"plot_set_dest: {len(x)}, {len(y)} x and y: {x}, {y}")
  point_size = np.full(len(x),10) # numpy array of size len(x), filled with character 2
  print(f"plot_points_on_the_court: size array: {point_size}")
  plt.scatter( x, y, s = point_size )  
  plot_court_background()
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()

def plot_court_background():
  xpts = np.array([0,8,8,0,0,0,8])
  ypts = np.array([-8,-8,8,8,-8,0,0])
  plt.plot( xpts, ypts, c = 'black', linewidth = '3')
  plt.grid()
  return True

def plot_attack_zones( ppr_df, disp_player, plt_num):
  # working to plot 6 across, zones 1 - 5, then option based on player as passer

  # set up the 6 plots, start on plot 1
  for i in [1,2,3,4,5]:
    plt.subplot(1,6,i) # 1 row, 6 across, working on plot 1
    plot_court_background()
    tmp_df = ppr_df[ (ppr_df['pass_player'] == disp_player) & 
                     (ppr_df['att_src_zone_net'] == i) & 
                     (ppr_df['tactic'] != 'option') ]
    plot_lines_on_court( tmp_df['att_src_x'],tmp_df['att_dest_x'],tmp_df['att_src_y'],tmp_df['att_dest_y'],i)

  # now plot the options
  plt.subplot(1,6,6) # 1 row, 6 across, working on plot 1
  plot_court_background()
  tmp_df = ppr_df[ (ppr_df['pass_player'] == disp_player) & 
                   (ppr_df['tactic'] == 'option') ]
  plot_lines_on_court( tmp_df['att_src_x'],tmp_df['att_dest_x'],tmp_df['att_src_y'],tmp_df['att_dest_y'],6)  
                         
    
  
  
  

  

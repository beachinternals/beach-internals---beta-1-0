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
def plot_lines_on_court( ppr_df, action, plt_num):
  #
  # line drawing of the serve from source to destination
  #

  #. a little documentation ...
  #plot(x, y, color='green', marker='o', linestyle='dashed',
  #   linewidth=2, markersize=12)
  
  # we want a line drawing with on line for each serve, or each instance.

  err = ''
  kill = ''
  if action == 'srv':
    x1 = 'serve_src_x'
    x2 = 'serve_dest_x'
    y1 = 'serve_src_y'
    y2 = 'serve_dest_y'
    err = 'TSE'
    kill = 'TSA'
  elif action == 'pass':
    x1 = 'pass_src_x'
    x2 = 'pass_dest_x'
    y1 = 'pass_src_y'
    y2 = 'pass_dest_y'
  elif action == 'set':
    x1 = 'set_src_x'
    x2 = 'set_dest_x'
    y1 = 'set_src_y'
    y2 = 'set_dest_y'
  elif action == 'att':
    x1 = 'att_src_x'
    x2 = 'att_dest_x'
    y1 = 'att_src_y'
    y2 = 'att_dest_y'
    kill = 'FBK'
    err = 'FBE'
  else:
    print(f"plot_lines_on_court: Invalid action passed : {action}")
    
  # Plot it in the normal Matplotlib way
  plt.figure(plt_num, figsize=(10,20))
  
  for index, ppr_r in ppr_df.iterrows():
    if ppr_r['point_outcome'] == err:
      line_color = 'red'
      l_style='dashed'
      m_style ='X'
    if ppr_r['point_outcome'] == kill:
      line_color = 'green'
      l_style='solid'
      m_style ='o'
    else:
      line_color = 'blue'
      l_style = 'dotted'
      m_style = '.'
      
    plt.plot( [ppr_r[x1], ppr_r[x2]], [ppr_r[y1], ppr_r[y2]], line_color, l_style, m_style) 

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
  point_size = np.full(len(x),100) # numpy array of size len(x), filled with character 2
  print(f"plot_points_on_the_court: size array: {point_size}")
  plt.scatter( x, y, s = point_size )  
  plot_court_background()
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()

def plot_court_background():
  xpts = np.array([0,8,8,0,0,0])
  ypts = np.array([-8,-8,8,8,-8,0])
  plt.plot( xpts, ypts, c = 'black', linewidth = '3')
  xpts = np.array([-1,9])
  ypts = np.array([0,0])
  plt.plot( xpts, ypts, c = 'black', linewidth = '9')
  plt.grid()
  return True

def plot_attack_zones( ppr_df, plt_num):
  # working to plot 6 across, zones 1 - 5, then option based on player as passer

  # ppr_df should be limited to the pass player and the pass zone(s) desited
  # set up the 6 plots, start on plot 1

  # court boundries
  xpts = np.array([0,8,8,0,0,0,8])
  ypts = np.array([-8,-8,8,8,-8,0,0])

  fig, (pz1, pz2, pz3, pz4, pz5, popt) = plt.subplots(1,6, figsize = (100,30)) # 1 row, 6 across, working on plot 1
  fig.suptitle('Attacking Profile, Zone 1 -5 and Second Ball Option')

  # plot zone 1:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 1) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz1.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz1.set_title('Zone 1 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz1.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz1.grid()

  # plot zone 2:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 2) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz2.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz2.set_title('Zone 2 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz2.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz2.grid()
  
  # plot zone 3:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 3) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz3.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz3.set_title('Zone 3 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz3.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz3.grid()
  
  # plot zone 4:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 4) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz4.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz4.set_title('Zone 4 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz4.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz4.grid()
  
  # plot zone 5:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 5) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz5.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz5.set_title('Zone 5 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz5.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz5.grid()
  
  # plot Option:
  tmp_df = ppr_df [ (ppr_df['tactic'] == 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    popt.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  popt.set_title('On 2 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  popt.plot( xpts, ypts, c = 'black', linewidth = '3')
  popt.grid()
  
  pz1.plot()

  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()
                         
    
  
  
  

  

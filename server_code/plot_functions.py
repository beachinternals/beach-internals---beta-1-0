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
import matplotlib.patches as patches
import math
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
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  #plt.figure(plt_num, figsize=(10,18))
  
  for index, ppr_r in ppr_df.iterrows():
    line_color = 'blue'
    l_style = 'solid'
    m_style = '>'
    l_width = 2.5
    m_every = [-1] # to mark only the end point
    if ppr_r['point_outcome'] == err:
      line_color = 'red'
      l_style='solid'
      m_style ='X'
      l_width = 2.5
      m_every = [-1] # to mark only the end point
    if ppr_r['point_outcome'] == kill:
      line_color = 'green'
      l_style='solid'
      m_style ='o'
      l_width = 2.5
      m_every = [-1] # to mark only the end point

    # line direction, to calculate dx and dy for the arrow
    distance = math.dist([ppr_r[x1], ppr_r[y1]], [ppr_r[x2], ppr_r[y2]])
    if distance != 0:
      dx = ((ppr_r[x2] - ppr_r[x1])/distance)*0.1
      dy = ((ppr_r[y2] - ppr_r[y1])/distance)*0.1
    else:
      dx = 0
      dy = 0
    #print(f"plot_lines_on_court: x,y: {ppr_r[x2]}, {ppr_r[y2]}, Distance: {distance}, dx, dy: {dx}, {dy}")
    #print(f"Plotting points: x1,x2:{ppr_r[x1], ppr_r[x2]}, y1,y2:{ppr_r[y1], ppr_r[y2]}, outcome:{ppr_r['point_outcome']}, line color: {line_color}, line style {l_style}, marker = {m_style} ")
    ax.plot( [ppr_r[x1], ppr_r[x2]], [ppr_r[y1], ppr_r[y2]], c=line_color, linestyle=l_style, linewidth = l_width, markevery = m_every )
    if (ppr_r[x1] and ppr_r[y1]):
      ax.scatter(ppr_r[x1],ppr_r[y1], s = 50, c=line_color) # marker only at first point
    if (ppr_r[x2] and ppr_r[y2]):
      ax.arrow(ppr_r[x2], ppr_r[y2], dx, dy, shape='full', lw=2, length_includes_head=True, head_width=.10, head_length = .25, color = line_color)

  plot_court_background(fig,ax)
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()

@anvil.server.callable
def plot_points_on_the_court( ppr_x,ppr_y, plt_num, video_id, action_num, plot_ellispe, point_outcome ):
  # this enters with an array of x, y, and outcomes, but some x and y are na, so we need to drop them, but
  #
  # line drawing of the set location(s) as dot
  #
  fig, ax = plt.subplots(figsize=(10,18))
  #plt.figure(plt_num, figsize=(10,18))
  x = ppr_x.values
  y = ppr_y.values
  #print(f"plot_points_on_the_court : point_outcome: {point_outcome}")
  pt_color = point_outcome.dropna()
  #print(f"plot_points_on_the_court : pt_color: {pt_color}")
  vectorize_pt_color = np.vectorize(point_colors)
  #print(f"plot_points_on_the_court : vectorize pt color: {vectorize_pt_color}")
  plot_point_color = vectorize_pt_color(pt_color)
  #print(f"plot_points_on_the_court : plot_point_color: {plot_point_color}")
  #print(f"plot_set_dest: {len(x)}, {len(y)} x and y: {x}, {y}")
  point_size = np.full(len(x),75) # numpy array of size len(x), filled with character 2
  #print(f"plot_points_on_the_court: size array: {point_size}")
  ax.scatter( x, y, s = point_size , c = plot_point_color, url = 'http://app.balltime.com/video/'+video_id+'?actionIds='+str(action_num))  

  if plot_ellispe:
    # calcualte elispe information
    print(f"Converting x and y: Type: {type(x)}, {type(y)}, Number of Points: {len(x)}, {len(y)}")
    el_points = pd.concat( [ppr_x, ppr_y], axis = 1)
    print(f" el_points {el_points}")
    el_points = el_points.dropna().values
    el_mean, el_width, el_height, el_angle =  calculate_standard_deviation_ellipse(el_points, confidence=1.0)
    print(f" Ellispe details: mean: {el_mean[0]}, {el_mean[1]} width: {el_width}, height : {el_height}, angle: {el_angle}")
    xy_center = (el_mean[0],el_mean[1])
    ellipse = patches.Ellipse(xy = xy_center, width = el_width, height = el_height, angle = el_angle, edgecolor='b', facecolor='blue', linewidth=2, label="1 Std Dev Ellipse", alpha=0.05)
    ax.add_patch(ellipse)
    
  plot_court_background(fig,ax)
  
  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()

#
# a quick functin to create an array of colors when plotting points
def point_colors(pt_outcome):
  match pt_outcome:
    case 'FBK':
      color = 'g'
    case 'TSA':
      color = 'g'
    case 'FBE':
      color = 'r'
    case 'TSE':
      color = 'r'
    case _:
      color = 'b' 
  return color

  
def plot_court_background(fig,ax):
  plt.xlim( -1, 9)
  plt.ylim( -9, 9)
  xpts = np.array([0,8,8,0,0,0])
  ypts = np.array([-8,-8,8,8,-8,0])
  ax.plot( xpts, ypts, c = 'black', linewidth = '3')
  xpts = np.array([-1,9])
  ypts = np.array([0,0])
  ax.plot( xpts, ypts, c = 'black', linewidth = '9')
  ax.grid()
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
  pz1.xlim( -1, 9)
  pz1.ylim( -9, 9)
  pz1.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz1.grid()

  # plot zone 2:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 2) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz2.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz2.set_title('Zone 2 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz2.xlim( -1, 9)
  pz2.ylim( -9, 9)
  pz2.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz2.grid()
  
  # plot zone 3:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 3) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz3.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz3.set_title('Zone 3 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz3.xlim( -1, 9)
  pz3.ylim( -9, 9)
  pz3.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz3.grid()
  
  # plot zone 4:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 4) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz4.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz4.set_title('Zone 4 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz4.xlim( -1, 9)
  pz4.ylim( -9, 9)
  pz4.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz4.grid()
  
  # plot zone 5:
  tmp_df = ppr_df [ (ppr_df['att_src_zone_net'] == 5) & (ppr_df['tactic'] != 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    pz5.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  pz5.set_title('Zone 5 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  pz5.xlim( -1, 9)
  pz5.ylim( -9, 9)
  pz5.plot( xpts, ypts, c = 'black', linewidth = '3')
  pz5.grid()
  
  # plot Option:
  tmp_df = ppr_df [ (ppr_df['tactic'] == 'option') ]
  for index,ppr_r in tmp_df.iterrows():
    popt.plot([ppr_r['att_src_x'], ppr_r['att_dest_x']], [ppr_r['att_src_y'], ppr_r['att_dest_y']], 'blue')
  popt.set_title('On 2 Attacks',loc='center',fontstyle='oblique', fontsize='medium')
  popt.xlim( -1, 9)
  popt.ylim( -9, 9)
  popt.plot( xpts, ypts, c = 'black', linewidth = '3')
  popt.grid()
  
  pz1.plot()

  # Return this plot as a PNG image in a Media object
  return anvil.mpl_util.plot_image()
                         

def calculate_standard_deviation_ellipse(points, confidence=1.0):
   """
   Calculate and plot the standard deviation ellipse for a set of 2D points.

   Args:
       points (numpy.ndarray): A 2D array of shape (n, 2) where each row is a point [x, y].
       confidence (float): Scaling factor for the ellipse size (1.0 for 1 standard deviation).

   Returns:
       center (tuple): The center of the ellipse (mean of the points).
       width (float): The width of the ellipse (major axis length).
       height (float): The height of the ellipse (minor axis length).
       angle (float): The rotation angle of the ellipse in degrees.
   """
   # Compute the mean of the points
   mean = np.mean(points, axis=0)

   # Calculate the covariance matrix
   cov_matrix = np.cov(points, rowvar=False)

   # Eigen decomposition
   eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)

   # Sort eigenvalues and eigenvectors
   order = eigenvalues.argsort()[::-1]
   eigenvalues = eigenvalues[order]
   eigenvectors = eigenvectors[:, order]

   # Compute the ellipse parameters
   width = 2 * confidence * np.sqrt(eigenvalues[0])  # Major axis
   height = 2 * confidence * np.sqrt(eigenvalues[1])  # Minor axis
   angle = np.degrees(np.arctan2(*eigenvectors[:, 0][::-1]))

   return mean, width, height, angle



def calculate_ellipse_area(width, height):
   """
   Calculate the area of an ellipse.

   Args:
       width (float): The full width (major axis) of the ellipse.
       height (float): The full height (minor axis) of the ellipse.

   Returns:
       float: The area of the ellipse.
   """
   return math.pi * (width / 2) * (height / 2)


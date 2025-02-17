import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from server_functions import *
from pair_functions import *
import pandas as pd
from plot_functions import *
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-------------------------------------------------------
#
#       Scouting Report Functions
#
#------------------------------------------------------

# Scouting Report on a single serving strategy (whatever to and from are selected)
@anvil.server.callable
def scout_srv_strategy(disp_league,
                      disp_gender,
                      disp_year,
                      disp_team,
                      disp_pair,
                      disp_player,
                      comp_l1_checked,
                      disp_comp_l1,
                      comp_l2_checked,
                      disp_comp_l2,
                      comp_l3_checked,
                      disp_comp_l3,
                      date_checked,
                      disp_start_date,
                      disp_end_date,
                      scout,
                      explain_text, title_text,
                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5
  ):

  # try to build to scouting report - Serving Strategy
  player_label = disp_player

  # lets find the serve strategy Text:
  srv_2_net_txt = ['','Line','Body(l)','Seam','Body(r)','Line']
  if (srv_to_1[0]) or (srv_to_1[1]) or (srv_to_1[2]) :
    srv_line_l = True 
  else:
    srv_line_l = False
  if (srv_to_5[0]) or (srv_to_5[1]) or (srv_to_5[2]):
    srv_line_r = True
  else:
    srv_line_r = False
  if (srv_to_2[0]) or (srv_to_2[1]) or (srv_to_2[2]):
    srv_body_l = True 
  else:
    srv_body_l = False
  if (srv_to_4[0]) or (srv_to_4[1]) or (srv_to_4[2]):
    srv_body_r = True
  else:
    srv_body_r = False
  if (srv_to_3[0]) or (srv_to_3[1]) or (srv_to_3[2]):
    srv_seam = True 
  else:
    srv_seam = False

  srv_deep =  True if (srv_to_1[0]) or (srv_to_2[0]) or (srv_to_3[0]) or (srv_to_4[0]) or (srv_to_5[0]) else False
  srv_mid =   True if (srv_to_1[1]) or (srv_to_2[1]) or (srv_to_3[1]) or (srv_to_4[1]) or (srv_to_5[1]) else False
  srv_short = True if (srv_to_1[2]) or (srv_to_2[2]) or (srv_to_3[2]) or (srv_to_4[2]) or (srv_to_5[2]) else False

  srv_to_text = ''
  if srv_line_r:
    srv_to_text = 'Line(r):'
    if srv_short:
      srv_to_text = srv_to_text + 'Short,'
    if srv_mid:
      srv_to_text = srv_to_text + 'Mid,'
    if srv_deep:
      srv_to_text = srv_to_text + 'Deep,'
  if srv_body_r:
    srv_to_text = srv_to_text + ' Body(r):'
    if srv_short:
      srv_to_text = srv_to_text + 'Short,'
    if srv_mid:
      srv_to_text = srv_to_text + 'Mid,'
    if srv_deep:
      srv_to_text = srv_to_text + 'Deep,'
  if srv_seam:
    srv_to_text = srv_to_text + ' Seam:'
    if srv_short:
      srv_to_text = srv_to_text + 'Short,'
    if srv_mid:
      srv_to_text = srv_to_text + 'Mid,'
    if srv_deep:
      srv_to_text = srv_to_text + 'Deep,'
  if srv_body_l:
    srv_to_text = srv_to_text + ' Body(l):'
    if srv_short:
      srv_to_text = srv_to_text + 'Short,'
    if srv_mid:
      srv_to_text = srv_to_text + 'Mid,'
    if srv_deep:
      srv_to_text = srv_to_text + 'Deep,'
  if srv_line_l:
    srv_to_text = srv_to_text + ' Line(l):'
    if srv_short:
      srv_to_text = srv_to_text + 'Short,'
    if srv_mid:
      srv_to_text = srv_to_text + 'Mid,'
    if srv_deep:
      srv_to_text = srv_to_text + 'Deep,'
      
  srv_fr_text = ''
  srv_fr_txt = ['Line (left)','Middle','Line (right)'] 
  if srv_fr[0]:
    srv_fr_text = 'Line (Left)'
  if srv_fr[1]:
    srv_fr_text = srv_fr_text + ' & Middle' if len(srv_fr_text) != 0 else 'Middle'
  if srv_fr[2]:
    srv_fr_text = srv_fr_text + ' & Line (Right)' if len(srv_fr_text) != 0 else 'Line (Right)'
  #print(f"scout_srv_strategy:  srv_fr : {srv_fr}")

  srv_strategy_title = 'Strategy when Serving to '+disp_player+' : Serving from '+srv_fr_text+' to '+srv_to_text
  #print(f"Serving Strategy: {srv_strategy_title}")
  
  # make a list of tuples for the serve to zones
  srv_2 = []
  srv_2_txt = []
  #print(f"Serve To 1:{srv_to_1}")
  #print(f"Serve To 2:{srv_to_2}")
  #print(f"Serve To 3:{srv_to_3}")
  #print(f"Serve To 4:{srv_to_4}")
  #print(f"Serve To 5:{srv_to_5}")
  depth_list = ['E','D','C']
  depth_text = ['Deep','Mid','Short']
  for j in (0,1,2):
    if srv_to_1[j]:
      srv_2.append([1,depth_list[j]])
      srv_2_txt.append([1,depth_text[j]])
    if srv_to_2[j]:
      srv_2.append([2,depth_list[j]])
      srv_2_txt.append([2,depth_text[j]])
    if srv_to_3[j]:
      srv_2.append([3,depth_list[j]])
      srv_2_txt.append([3,depth_text[j]])
    if srv_to_4[j]:
      srv_2.append([4,depth_list[j]])
      srv_2_txt.append([4,depth_text[j]])
    if srv_to_5[j]:
      srv_2.append([5,depth_list[j]])
      srv_2_txt.append([5,depth_text[j]])
  # let's see what we have
  #print(f"scout_srv_strategy: svr to list of tuples {srv_2}")
  # this list should now have as many tuples as points selected.  First number is 1 - 5 for net zones, second number is depth: 0=E, 1=D, 2+A,B,C
  
  # get the ppr data
  #print(f"scout_srv_strategy: league: {disp_league}, gender: {disp_gender}, year: {disp_year}, team: {disp_team}")
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  #print(f"ppr_df all:{ppr_df.shape[0]}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  #print(f"scout_srv_strategy: ppr_df date and comp lmited:{ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest
  #print(f"scout_srv_strategy: ppr_df pair lmited:{ppr_df.shape[0]}")

  # lastly, lmit this by the source and desitnation locations
  ppr_df = ppr_df[ ((ppr_df['serve_src_zone_net'] == 1) & (srv_fr[0])) | 
                   ((ppr_df['serve_src_zone_net'] == 3) & (srv_fr[1])) |
                   ((ppr_df['serve_src_zone_net'] == 5) & (srv_fr[2]))
  ]
  #print(f"scout_srv_strategy: ppr_df lmited srv from zones:{ppr_df.shape[0]}")

  # now start a loop of the number of desitnation tuples (srv2[])
  first_zone = True
  for i in range(0,len(srv_2),1):
    #print(f" i:{i}, srv_2[i,0] {srv_2[i][0]}, srv_2[i,1] {srv_2[i][1]}")
    tmp_df = ppr_df[ (ppr_df['serve_dest_zone_net'] == srv_2[i][0]) & (ppr_df['serve_dest_zone_depth'] == srv_2[i][1]) ]
    #print(f"Number of rows in Filter db by serve dest: {tmp_df.shape[0]}")
    if not first_zone:
      new_ppr = pd.concat([new_ppr,tmp_df])
    else:
      new_ppr = tmp_df
      first_zone = False
     
  #print(f"Number of final db to analze: {new_ppr.shape[0]}")
  
  # calculate a quick table FBHE
  fbhe_vector = fbhe(new_ppr, disp_player, 'att',True)
  oos_vector =count_out_of_system(new_ppr, disp_player, 'att')
  #print(f"fbhe Vector: {fbhe_vector}")
  srv_strat_dict = {'From':[0],
                     'To':[0],
                    'Attempts':[0],
                    'FBSO':[0],
                    'FBHE':[0],
                    'Out of Sys':[0],
                    'URL':[0]
                   }
  srv_strat_df = pd.DataFrame.from_dict(srv_strat_dict)
  srv_strat_df.at[0,'From'] = 'All'
  srv_strat_df.at[0,'To'] = 'All'
  srv_strat_df.at[0,'Attempts'] = fbhe_vector[3]
  srv_strat_df.at[0,'FBSO'] = fbhe_vector[4]
  srv_strat_df.at[0,'FBHE'] = fbhe_vector[0]
  srv_strat_df.at[0,'Out of Sys'] = oos_vector[0] if oos_vector[0] else 0
  srv_strat_df.at[0,'URL'] = fbhe_vector[5]  

  # now a loop over the different serving options:
  rows = 1
  for i in [0,1,2]:
    srv_src = i*2 + 1
    if srv_fr[i]:
      for j in range(0,len(srv_2),1):
        rows = rows+1
        fbhe_vector = fbhe( (new_ppr[( new_ppr['serve_src_zone_net'] == srv_src) & 
                              ( new_ppr['serve_dest_zone_net'] == srv_2[j][0] ) & 
                              ( new_ppr['serve_dest_zone_depth'] == srv_2[j][1]) ]),
                              disp_player,
                              'att',
                              True 
                            )
        oos_vector = count_out_of_system( (new_ppr[( new_ppr['serve_src_zone_net'] == srv_src) & 
                              ( new_ppr['serve_dest_zone_net'] == srv_2[j][0] ) & 
                              ( new_ppr['serve_dest_zone_depth'] == srv_2[j][1]) ]),
                              disp_player,
                              'att'
                                       )
        srv_strat_df.at[rows,'From'] = srv_fr_txt[i]
        srv_strat_df.at[rows,'To'] = str(srv_2_net_txt[srv_2_txt[j][0]]) + " " + str(srv_2_txt[j][1])
        srv_strat_df.at[rows,'Attempts'] = fbhe_vector[3]
        srv_strat_df.at[rows,'FBSO'] = fbhe_vector[4]
        srv_strat_df.at[rows,'FBHE'] = fbhe_vector[0]
        srv_strat_df.at[rows,'Out of Sys'] = oos_vector[0]
        srv_strat_df.at[rows,'URL'] = fbhe_vector[5]  
                                  
  #print(f"Srv Strat DF: {srv_strat_df}")
  # fo rnow, I want to drop the 'All' first row (the .iloc)
  srv_strat_md = pd.DataFrame.to_markdown(srv_strat_df.iloc[1:], index=False)

  # now, time to make plots.
  # want to plot data from new_ppr

  # limit the data to passes by the player
  new_ppr = new_ppr[ new_ppr['pass_player'] == disp_player]

  # make a plot to chart the serves: (line chart, court in the background)
  serve_diagram_plot_object = plot_lines_on_court(new_ppr, 'srv', 1)
  #serve_diagram_plot_object = ''

  # make a plot to chart the pass locations: (dot splatter with half court in background)
  pass_locations_plot_object = plot_points_on_the_court(new_ppr['pass_dest_x'],new_ppr['pass_dest_y'], 2, new_ppr['video_id'], new_ppr['pass_action_id'],True,new_ppr['point_outcome'])
  #pass_locations_plot_object = ''

  # make a plot to chart the set locations: (dot splatter with half court in background)
  set_locations_plot_object = plot_points_on_the_court(new_ppr['set_dest_x'],new_ppr['set_dest_y'], 3, new_ppr['video_id'], new_ppr['set_action_id'],False,new_ppr['point_outcome'])
  #set_locations_plot_object = ''

  # Next, build the 6 plots across the page as subplots, zone's 1 - 5 plus Optioon

  # set up 6 sub plots
  #print("scout_srv_strategy, Plotting Zone 1 Attempts")
  attack_z1_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')],'att',4)  
  #print("scout_srv_strategy, Plotting Zone 2 Attempts")  
  attack_z2_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')],'att',5)
  #print("scout_srv_strategy, Plotting Zone 3 Attempts")
  attack_z3_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')],'att',6)
  #print("scout_srv_strategy, Plotting Zone 4 Attempts")
  attack_z4_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')],'att',7)
  #print("scout_srv_strategy, Plotting Zone 5 Attempts")
  attack_z5_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')],'att',8)
  #print("scout_srv_strategy, Plotting Option Attempts")
  attack_opt_plot_object = plot_lines_on_court(new_ppr[ new_ppr['tactic'] == 'option'],'att',9)

  # set up 6 small tables below with:
  #. 0 = FBHE
  #. 1 = FBSO
  #  2 = Attempts
  #. 3 = Out of System
  #. 4 = URL

  zone_dict = {'1':['FBHE','FBSO','ATT','URL'],'Value':[0,0,0,'']}
  z1_df = pd.DataFrame.from_dict(zone_dict)
  z2_df = pd.DataFrame.from_dict(zone_dict)
  z3_df = pd.DataFrame.from_dict(zone_dict)
  z4_df = pd.DataFrame.from_dict(zone_dict)
  z5_df = pd.DataFrame.from_dict(zone_dict)
  opt_df = pd.DataFrame.from_dict(zone_dict)

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z1_df.at[0,'Value'] = fbhe_vector[0]
  z1_df.at[1,'Value'] = fbhe_vector[4]
  z1_df.at[2,'Value'] = fbhe_vector[3]
  z1_df.at[3,'Value'] = fbhe_vector[5]
  #z1_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z2_df.at[0,'Value'] = fbhe_vector[0]
  z2_df.at[1,'Value'] = fbhe_vector[4]
  z2_df.at[2,'Value'] = fbhe_vector[3]
  z2_df.at[3,'Value'] = fbhe_vector[5]
  #z2_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z3_df.at[0,'Value'] = fbhe_vector[0]
  z3_df.at[1,'Value'] = fbhe_vector[4]
  z3_df.at[2,'Value'] = fbhe_vector[3]
  z3_df.at[3,'Value'] = fbhe_vector[5]
  #z3_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z4_df.at[0,'Value'] = fbhe_vector[0]
  z4_df.at[1,'Value'] = fbhe_vector[4]
  z4_df.at[2,'Value'] = fbhe_vector[3]
  z4_df.at[3,'Value'] = fbhe_vector[5]
  #z4_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z5_df.at[0,'Value'] = fbhe_vector[0]
  z5_df.at[1,'Value'] = fbhe_vector[4]
  z5_df.at[2,'Value'] = fbhe_vector[3]
  z5_df.at[3,'Value'] = fbhe_vector[5]
  #z5_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['tactic'] == 'option')], disp_player, 'pass', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['tactic'] == 'option')], disp_player, 'pass' )
  opt_df.at[0,'Value'] = fbhe_vector[0]
  opt_df.at[1,'Value'] = fbhe_vector[4]
  opt_df.at[2,'Value'] = fbhe_vector[3]
  opt_df.at[3,'Value'] = fbhe_vector[5]
  #opt_df.at[3,'Value'] = oos_vector[0]

  z1_mkdn = pd.DataFrame.to_markdown(z1_df, index=False, headers=['',''] )
  z2_mkdn = pd.DataFrame.to_markdown(z2_df, index=False, headers=['',''])
  z3_mkdn = pd.DataFrame.to_markdown(z3_df, index=False, headers=['',''])
  z4_mkdn = pd.DataFrame.to_markdown(z4_df, index=False, headers=['',''])
  z5_mkdn = pd.DataFrame.to_markdown(z5_df, index=False, headers=['',''])
  opt_mkdn = pd.DataFrame.to_markdown(opt_df, index=False, headers=['',''])
  

  
  return srv_strategy_title, srv_strat_md, serve_diagram_plot_object, pass_locations_plot_object, set_locations_plot_object, attack_z1_plot_object, attack_z2_plot_object, attack_z3_plot_object, attack_z4_plot_object, attack_z5_plot_object, attack_opt_plot_object, z1_mkdn, z2_mkdn,z3_mkdn,z4_mkdn,z5_mkdn,opt_mkdn

#---------------------------------------------------------------------------------------
#
# Scouting Report on a Player's Serving
#
#---------------------------------------------------------------------------------------
@anvil.server.callable
def scout_players_serve(disp_league,
                      disp_gender,
                      disp_year,
                      disp_team,
                      disp_pair,
                      disp_player,
                      comp_l1_checked,
                      disp_comp_l1,
                      comp_l2_checked,
                      disp_comp_l2,
                      comp_l3_checked,
                      disp_comp_l3,
                      date_checked,
                      disp_start_date,
                      disp_end_date,
                      scout,
                      explain_text, title_text,
                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5
  ):

  # try to build to scouting report - Serving Strategy
  player_label = disp_player

  # check logical limit, need at least one srv_fr
  if not (srv_fr[0] or srv_fr[1] or srv_fr[2]):
    # so at least one serve zone is true.  now we need atleast one srv_to zone to be true
    if not ( (srv_to_1[0] or srv_to_1[1] or srv_to_1[2]) or (srv_to_2[0] or srv_to_2[1] or srv_to_2[2]) or (srv_to_3[0] or srv_to_3[1] or srv_to_3[2]) or (srv_to_4[0] or srv_to_4[1] or srv_to_4[2]) or (srv_to_5[0] or srv_to_5[1] or srv_to_5[2]) ):
      # then we need to exti, no valid serve path
      return 'No Valid Serve Path Provided'

    
  # lets find the serve strategy Text:
  srv_2_net_txt = ['','Line','Body','Seam','Body','Line']
  if (srv_to_1[0]) or (srv_to_1[1]) or (srv_to_1[2]) or (srv_to_5[0]) or (srv_to_5[1]) or (srv_to_5[2]):
    srv_line = True 
  else:
    srv_line = False
  if (srv_to_2[0]) or (srv_to_2[1]) or (srv_to_2[2]) or (srv_to_4[0]) or (srv_to_4[1]) or (srv_to_4[2]):
    srv_body = True 
  else:
    srv_body = False
  if (srv_to_3[0]) or (srv_to_3[1]) or (srv_to_3[2]):
    srv_seam = True 
  else:
    srv_seam = False

  srv_deep =  True if (srv_to_1[0]) or (srv_to_2[0]) or (srv_to_3[0]) or (srv_to_4[0]) or (srv_to_5[0]) else False
  srv_mid =   True if (srv_to_1[1]) or (srv_to_2[1]) or (srv_to_3[1]) or (srv_to_4[1]) or (srv_to_5[1]) else False
  srv_short = True if (srv_to_1[2]) or (srv_to_2[2]) or (srv_to_3[2]) or (srv_to_4[2]) or (srv_to_5[2]) else False

  srv_to_text = ''
  if srv_short:
    srv_to_text = 'Short:'
    if srv_line:
      srv_to_text = srv_to_text + ' Line,'
    if srv_body:
      srv_to_text = srv_to_text + ' Body,'
    if srv_seam:
      srv_to_text = srv_to_text + ' Seam,'
      
  if srv_mid:
    srv_to_text = srv_to_text + ' Mid:'
    if srv_line:
      srv_to_text = srv_to_text + ' Line,'
    if srv_body:
      srv_to_text = srv_to_text + ' Body,'
    if srv_seam:
      srv_to_text = srv_to_text + ' Seam,'
      
  if srv_deep:
    srv_to_text = srv_to_text + ' Deep:'
    if srv_line:
      srv_to_text = srv_to_text + ' Line,'
    if srv_body:
      srv_to_text = srv_to_text + ' Body,'
    if srv_seam:
      srv_to_text = srv_to_text + ' Seam'
      
  srv_fr_text = ''
  srv_fr_txt = ['Line (left)','Middle','Line (right)'] 
  if srv_fr[0]:
    srv_fr_text = 'Line (Left)'
  if srv_fr[1]:
    srv_fr_text = srv_fr_text + ' & Middle' if len(srv_fr_text) == 0 else 'Middle'
  if srv_fr[2]:
    srv_fr_text = srv_fr_text + ' & Line (Right)' if len(srv_fr_text) == 0 else 'Line (Right)'
  #print(f"scout_srv_strategy:  srv_fr : {srv_fr}")

  srv_strategy_title = '** Serving Tendency ** : '+disp_player+' is serving from '+srv_fr_text+' to '+srv_to_text
  #print(f"Serving Tendencies: {srv_strategy_title}")
  
  # make a list of tuples for the serve to zones
  srv_2 = []
  srv_2_txt = []
  #print(f"Serve To 1:{srv_to_1}")
  #print(f"Serve To 2:{srv_to_2}")
  #print(f"Serve To 3:{srv_to_3}")
  #print(f"Serve To 4:{srv_to_4}")
  #print(f"Serve To 5:{srv_to_5}")
  depth_list = ['E','D','C']
  depth_text = ['Deep','Mid','Short']
  for j in (0,1,2):
    if srv_to_1[j]:
      srv_2.append([1,depth_list[j]])
      srv_2_txt.append([1,depth_text[j]])
    if srv_to_2[j]:
      srv_2.append([2,depth_list[j]])
      srv_2_txt.append([2,depth_text[j]])
    if srv_to_3[j]:
      srv_2.append([3,depth_list[j]])
      srv_2_txt.append([3,depth_text[j]])
    if srv_to_4[j]:
      srv_2.append([4,depth_list[j]])
      srv_2_txt.append([4,depth_text[j]])
    if srv_to_5[j]:
      srv_2.append([5,depth_list[j]])
      srv_2_txt.append([5,depth_text[j]])
  # let's see what we have
  #print(f"scout_players_serv: svr to list of tuples {srv_2}")
  # this list should now have as many tuples as points selected.  First number is 1 - 5 for net zones, second number is depth: 0=E, 1=D, 2+A,B,C
  
  # get the ppr data
  #print(f"scout_srv_strategy: league: {disp_league}, gender: {disp_gender}, year: {disp_year}, team: {disp_team}")
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  #print(f"ppr_df all:{ppr_df.shape[0]}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  #print(f"scout_srv_strategy: ppr_df date and comp lmited:{ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest
  #print(f"scout_srv_strategy: ppr_df pair lmited:{ppr_df.shape[0]}")

  # lastly, lmit this by the source and desitnation locations
  ppr_df = ppr_df[ ((ppr_df['serve_src_zone_net'] == 1) & (srv_fr[0])) | 
                   ((ppr_df['serve_src_zone_net'] == 3) & (srv_fr[1])) |
                   ((ppr_df['serve_src_zone_net'] == 5) & (srv_fr[2]))
  ]
  #print(f"scout_srv_strategy: ppr_df lmited srv from zones:{ppr_df.shape[0]}")

  # now start a loop of the number of desitnation tuples (srv2[])
  first_zone = True
  for i in range(0,len(srv_2),1):
    #print(f" i:{i}, srv_2[i,0] {srv_2[i][0]}, srv_2[i,1] {srv_2[i][1]}")
    tmp_df = ppr_df[ (ppr_df['serve_dest_zone_net'] == srv_2[i][0]) & (ppr_df['serve_dest_zone_depth'] == srv_2[i][1]) ]
    #print(f"Number of rows in Filter db by serve dest: {tmp_df.shape[0]}")
    if not first_zone:
      new_ppr = pd.concat([new_ppr,tmp_df])
    else:
      new_ppr = tmp_df
      first_zone = False
     
  #print(f"Number of final db to analze: {new_ppr.shape[0]}")
  
  # calculate a quick table FBHE
  fbhe_vector = fbhe(new_ppr, disp_player, 'srv',True)
  oos_vector =count_out_of_system(new_ppr, disp_player, 'srv')
  #print(f"fbhe Vector: {fbhe_vector}")
  srv_strat_dict = {'From':[0],
                     'To':[0],
                    'Attempts':[0],
                    'Opp FBSO':[0],
                    'Opp FBHE':[0],
                    'Opp Out of Sys':[0],
                    'URL':[0]
                   }
  srv_strat_df = pd.DataFrame.from_dict(srv_strat_dict)
  srv_strat_df.at[0,'From'] = 'All'
  srv_strat_df.at[0,'To'] = 'All'
  srv_strat_df.at[0,'Attempts'] = fbhe_vector[3]
  srv_strat_df.at[0,'Opp FBSO'] = fbhe_vector[4]
  srv_strat_df.at[0,'Opp FBHE'] = fbhe_vector[0]
  srv_strat_df.at[0,'Opp Out of Sys'] = oos_vector[0] if oos_vector[0] else 0
  srv_strat_df.at[0,'URL'] = fbhe_vector[5]  

  # now a loop over the different serving options:
  rows = 1
  for i in [0,1,2]:
    srv_src = i*2 + 1
    if srv_fr[i]:
      for j in range(0,len(srv_2),1):
        rows = rows+1
        fbhe_vector = fbhe( (new_ppr[( new_ppr['serve_src_zone_net'] == srv_src) & 
                              ( new_ppr['serve_dest_zone_net'] == srv_2[j][0] ) & 
                              ( new_ppr['serve_dest_zone_depth'] == srv_2[j][1]) ]),
                              disp_player,
                              'srv',
                              True 
                            )
        oos_vector = count_out_of_system( (new_ppr[( new_ppr['serve_src_zone_net'] == srv_src) & 
                              ( new_ppr['serve_dest_zone_net'] == srv_2[j][0] ) & 
                              ( new_ppr['serve_dest_zone_depth'] == srv_2[j][1]) ]),
                              disp_player,
                              'srv'
                                       )
        srv_strat_df.at[rows,'From'] = srv_fr_txt[i]
        srv_strat_df.at[rows,'To'] = str(srv_2_net_txt[srv_2_txt[j][0]]) + " " + str(srv_2_txt[j][1])
        srv_strat_df.at[rows,'Attempts'] = fbhe_vector[3]
        srv_strat_df.at[rows,'Opp FBSO'] = fbhe_vector[4]
        srv_strat_df.at[rows,'Opp FBHE'] = fbhe_vector[0]
        srv_strat_df.at[rows,'Opp Out of Sys'] = oos_vector[0]
        srv_strat_df.at[rows,'URL'] = fbhe_vector[5]  
                                  
  #print(f"Srv Strat DF: {srv_strat_df}")
  srv_strat_md = pd.DataFrame.to_markdown(srv_strat_df, index=False)

  # now, time to make plots.
  # want to plot data from new_ppr

  # limit the data to serves by the player
  new_ppr = new_ppr[ new_ppr['serve_player'] == disp_player]

  # make a plot to chart the serves: (line chart, court in the background)
  serve_diagram_plot_object = plot_lines_on_court(new_ppr, 'srv', 1)
  #serve_diagram_plot_object = ''

  # make a plot to chart the pass locations: (dot splatter with half court in background)
  pass_locations_plot_object = plot_points_on_the_court(new_ppr['pass_dest_x'],new_ppr['pass_dest_y'], 2,new_ppr['video_id'], new_ppr['pass_action_id'],True,new_ppr['point_outcome'])
  #pass_locations_plot_object = ''

  # make a plot to chart the set locations: (dot splatter with half court in background)
  set_locations_plot_object = plot_points_on_the_court(new_ppr['set_dest_x'],new_ppr['set_dest_y'], 3,new_ppr['video_id'], new_ppr['set_action_id'],False,new_ppr['point_outcome'])
  #set_locations_plot_object = ''

  # Next, build the 6 plots across the page as subplots, zone's 1 - 5 plus Optioon

  # set up 6 sub plots
  attack_z1_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')],'att',4)
  attack_z2_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')],'att',5)
  attack_z3_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')],'att',6)
  attack_z4_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')],'att',7)
  attack_z5_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')],'att',8)
  attack_opt_plot_object = plot_lines_on_court(new_ppr[ new_ppr['tactic'] == 'option'],'att',9)

  # set up 6 small tables below with:
  #. 0 = FBHE
  #. 1 = FBSO
  #  2 = Attempts
  #. 3 = Out of System
  #. 4 = URL

  zone_dict = {'1':['FBHE','FBSO','ATT','URL'],'Value':[0,0,0,'']}
  z1_df = pd.DataFrame.from_dict(zone_dict)
  z2_df = pd.DataFrame.from_dict(zone_dict)
  z3_df = pd.DataFrame.from_dict(zone_dict)
  z4_df = pd.DataFrame.from_dict(zone_dict)
  z5_df = pd.DataFrame.from_dict(zone_dict)
  opt_df = pd.DataFrame.from_dict(zone_dict)

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z1_df.at[0,'Value'] = fbhe_vector[0]
  z1_df.at[1,'Value'] = fbhe_vector[4]
  z1_df.at[2,'Value'] = fbhe_vector[3]
  z1_df.at[3,'Value'] = fbhe_vector[5]
  #z1_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z2_df.at[0,'Value'] = fbhe_vector[0]
  z2_df.at[1,'Value'] = fbhe_vector[4]
  z2_df.at[2,'Value'] = fbhe_vector[3]
  z2_df.at[3,'Value'] = fbhe_vector[5]
  #z2_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z3_df.at[0,'Value'] = fbhe_vector[0]
  z3_df.at[1,'Value'] = fbhe_vector[4]
  z3_df.at[2,'Value'] = fbhe_vector[3]
  z3_df.at[3,'Value'] = fbhe_vector[5]
  #z3_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z4_df.at[0,'Value'] = fbhe_vector[0]
  z4_df.at[1,'Value'] = fbhe_vector[4]
  z4_df.at[2,'Value'] = fbhe_vector[3]
  z4_df.at[3,'Value'] = fbhe_vector[5]
  #z4_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z5_df.at[0,'Value'] = fbhe_vector[0]
  z5_df.at[1,'Value'] = fbhe_vector[4]
  z5_df.at[2,'Value'] = fbhe_vector[3]
  z5_df.at[3,'Value'] = fbhe_vector[5]
  #z5_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['tactic'] == 'option')], disp_player, 'srv', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['tactic'] == 'option')], disp_player, 'pass' )
  opt_df.at[0,'Value'] = fbhe_vector[0]
  opt_df.at[1,'Value'] = fbhe_vector[4]
  opt_df.at[2,'Value'] = fbhe_vector[3]
  opt_df.at[3,'Value'] = fbhe_vector[5]
  #opt_df.at[3,'Value'] = oos_vector[0]

  z1_mkdn = pd.DataFrame.to_markdown(z1_df, index=False, headers=['',''] )
  z2_mkdn = pd.DataFrame.to_markdown(z2_df, index=False, headers=['',''])
  z3_mkdn = pd.DataFrame.to_markdown(z3_df, index=False, headers=['',''])
  z4_mkdn = pd.DataFrame.to_markdown(z4_df, index=False, headers=['',''])
  z5_mkdn = pd.DataFrame.to_markdown(z5_df, index=False, headers=['',''])
  opt_mkdn = pd.DataFrame.to_markdown(opt_df, index=False, headers=['',''])
  

  
  return srv_strategy_title, srv_strat_md, serve_diagram_plot_object, pass_locations_plot_object, set_locations_plot_object, attack_z1_plot_object, attack_z2_plot_object, attack_z3_plot_object, attack_z4_plot_object, attack_z5_plot_object, attack_opt_plot_object, z1_mkdn, z2_mkdn,z3_mkdn,z4_mkdn,z5_mkdn,opt_mkdn


#---------------------------------------------------------------------------------
# Scouting Report : graph of attacking effectiveness (fbhe) from teh 45 serve strategies
#----------------------------------------------------------------------------------------
@anvil.server.callable
def scout_attack_pass_zones(disp_league,
                      disp_gender,
                      disp_year,
                      disp_team,
                      disp_pair,
                      disp_player,
                      comp_l1_checked,
                      disp_comp_l1,
                      comp_l2_checked,
                      disp_comp_l2,
                      comp_l3_checked,
                      disp_comp_l3,
                      date_checked,
                      disp_start_date,
                      disp_end_date,
                      scout,
                      explain_text, title_text,
                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5
  ):

  # we keep all the parameters, but we are not using many.  (serve to and from, and filters on the data since the data is taken from the pair_stats table
  

  # get teh pair data
  pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
  cmax = pair_stats_df['fbhe_mean']+pair_stats_df['fbhe_stdev']
  cmin = pair_stats_df['fbhe_mean']-pair_stats_df['fbhe_stdev']
  
  # get the index of the row we need for the pair/plahyer
  pair_data_index = pair_data_df.loc[ (pair_data_df['pair'] == disp_pair) & (pair_data_df['player'] == disp_player) ].index[0]

  # the order of the index
  '''
  index.  from.   to   
  0.      1.       1C.  x = 0.8. y = 4.0
  1.      1.       1D.  x = 0.8. y = 5.6
  2.       1.      1E.   x = 0.8 y = 7.2
  3       1.      2C.    x = 2.4. y = 4.0
  4       1       2D.    x = 2.4  y = 5.6
  5.      1.      2E.    x = 2.4. y = 7.2
  6                3C.   x = 3.6. y = 4.0
  7                3D.    x = 3.6. y = 5.6
  8                3E.   x = 3.6. y = 7.2
  9                4C
  10               4D
  '''
  # now, get the variables 
  pass_x = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass_y = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  
  # now, loop thru the list for serves from zone 1
  index = 0
  x = 8.8
  for i in [1,2,3,4,5]:  # j is along the net
    x = x - 1.6
    y = 2.4
    for j in ['c','d','e']: # k is depth+
      y = y + 1.6
      fbhe1_var = 'fbhe_1_'+str(i)+str(j)
      fbhe3_var = 'fbhe_3_'+str(i)+str(j)
      fbhe5_var = 'fbhe_5_'+str(i)+str(j)
      pass1_val[index] = pair_data_df.loc[pair_data_index,fbhe1_var]
      pass3_val[index] = pair_data_df.loc[pair_data_index,fbhe3_var]
      pass5_val[index] = pair_data_df.loc[pair_data_index,fbhe5_var]
      pass_x[index] = x
      pass_y[index] = y
      index = index + 1
        
  # I should now have the tables required
  #print(f"x,y : {pass_x}, {pass_y}")
  #print(f"pass value 1, 3, 5: {pass1_val}, {pass3_val}, {pass5_val}")

  # make x,y for serve lines:
  x11 = [0.5,0.5,0.5]
  x12 = [0,4,8]
  x31 = [4,4,4]
  x51 = [7.5,7.5,7.5]
  y1 = [-8,-8,-8]
  y2 = [8,8,8]

  # Create the plot for serves from Zone 1 - define the figure, plot the court, plot a few serve lines, plot the dots
  #cm = mpl.cm.cool
  #norm = mpl.colors.Normalize(vmin=-1, vmax=1)
  
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  z5_plt = anvil.mpl_util.plot_image()


  # now, leets sta
  srv_strategy_title = 'Attack Effectiveness based on Serve Source & Desitnation'
  
  return srv_strategy_title, '', z1_plt, z3_plt, z5_plt, '','','','','','','','','','','',''


@anvil.server.callable
def full_scouting_report_pair(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, title_text,
                    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                    ):
  
  # this merges a hard coded series of reports
  '''
  This is typically called in report manager for report type : 'scouting - pdf only'"

  funct_name should be 'full_pair_scouting_report'
  
  The reports merged together are (all pair reports):
  - pair sw report
  - pair fbhe by attack
  - pair fbhe by pass
  - scout attack pass zones
  - scout srv strategy
  - scout players serve

  Input parameters include full set of report parameters, calling this creates PDF for the disp_player, 
  for full pair, call twice, once for each disp_player.

  return is the pdf file

  funct_name here should be 
  '''
  
  pdf_file = '' # temp storage while coding

  # create and store the pair_sw_report

  # create and store the fbhe by attack

  # create and store the dbhe by passer

  # create and store the scout attack pass zones

  # now scout srv strategy

  # scout players serve

  # return the pdf file!
  
  return pdf_file
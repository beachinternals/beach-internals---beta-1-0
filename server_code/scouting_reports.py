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
                      explain_text,
                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5
  ):

  # try to build to scouting report - Serving Strategy
  player_label = disp_player

  # lets find the serve strategy Text:
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
  if srv_line:
    srv_to_text = 'Line:'
  elif srv_body:
    srv_to_text = 'Body:'
  elif srv_seam:
    srv_to_text = 'Seam:'

  if srv_short:
    srv_to_text = srv_to_text + ', Short'
  if srv_mid:
    srv_to_text = srv_to_text + ', Mid'
  if srv_deep:
    srv_to_text = srv_to_text + ", Deep"

  srv_fr_text = ''
  if srv_fr[0]:
    srv_fr_text = 'Line (Left)'
  elif srv_fr[1]:
    srv_fr_text = 'Middle'
  elif srv_fr[2]:
    srv_fr_text = 'Line (Right)'

  if srv_fr[0]:
    if srv_fr[1]:
      if srv_fr[2]:
        svr_fr_list = [1,3,5]
      else:
        svr_fr_list = [1,3]
    elif srv_fr[2]:
      srv_fr_list = [1,5] 
  elif srv_fr[1]:
    if srv_fr[2]:
      srv_fr_list = [3,4]
    else:
        srv_fr_list = [3]
  elif srv_fr[2]:
    srv_fr_list = [5]

  srv_strategy_title = 'Serving '+disp_player+' from '+srv_fr_text+' to '+srv_to_text
  print(f"Serving Strategy: {srv_strategy_title}")
  
  # make a list of tuples for the serve to zones
  srv_2 = []
  print(f"Serve To 1:{srv_to_1}")
  print(f"Serve To 2:{srv_to_2}")
  print(f"Serve To 3:{srv_to_3}")
  print(f"Serve To 4:{srv_to_4}")
  print(f"Serve To 5:{srv_to_5}")
  depth_list = ['E','D','C']
  for j in (0,1,2):
    if srv_to_1[j]:
      srv_2.append([1,depth_list[j]])
    if srv_to_2[j]:
      srv_2.append([2,depth_list[j]])
    if srv_to_3[j]:
      srv_2.append([3,depth_list[j]])
    if srv_to_4[j]:
      srv_2.append([4,depth_list[j]])
    if srv_to_5[j]:
      srv_2.append([5,depth_list[j]])
  # let's see what we have
  print(f"svr list of tuples {srv_2}")
  # this list should now have as many tuples as points selected.  First number is 1 - 5 for net zones, second number is depth: 0=E, 1=D, 2+A,B,C
  
  # get the ppr data
  print(f"league: {disp_league}, gender: {disp_gender}, year: {disp_year}, team: {disp_team}")
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  print(f"ppr_df all:{ppr_df.shape[0]}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  print(f"ppr_df date and comp lmited:{ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest
  print(f"ppr_df pair lmited:{ppr_df.shape[0]}")

  # lastly, lmit this by the source and desitnation locations
  ppr_df = ppr_df[ ((ppr_df['serve_src_zone_net'] == 1) & (srv_fr[0])) | 
                   ((ppr_df['serve_src_zone_net'] == 3) & (srv_fr[1])) |
                   ((ppr_df['serve_src_zone_net'] == 5) & (srv_fr[2]))
  ]
  print(f"ppr_df lmited srv from zones:{ppr_df.shape[0]}")

  # now start a loop of the number of desitnation tuples (srv2[])
  first_zone = True
  for i in range(0,len(srv_2),1):
    print(f" i:{i}, srv_2[i,0] {srv_2[i][0]}, srv_2[i,1] {srv_2[i][1]}")
    tmp_df = ppr_df[ (ppr_df['serve_dest_zone_net'] == srv_2[i][0]) & (ppr_df['serve_dest_zone_depth'] == srv_2[i][1]) ]
    print(f"Number of rows in Filter db by serve dest: {tmp_df.shape[0]}")
    if not first_zone:
      new_ppr = pd.concat([new_ppr,tmp_df])
    else:
      new_ppr = tmp_df
      first_zone = False
     
  print(f"Number of final db to analze: {new_ppr.shape[0]}")
  
  # calculate a quick table FBHE
  fbhe_vector = fbhe(new_ppr, disp_player, 'att',True)
  print(f"fbhe Vector: {fbhe_vector}")
  srv_strat_dict = {'From':[0],
                     'To':[0],
                    'Attempts':[0],
                    'FBSO':[0],
                    'FBHE':[0],
                    'URL':[0]
                   }
  srv_strat_df = pd.DataFrame.from_dict(srv_strat_dict)
  srv_strat_df.at[0,'From'] = 'All'
  srv_strat_df.at[0,'To'] = 'All'
  srv_strat_df.at[0,'Attempts'] = fbhe_vector[3]
  srv_strat_df.at[0,'FBSO'] = float("{:.3}".format(fbhe_vector[4]))
  srv_strat_df.at[0,'FBHE'] = float("{:.3}".format(fbhe_vector[0]))
  srv_strat_df.at[0,'URL'] = fbhe_vector[5]  

  # now a loop over the different serving options:
  for i in [0,1,2]:
    if srv_fr[i]:
      srv_src = i*2+1
      for j in range(0,len(srv_2),1):
        fbhe_vector = fbhe( (new_ppr[( new_ppr['serve_src_zone_net'] == srv_src) & 
                              ( new_ppr['serve_dest_zone_net'] == srv_2[j][0] ) & 
                              ( new_ppr['serve_dest_zone_depth'] == srv_2[j][1])]),
                              disp_player,
                              'att',
                              True 
                            )
        srv_strat_df.at[i+j,'From'] = srv_src
        srv_strat_df.at[i+j,'To'] = str(srv_2[j][0]) + str(srv_2[j][1])
        srv_strat_df.at[i+j,'Attempts'] = fbhe_vector[3]
        srv_strat_df.at[i+j,'FBSO'] = fbhe_vector[4]
        srv_strat_df.at[i+j,'FBHE'] = fbhe_vector[0]
        srv_strat_df.at[i+j,'URL'] = fbhe_vector[5]  
                                  
  print(f"Srv Strat DF: {srv_strat_df}")
  srv_strat_md = pd.DataFrame.to_markdown(srv_strat_df)

  return srv_strategy_title, srv_strat_md
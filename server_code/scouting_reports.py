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
                      start_date,
                      end_date,
                      scout,
                      explain_text,
                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5
  ):

  # try to build to scouting report - Serving Strategy
  #player_label = disp_player

  # lets find the serve strategy Text:
  if (srv_to_1[0] == True) or (srv_to_1[1] == True) or (srv_to_1[2] == True) or (srv_to_5[0] == True) or (srv_to_5[1] == True) or (srv_to_5[2] == True):
    srv_line = True 
  else:
    srv_line = False
  if (srv_to_2[0] == True) or (srv_to_2[1] == True) or (srv_to_2[2] == True) or (srv_to_4[0] == True) or (srv_to_4[1] == True) or (srv_to_4[2] == True):
    srv_body = True 
  else:
    srv_body = False
  if (srv_to_3[0] == True) or (srv_to_3[1] == True) or (srv_to_3[2] == True):
    srv_seam = True 
  else:
    srv_seam = False

  srv_deep =  True if (srv_to_1[0] == True) or (srv_to_2[0] == True) or (srv_to_3[0] == True)or (srv_to_4[0] == True)or (srv_to_5[0] == True) else False
  srv_mid =   True if (srv_to_1[1] == True) or (srv_to_2[1] == True) or (srv_to_3[1] == True)or (srv_to_4[1] == True)or (srv_to_5[1] == True) else False
  srv_short = True if (srv_to_1[2] == True) or (srv_to_2[2] == True) or (srv_to_3[2] == True)or (srv_to_4[2] == True)or (srv_to_5[2] == True) else False

  srv_to_text = ''
  if srv_line:
    srv_to_text = 'Line'
  elif srv_body:
    srv_to_text = 'Body'
  elif srv_seam:
    srv_to_tesxt = 'Seam'

  if srv_short:
    srv_to_text = srv_to_text + ', Short '
  elif srv_mid:
    srv_to_text = srv_to_text + ', Mid '
  elif srv_deep:
    srv_to_text = srv_to_text + ", Deep "

  srv_fr_text = ''
  if srv_fr[0]:
    srv_fr_text = 'Left Line'
  elif srv_fr[1]:
    srv_fr_text = "Middle"
  elif srv_fr[2]:
    svr_fr_text = 'Right Line'

  if srv_fr[0]:
    if srv_fr[1]:
      if srv_fr[2]:
        svr_fr_list = [1,3,5]
      else:
        svr_fr_list = [1,3]
    elif srv_fr[2]:
      svr_fr_list = [1,5] 
  elif srv_fr[1]:
    if srv_fr[2]:
      srv_fr_list = [3,4]
    else:
        srv_fr_list = [3]
  elif srv_fr[2]:
    srv_fr_list = [5]
    
  srv_strategy_title = 'Serving '+disp_player+' from '+srv_fr_text+' to '+srv_to_text

  # get the ppr data
  ppr_df =  get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest

  # lastly, lmit this by the source and desitnation locations
  ppr_df = ppr_df[ppr_df['serve_src_net_zone'] == srv_fr_list]
  ppr_df = ppr_df[ppr_df['serve_dest_net_zone'] == srv_to_list]
  
  # calculate a quick table FBHE
  fbhe_vector = fbhe(ppr_df, disp_pair, 'att',True)
  serves_plot
  pass_loc_plot
  set_loc_plot
  rpt_disp_box = 


  return
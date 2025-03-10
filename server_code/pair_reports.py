import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from pair_functions import *
from server_functions import *
from player_reports import *
import pandas as pd
import scipy.stats as stats

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-------------------------------------------------------
#
#         Pair Reports
#
#--------------------------------------------------------

#-------------------------------------------------------
#
#         Pair Attacking - All
#
#--------------------------------------------------------
@anvil.server.callable
def pair_attacking(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  # kind of mergin the two below to getone page with all of it
  pair_attack_mkdn, p1_attack_mkdn, p2_attack_mkdn = pair_fbhe_net(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                )

  pair_pass_mkdn, p1_pass_mkdn, p2_pass_mkdn = pair_fbhe_pass(disp_league, disp_gender, disp_year, 
                disp_team, disp_pair, disp_player,
                comp_l1_checked, disp_comp_l1,
                comp_l2_checked, disp_comp_l2,
                comp_l3_checked, disp_comp_l3,
                date_checked, disp_start_date, disp_end_date,
                scout, explain_text
              )

  return p1_attack_mkdn, p1_pass_mkdn, '', '', p2_attack_mkdn, p2_pass_mkdn,'',''

@anvil.server.callable
def pair_fbhe_net(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):

  # First, get the data, and narrow to the filter, now, limit to only pair players
  disp_player1, disp_player2 = pair_players(disp_pair)
  #print(f"Pair: {disp_pair}, P1: {disp_player1}, P2: {disp_player2}")
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True )
  #print(f"ppr_df size: {ppr_df.shape[0]}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         )
  #print(f"ppr_df size after limits: {ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair)
  #print(f"ppr_df size after pair only points: {ppr_df.shape[0]}")

  # create the output dataframe
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,' ']
            }
  pair_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p1_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p2_fbhe_table = pd.DataFrame.from_dict( df_dict )
  
  # calcualte for player 1
  fbhe_vector = fbhe(ppr_df,disp_player1,'att',True)
  p1_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'All'] = fbhe_vector[4]  # fbso
  p1_fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
  #p1_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p1_fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr_df,disp_player2,'att',True)
  p2_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'All'] = fbhe_vector[4]  # fbso
  p2_fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
  #p2_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p2_fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[2,'All'] = p1_fbhe_table.at[2,'All'] + p2_fbhe_table.at[2,'All']
  pair_fbhe_table.at[3,'All'] = p1_fbhe_table.at[3,'All'] + p2_fbhe_table.at[3,'All']
  pair_fbhe_table.at[4,'All'] = p1_fbhe_table.at[4,'All'] + p2_fbhe_table.at[4,'All']
  if pair_fbhe_table.at[4,'All'] != 0:
    pair_fbhe_table.at[0,'All'] = (pair_fbhe_table.at[2,'All'] - pair_fbhe_table.at[3,'All']) / (pair_fbhe_table.at[4,'All'])
    pair_fbhe_table.at[1,'All'] = (pair_fbhe_table.at[2,'All']) / (pair_fbhe_table.at[4,'All'])
  pair_fbhe_table.at[0,'All'] = float("{:.3f}".format(pair_fbhe_table.at[0,'All']))
  pair_fbhe_table.at[1,'All'] = float("{:.3f}".format(pair_fbhe_table.at[1,'All']))

  # now, zones 1 thru 5
  # calculate for zones 1 - 5
  column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','No Zone']
  for i in [1,2,3,4,5,0]:
    # calculate for player 1
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player1, 'att', True )
    p1_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p1_fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # fbhe
    p1_fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
    p1_fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
    p1_fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
    p1_fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for player 2
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player2, 'att', True )
    p2_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p2_fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # fbhe
    p2_fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
    p2_fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
    p2_fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
    p2_fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for pair
    pair_fbhe_table.at[2,column[i-1]] = p1_fbhe_table.at[2,column[i-1]] + p2_fbhe_table.at[2,column[i-1]]
    pair_fbhe_table.at[3,column[i-1]] = p1_fbhe_table.at[3,column[i-1]] + p2_fbhe_table.at[3,column[i-1]]
    pair_fbhe_table.at[4,column[i-1]] = p1_fbhe_table.at[4,column[i-1]] + p2_fbhe_table.at[4,column[i-1]]
    if ( pair_fbhe_table.at[4,column[i-1]]) != 0:
      pair_fbhe_table.at[0,column[i-1]] = (pair_fbhe_table.at[2,column[i-1]] - pair_fbhe_table.at[3,column[i-1]]) / (pair_fbhe_table.at[4,column[i-1]])
      pair_fbhe_table.at[1,column[i-1]] = (pair_fbhe_table.at[2,column[i-1]] ) / (pair_fbhe_table.at[4,column[i-1]])
    pair_fbhe_table.at[0,column[i-1]] = float("{:.3f}".format(pair_fbhe_table.at[0,column[i-1]]))
    pair_fbhe_table.at[1,column[i-1]] = float("{:.3f}".format(pair_fbhe_table.at[1,column[i-1]]))

    # So I think we are done:-)
  # now, turn them all into markup
  pair_fbhe_table.drop(pair_fbhe_table.index[5], inplace=True)
  pair_markdown = pd.DataFrame.to_markdown(pair_fbhe_table, index = False )
  p1_markdown = pd.DataFrame.to_markdown(p1_fbhe_table, index = False )
  p2_markdown = pd.DataFrame.to_markdown(p2_fbhe_table, index = False )
  
  return pair_markdown, p1_markdown, p2_markdown



@anvil.server.callable
def pair_fbhe_pass(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):

  # First, get the data, and narrow to the filter, now, limit to only pair players
  disp_player1, disp_player2 = pair_players(disp_pair)
  #print(f"Pair: {disp_pair}, P1: {disp_player1}, P2: {disp_player2}")
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True )
  #print(f"ppr_df size: {ppr_df.shape[0]}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         )
  #print(f"ppr_df size after limits: {ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair)
  ppr_df_no_option = ppr_df[ ppr_df['tactic'] != 'option']
  ppr_df_option = ppr_df[ ppr_df['tactic'] == 'option']
  #print(f"ppr_df size after pair only points: {ppr_df.shape[0]}")
  #print(f"PPD DF Size: All:{ppr_df.shape[0]}, No Option: {ppr_df_no_option.shape[0]}, Option: {ppr_df_option.shape[0]}")

  # create the output dataframe
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','% out of System','URL'],
             'All':[0,0,0,0,0,' ',0],
             'Zone 1':[0,0,0,0,0,' ',0],
             "Zone 2":[0,0,0,0,0,' ',0],
             'Zone 3':[0,0,0,0,0,' ',0],
             'Zone 4':[0,0,0,0,0,' ',0],
             'Zone 5':[0,0,0,0,0,' ',0],
             'No Zone':[0,0,0,0,0,' ',0],
             'Option':[0,0,0,0,0,' ',0]
            }
  pair_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p1_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p2_fbhe_table = pd.DataFrame.from_dict( df_dict )

  # Calculate for ALl attempts
  #----------------------------------------------------
  # calcualte for player 1
  fbhe_vector = fbhe(ppr_df,disp_player1,'pass',True)
  oos_vector1 = count_out_of_system(ppr_df,disp_player1,'pass')
  p1_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'All'] = fbhe_vector[4]  # fbso
  p1_fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
  p1_fbhe_table.at[5,'All'] = str("{:.0%}").format(oos_vector1[1])  # percent out of system
  p1_fbhe_table.at[6,'All'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr_df,disp_player2,'pass',True)
  oos_vector2 = count_out_of_system(ppr_df,disp_player2,'pass')
  p2_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'All'] = fbhe_vector[4]  # fbso
  p2_fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
  p2_fbhe_table.at[5,'All'] =  str("{:.0%}").format(oos_vector2[1])  # percent out of system
  p2_fbhe_table.at[6,'All'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[2,'All'] = p1_fbhe_table.at[2,'All'] + p2_fbhe_table.at[2,'All']
  pair_fbhe_table.at[3,'All'] = p1_fbhe_table.at[3,'All'] + p2_fbhe_table.at[3,'All']
  pair_fbhe_table.at[4,'All'] = p1_fbhe_table.at[4,'All'] + p2_fbhe_table.at[4,'All']
  if pair_fbhe_table.at[4,'All'] != 0:
    pair_fbhe_table.at[0,'All'] = (pair_fbhe_table.at[2,'All'] - pair_fbhe_table.at[3,'All']) / (pair_fbhe_table.at[4,'All'])
    pair_fbhe_table.at[1,'All'] = (pair_fbhe_table.at[2,'All']) / (pair_fbhe_table.at[4,'All'])
  pair_fbhe_table.at[0,'All'] = float("{:.3f}".format(pair_fbhe_table.at[0,'All']))
  pair_fbhe_table.at[1,'All'] = float("{:.3f}".format(pair_fbhe_table.at[1,'All']))
  oos_oos = oos_vector1[0] + oos_vector2[0]
  oos_att = oos_vector1[2] + oos_vector2[2]
  oos_per = oos_oos/oos_att if oos_att != 0 else 0
  pair_fbhe_table.at[5,'All'] = str("{:.0%}".format(oos_per))
    
  # now, zones 1 thru 5
  #----------------------------------------
  # calculate for zones 1 - 5
  column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']

  for i in [1,2,3,4,5]:
    # calculate for player 1
    fbhe_vector = fbhe( ppr_df_no_option[ppr_df_no_option['att_src_zone_net']==i], disp_player1, 'pass', True )
    oos_vector1 = count_out_of_system(ppr_df_no_option[ppr_df_no_option['att_src_zone_net']==i], disp_player1, 'pass')
    p1_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p1_fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # fbhe
    p1_fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
    p1_fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
    p1_fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
    p1_fbhe_table.at[5,column[i-1]] = str('{:.0%}').format(oos_vector1[1])  # attempts
    p1_fbhe_table.at[6,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for player 2
    fbhe_vector = fbhe( ppr_df_no_option[ppr_df_no_option['att_src_zone_net']==i], disp_player2, 'pass', True )
    oos_vector2 = count_out_of_system(ppr_df_no_option[ppr_df_no_option['att_src_zone_net']==i], disp_player2, 'pass')
    p2_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p2_fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # fbhe
    p2_fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
    p2_fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
    p2_fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
    p2_fbhe_table.at[5,column[i-1]] = str('{:.0%}').format(oos_vector2[1])  # attempts
    p2_fbhe_table.at[6,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for pair
    pair_fbhe_table.at[2,column[i-1]] = p1_fbhe_table.at[2,column[i-1]] + p2_fbhe_table.at[2,column[i-1]]
    pair_fbhe_table.at[3,column[i-1]] = p1_fbhe_table.at[3,column[i-1]] + p2_fbhe_table.at[3,column[i-1]]
    pair_fbhe_table.at[4,column[i-1]] = p1_fbhe_table.at[4,column[i-1]] + p2_fbhe_table.at[4,column[i-1]]
    if ( pair_fbhe_table.at[4,column[i-1]]) != 0:
      pair_fbhe_table.at[0,column[i-1]] = (pair_fbhe_table.at[2,column[i-1]] - pair_fbhe_table.at[3,column[i-1]]) / (pair_fbhe_table.at[4,column[i-1]])
      pair_fbhe_table.at[1,column[i-1]] = (pair_fbhe_table.at[2,column[i-1]] ) / (pair_fbhe_table.at[4,column[i-1]])
    pair_fbhe_table.at[0,column[i-1]] = float("{:.3f}".format(pair_fbhe_table.at[0,column[i-1]]))
    pair_fbhe_table.at[1,column[i-1]] = float("{:.3f}".format(pair_fbhe_table.at[1,column[i-1]]))
    oos_oos = oos_vector1[0] + oos_vector2[0]
    oos_att = oos_vector1[2] + oos_vector2[2]
    oos_per = oos_oos/oos_att if oos_att != 0 else 0
    pair_fbhe_table.at[5,column[i-1]] = str("{:.0%}".format(oos_per))

  # now calculate for the those without a zone
  #-----------------------------------------
  # calculate for player 1
  fbhe_vector = fbhe(ppr_df_no_option[ ppr_df_no_option['att_src_zone_net'] == 0],disp_player1,'pass',True)
  oos_vector1 = count_out_of_system(ppr_df_no_option[ ppr_df_no_option['att_src_zone_net'] == 0],disp_player1,'pass')
  p1_fbhe_table.at[0,'No Zone'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'No Zone'] = fbhe_vector[4]  # fbso
  p1_fbhe_table.at[2,'No Zone'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[3,'No Zone'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[4,'No Zone'] = fbhe_vector[3]  # attempts
  p1_fbhe_table.at[5,'No Zone'] = str('{:.0%}').format(oos_vector1[1])   # confidence interval
  p1_fbhe_table.at[6,'No Zone'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr_df_no_option[ ppr_df_no_option['att_src_zone_net'] == 0],disp_player2,'pass',True)
  oos_vector2 = count_out_of_system(ppr_df_no_option[ ppr_df_no_option['att_src_zone_net'] == 0],disp_player2,'pass')
  p2_fbhe_table.at[0,'No Zone'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'No Zone'] = fbhe_vector[4]  # fbso
  p2_fbhe_table.at[2,'No Zone'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[3,'No Zone'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[4,'No Zone'] = fbhe_vector[3]  # attempts
  p2_fbhe_table.at[5,'No Zone'] = str('{:.0%}').format(oos_vector2[1])  # confidence interval
  p2_fbhe_table.at[6,'No Zone'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[2,'No Zone'] = p1_fbhe_table.at[2,'No Zone'] + p2_fbhe_table.at[2,'No Zone']
  pair_fbhe_table.at[3,'No Zone'] = p1_fbhe_table.at[3,'No Zone'] + p2_fbhe_table.at[3,'No Zone']
  pair_fbhe_table.at[4,'No Zone'] = p1_fbhe_table.at[4,'No Zone'] + p2_fbhe_table.at[4,'No Zone']
  if pair_fbhe_table.at[4,'No Zone'] != 0:
    pair_fbhe_table.at[0,'No Zone'] = (pair_fbhe_table.at[2,'No Zone'] - pair_fbhe_table.at[3,'No Zone']) / (pair_fbhe_table.at[4,'No Zone'])
    pair_fbhe_table.at[1,'No Zone'] = (pair_fbhe_table.at[2,'No Zone']) / (pair_fbhe_table.at[4,'No Zone'])
  pair_fbhe_table.at[0,'No Zone'] = float("{:.3f}".format(pair_fbhe_table.at[0,'No Zone']))
  pair_fbhe_table.at[1,'No Zone'] = float("{:.3f}".format(pair_fbhe_table.at[1,'No Zone']))
  oos_oos = oos_vector1[0] + oos_vector2[0]
  oos_att = oos_vector1[2] + oos_vector2[2]
  oos_per = oos_oos/oos_att if oos_att != 0 else 0
  pair_fbhe_table.at[5,'No Zone'] = str("{:.0%}".format(oos_per))

  
  # now calculate for the options
  #-----------------------------------------
  # calculate for player 1
  fbhe_vector = fbhe(ppr_df_option,disp_player1,'pass',True)
  oos_vector1 = count_out_of_system(ppr_df_option,disp_player1,'pass')
  p1_fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'Option'] = fbhe_vector[4]  # fbso
  p1_fbhe_table.at[2,'Option'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[3,'Option'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[4,'Option'] = fbhe_vector[3]  # attempts
  p1_fbhe_table.at[5,'Option'] = str('{:.0%}').format(oos_vector1[1])   # confidence interval
  p1_fbhe_table.at[6,'Option'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr_df_option,disp_player2,'pass',True)
  oos_vector2 = count_out_of_system(ppr_df_option,disp_player2,'pass')
  p2_fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'Option'] = fbhe_vector[4]  # fbso
  p2_fbhe_table.at[2,'Option'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[3,'Option'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[4,'Option'] = fbhe_vector[3]  # attempts
  p2_fbhe_table.at[5,'Option'] = str('{:.0%}').format(oos_vector2[1])  # confidence interval
  p2_fbhe_table.at[6,'Option'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[2,'Option'] = p1_fbhe_table.at[2,'Option'] + p2_fbhe_table.at[2,'Option']
  pair_fbhe_table.at[3,'Option'] = p1_fbhe_table.at[3,'Option'] + p2_fbhe_table.at[3,'Option']
  pair_fbhe_table.at[4,'Option'] = p1_fbhe_table.at[4,'Option'] + p2_fbhe_table.at[4,'Option']
  if pair_fbhe_table.at[4,'Option'] != 0:
    pair_fbhe_table.at[0,'Option'] = (pair_fbhe_table.at[2,'Option'] - pair_fbhe_table.at[3,'Option']) / (pair_fbhe_table.at[4,'Option'])
    pair_fbhe_table.at[1,'Option'] = (pair_fbhe_table.at[2,'Option']) / (pair_fbhe_table.at[4,'Option'])
  pair_fbhe_table.at[0,'Option'] = float("{:.3f}".format(pair_fbhe_table.at[0,'Option']))
  pair_fbhe_table.at[1,'Option'] = float("{:.3f}".format(pair_fbhe_table.at[1,'Option']))
  oos_oos = oos_vector1[0] + oos_vector2[0]
  oos_att = oos_vector1[2] + oos_vector2[2]
  oos_per = oos_oos/oos_att if oos_att != 0 else 0
  pair_fbhe_table.at[5,'Option'] = str("{:.0%}".format(oos_per))
  
  # So I think we are done:-)
  # now, turn them all into markup

  # last item, dro the last row in the pari df since we do not calcualte/show URL for the pair
  pair_fbhe_table.drop(pair_fbhe_table.index[6], inplace=True)
  pair_markdown = pd.DataFrame.to_markdown(pair_fbhe_table, index = False )
  p1_markdown = pd.DataFrame.to_markdown(p1_fbhe_table, index = False )
  p2_markdown = pd.DataFrame.to_markdown(p2_fbhe_table, index = False )
  
  return pair_markdown, p1_markdown, p2_markdown




#-------------------------------------------------------
#
#         Pair Summary Repport
#
#--------------------------------------------------------
@anvil.server.callable
def pair_summary_rpt(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  # note, added disp_pair in the arguments
  disp_player1, disp_player2 = pair_players(disp_pair) # looks iinto master_pairs to get player 1 and 2 for the given pair
  disp_player1_team_num = get_team_num(disp_player1)
  disp_player2_team_num = get_team_num(disp_player2)

  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  #print(f"Initial Call: ppr_df size:{ppr_df.shape[0]}, league {disp_league}, gender:{disp_gender}, year:{disp_year}, team:{disp_team}")
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  #print(f"Initial Call: ppr_df size:{ppr_df.shape[0]}")
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest
  #print(f"Initial Call: ppr_df size:{ppr_df.shape[0]}")
  if ppr_df.shape[0] == 0:
    return 'No Data Available','','','','','','',''
    
  scor_dict = {' ':['Pt Diff','Term Srv','Live Rallies',   # 0,1,2
                    'SideOut', 'Opp Sideout', # 3,4
                    'FB Sideout','OppFb Sideout', # 5,6
                    'FB Stop', 'Opp FB Stop', # 7,8
                    'FB Win', # 9
                    'Trans Win','Opp Trans Win', # 10,11
                    #'Blocking','Digging','Create', # 12,13,14
                   'Serving','First Ball Attacking','Transition Attacking'], # 15,16,17
             '#':[0,0,0,0,0,0,0,0,'',0,0,0,0,0,0],
             '%':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
            }
  scor_table = pd.DataFrame.from_dict( scor_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  # Call number of points
  #print(f"Points for the Pair: {disp_pair}")
  pts_df = pair_pt_total(ppr_df,disp_pair)
  #print(f"Points for the player: {disp_player1}")
  p1_df = player_pt_total(ppr_df,disp_player1)
  #print(f"Points for the player: {disp_player2}")
  p2_df = player_pt_total(ppr_df,disp_player2)
  #print(f"Points for {disp_player1} when Serving")
  pts1s_df = player_pt_total(ppr_df[ppr_df['serve_player']==disp_player1],disp_player1)
  #print(f"Points for {disp_player1} when Passing")
  pts1r_df = player_pt_total(ppr_df[ppr_df['pass_player']==disp_player1],disp_player1)
  #print(f"Points for {disp_player2} when Serving")
  pts2s_df = player_pt_total(ppr_df[ppr_df['serve_player']==disp_player2],disp_player2)
 # print(f"Points for {disp_player2} when Passing")
  pts2r_df = player_pt_total(ppr_df[ppr_df['pass_player']==disp_player2],disp_player2)
  
  if pts_df.shape[0] != 0:
    # now store the values into t scor_return array
    # point differential
    scor_table.at[0,'#'] = ( ( pts_df.at[0,'p_tsa'] + pts_df.at[0,'p_fbk'] + pts_df.at[0,'p_tk_s'] + pts_df.at[0,'p_tk_r'] + 
                               pts_df.at[0,'o_tse'] + pts_df.at[0,'o_fbe'] + pts_df.at[0,'o_te_s'] + pts_df.at[0,'o_te_r'] ) -
                             ( pts_df.at[0,'p_tse'] + pts_df.at[0,'p_fbe'] + pts_df.at[0,'p_te_s'] + pts_df.at[0,'p_te_s'] + 
                               pts_df.at[0,'o_tsa'] + pts_df.at[0,'o_fbk'] + pts_df.at[0,'o_tk_s'] + pts_df.at[0,'o_tk_r']  ) )
    scor_table.at[0,'%'] = scor_table.at[0,'#']/pts_df.at[0,'pts_total']
    scor_table.at[0,'%'] = str('{:.1%}'.format(scor_table.at[0,'%'])) 

    # Terminal Serves
    scor_table.at[1,'#'] = ( pts_df.at[0,'p_tsa'] + pts_df.at[0,'o_tse']) - ( pts_df.at[0,'o_tsa'] + pts_df.at[0,'p_tse'])
    scor_table.at[1,'%'] = scor_table.at[1,'#']/(pts_df.at[0,'p_tse']+pts_df.at[0,'p_tsa']+pts_df.at[0,'o_tse']+pts_df.at[0,'p_tsa'])
    scor_table.at[1,'%'] = str('{:.1%}'.format(scor_table.at[1,'%'])) 

    # live Rallies -- guessing all transition points??
    scor_table.at[2,'#'] =(( pts_df.at[0,'p_tk_s'] + pts_df.at[0,'p_tk_r'] + pts_df.at[0,'o_te_s'] + pts_df.at[0,'o_te_r']) -
                          ( pts_df.at[0,'p_te_s'] + pts_df.at[0,'p_te_r'] + pts_df.at[0,'o_tk_s'] + pts_df.at[0,'o_tk_r'] ))
    scor_table.at[2,"%"] = scor_table.at[2,'#'] / (( pts_df.at[0,'p_tk_s'] + pts_df.at[0,'p_tk_r'] + pts_df.at[0,'o_te_s'] + pts_df.at[0,'o_te_r']) +
                          ( pts_df.at[0,'p_te_s'] + pts_df.at[0,'p_te_r'] + pts_df.at[0,'o_tk_s'] + pts_df.at[0,'o_tk_r'] ))
    scor_table.at[2,'%'] = str('{:.1%}'.format(scor_table.at[2,'%'])) 

    # blank row
    # Side out = FBK + TK
    #----------------------------
    #... i think we have an issue here as we want TK only when we were served?  Or both TE and TK when we were served?
    #----------------------
    scor_table.at[3,'#'] = pts_df.at[0,'p_fbk'] + pts_df.at[0,'p_tk_r'] + pts_df.at[0,'o_te_s']
    scor_table.at[3,"%"] = scor_table.at[3,'#'] / (pts_df.at[0,'o_serves']-pts_df.at[0,'o_tse'] )
    scor_table.at[3,'%'] = str('{:.0%}'.format(scor_table.at[3,'%'])) 
    scor_table.at[4,'#'] = pts_df.at[0,'o_fbk'] + pts_df.at[0,'o_tk_r'] + pts_df.at[0,'p_te_s'] 
    scor_table.at[4,"%"] = scor_table.at[4,'#'] / (pts_df.at[0,'p_serves']-pts_df.at[0,'p_tse'] )
    scor_table.at[4,'%'] = str('{:.0%}'.format(scor_table.at[4,'%'])) 

    # first ball sideout - FBK/points
    scor_table.at[5,"#"] = pts_df.at[0,'p_fbk']
    if pts_df.at[0,'o_serves'] != 0:
      scor_table.at[5,"%"] = pts_df.at[0,'p_fbk']/(pts_df.at[0,'o_serves']-pts_df.at[0,'o_tse'])
      scor_table.at[5,'%'] = str('{:.0%}'.format(scor_table.at[5,'%'])) 
    scor_table.at[6,"#"] = pts_df.at[0,'o_fbk']
    if pts_df.at[0,'p_serves'] != 0:
      scor_table.at[6,"%"] = pts_df.at[0,'o_fbk']/(pts_df.at[0,'p_serves']-pts_df.at[0,'p_tse'])
      scor_table.at[6,'%'] = str('{:.0%}'.format(scor_table.at[6,'%'])) 

    # blank row
    # first ball stop - fbe / serves (attempts)
    scor_table.at[7,"#"] = pts_df.at[0,'o_fbe']
    scor_table.at[7,"%"] = pts_df.at[0,'o_fbe']/(pts_df.at[0,'p_serves'] - pts_df.at[0,'p_tse'])
    scor_table.at[7,'%'] = str('{:.0%}'.format(scor_table.at[7,'%'])) 
    scor_table.at[8,"#"] = pts_df.at[0,'p_fbe']
    scor_table.at[8,"%"] = pts_df.at[0,'p_fbe']/(pts_df.at[0,'o_serves'] - pts_df.at[0,'o_tse'])
    scor_table.at[8,'%'] = str('{:.0%}'.format(scor_table.at[8,'%'])) 

    # blank row
    # first ball win %
    scor_table.at[9,'#'] = ''
    scor_table.at[9,'%'] = ( pts_df.at[0,'p_fbk'] + pts_df.at[0,'o_fbe']) / (pts_df.at[0,'pts_total']-pts_df.at[0,'o_tse']-pts_df.at[0,'p_tse'])
    scor_table.at[9,'%'] = str('{:.0%}'.format(scor_table.at[9,'%'])) 

    # blank row
    #Transition Win - Number of transitiono points we won! and they won
    scor_table.at[10,'#'] = ( pts_df.at[0,'p_tk_s'] + pts_df.at[0,'p_tk_r']+ pts_df.at[0,'o_te_s']+ pts_df.at[0,'o_te_r'])
    scor_table.at[10,'%'] = scor_table.at[10,"#"] / (pts_df.at[0,'p_tk_r']+pts_df.at[0,'p_te_r']+pts_df.at[0,'p_tk_s']+pts_df.at[0,'p_te_s'] + pts_df.at[0,'o_tk_r']+pts_df.at[0,'o_te_r']+pts_df.at[0,'o_tk_s']+pts_df.at[0,'o_te_s'])
    scor_table.at[10,'%'] = str('{:.0%}'.format(scor_table.at[10,'%'])) 
    scor_table.at[11,'#'] = ( pts_df.at[0,'o_tk_s'] + pts_df.at[0,'o_tk_r']+ pts_df.at[0,'p_te_s']+ pts_df.at[0,'p_te_r'])
    scor_table.at[11,'%'] = scor_table.at[11,"#"] / (pts_df.at[0,'p_tk_r']+pts_df.at[0,'p_te_r']+pts_df.at[0,'p_tk_s']+pts_df.at[0,'p_te_s'] + pts_df.at[0,'o_tk_r']+pts_df.at[0,'o_te_r']+pts_df.at[0,'o_tk_s']+pts_df.at[0,'o_te_s'])
    scor_table.at[11,'%'] = str('{:.0%}'.format(scor_table.at[11,'%'])) 

    # digging ?? Need to look at this
    #scor_table.at[12,'#'] = ''
    #scor_table.at[12,'%'] = ''
    #scor_table.at[13,'#'] = ''
    #scor_table.at[13,'%'] = ''
    #scor_table.at[14,'#'] = ''
    #scor_table.at[14,'%'] = ''
    
    # Point Breakdowns
    scor_table.at[12,'#'] = (pts_df.at[0,'p_tsa']+pts_df.at[0,'o_tse']+pts_df.at[0,'o_tsa']+pts_df.at[0,'p_tse']) # terminal serve points
    scor_table.at[12,'%'] = scor_table.at[12,'#']/(pts_df.at[0,'pts_total'])
    scor_table.at[12,'%'] = str('{:.0%}'.format(scor_table.at[12,'%'])) 
    scor_table.at[13,'#'] = (pts_df.at[0,'p_fbk']+pts_df.at[0,'p_fbe']+pts_df.at[0,'o_fbk']+pts_df.at[0,'o_fbe']) # first ball points
    scor_table.at[13,'%'] = scor_table.at[13,'#']/(pts_df.at[0,'pts_total'])  
    scor_table.at[13,'%'] = str('{:.0%}'.format(scor_table.at[13,'%'])) 
    scor_table.at[14,'#'] = ((pts_df.at[0,'p_tk_s']+pts_df.at[0,'p_tk_r']+pts_df.at[0,'p_te_s']+pts_df.at[0,'p_te_r']) + 
                             (pts_df.at[0,'o_tk_s']+pts_df.at[0,'o_tk_r']+pts_df.at[0,'o_te_s']+pts_df.at[0,'o_te_r'])) # transition points
    scor_table.at[14,'%'] = scor_table.at[14,'#']/(pts_df.at[0,'pts_total'])  
    scor_table.at[14,'%'] = str('{:.0%}'.format(scor_table.at[14,'%'])) 
    
    # now create the markdown text to return
    scor_markdown = pd.DataFrame.to_markdown(scor_table, index = False )
  else:
    scor_markdown = "No Data Found"
    # So I think we are done:-)

  # now, next section, Rotations
  rot_dict = {'col1':[ disp_player1_team_num,'FB Wins','Ace','Opp FB Err','', # 1,2,3
                                'FB Loss', 'Srv Err','Opp FB Kill','',  # 4,5,6,7
                                'Trans Win', 'Kill','Opp Err','', #8,9,10,11
                                'Trans Loss','Att Err','Opp Kill','', #12,13,14,15
                                'Opp SO','Opp FBSO','Opp FB Stop','Trans Win'], #16,17,88,19
              'p1_s':['Serve',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,0],
              'col3':[ disp_player1_team_num,'FB Wins','Opp Srv Err','FB Kill','', # 1,2,3
                                'FB Loss', 'Opp Srv Ace','FB Err','',  # 4,5,6,7
                                'Trans Win', 'Kill','Opp Err','', #8,9,10,11
                                'Trans Loss','Att Err','Opp Kill','', #12,13,14,15
                                'Sideout','FBSO','FB Stop','Trans Win'], #16,17,88,19
              'p1_r':['Receive',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,0],
              'col5':[ disp_player2_team_num,'FB Wins','Ace','Opp FB Err','', # 1,2,3
                                'FB Loss', 'Srv Err','Opp FB Kill','',  # 4,5,6,7
                                'Trans Win', 'Kill','Opp Err','', #8,9,10,11
                                'Trans Loss','Att Err','Opp Kill','', #12,13,14,15
                                'Opp SO','Opp FBSO','Opp FB Stop','Trans Win'], #16,17,88,19
              'p2_s':['Serve',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,0],
              'col7':[ disp_player2_team_num,'FB Wins','Opp Srv Err','FB Kill','', # 1,2,3
                                'FB Loss', 'Opp Srv Ace','FB Err','',  # 4,5,6,7
                                'Trans Win', 'Kill','Opp Err','', #8,9,10,11
                                'Trans Loss','Att Err','Opp Kill','', #12,13,14,15
                                'Sideout','FBSO','FB Stop','Trans Win'], #16,17,88,19
              'p2_r':['Receive',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,'',0,0,0,0]
             }

  rot_table = pd.DataFrame.from_dict( rot_dict )
  #print(f"Pair Total Rotation Table : ppr_df size:{ppr_df.shape[0]}")
  
  # first ball wins
  rot_table.at[1,'p1_s'] = p1_df.at[0,'o_fbe'] + p1_df.at[0,'p_tsa']
  rot_table.at[1,'p1_r'] = p1_df.at[0,'p_fbk'] + p1_df.at[0,'o_tse']
  rot_table.at[1,'p2_s'] = p2_df.at[0,'o_fbe'] + p2_df.at[0,'p_tsa']
  rot_table.at[1,'p2_r'] = p2_df.at[0,'p_fbk'] + p2_df.at[0,'o_tse']
  # Service Ace/Error
  rot_table.at[2,'p1_s'] = p1_df.at[0,'p_tsa']
  rot_table.at[2,'p1_r'] = p1_df.at[0,'o_tse']
  rot_table.at[2,'p2_s'] = p2_df.at[0,'p_tsa']
  rot_table.at[2,'p2_r'] = p2_df.at[0,'o_tse']
  # Frist Ball Kill/Error
  rot_table.at[3,'p1_s'] = p1_df.at[0,'o_fbe']
  rot_table.at[3,'p1_r'] = p1_df.at[0,'p_fbk']
  rot_table.at[3,'p2_s'] = p2_df.at[0,'o_fbe']
  rot_table.at[3,'p2_r'] = p2_df.at[0,'p_fbk']
  
  # first ball loss
  rot_table.at[5,'p1_s'] = p1_df.at[0,'p_tse'] + p1_df.at[0,'o_fbk']
  rot_table.at[5,'p1_r'] = p1_df.at[0,'o_tsa'] + p1_df.at[0,'p_fbe']
  rot_table.at[5,'p2_s'] = p2_df.at[0,'p_tse'] + p2_df.at[0,'o_fbk']
  rot_table.at[5,'p2_r'] = p2_df.at[0,'o_tsa'] + p2_df.at[0,'p_fbe']
  # Service Ace/Error
  rot_table.at[6,'p1_s'] = p1_df.at[0,'p_tse']
  rot_table.at[6,'p1_r'] = p1_df.at[0,'o_tsa']
  rot_table.at[6,'p2_s'] = p2_df.at[0,'p_tse']
  rot_table.at[6,'p2_r'] = p2_df.at[0,'o_tsa']
  # Frist Ball Kill/Error
  rot_table.at[7,'p1_s'] = p1_df.at[0,'o_fbk']
  rot_table.at[7,'p1_r'] = p1_df.at[0,'p_fbe']
  rot_table.at[7,'p2_s'] = p2_df.at[0,'o_fbk']
  rot_table.at[7,'p2_r'] = p2_df.at[0,'p_fbe']  
  
  # Transition Wins
  rot_table.at[9,'p1_s'] = p1_df.at[0,'p_tk_s'] + p1_df.at[0,'o_te_r']
  rot_table.at[9,'p1_r'] = p1_df.at[0,'p_tk_r'] + p1_df.at[0,'o_te_s']
  rot_table.at[9,'p2_s'] = p2_df.at[0,'p_tk_s'] + p2_df.at[0,'o_te_r']
  rot_table.at[9,'p2_r'] = p2_df.at[0,'p_tk_r'] + p2_df.at[0,'o_te_s']
  # Trans Ace/Error - Player
  rot_table.at[10,'p1_s'] = p1_df.at[0,'p_tk_s']
  rot_table.at[10,'p1_r'] = p1_df.at[0,'p_tk_r']
  rot_table.at[10,'p2_s'] = p2_df.at[0,'p_tk_s']
  rot_table.at[10,'p2_r'] = p2_df.at[0,'p_tk_r']
  # Trans Ball Kill/Error - Opponent
  rot_table.at[11,'p1_s'] = p1_df.at[0,'o_te_r']
  rot_table.at[11,'p1_r'] = p1_df.at[0,'o_te_s']
  rot_table.at[11,'p2_s'] = p2_df.at[0,'o_te_r']
  rot_table.at[11,'p2_r'] = p2_df.at[0,'o_te_s']  

  # Transition Losses
  rot_table.at[13,'p1_s'] = p1_df.at[0,'p_te_s'] + p1_df.at[0,'o_tk_r']
  rot_table.at[13,'p1_r'] = p1_df.at[0,'p_te_r'] + p1_df.at[0,'o_tk_s']
  rot_table.at[13,'p2_s'] = p2_df.at[0,'p_te_s'] + p2_df.at[0,'o_tk_r']
  rot_table.at[13,'p2_r'] = p2_df.at[0,'p_te_r'] + p2_df.at[0,'o_tk_s']
  # Trans Ace/Error - Player
  rot_table.at[14,'p1_s'] = p1_df.at[0,'p_te_s']
  rot_table.at[14,'p1_r'] = p1_df.at[0,'p_te_r']
  rot_table.at[14,'p2_s'] = p2_df.at[0,'p_te_s']
  rot_table.at[14,'p2_r'] = p2_df.at[0,'p_te_r']
  # Trans Ball Kill/Error - Opponent
  rot_table.at[15,'p1_s'] = p1_df.at[0,'o_tk_r']
  rot_table.at[15,'p1_r'] = p1_df.at[0,'o_tk_s']
  rot_table.at[15,'p2_s'] = p2_df.at[0,'o_tk_r']
  rot_table.at[15,'p2_r'] = p2_df.at[0,'o_tk_s'] 



  # Opp SO, our Side Out - All %
  # (Opp FBK + opp tk + fbe + te)/(pair serves - serve errors )
  # Opponent Sideoout
  rot_table.at[17,'p1_s'] = ( pts1s_df.at[0,'o_fbk'] + pts1s_df.at[0,'o_tk_r'] + pts1s_df.at[0,'p_te_s'] )/(pts1s_df.at[0,'p_serves']-pts1s_df.at[0,'p_tse'])
  rot_table.at[17,'p1_s'] = str('{:.0%}'.format(rot_table.at[17,'p1_s']))
  # Pair Sideout
  rot_table.at[17,'p1_r'] = ( pts1r_df.at[0,'p_fbk'] + pts1r_df.at[0,'p_tk_r'] + pts1r_df.at[0,'o_te_s'] )/(pts1r_df.at[0,'o_serves']-pts1r_df.at[0,'o_tse'])
  rot_table.at[17,'p1_r'] = str('{:.0%}'.format(rot_table.at[17,'p1_r']))

  rot_table.at[17,'p2_s'] = ( pts2s_df.at[0,'o_fbk'] + pts2s_df.at[0,'o_tk_s'] + pts2s_df.at[0,'o_tk_r'] + pts2s_df.at[0,'p_fbe'] + pts2s_df.at[0,'p_te_s'] + pts2s_df.at[0,'p_te_r'])/(pts2s_df.at[0,'p_serves']-pts2s_df.at[0,'p_tse'])
  rot_table.at[17,'p2_s'] = str('{:.0%}'.format(rot_table.at[17,'p2_s']))
  rot_table.at[17,'p2_r'] = ( pts2r_df.at[0,'p_fbk'] + pts2r_df.at[0,'p_tk_s'] + pts2r_df.at[0,'p_tk_r'] + pts2r_df.at[0,'o_fbe'] + pts2r_df.at[0,'o_te_s'] + pts2r_df.at[0,'o_te_r'])/(pts2r_df.at[0,'o_serves']-pts2r_df.at[0,'o_tse'])
  rot_table.at[17,'p2_r'] = str('{:.0%}'.format(rot_table.at[17,'p2_r']))
  
  # opp FBSO, our FBSO : fbk/(serves-serve errors)
  rot_table.at[18,'p1_s'] = ( pts1s_df.at[0,'o_fbk'])/(pts1s_df.at[0,'p_serves']-pts1s_df.at[0,'p_tse'])
  rot_table.at[18,'p1_s'] = str('{:.0%}'.format(rot_table.at[18,'p1_s']))
  rot_table.at[18,'p1_r'] = ( pts1r_df.at[0,'p_fbk'])/(pts1r_df.at[0,'o_serves']-pts1r_df.at[0,'o_tse'])
  rot_table.at[18,'p1_r'] = str('{:.0%}'.format(rot_table.at[18,'p1_r']))

  rot_table.at[18,'p2_s'] = ( pts2s_df.at[0,'o_fbk'])/(pts2s_df.at[0,'p_serves']-pts2s_df.at[0,'p_tse'])
  rot_table.at[18,'p2_s'] = str('{:.0%}'.format(rot_table.at[18,'p2_s']))
  rot_table.at[18,'p2_r'] = ( pts2r_df.at[0,'p_fbk'])/(pts2r_df.at[0,'o_serves']-pts2r_df.at[0,'o_tse'])
  rot_table.at[18,'p2_r'] = str('{:.0%}'.format(rot_table.at[18,'p2_r']))
  
  # opp FB Stop, our FB Stop
  rot_table.at[19,'p1_s'] = ( pts1s_df.at[0,'o_fbe'])/(pts1s_df.at[0,'p_serves']-pts1s_df.at[0,'p_tse'])
  rot_table.at[19,'p1_s'] = str('{:.0%}'.format(rot_table.at[19,'p1_s']))
  rot_table.at[19,'p1_r'] = ( pts1r_df.at[0,'p_fbe'])/(pts1r_df.at[0,'o_serves']-pts1r_df.at[0,'o_tse'])
  rot_table.at[19,'p1_r'] = str('{:.0%}'.format(rot_table.at[19,'p1_r']))

  rot_table.at[19,'p2_s'] = ( pts2s_df.at[0,'o_fbe'])/(pts2s_df.at[0,'p_serves']-pts2s_df.at[0,'p_tse'])
  rot_table.at[19,'p2_s'] = str('{:.0%}'.format(rot_table.at[19,'p2_s']))
  rot_table.at[19,'p2_r'] = ( pts2r_df.at[0,'p_fbe'])/(pts2r_df.at[0,'o_serves']-pts2r_df.at[0,'o_tse'])
  rot_table.at[19,'p2_r'] = str('{:.0%}'.format(rot_table.at[19,'p2_r']))
  
  # opp trans win, our trans win
  rot_table.at[20,'p1_s'] = ( pts1s_df.at[0,'o_tk_s']+pts1s_df.at[0,'o_tk_r']+pts1s_df.at[0,'p_te_s']+pts1s_df.at[0,'p_te_r']
                            ) / (
                            pts1s_df.at[0,'o_tk_s']+pts1s_df.at[0,'o_tk_r']+pts1s_df.at[0,'o_te_s']+pts1s_df.at[0,'o_te_r'] + 
                            pts1s_df.at[0,'p_tk_s']+pts1s_df.at[0,'p_tk_r']+pts1s_df.at[0,'p_te_s']+pts1s_df.at[0,'p_te_r'])
  rot_table.at[20,'p1_s'] = str('{:.0%}'.format(rot_table.at[20,'p1_s']))
  rot_table.at[20,'p1_r'] = ( pts1r_df.at[0,'o_tk_s']+pts1r_df.at[0,'o_tk_r']+pts1r_df.at[0,'p_te_s']+pts1r_df.at[0,'p_te_r']
                            ) / (
                            pts1r_df.at[0,'o_tk_s']+pts1r_df.at[0,'o_tk_r']+pts1r_df.at[0,'o_te_s']+pts1r_df.at[0,'o_te_r'] + 
                            pts1r_df.at[0,'p_tk_s']+pts1r_df.at[0,'p_tk_r']+pts1r_df.at[0,'p_te_s']+pts1r_df.at[0,'p_te_r'])
  rot_table.at[20,'p1_r'] = str('{:.0%}'.format(rot_table.at[20,'p1_r']))

  rot_table.at[20,'p2_s'] = ( pts2s_df.at[0,'o_tk_s']+pts2s_df.at[0,'o_tk_r']+pts2s_df.at[0,'p_te_s']+pts2s_df.at[0,'p_te_r']
                            ) / (
                            pts2s_df.at[0,'o_tk_s']+pts2s_df.at[0,'o_tk_r']+pts2s_df.at[0,'o_te_s']+pts2s_df.at[0,'o_te_r'] + 
                            pts2s_df.at[0,'p_tk_s']+pts2s_df.at[0,'p_tk_r']+pts2s_df.at[0,'p_te_s']+pts2s_df.at[0,'p_te_r'])
  rot_table.at[20,'p2_s'] = str('{:.0%}'.format(rot_table.at[20,'p2_s']))
  rot_table.at[20,'p2_r'] = ( pts2r_df.at[0,'o_tk_s']+pts2r_df.at[0,'o_tk_r']+pts2r_df.at[0,'p_te_s']+pts2r_df.at[0,'p_te_r']
                            ) / (
                            pts2r_df.at[0,'o_tk_s']+pts2r_df.at[0,'o_tk_r']+pts2r_df.at[0,'o_te_s']+pts2r_df.at[0,'o_te_r'] + 
                            pts2r_df.at[0,'p_tk_s']+pts2r_df.at[0,'p_tk_r']+pts2r_df.at[0,'p_te_s']+pts2r_df.at[0,'p_te_r'])
  rot_table.at[20,'p2_r'] = str('{:.0%}'.format(rot_table.at[20,'p2_r']))
  
  rot_markdown = pd.DataFrame.to_markdown(rot_table, index = False, headers = 'firstrow')

  # now, next section, Rotations
  stat_dict = {'col1':[ disp_player1_team_num,'Serving KO %','Aces','Bad Pass', 'Good Pass','Serve Error ', 
                                'Term Srv', 
                                #'Digging %','Digs','Dig Err', 
                                #'Block Ratio', 'Blocks','Block Errors',
                                'Passing %',
                                'Bad Pass','Good Pass','Opp Srv Err',
                                'First Ball Eff','Kills','Att Error','Continues',
                                'Make Them Pay',
                                'Trans Eff','Kills','Atk Err','Continues'],
              'p1':['',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              'col3':[ disp_player2_team_num,'Serving KO %','Aces','Bad Pass', 'Good Pass','Serve Error ', 
                                'Term Srv', 
                                #'Digging %','Digs','Dig Err', 
                                #'Block Ratio', 'Blocks','Block Errors',
                                'Passing %',
                                'Bad Pass','Good Pass','Opp Srv Err',
                                'First Ball Eff','Kills','Att Error', 'Continues',
                                'Make Them Pay',
                                'Trans Eff','Kills','Atk Err','Continues'],
              'p2':['',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
             }

  stat_table = pd.DataFrame.from_dict( stat_dict )

  # serve knock out
  stat_table.at[1,'p1'] = (pts1s_df.at[0,'p_tsa'] + pts1s_df.at[0,'o_bad_pass'])/(pts1s_df.at[0,'p_serves'])
  stat_table.at[1,'p1']  = str('{:0.0%}'.format(stat_table.at[1,'p1']))
  stat_table.at[1,'p2'] = (pts2s_df.at[0,'p_tsa'] + pts2s_df.at[0,'o_bad_pass'])/(pts2s_df.at[0,'p_serves'])
  stat_table.at[1,'p2']  = str('{:0.0%}'.format(stat_table.at[1,'p2']))

  # Aces
  stat_table.at[2,'p1'] = pts1s_df.at[0,'p_tsa'] 
  stat_table.at[2,'p2'] = pts2s_df.at[0,'p_tsa']
  
  # bad Pass result when serving
  stat_table.at[3,'p1'] = pts1s_df.at[0,'p_bad_pass'] 
  stat_table.at[3,'p2'] = pts2s_df.at[0,'p_bad_pass']
  
  # good Pass, result when serving
  stat_table.at[4,'p1'] = pts1s_df.at[0,'p_good_pass'] 
  stat_table.at[4,'p2'] = pts2s_df.at[0,'p_good_pass']
  
  # service errrors
  stat_table.at[5,'p1'] = pts1s_df.at[0,'p_tse'] 
  stat_table.at[5,'p2'] = pts2s_df.at[0,'p_tse']

  # Terminal Serves, My aces plus my errors divided by my number of serves
  stat_table.at[6,'p1'] = (pts1s_df.at[0,'p_tsa'] + pts1s_df.at[0,'p_tse'])/(pts1s_df.at[0,'p_serves'])
  stat_table.at[6,'p1']  = str('{:0.0%}'.format(stat_table.at[6,'p1']))
  stat_table.at[6,'p2'] = (pts2s_df.at[0,'p_tsa'] + pts2s_df.at[0,'p_tse'])/(pts2s_df.at[0,'p_serves'])
  stat_table.at[6,'p2']  = str('{:0.0%}'.format(stat_table.at[6,'p2']))

  # Digging ... we don't really have this
  # three lines here

  # Blocking, also don't have this, three more lines

  # Passing, percent is good passes plus opp service errors divided by serves received
  stat_table.at[7,'p1'] = (pts1r_df.at[0,'o_good_pass'])/(pts1r_df.at[0,'o_serves']-pts1r_df.at[0,'o_tse'])
  stat_table.at[7,'p1']  = str('{:0.0%}'.format(stat_table.at[7,'p1']))
  stat_table.at[7,'p2'] = (pts2r_df.at[0,'o_good_pass'])/(pts2r_df.at[0,'o_serves']-pts2r_df.at[0,'o_tse'])
  stat_table.at[7,'p2']  = str('{:0.0%}'.format(stat_table.at[7,'p2']))

  # aced (hal fof total?  Or jsut leave it along right now)
  
  # bad pass
  stat_table.at[8,'p1'] = (pts1r_df.at[0,'o_bad_pass'])
  stat_table.at[8,'p2'] = (pts2r_df.at[0,'o_bad_pass'])

  # good pass
  stat_table.at[9,'p1'] = (pts1r_df.at[0,'o_good_pass'])
  stat_table.at[9,'p2'] = (pts2r_df.at[0,'o_good_pass'])

  # opp serve err
  stat_table.at[10,'p1'] = (pts1r_df.at[0,'o_tse'])
  stat_table.at[10,'p2'] = (pts2r_df.at[0,'o_tse'])

  # callfbnhe for the two players
  fbhe_vector1 = fbhe(ppr_df,disp_player1,'att', 'N')
  fbhe_vector2 = fbhe(ppr_df,disp_player2,'att', 'N')

  # 23 - FBHE
  stat_table.at[11,'p1'] = fbhe_vector1[0]
  stat_table.at[11,'p2'] = fbhe_vector2[0]
  
  # 24 - kills
  stat_table.at[12,'p1'] = fbhe_vector1[1]
  stat_table.at[12,'p2'] = fbhe_vector2[1]
  
  # 25 - Errors
  stat_table.at[13,'p1'] = fbhe_vector1[2]
  stat_table.at[13,'p2'] = fbhe_vector2[2]

  # 26 - Continues
  stat_table.at[14,'p1'] = fbhe_vector1[3] - fbhe_vector1[1] - fbhe_vector1[2]
  stat_table.at[14,'p2'] = fbhe_vector2[3] - fbhe_vector2[1] - fbhe_vector2[2]
  
  # make them play (fbk + tk + tsa / points) - Let's call error density for each player
  # make them play is the inverse of erropr density, 1 - ed
  #print(f"ppr size and display players: {ppr_df.shape[0]},{disp_player1},{disp_player2}")
  error_vector_p1 = calc_error_den(ppr_df, disp_player1)
  error_vector_p2 = calc_error_den(ppr_df, disp_player2)
  stat_table.at[15,'p1'] = 100 - float(error_vector_p1[0][:-1])
  #stat_table.at[28,'p1'] = str('{:0.0%}'.format(stat_table.at[28,'p1']))
  stat_table.at[15,'p2'] = 100 - float(error_vector_p2[0][:-1])
  #stat_table.at[28,'p2'] = str('{:0.0%}'.format(stat_table.at[28,'p2']))
  
  # transition conversion
  tcr_v1 = calc_trans(ppr_df, disp_player1, 'srv')
  tcr_v2 = calc_trans(ppr_df, disp_player2, 'srv')
  #print(f"TCR V1: {tcr_v1}, TCR V2 : {tcr_v2}")
  
  # 30 trans efficiency
  if tcr_v1[9] != 0 :
    stat_table.at[16,'p1'] = (tcr_v1[3]-tcr_v1[6])/tcr_v1[9] 
  else:
    stat_table.at[16,'p1'] = 0
  stat_table.at[16,'p1'] = str('{:0.0%}'.format(stat_table.at[16,'p1']))
  if tcr_v2[9] != 0:
    stat_table.at[16,'p2'] = (tcr_v2[3]-tcr_v2[6])/tcr_v2[9]
  else:
    tcr_v2[0] = 0
  stat_table.at[16,'p2'] = str('{:0.0%}'.format(stat_table.at[16,'p2']))
  
  # 31 trans kills
  stat_table.at[17,'p1'] = tcr_v1[3]
  stat_table.at[17,'p2'] = tcr_v2[3]
  
  # 32 trans errors
  stat_table.at[18,'p1'] = tcr_v1[6]
  stat_table.at[18,'p2'] = tcr_v2[6]  

  # 33 trans continues
  stat_table.at[19,'p1'] = tcr_v1[9] - tcr_v1[6] - tcr_v1[3]
  stat_table.at[19,'p2'] = tcr_v2[9] - tcr_v2[6] - tcr_v2[3]
  
  stat_markdown = pd.DataFrame.to_markdown(stat_table, index = False,  headers = 'firstrow' )

  # for consistency with other pair reports, so we can use the same pdf generation routine, we need to return either field.  This one only uses 3, but we need to pass 8
  return scor_markdown,  rot_markdown, stat_markdown, '','','','',''
  

#-=-=-=-=-=-=-=-=--==-=-======-=---==----------------------------------
#
#          Strengths and Weaknesses for a Pair
#
#-------------++---------+_+_+_-----------------------------------------
@anvil.server.callable
def pair_sw_report(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  #

  '''
  ## Currently do not need to load the ppr tables, all this data is coming from the s_w media object(s) in the master pair table
  
  disp_player1, disp_player2 = pair_players(disp_pair) # looks iinto master_pairs to get player 1 and 2 for the given pair
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest
  '''
  
  # find the pair in the master pair file, get player1, player2, sw_player1, sw_player2
  pair_row = fetch_pair_row(disp_league,disp_gender,disp_year,disp_pair)
  if type(pair_row) == str():
    return 'Failed to find Pair'+pair_row

  # now we need to make the markup rtf files for each player
  for d_player in [ pair_row['player1'],pair_row['player2'] ]:
    
    # which player are we reporting on? 1 or 2?
    if d_player.strip() == pair_row['player1'].strip():
      sw_field = 's_w_player1'
      player_num = 1
    elif d_player.strip() == pair_row['player2'].strip():
      sw_field = 's_w_player2'
      player_num = 2
    else:
      print(f"pair_sw_report : Display Player not one of the pairs: {disp_pair}, {pair_row['player1']}, {pair_row['player2']}")

    #print(f"pair_sw_report:  pair in question: {pair_row['pair']}, sw_field = {sw_field} Player_num = {player_num}, d_player : {d_player}")
    # check if this pair has sw_df's in thesw_field
    if pair_row[sw_field]:
      sw_df = pd.read_csv(io.BytesIO( pair_row[sw_field].get_bytes()))
    else:
      sw_df = 'No Strenghts and Weakness Data Found'
      
    # now open the pair_data file and get the row, and get the row from the pair_stats file
    pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
    #print(f"pair_sw_report: Pair Data and Pair stats {pair_data_df}, {pair_stats_df}")
    #pair_data_df = pair_data_df[(pair_data_df['pair'] == disp_pair) & (pair_data_df['player'] == disp_player) ]
    pair_data_index = pair_data_df.loc[ (pair_data_df['pair'] == disp_pair) & (pair_data_df['player'] == disp_player) ].index[0]
    #print(f"pair_sw_report: pair_data_df index : {pair_data_index}")

    #------------ Offense ------------------------------
    # create the offense header text including FBHE and percentile of FBHE
    # not currently doing this, displaying the header, using a field in the sw table for this purpose
    #p_fbhe = pair_data_df.loc[pair_data_index,'fbhe']
    #print(f"pair_sw_report: pair_data fbhe : {p_fbhe}, {pair_data_df.loc[pair_data_index,'pair']}, {pair_data_df.loc[pair_data_index,'player']}")
    #p_fbhe_per = stats.norm.cdf( (pair_data_df.loc[pair_data_index,'fbhe'] - pair_stats_df.at[0,'fbhe_mean'])/pair_stats_df.at[0,'fbhe_stdev'] )
    #p_att_txt = "Offense, Attacking & Passing : " + disp_player + "`s FBHE="+ str(pair_data_df.loc[pair_data_index,'fbhe']) + ", Percentile=" + str(p_fbhe_per)
    #print(f"pair_sw_report: player attack text: {p_att_txt}")

    # now calculate the Offense strength and weakness markdown
    off_df = sw_df[ sw_df['Section'] == 'Offense'] 
    off_df = off_df.sort_values(by='Category', ascending=True, na_position='last')
    #print(f"pair_sw_report: off_df: {off_df}")
    off_df = off_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    off_df = off_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
    #off_df['Mean'] = pair_stats_df.at[0,'fbhe_mean']
    #print(f"offense table for S&W: {off_df}")
    #off_df = off_df.drop([0], axis = 1 )
    #off_mkdn = pd.DataFrame.to_markdown(off_df, index = False)

    # now calculate the Deffense strength and weakness markdown
    def_df = sw_df[ sw_df['Section'] == 'Serving'] 
    def_df = def_df.sort_values(by='Category', ascending=True, na_position='last')
    def_df = def_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    def_df = def_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
    #print(f"Def df: {def_df}")
    #def_mkdn = pd.DataFrame.to_markdown(def_df, index = False)

    # now calculate the Errors strength and weakness markdown
    err_df = sw_df[ sw_df['Section'] == 'Error & Transitison'] 
    err_df = err_df.sort_values(by='Category', ascending=True, na_position='last')
    err_df = err_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    err_df = err_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
    #err_mkdn = pd.DataFrame.to_markdown(err_df, index = False)

    # now calculate the COnsistency strength and weakness markdown
    con_df = sw_df[ sw_df['Section'] == 'Consistency'] 
    con_df = con_df.sort_values(by='Category', ascending=True, na_position='last')
    con_df = con_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    con_df = con_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
    #con_mkdn = pd.DataFrame.to_markdown(con_df, index = False)

    # last step, convert the DF into mkdn for each player
    if player_num == 1:
      off1_mkdn = pd.DataFrame.to_markdown(off_df, index = False)
      def1_mkdn = pd.DataFrame.to_markdown(def_df, index = False)
      err1_mkdn = pd.DataFrame.to_markdown(err_df, index = False)
      con1_mkdn = pd.DataFrame.to_markdown(con_df, index = False)
    else:
      off2_mkdn = pd.DataFrame.to_markdown(off_df, index = False)
      def2_mkdn = pd.DataFrame.to_markdown(def_df, index = False)
      err2_mkdn = pd.DataFrame.to_markdown(err_df, index = False)
      con2_mkdn = pd.DataFrame.to_markdown(con_df, index = False)
    

  return off1_mkdn, def1_mkdn, err1_mkdn, con1_mkdn, off2_mkdn, def2_mkdn, err2_mkdn, con2_mkdn, 
  


  
#-------------------------------------------------------
#
#         Pair Report Stub
#
#--------------------------------------------------------
@anvil.server.callable
def pair_rpt_stub(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  
  # note, added disp_pair in the arguments

  disp_player1, disp_player2 = pair_players(disp_pair) # looks iinto master_pairs to get player 1 and 2 for the given pair
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest

  # create the output dataframe
  df_dict = {' ':['FBHE','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,' '],
             'Zone 1':[0,0,0,0,' '],
             "Zone 2":[0,0,0,0,' '],
             'Zone 3':[0,0,0,0,' '],
             'Zone 4':[0,0,0,0,' '],
             'Zone 5':[0,0,0,0,' ']
            }
  pair_table = pd.DataFrame.from_dict( df_dict )
  p1_table = pd.DataFrame.from_dict( df_dict )
  p2_table = pd.DataFrame.from_dict( df_dict )
  
  # calcualte for player 1 - All attempts
  fbhe_vector = fbhe(ppr_df,disp_player1,'att',True)
  p1_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p1_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p1_table.at[2,'All'] = fbhe_vector[2]  # errors
  p1_table.at[3,'All'] = fbhe_vector[3]  # attempts
  # ignorinig the confienence interval
  p1_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calcualte for player 2 - all attempts
  fbhe_vector = fbhe(ppr_df,disp_player2,'att',True)
  p2_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p2_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p2_table.at[2,'All'] = fbhe_vector[2]  # errors
  p2_table.at[3,'All'] = fbhe_vector[3]  # attempts
  # ignore confidence internval
  p2_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_table.at[1,'All'] = p1_table.at[1,'All'] + p2_table.at[2,'All']
  pair_table.at[2,'All'] = p1_table.at[2,'All'] + p2_table.at[2,'All']
  pair_table.at[3,'All'] = p1_table.at[3,'All'] + p2_table.at[3,'All']
  pair_table.at[0,'All'] = (pair_table.at[1,'All'] - pair_table.at[2,'All']) / (pair_table.at[3,'All'])
  pair_table.at[0,'All'] = float("{:.3f}".format(pair_table.at[0,'All']))

  # calculate for zones 1 - 5
  column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
  for i in [1,2,3,4,5]:
    # calculate for player 1
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player1, 'att', True )
    p1_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p1_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p1_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p1_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p1_table.at[4,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for player 2
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player2, 'att', True )
    p2_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p2_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p2_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p2_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p2_table.at[4,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for pair
    pair_table.at[1,column[i-1]] = p1_table.at[1,column[i-1]] + p2_table.at[1,column[i-1]]
    pair_table.at[2,column[i-1]] = p1_table.at[2,column[i-1]] + p2_table.at[2,column[i-1]]
    pair_table.at[3,column[i-1]] = p1_table.at[3,column[i-1]] + p2_table.at[3,column[i-1]]
    pair_table.at[0,column[i-1]] = (pair_table.at[1,column[i-1]] - pair_table.at[2,column[i-1]]) / (pair_table.at[3,column[i-1]])
    pair_table.at[0,column[i-1]] = float("{:.3f}".format(pair_table.at[0,column[i-1]]))

    # So I think we are done:-)
  # now, turn them all into markup
  pair_markdown = pd.DataFrame.to_markdown(pair_table, index = False )
  p1_markdown = pd.DataFrame.to_markdown(p1_table, index = False )
  p2_markdown = pd.DataFrame.to_markdown(p2_table, index = False )
  
  return pair_markdown, p1_markdown, p2_markdown

#
#        Opponent's FBHE and FBSO when each player is serving
#
#.        Return:
@anvil.server.callable
def pair_serving_effectiveness(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  # kind of mergin the two below to getone page with all of it

  a = ''
  b = '' # placeholders for the call to srv_eff

  # make sure we get player 1
  player1, player2 = pair_players( disp_pair)
  p1_srv_eff_mkdn, a, b = srv_eff(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, player1,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               )
  
  p2_srv_eff_mkdn, a, b = srv_eff(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, player2,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               )

  return p1_srv_eff_mkdn, '', '', '', p2_srv_eff_mkdn, '','',''


@anvil.server.callable
def pair_tri_score(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               ):
  
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  tri_df, tri_data_found = get_tri_data( disp_league, disp_gender, disp_year, date_checked, disp_start_date, disp_end_date )
  #print(f" Data Found? {tri_data_found}m=, records={tri_df.shape[0]}, Displayer Player: {disp_player}")
  
  if (tri_df.shape[0] == 0):
    return "No Triangle Scoring Data Found"

  disp_pair = disp_pair.strip()
  tri1_df = tri_df[ tri_df['teama'].str.strip() == disp_pair ]
  tri2_df = tri_df[ tri_df['teamb'].str.strip() == disp_pair ]


  tri_df = pd.concat([ tri1_df, tri2_df ])
  #print(f"tri_df size:{tri_df.shape[0]}, Tri1 = {tri1_df.shape[0]}, Tri2 = {tri2_df.shape[0]}, Tri3 = {tri3_df.shape[0]}, Tri4 = {tri4_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  tri_dict = {'Team A':[' '],
             'Team B':[' '],
              'Winner':[' '],
              'Set':[0],
             'TS +/-':[0],
             'FB +/-':[0],
             'Tran +/-':[0],
             'FBHE':[0],
              'FBHE Diff':[0],
             'TCR':[0],
             'Err Den':[0]
            }
  tri_table = pd.DataFrame.from_dict( tri_dict )


  ############### Third Populate the dataframe, assuming we have data returned
  num_row = tri_df.shape[0]
  #print(f"Number of Rows in Tri Data:{num_row}")
  #print(tri_df)
  i = 0
  for index, row in tri_df.iterrows():
    #print(f"i: {i}, Tri Table:{tri_table}")

    if ( i != 0):
      # need to add a row to tri_table
      tri_table.loc[len(tri_table.index)] = tri_dict
      
    disp_team = row['teama'] if disp_player.strip() in row['teama'].strip() else row['teamb']
    tri_table.at[i,'Team A'] = row['teama']
    tri_table.at[i,'Team B'] = row['teamb']
    tri_table.at[i,'Winner'] = row['winning_team']
    tri_table.at[i,'Set'] = row['set']
    tri_table.at[i,'TS +/-'] = row['tsrv_adv_a'] if disp_team.strip() in row['teama'].strip() else -row['tsrv_adv_a']
    tri_table.at[i,'FB +/-'] = row['fb_adv_a']if disp_team.strip() in row['teama'].strip()  else -row['fb_adv_a']
    tri_table.at[i,'Tran +/-'] = '{:.3}'.format(row['tran_adv_a']) if disp_team.strip() in row['teama'].strip()  else '{:.3}'.format(-row['tran_adv_a'])
    tri_table.at[i,'FBHE'] = '{:.3}'.format(row['fbhe_a_noace']) if disp_team.strip() in row['teama'].strip()  else '{:.3}'.format(-row['fbhe_b_noace'])
    tri_table.at[i,'FBHE Diff'] = '{:.3}'.format(row['fbhe_diff_noace']) if disp_team.strip() in row['winning_team'].strip() else '{:.3}'.format(-row['fbhe_diff_noace'])
    tri_table.at[i,'TCR'] = '{:.1%}'.format(row['tcr_a']) if disp_team.strip() in row['teama'].strip()  else '{:.1%}'.format(row['tcr_b'])
    tri_table.at[i,'Err Den'] = '{:.1%}'.format(row['err_den_a']) if disp_team.strip() in row['teama'].strip()  else '{:.1%}'.format(row['err_den_b'])
    i = i + 1
    
  # now create the markdown text to return
  tri_return = pd.DataFrame.to_markdown(tri_table, index = False )
  
  return tri_return, ' ', ' ',' ', ' ',' ', ' ',' '


@anvil.server.callable
def pair_team_change_overtime(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair, disp_player,
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):
  
  # note, added disp_pair in the arguments

  disp_player1, disp_player2 = pair_players(disp_pair) # looks iinto master_pairs to get player 1 and 2 for the given pair
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on

  # for this, we want only data for this 'team'
  ppr_df = pair_team_filter(ppr_df,disp_team)
  ppr_df_filter = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  # we should not have all data and filtered data for the disp_team
  print(f" got ppr data, all: {ppr_df.shape[0]}, filter: {ppr_df_filter.shape[0]} filtered pass player: {ppr_df_filter['pass_player']}")

  # create the output dataframe
  df_dict = {'Pair':str(),
             'Player':str(),
             'FBHE-All':[0.0],
             'FBHE-Recent':[0.0],
             'FBHE-Diff':[0.0],
             'Points-All':[0],
             'Points-Recent':[0],
             'Points-Diff':[0],
             'KnockOut-All':[0],
             'KnockOut-Recent':[0],
             'KnockOut-Diff':[0],
             'Passing-All':[0],
             'Passing-Recent':[0],
             'Passing-Diff':[0]
            }
  perf_table = pd.DataFrame.from_dict( df_dict )

  # now start a loop over the pairs in the ppr_df_filter dataframe
  # get a list of pairs in the ppr_df_filter df:
  pair_list = pair_team_list(ppr_df_filter, disp_team)
  print(f"Pair list size: {pair_list.shape[0]}")

  index = 0
  for row in pair_list.itertuples():
    #print(f"Pair List: {pair_list}, Pair Row {row}")
    print(f"pair row in pair list: team: {row.team}, Player 1: {row.player1}, Player 2: {row.player2}")

    # limit the two ppr_df's to just this pair data
    pair_ppr_df_all = pair_filter(ppr_df, row.team)
    pair_ppr_df_filter = pair_filter(ppr_df_filter, row.team)
    print(f" ppr data, filter for: {row.team}, all: {pair_ppr_df_all.shape[0]}, filter: {pair_ppr_df_filter.shape[0]}, filtered pass player: {ppr_df_filter['pass_player']}")
    if pair_ppr_df_filter.shape[0] > 0:
      for p_num in [1,2]:
        # now store pair and player
        perf_table.at[index,'Pair'] = row.team
        perf_table.at[index,'Player'] = row.player1 if p_num == 1 else row.player2
        disp_player = row.player1 if p_num == 1 else row.player2

        # calculate the fbhe
        fbhe_vector_all = fbhe(pair_ppr_df_all,disp_player,'att', False)
        fbhe_vector_filter = fbhe(pair_ppr_df_filter,disp_player,'att', False)
        print(f" FBHE for pair, Pair: {row.team}, for player: {disp_player}, ppr all size: {pair_ppr_df_all.shape[0]}, ppr filter size {pair_ppr_df_filter.shape[0]}, fbhe vector all {fbhe_vector_all}, fbhe vector filter {fbhe_vector_filter} ")
        print(f"pair_ppr_df_filter pass players: {pair_ppr_df_filter['pass_player']}")
        perf_table.at[index,'FBHE-All'] = fbhe_vector_all[0]
        perf_table.at[index,'FBHE-Recent'] = fbhe_vector_filter[0]
        perf_table.at[index,'FBHE-Diff'] = fbhe_vector_filter[0] - fbhe_vector_all[0]
        perf_table.at[index,'FBHE-All'] = float('{:.3f}'.format(perf_table.at[index,'FBHE-All']))
        perf_table.at[index,'FBHE-Recent'] = float('{:.3f}'.format(perf_table.at[index,'FBHE-Recent']))
        perf_table.at[index,'FBHE-Diff'] = float('{:.3f}'.format(perf_table.at[index,'FBHE-Diff']))

        # count the points for this player within this pair:
        player_pt_totals_all = player_pt_total(pair_ppr_df_all, disp_player)
        player_pt_totals_filter = player_pt_total(pair_ppr_df_filter, disp_player)
      
        # calculate the Point Differentials
        # point totals shouldbe (points won - points lost)/total points
        perf_table.at[index,'Points-All'] = ( (player_pt_totals_all.at[0,'p_tsa'] + 
                                             player_pt_totals_all.at[0,'p_fbk'] + 
                                             player_pt_totals_all.at[0,'p_tk_s'] + 
                                             player_pt_totals_all.at[0,'p_tk_r'] + 
                                             player_pt_totals_all.at[0,'o_tse'] + 
                                             player_pt_totals_all.at[0,'o_fbe'] + 
                                             player_pt_totals_all.at[0,'o_te_s'] + 
                                             player_pt_totals_all.at[0,'o_te_r'] ) - 
                                            (player_pt_totals_all.at[0,'o_tsa'] + 
                                             player_pt_totals_all.at[0,'o_fbk'] + 
                                             player_pt_totals_all.at[0,'o_tk_s'] + 
                                             player_pt_totals_all.at[0,'o_tk_r'] + 
                                             player_pt_totals_all.at[0,'p_tse'] + 
                                             player_pt_totals_all.at[0,'p_fbe'] + 
                                             player_pt_totals_all.at[0,'p_te_s'] + 
                                             player_pt_totals_all.at[0,'p_te_r'] ) )  /  ( player_pt_totals_all.at[0,'pts_total']) 
        perf_table.at[index,'Points-Recent'] = ( (player_pt_totals_filter.at[0,'p_tsa'] + 
                                             player_pt_totals_filter.at[0,'p_fbk'] + 
                                             player_pt_totals_filter.at[0,'p_tk_s'] + 
                                             player_pt_totals_filter.at[0,'p_tk_r'] + 
                                             player_pt_totals_filter.at[0,'o_tse'] + 
                                             player_pt_totals_filter.at[0,'o_fbe'] + 
                                             player_pt_totals_filter.at[0,'o_te_s'] + 
                                             player_pt_totals_filter.at[0,'o_te_r'] ) - 
                                            (player_pt_totals_filter.at[0,'o_tsa'] + 
                                             player_pt_totals_filter.at[0,'o_fbk'] + 
                                             player_pt_totals_filter.at[0,'o_tk_s'] + 
                                             player_pt_totals_filter.at[0,'o_tk_r'] + 
                                             player_pt_totals_filter.at[0,'p_tse'] + 
                                             player_pt_totals_filter.at[0,'p_fbe'] + 
                                             player_pt_totals_filter.at[0,'p_te_s'] + 
                                             player_pt_totals_filter.at[0,'p_te_r'] ) )  /  ( player_pt_totals_filter.at[0,'pts_total']) 
        perf_table.at[index,'Points-Diff'] = perf_table.at[index,'Points-Recent']-perf_table.at[index,'Points-All']
        perf_table.at[index,'Points-All'] = str('{:.1%}'.format(perf_table.at[index,'Points-All']))
        perf_table.at[index,'Points-Recent'] = str('{:.1%}'.format(perf_table.at[index,'Points-Recent']))
        perf_table.at[index,'Points-Diff'] = str('{:.2%}'.format(perf_table.at[index,'Points-Diff']))

        # calculate knock out numbers
        perf_table.at[index,'KnockOut-All'] = (player_pt_totals_all.at[0,'p_tsa']+player_pt_totals_all.at[0,'o_bad_pass'] )/player_pt_totals_all.at[0,'p_serves']
        perf_table.at[index,'KnockOut-Recent'] = (player_pt_totals_filter.at[0,'p_tsa']+player_pt_totals_filter.at[0,'o_bad_pass'] )/player_pt_totals_filter.at[0,'p_serves']
        perf_table.at[index,'KnockOut-Diff'] = perf_table.at[index,'KnockOut-Recent']-perf_table.at[index,'KnockOut-All']
        perf_table.at[index,'KnockOut-All'] = str('{:.1%}'.format(perf_table.at[index,'KnockOut-All']))
        perf_table.at[index,'KnockOut-Recent'] = str('{:.1%}'.format(perf_table.at[index,'KnockOut-Recent']))
        perf_table.at[index,'KnockOut-Diff'] = str('{:.2%}'.format(perf_table.at[index,'KnockOut-Diff']))
  
        # calculate the passing percent
        # percent of insystem passes
        oos_vector_all = count_out_of_system(pair_ppr_df_all,disp_player,'pass')
        oos_vector_filter = count_out_of_system(pair_ppr_df_filter,disp_player,'pass')
        perf_table.at[index,'Passing-All'] = str('{:.1%}'.format(oos_vector_all[1]))
        perf_table.at[index,'Passing-Recent'] = str('{:.1%}'.format(oos_vector_filter[1]))
        perf_table.at[index,'Passing-Diff'] = str('{:.2%}'.format(oos_vector_filter[1] - fbhe_vector_all[1]))

        # done with this row, increment the index and go back to the loop over player with the pair
        index = index+1


  # now, turn it into markdown
  perf_markdown = pd.DataFrame.to_markdown(perf_table, index = False )

  # return 3 markdown's for consistency witht the display format
  return perf_markdown, '', ''

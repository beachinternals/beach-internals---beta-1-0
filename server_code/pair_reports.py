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
import pandas as pd

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-------------------------------------------------------
#
#         Pair Reports
#
#--------------------------------------------------------
@anvil.server.callable
def pair_fbhe_net(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair,
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
    p2_fbhe_table.at[1,column[i-1]] = fbhe_vector[0]  # fbhe
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
                  disp_team, disp_pair,
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
                  disp_team, disp_pair,
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
  #print(f"Initial Call: ppr_df size:{ppr_df.shape[0]}")
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
    return 'No Data Available','',''
    
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
  stat_table.at[1,'p1'] = (pts1s_df.at[0,'p_tsa'] + pts1s_df.at[0,'p_bad_pass'])/(pts1s_df.at[0,'p_serves'])
  stat_table.at[1,'p1']  = str('{:0.0%}'.format(stat_table.at[1,'p1']))
  stat_table.at[1,'p2'] = (pts2s_df.at[0,'p_tsa'] + pts2s_df.at[0,'p_bad_pass'])/(pts2s_df.at[0,'p_serves'])
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
  
  return scor_markdown,  rot_markdown, stat_markdown
  

#-------------------------------------------------------
#
#         Pair Report Stub
#
#--------------------------------------------------------
@anvil.server.callable
def pair_rpt_stub(disp_league, disp_gender, disp_year, 
                  disp_team, disp_pair,
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
  
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

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-------------------------------------------------------
#
#         Pair Reports
#
#--------------------------------------------------------
@anvil.server.callable
def pair_fbhe_net(player1, player2,
                  disp_league, disp_gender, disp_year, 
                  disp_team, 
                  comp_l1_checked, disp_comp_l1,
                  comp_l2_checked, disp_comp_l2,
                  comp_l3_checked, disp_comp_l3,
                  date_checked, disp_start_date, disp_end_date,
                  scout, explain_text
                ):

  # First, get the data, and narrow to the filter, now, limit to only pair players
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True )
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         )
  ppr_df = pair_filter(ppr_df, player1, player2)

  # create the output dataframe
  df_dict = {' ':['FBHE','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,' '],
             'Zone 1':[0,0,0,0,' '],
             "Zone 2":[0,0,0,0,' '],
             'Zone 3':[0,0,0,0,' '],
             'Zone 4':[0,0,0,0,' '],
             'Zone 5':[0,0,0,0,' ']
            }
  pair_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p1_fbhe_table = pd.DataFrame.from_dict( df_dict )
  p2_fbhe_table = pd.DataFrame.from_dict( df_dict )
  
  # calcualte for player 1
  fbhe_vector = fbhe(ppr,player1,'att',True)
  p1_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
  #p1_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p1_fbhe_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr,player2,'att',True)
  p2_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
  #p2_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p2_fbhe_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[1,'All'] = p1_fbhe_table.at[1,'All'] + p2_fbhe_table[1,'All']
  pair_fbhe_table.at[2,'All'] = p1_fbhe_table.at[2,'All'] + p2_fbhe_table[2,'All']
  pair_fbhe_table.at[3,'All'] = p1_fbhe_table.at[3,'All'] + p2_fbhe_table[3,'All']
  pair_fbhe_table.at[0,'All'] = (p1_fbhe_table.at[1,'All']+p2_fbhe_table[1,'All']) / (p1_fbhe_table.at[3,'All']+p2_fbhe_table.at[3,'All'])

  # now, zones 1 thru 5
  # calculate for zones 1 - 5
  column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']


  for i in [1,2,3,4,5]:
    # calculate for player 1
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], player1, 'att', True )
    p1_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p1_fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p1_fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p1_fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p1_fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for player 1
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], player1, 'att', True )
    p2_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p2_fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p2_fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p2_fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p2_fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for pair
    pair_fbhe_table.at[1,column[i-1]] = p1_fbhe_table.at[1,column[i-1]] + p2_fbhe_table[1,column[i-1]]
    pair_fbhe_table.at[2,column[i-1]] = p1_fbhe_table.at[2,column[i-1]] + p2_fbhe_table[2,column[i-1]]
    pair_fbhe_table.at[3,column[i-1]] = p1_fbhe_table.at[3,column[i-1]] + p2_fbhe_table[3,column[i-1]]
    pair_fbhe_table.at[0,column[i-1]] = (p1_fbhe_table.at[1,column[i-1]]+p2_fbhe_table[1,column[i-1]]) / (p1_fbhe_table.at[3,column[i-1]]+p2_fbhe_table.at[3,column[i-1]])

    # So I think we are done:-)
  return pair_fbhe_table, p1_fbhe_table, p2_fbhe_table
  
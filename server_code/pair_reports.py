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
  print(f"Pair: {disp_pair}, P1: {disp_player1}, P2: {disp_player2}")
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
  print(f"ppr_df size after pair only points: {ppr_df.shape[0]}")

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
  fbhe_vector = fbhe(ppr_df,disp_player1,'att',True)
  p1_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p1_fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p1_fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
  p1_fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
  #p1_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p1_fbhe_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calcualte for player 2
  fbhe_vector = fbhe(ppr_df,disp_player2,'att',True)
  p2_fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
  p2_fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
  p2_fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
  p2_fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
  #p2_fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
  p2_fbhe_table.at[4,'All'] = fbhe_vector[5]  # URL

  # calculate the df for the pair
  pair_fbhe_table.at[1,'All'] = p1_fbhe_table.at[1,'All'] + p2_fbhe_table.at[2,'All']
  pair_fbhe_table.at[2,'All'] = p1_fbhe_table.at[2,'All'] + p2_fbhe_table.at[2,'All']
  pair_fbhe_table.at[3,'All'] = p1_fbhe_table.at[3,'All'] + p2_fbhe_table.at[3,'All']
  pair_fbhe_table.at[0,'All'] = (pair_fbhe_table.at[1,'All'] - pair_fbhe_table.at[2,'All']) / (pair_fbhe_table.at[3,'All'])
  pair_fbhe_table.at[0,'All'] = float("{:.3f}".format(pair_fbhe_table.at[0,'All']))

  # now, zones 1 thru 5
  # calculate for zones 1 - 5
  column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']


  for i in [1,2,3,4,5]:
    # calculate for player 1
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player1, 'att', True )
    p1_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p1_fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p1_fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p1_fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p1_fbhe_table.at[4,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for player 2
    fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==i], disp_player2, 'att', True )
    p2_fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
    p2_fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
    p2_fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
    p2_fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
    p2_fbhe_table.at[4,column[i-1]] = fbhe_vector[5]  # URL

    # calculate for pair
    pair_fbhe_table.at[1,column[i-1]] = p1_fbhe_table.at[1,column[i-1]] + p2_fbhe_table.at[1,column[i-1]]
    pair_fbhe_table.at[2,column[i-1]] = p1_fbhe_table.at[2,column[i-1]] + p2_fbhe_table.at[2,column[i-1]]
    pair_fbhe_table.at[3,column[i-1]] = p1_fbhe_table.at[3,column[i-1]] + p2_fbhe_table.at[3,column[i-1]]
    pair_fbhe_table.at[0,column[i-1]] = (pair_fbhe_table.at[1,column[i-1]] - pair_fbhe_table.at[2,column[i-1]]) / (pair_fbhe_table.at[3,column[i-1]])
    pair_fbhe_table.at[0,column[i-1]] = float("{:.3f}".format(pair_fbhe_table.at[0,column[i-1]]))

    # So I think we are done:-)
  # now, turn them all into markup
  pair_markdown = pd.DataFrame.to_markdown(pair_fbhe_table)
  p1_markdown = pd.DataFrame.to_markdown(p1_fbhe_table)
  p2_markdown = pd.DataFrame.to_markdown(p2_fbhe_table)
  
  return pair_markdown, p1_markdown, p2_markdown

#-------------------------------------------------------
#
#         Pair Report Stub
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
  ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True ) # gets the ppr data, this should be all the data available to report on
  ppr_df = ppr_df_limit( ppr_df, 
                         comp_l1_checked, disp_comp_l1, 
                         comp_l2_checked, disp_comp_l2, 
                         comp_l3_checked, disp_comp_l3, 
                         date_checked, disp_start_date, disp_end_date
                         ) # limit all data available to the parameters given for comp level 1,2,3 and dates.
  ppr_df = pair_filter(ppr_df, disp_pair) # lastly, filter the data to all play that include the pair of interest

  scor_dict = {' ':['Pt Diff','Term Srv','Live Rallies', '',
                    'SideOut', 'Opp Sideout','',
                    'FB Stop', 'Opp FB Stop','',
                    'FB Win','',
                    'Trans Win','Opp Trans Win','',
                    'Blocking','Digging','Create','',
                   'Serving','First Ball Attacking','Transition Attacking'],
             '#':[0,0,0,' ', 0,0,' ',0,0,' ',0,' ',0,0,' ',0,0,0,' ',0,0,0],
             '%':[0,0,0,' ', 0,0,' ',0,0,' ',0,' ',0,0,' ',0,0,0,' ',0,0,0]
            }
  scor_table = pd.DataFrame.from_dict( scor_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  # Call number of points
  pts_df = pair_pt_total(ppr_df,disp_pair)
  #p1_df = player_pt_total(ppr_df,disp_player1)
  #p2_df = player_pt_total(ppr_df,disp_player2)

  
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
    scor_table.at[4,'#'] = pts_df.at[0,'p_fbk'] + pts_df.at[0,'p_tk_r'] + pts_df.at[0,'o_te_s']
    scor_table.at[4,"%"] = scor_table.at[4,'#'] / (pts_df.at[0,'o_serves']-pts_df.at[0,'o_tse'] )
    scor_table.at[4,'%'] = str('{:.1%}'.format(scor_table.at[4,'%'])) 
    scor_table.at[5,'#'] = pts_df.at[0,'o_fbk'] + pts_df.at[0,'o_tk_r'] + pts_df.at[0,'p_te_s'] 
    scor_table.at[5,"%"] = scor_table.at[5,'#'] // (pts_df.at[0,'p_serves']-pts_df.at[0,'p_tse'] )
    scor_table.at[5,'%'] = str('{:.1%}'.format(scor_table.at[5,'%'])) 

    # first ball sideout - FBK/points
    scor_table.at[7,"#"] = pts_df.at[0,'p_fbk']
    if pts_df.at[0,'o_serves'] != 0:
      scor_table.at[7,"%"] = pts_df.at[0,'p_fbk']/(pts_df.at[0,'o_serves']-pts_df.at[0,'o_tse'])
      scor_table.at[7,'%'] = str('{:.1%}'.format(scor_table.at[7,'%'])) 
    scor_table.at[8,"#"] = pts_df.at[0,'o_fbk']
    if pts_df.at[0,'p_serves'] != 0:
      scor_table.at[8,"%"] = pts_df.at[0,'o_fbk']/(pts_df.at[0,'p_serves']-pts_df.at[0,'p_tse'])
      scor_table.at[8,'%'] = str('{:.1%}'.format(scor_table.at[8,'%'])) 

    # blank row
    # first ball stop - fbe / serves (attempts)
    scor_table.at[10,"#"] = pts_df.at[0,'o_fbe']
    scor_table.at[10,"%"] = pts_df.at[0,'o_fbe']/(pts_df.at[0,'p_serves'] - pts_df.at[0,'p_tse'])
    scor_table.at[10,'%'] = str('{:.1%}'.format(scor_table.at[10,'%'])) 
    scor_table.at[11,"#"] = pts_df.at[0,'p_fbe']
    scor_table.at[11,"%"] = pts_df.at[0,'p_fbe']/(pts_df.at[0,'o_serves'] - pts_df.at[0,'o_tse'])
    scor_table.at[11,'%'] = str('{:.1%}'.format(scor_table.at[11,'%'])) 

    # blank row
    # first ball win %
    scor_table.at[13,'%'] = ( pts_df.at[0,'p_fbk'] + pts_df.at[0,'o_fbe']) / (pts_df.at[0,'pts_total']-pts_df.at[0,'p_tse']-pts_df.at[0,'o_tse'])
    scor_table.at[13,'%'] = str('{:.1%}'.format(scor_table.at[13,'%'])) 

    # blank row
    #Transition Win - Number of transitiono points we won! and they won
    scor_table.at[15,'#'] = ( pts_df.at[0,'p_tk_s'] + pts_df.at[0,'p_tk_r']+ pts_df.at[0,'o_te_s']+ pts_df.at[0,'o_te_r'])
    scor_table.at[15,'%'] = scor_table.at[15,"#"] / (pts_df.at[0,'pts_total']-pts_df.at[0,'p_tse']-pts_df.at[0,'o_tse'])
    scor_table.at[15,'%'] = str('{:.1%}'.format(scor_table.at[15,'%'])) 
    scor_table.at[16,'#'] = ( pts_df.at[0,'o_tk_s'] + pts_df.at[0,'o_tk_r']+ pts_df.at[0,'p_te_s']+ pts_df.at[0,'p_te_r'])
    scor_table.at[16,'%'] = scor_table.at[16,"#"] / (pts_df.at[0,'pts_total']-pts_df.at[0,'p_tse']-pts_df.at[0,'o_tse'])
    scor_table.at[16,'%'] = str('{:.1%}'.format(scor_table.at[16,'%'])) 

    # digging ?? Need to look at this

    # Point Breakdowns
    scor_table.at[22,'#'] = (pts_df.at[0,'p_tsa']+pts_df.at[0,'o_tse']+pts_df.at[0,'o_tsa']+pts_df.at[0,'p_tse']) # terminal serve points
    scor_table.at[22,'%'] = scor_table.at[22,'#']/(pts_df.at[0,'pts_total'])
    scor_table.at[22,'%'] = str('{:.1%}'.format(scor_table.at[22,'%'])) 
    scor_table.at[23,'#'] = (pts_df.at[0,'p_fbk']+pts_df.at[0,'p_fbe']+pts_df.at[0,'o_fbk']+pts_df.at[0,'o_fbe']) # first ball points
    scor_table.at[23,'%'] = scor_table.at[23,'#']/(pts_df.at[0,'pts_total'])  
    scor_table.at[23,'%'] = str('{:.1%}'.format(scor_table.at[23,'%'])) 
    scor_table.at[24,'#'] = ((pts_df.at[0,'p_tk_s']+pts_df.at[0,'p_tk_r']+pts_df.at[0,'p_te_s']+pts_df.at[0,'p_te_r']) + 
                             (pts_df.at[0,'o_tk_s']+pts_df.at[0,'o_tk_r']+pts_df.at[0,'o_te_s']+pts_df.at[0,'o_te_r'])) # transition points
    scor_table.at[24,'%'] = scor_table.at[24,'#']/(pts_df.at[0,'pts_total'])  
    scor_table.at[24,'%'] = str('{:.1%}'.format(scor_table.at[24,'%'])) 
    
    # now create the markdown text to return
    scor_markdown = pd.DataFrame.to_markdown(scor_table)
  else:
    scor_markdown = "No Data Found"
    # So I think we are done:-)

  # now, next section, Rotations
  rot_dict = {'col1':[ disp_player1+' Serve','FB Wins','Ace','Blk', 'Opp Err',' ',
                                'FB Loss', 'Srv Err','Blk Err','Dig Err', ' ',
                                'Trans Win', 'Att Kill','Blk','Opp Err',' ',
                                'Trans Loss','Att Err','Atk Blk','Blk Err','Dig Err',' ',
                                'Opp SO','Opp FBSO','FB Stop','Trans Win'],
              'col2':['',0,0,'',0,'',0,0,0,0,'',0,0,0,0,'',0,0,0,0,0,'',0,0,0,0],
              'col3':[ disp_player1+' Receive','FB Wins','Ace','Blk', 'Opp Err',' ',
                                'FB Loss', 'Srv Err','Blk Err','Dig Err', ' ',
                                'Trans Win', 'Att Kill','Blk','Opp Err',' ',
                                'Trans Loss','Att Err','Atk Blk','Blk Err','Dig Err',' ',
                                'Opp SO','Opp FBSO','FB Stop','Trans Win'],
              'col4':['',0,0,0,0,'',0,0,0,0,'',0,0,0,0,'',0,0,0,0,0,'',0,0,0,0],
              'col5':[ disp_player2+' Serve','FB Wins','Ace','Blk', 'Opp Err',' ',
                                'FB Loss', 'Srv Err','Blk Err','Dig Err', ' ',
                                'Trans Win', 'Att Kill','Blk','Opp Err',' ',
                                'Trans Loss','Att Err','Atk Blk','Blk Err','Dig Err',' ',
                                'Opp SO','Opp FBSO','FB Stop','Trans Win'],
              'col6':['',0,0,'',0,'',0,0,0,0,'',0,0,0,0,'',0,0,0,0,0,'',0,0,0,0],
              'col7':[ disp_player2+' Receive','FB Wins','Ace','Blk', 'Opp Err',' ',
                                'FB Loss', 'Srv Err','Blk Err','Dig Err', ' ',
                                'Trans Win', 'Att Kill','Blk','Opp Err',' ',
                                'Trans Loss','Att Err','Atk Blk','Blk Err','Dig Err',' ',
                                'Opp SO','Opp FBSO','FB Stop','Trans Win'],
              'col8':['',0,0,0,0,'',0,0,0,0,'',0,0,0,0,'',0,0,0,0,0,'',0,0,0,0]
             }

  rot_table = pd.DataFrame.from_dict( rot_dict )

  rot_markdown = pd.DataFrame.to_markdown(rot_table)

  # now, next section, Rotations
  stat_dict = {'col1':[ disp_player1+' Stats','Serving KO %','Aces','Bad Pass', 'Good Pass','Serve Error ', ' ',
                                'Terminal Serves', ' ',
                                'Digging %','Digs','Dig Err', ' ',
                                'Block Ratio', 'Blocks','Block Errors',' ',
                                'Passing %','Aced','Bad Pass','Good Pass','Opp Serve Err',' ',
                                'First Ball Eff','Kills','Atk Blocked','Att Error','Continues',' ',
                                'Make Them Pay','',
                                'Trans Eff','Kills','Atk Blocked','Atk Err','Continues'],
              'col2':['',0,0,0,0,0,'',0,'',0,0,0,'',0,0,0,'',0,0,0,0,0,'',0,0,0,0,0,'',0,'',0,0,0,0,0],
              'col3':[ disp_player2+' Stats','Serving KO %','Aces','Bad Pass', 'Good Pass','Serve Error ', ' ',
                                'Terminal Serves', ' ',
                                'Digging %','Digs','Dig Err', ' ',
                                'Block Ratio', 'Blocks','Block Errors',' ',
                                'Passing %','Aced','Bad Pass','Good Pass','Opp Serve Err',' ',
                                'First Ball Eff','Kills','Atk Blocked','Att Error', 'Continues',' ',
                                'Make Them Pay','',
                                'Trans Eff','Kills','Atk Blocked','Atk Err','Continues'],
              'col4':['',0,0,0,0,0,'',0,'',0,0,0,'',0,0,0,'',0,0,0,0,0,'',0,0,0,0,0,'',0,'',0,0,0,0,0]
             }

  stat_table = pd.DataFrame.from_dict( stat_dict )

  stat_markdown = pd.DataFrame.to_markdown(stat_table)
  
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
  pair_markdown = pd.DataFrame.to_markdown(pair_table)
  p1_markdown = pd.DataFrame.to_markdown(p1_table)
  p2_markdown = pd.DataFrame.to_markdown(p2_table)
  
  return pair_markdown, p1_markdown, p2_markdown
  
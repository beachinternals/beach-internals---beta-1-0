import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#------------------------------------------------------------------
#
#        Pair Functions 
#
#------------------------------------------------------------------

#------------------------------------------------------------------
#           Pair Filter - limit data to only points with disp_pair 
#------------------------------------------------------------------
def pair_filter(ppr_df, disp_pair):
  # filter a ppr dataframe down to all plays with the pair palyer 1 and 2
  disp_pair = disp_pair.strip()
  tmp_df = ppr_df[ (ppr_df['teama'].str.strip() == disp_pair) |
                   (ppr_df['teamb'].str.strip() == disp_pair) 
                  ]
  return tmp_df

#------------------------------------------------------------------
#           Pair Players - lireturn the two players in the pair 
#------------------------------------------------------------------
def pair_players(disp_pair):
  # search the master pair table to find the two players
  disp_pair = disp_pair.strip()
  for pair_row in app_tables.master_pair.search(pair=disp_pair):
    disp_player1 = pair_row['player1']
    disp_player2 = pair_row['player2']
  return disp_player1, disp_player2

#------------------------------------------------------------------
#           Pair Point Totals - return a df with the point totals for the pair 
#------------------------------------------------------------------  
def pair_pt_total(ppr_df, disp_pair):
  # count the total number of points in each category

  # return datafarme with the details
  pts_dict = {'p_tsa':[0], # this pair, terminal serve ace
              'p_tse':[0],# this pair, terminal serve error
              'p_fbk':[0], # this pair, first ball kill
              'p_fbe':[0], # this pair, first ball error
              'p_tk':[0], # this pair, transition kills
              'p_te':[0], # this pair, transition errors
              'p_serves':[0], # this pair, total number of serves
              'o_tsa':[0], # opponent - terminal serve ace
              'o_tse':[0], # opponent - terminal serve error
              'o_fbk':[0], # opponent - first ball kill
              'o_fbe':[0], # opponent - first ball error
              'o_tk':[0], # opponent - transition kill
              'o_te':[0], # opponent - transition error
              'o_serves':[0], # opponent - total number of serves
              'pts_total':[0] # total number of points played
             }

  pts_df = pd.DataFrame.from_dict( pts_dict )
  disp_pair = disp_pair.strip() # just to make sure!
  # filter whoiel dataframe to this player:
  ppr_df = pair_filter(ppr_df,disp_pair)

  # let's start counting!
  tmp_df = ppr_df[ppr_df['point_outcome_team'] == disp_pair]
  pts_df.at[0,'p_tsa'] = tmp_df[ tmp_df['point_outcome'] == 'TSA' ].shape[0]
  pts_df.at[0,'p_tse'] = tmp_df[ tmp_df['point_outcome'] == 'TSE' ].shape[0]
  pts_df.at[0,'p_fbk'] = tmp_df[ tmp_df['point_outcome'] == 'FBK' ].shape[0]
  pts_df.at[0,'p_fbe'] = tmp_df[ tmp_df['point_outcome'] == 'FBE' ].shape[0]
  pts_df.at[0,'p_tk']  = tmp_df[ tmp_df['point_outcome'] == 'TK'  ].shape[0]
  pts_df.at[0,'p_te']  = tmp_df[ tmp_df['point_outcome'] == 'TE'  ].shape[0]
  pts_df.at[0,'p_serves'] = tmp_df[ disp_pair.contains(tmp_df['serve_player']) ]
  
  tmp_df = ppr_df[ ~ppr_df['point_outcome_team'] == disp_pair]
  pts_df.at[0,'o_tsa'] = tmp_df[ tmp_df['point_outcome'] == 'TSA' ].shape[0]
  pts_df.at[0,'o_tse'] = tmp_df[ tmp_df['point_outcome'] == 'TSE' ].shape[0]
  pts_df.at[0,'o_fbk'] = tmp_df[ tmp_df['point_outcome'] == 'FBK' ].shape[0]
  pts_df.at[0,'o_fbe'] = tmp_df[ tmp_df['point_outcome'] == 'FBE' ].shape[0]
  pts_df.at[0,'o_tk'] = tmp_df[ tmp_df['point_outcome'] == 'TK' ].shape[0]
  pts_df.at[0,'o_te'] = tmp_df[ tmp_df['point_outcome'] == 'TE' ].shape[0]
  pts_df.at[0,'o_serves'] = tmp_df[ ~disp_pair.str.contains(tmp_df['serve_player']) ]
  
  pts_df.at[0,'pts_total'] = ppr_df.shape[0]

  return pts_df
  
  
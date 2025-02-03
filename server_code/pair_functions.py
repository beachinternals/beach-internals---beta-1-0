import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import io

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
#           Player Filter - limit data to only points with disp_pair 
#------------------------------------------------------------------
def player_filter(ppr_df, disp_player):
  # filter a ppr dataframe down to all plays with the pair palyer 1 and 2
  disp_player = disp_player.strip()
  tmp_df = ppr_df[ (ppr_df['player_a1'].str.strip() == disp_player) |
                   (ppr_df['player_a2'].str.strip() == disp_player) |
                   (ppr_df['player_b1'].str.strip() == disp_player) |
                   (ppr_df['player_b2'].str.strip() == disp_player) 
                  ]
  return tmp_df

#------------------------------------------------------------------
#           Pair Players - lireturn the two players in the pair 
#------------------------------------------------------------------
@anvil.server.callable
def pair_players(disp_pair):
  # search the master pair table to find the two players
  disp_pair = disp_pair.strip()
  disp_player1 = ''
  disp_player2 = ''
  for pair_row in app_tables.master_pair.search(pair=disp_pair):
    disp_player1 = pair_row['player1']
    disp_player2 = pair_row['player2']
  return disp_player1, disp_player2

#-----------------------------------------------------------------
#
#           Get master pair row
#                Inputs: league, gender, year, pair
#                Rturn: Row from master pair db
#------------------------------------------------------------------
@anvil.server.callable
def fetch_pair_row( c_league, c_gender, c_year, c_pair):
  pair_r = app_tables.master_pair.get(league=c_league,gender=c_gender,year=c_year,pair=c_pair)
  if pair_r:
    return pair_r
  else:
    return 'Error, Pair Not Found:'+c_league+c_gender+c_year+c_pair+' fetch_pair_row()'

    
#-------------------------------------------------------------------------------------------------------
#.    Get Pair Data
#.    get the pair_data and pair_stats files, return the data fram
#-------------------------------------------------------------------------------------------------------
def get_pair_data( disp_league, disp_gender, disp_year):
  # return the player_data dataframe
  
  # find the play_data table
  # pull out the player_data csv file
  #print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = "League"
      ) )

  if ppr_csv_row:
    pair_data_df =  pd.read_csv(io.BytesIO( ppr_csv_row['pair_data'].get_bytes()))
    pair_stats_df =  pd.read_csv(io.BytesIO( ppr_csv_row['pair_data_stats'].get_bytes()))
  else:
    #print('No Rows Found')
    return ["No Player Data Found"], ["No Player Stats Found"]

  # somehow, we are getting a column called unamed: 0, so drop taht
  #print(player_data_df.to_dict())
  pair_data_df = pair_data_df.drop(['Unnamed: 0'], axis = 1 )
  pair_stats_df = pair_stats_df.drop(['Unnamed: 0'], axis = 1 )
  #print(player_data_df.to_dict())

  # need to replace a space with NaN 
  pair_data_df = pair_data_df.replace( " " , None )

  return pair_data_df, pair_stats_df


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
              'p_tk_s':[0], # this pair, transition kill when serving
              'p_te_s':[0], # this pair, transition error when serving
              'p_tk_r':[0], # this pair, transition kills when receiving
              'p_te_r':[0], # this pair, transition error when receiving
              'p_good_pass':[0], # when pair is serving, result is a good pass by oppewnent (not out of system)
              'p_bad_pass':[0], # when pair is serving, result is a bad pass, out of system (by opponent)
              'p_serves':[0], # this pair, total number of serves
              'o_tsa':[0], # opponent - terminal serve ace
              'o_tse':[0], # opponent - terminal serve error
              'o_fbk':[0], # opponent - first ball kill
              'o_fbe':[0], # opponent - first ball error
              'o_tk_s':[0], # this pair, transition kill when serving
              'o_te_s':[0], # this pair, transition error when serving
              'o_tk_r':[0], # this pair, transition kills when receiving
              'o_te_r':[0], # this pair, transition error when receiving
              'o_serves':[0], # opponent - total number of serves
              'o_good_pass':[0], # when opponent is serving, result is good pass (no out of system)
              'o_bad_pass':[0], # when opponent is serving, result in bad pass (out of system)
              'pts_total':[0] # total number of points played
             }

  player1, player2 = pair_players(disp_pair)
  pts_df = pd.DataFrame.from_dict( pts_dict )
  disp_pair = disp_pair.strip() # just to make sure!
  player1 = player1.strip()
  player2 = player2.strip()
  # filter whoiel dataframe to this player:
  ppr_df = pair_filter(ppr_df,disp_pair)

  # let's start counting!
  pts_df.at[0,'p_tsa'] = ppr_df[ (ppr_df['point_outcome'] == 'TSA') & ( ppr_df['point_outcome_team'] == disp_pair) ].shape[0]
  pts_df.at[0,'p_tse'] = ppr_df[ (ppr_df['point_outcome'] == 'TSE') & ( ppr_df['point_outcome_team'] == disp_pair) ].shape[0]
  pts_df.at[0,'p_fbk'] = ppr_df[ (ppr_df['point_outcome'] == 'FBK') & ( ppr_df['point_outcome_team'] == disp_pair) ].shape[0]
  pts_df.at[0,'p_fbe'] = ppr_df[ (ppr_df['point_outcome'] == 'FBE') & ( ppr_df['point_outcome_team'] == disp_pair) ].shape[0]
  pts_df.at[0,'p_tk_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & (ppr_df['point_outcome_team'] == disp_pair)].shape[0]
  pts_df.at[0,'p_te_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & ( ppr_df['point_outcome_team'] == disp_pair)].shape[0]
  pts_df.at[0,'p_tk_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2)) & ( ppr_df['point_outcome_team'] == disp_pair)].shape[0]
  pts_df.at[0,'p_te_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2)) & ( ppr_df['point_outcome_team'] == disp_pair)].shape[0]
  pts_df.at[0,'p_good_pass'] = ppr_df[ ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & (ppr_df['pass_oos'] <= 0) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'p_bad_pass']  = ppr_df[ ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & (ppr_df['pass_oos'] >= 1) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'p_serves'] = ppr_df[ ppr_df['serve_player'] == player1  ].shape[0] + ppr_df[ ppr_df['serve_player'] == player2  ].shape[0]
  
  pts_df.at[0,'o_tsa'] = ppr_df[ (ppr_df['point_outcome'] == 'TSA') & ( ppr_df['point_outcome_team'] != disp_pair) ].shape[0]
  pts_df.at[0,'o_tse'] = ppr_df[ (ppr_df['point_outcome'] == 'TSE') & ( ppr_df['point_outcome_team'] != disp_pair) ].shape[0]
  pts_df.at[0,'o_fbk'] = ppr_df[ (ppr_df['point_outcome'] == 'FBK') & ( ppr_df['point_outcome_team'] != disp_pair) ].shape[0]
  pts_df.at[0,'o_fbe'] = ppr_df[ (ppr_df['point_outcome'] == 'FBE') & ( ppr_df['point_outcome_team'] != disp_pair) ].shape[0]
  pts_df.at[0,'o_tk_s'] = ppr_df[ (ppr_df['point_outcome'] == 'TK') & ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2)) & (ppr_df['point_outcome_team'] != disp_pair)].shape[0]
  pts_df.at[0,'o_te_s'] = ppr_df[ (ppr_df['point_outcome'] == 'TE') & ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2 )) & (ppr_df['point_outcome_team'] != disp_pair)].shape[0]
  pts_df.at[0,'o_tk_r'] = ppr_df[ (ppr_df['point_outcome'] == 'TK') & ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & (ppr_df['point_outcome_team'] != disp_pair)].shape[0]
  pts_df.at[0,'o_te_r'] = ppr_df[ (ppr_df['point_outcome'] == 'TE') & ((ppr_df['serve_player'] == player1) | (ppr_df['serve_player'] == player2)) & (ppr_df['point_outcome_team'] != disp_pair)].shape[0]
  pts_df.at[0,'o_good_pass'] = ppr_df[ ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2)) & (ppr_df['pass_oos'] <= 0) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'o_bad_pass']  = ppr_df[ ((ppr_df['pass_player'] == player1) | (ppr_df['pass_player'] == player2)) & (ppr_df['pass_oos'] >= 1) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'o_serves'] = ppr_df[ (ppr_df['serve_player'] != player2) & (ppr_df['serve_player'] != player1)].shape[0]
  
  pts_df.at[0,'pts_total'] = ppr_df.shape[0]

  if False:  # make true if you want to check the numers
    print(f"Points Data Frame for Pair {disp_pair}")
    print(f"p_tsa: {pts_df.at[0,'p_tsa']}")
    print(f"p_tse: {pts_df.at[0,'p_tse']}")
    print(f"p_fbk: {pts_df.at[0,'p_fbk']}")
    print(f"p_fbe: {pts_df.at[0,'p_fbe']}")
    print(f"p_tk_s: {pts_df.at[0,'p_tk_s']}")
    print(f"p_te_s: {pts_df.at[0,'p_te_s']}")
    print(f"p_tk_r: {pts_df.at[0,'p_tk_r']}")
    print(f"p_te_r: {pts_df.at[0,'p_te_r']}")
    print(f"p_good_pass: {pts_df.at[0,'p_good_pass']}")
    print(f"p_bad_pass: {pts_df.at[0,'p_bad_pass']}")
    print(f"p_serves: {pts_df.at[0,'p_serves']}")
    print(f"o_tsa: {pts_df.at[0,'o_tsa']}")
    print(f"o_tse: {pts_df.at[0,'o_tse']}")
    print(f"o_fbk: {pts_df.at[0,'o_fbk']}")
    print(f"o_fbe: {pts_df.at[0,'o_fbe']}")
    print(f"o_tk_s: {pts_df.at[0,'o_tk_s']}")
    print(f"o_te_s: {pts_df.at[0,'o_te_s']}")
    print(f"o_tk_r: {pts_df.at[0,'o_tk_r']}")
    print(f"o_te_r: {pts_df.at[0,'o_te_r']}")
    print(f"o_good_pass: {pts_df.at[0,'o_good_pass']}")
    print(f"o_bad_pass: {pts_df.at[0,'o_bad_pass']}")
    print(f"o_serves: {pts_df.at[0,'o_serves']}")
    print(f"pts_total: {pts_df.at[0,'pts_total']}")
  
  return pts_df
  

#------------------------------------------------------------------
#           Pair Point Totals - return a df with the point totals for the pair 
#------------------------------------------------------------------  
def player_pt_total(ppr_df, disp_player):
  # count the total number of points in each category

  # return datafarme with the details
  pts_dict = {'p_tsa':[0], # this pair, terminal serve ace
              'p_tse':[0],# this pair, terminal serve error
              'p_fbk':[0], # this pair, first ball kill
              'p_fbe':[0], # this pair, first ball error
              'p_tk_s':[0], # this pair, transition kill when serving
              'p_te_s':[0], # this pair, transition error when serving
              'p_tk_r':[0], # this pair, transition kills when receiving
              'p_te_r':[0], # this pair, transition error when receiving
              'p_good_pass':[0], # when pair is serving, result is a good pass by oppewnent (not out of system)
              'p_bad_pass':[0], # when pair is serving, result is a bad pass, out of system (by opponent)
              'p_serves':[0], # this pair, total number of serves
              'o_tsa':[0], # opponent - terminal serve ace
              'o_tse':[0], # opponent - terminal serve error
              'o_fbk':[0], # opponent - first ball kill
              'o_fbe':[0], # opponent - first ball error
              'o_tk_s':[0], # this pair, transition kill when serving
              'o_te_s':[0], # this pair, transition error when serving
              'o_tk_r':[0], # this pair, transition kills when receiving
              'o_te_r':[0], # this pair, transition error when receiving
              'o_good_pass':[0], # when opponent is serving, result is good pass (no out of system)
              'o_bad_pass':[0], # when opponent is serving, result in bad pass (out of system)
              'o_serves':[0], # opponent - total number of serves
              'pts_total':[0] # total number of points played
             }

  pts_df = pd.DataFrame.from_dict( pts_dict )
  disp_player = disp_player.strip() # just to make sure!
  # filter whole dataframe to this player:
  ppr_df = player_filter(ppr_df,disp_player)

  # let's start counting!
  pts_df.at[0,'p_tsa'] = ppr_df[ (ppr_df['point_outcome'] == 'TSA') & (ppr_df['serve_player'] == disp_player )].shape[0]
  pts_df.at[0,'p_tse'] = ppr_df[ (ppr_df['point_outcome'] == 'TSE') & (ppr_df['serve_player'] == disp_player )].shape[0]
  pts_df.at[0,'p_fbk'] = ppr_df[ (ppr_df['point_outcome'] == 'FBK') & (ppr_df['att_player'] == disp_player ) & (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_fbe'] = ppr_df[ (ppr_df['point_outcome'] == 'FBE') & (ppr_df['att_player'] == disp_player ) & (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_tk_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & (ppr_df['serve_player'] == disp_player) & (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_te_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & (ppr_df['serve_player'] == disp_player) & (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_tk_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & (ppr_df['pass_player'] == disp_player) & (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_te_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & (ppr_df['pass_player'] == disp_player)& (ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'p_good_pass'] = ppr_df[ (ppr_df['serve_player'] == disp_player) & (ppr_df['pass_oos'] <= 0) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'p_bad_pass'] = ppr_df[ (ppr_df['serve_player'] == disp_player) & (ppr_df['pass_oos'] >= 1) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'p_serves'] = ppr_df[ ppr_df['serve_player'] == disp_player  ].shape[0]
  
  pts_df.at[0,'o_tsa'] = ppr_df[ (ppr_df['point_outcome'] == 'TSA') & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0] /2
  pts_df.at[0,'o_tse'] = ppr_df[ (ppr_df['point_outcome'] == 'TSE') & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0] /2
  pts_df.at[0,'o_fbk'] = ppr_df[ (ppr_df['point_outcome'] == 'FBK') & (ppr_df['serve_player'] == disp_player)].shape[0]
  pts_df.at[0,'o_fbe'] = ppr_df[ (ppr_df['point_outcome'] == 'FBE') & (ppr_df['serve_player'] == disp_player)].shape[0] 
  pts_df.at[0,'o_tk_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & (ppr_df['pass_player'] == disp_player) & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'o_te_s']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & (ppr_df['pass_player'] == disp_player) & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'o_tk_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TK') & (ppr_df['serve_player'] == disp_player) & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'o_te_r']  = ppr_df[ (ppr_df['point_outcome'] == 'TE') & (ppr_df['serve_player'] == disp_player) & ~(ppr_df['point_outcome_team'].str.contains(disp_player))].shape[0]
  pts_df.at[0,'o_good_pass'] = ppr_df[ (ppr_df['pass_player'] == disp_player) & (ppr_df['pass_oos'] <= 0) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'o_bad_pass'] = ppr_df[ (ppr_df['pass_player'] == disp_player) & (ppr_df['pass_oos'] >= 1) & (ppr_df['point_outcome'] != "TSE")].shape[0]
  pts_df.at[0,'o_serves'] = pts_df.at[0,'o_good_pass'] + pts_df.at[0,'o_bad_pass'] + pts_df.at[0,'o_tse'] + pts_df.at[0,'o_tsa']
  pts_df.at[0,'pts_total'] = ppr_df.shape[0]

  if False:  # make true if you want to check the numers
    print(f"Points Data Frame for Player  {disp_player}")
    print(f"p_tsa: {pts_df.at[0,'p_tsa']}")
    print(f"p_tse: {pts_df.at[0,'p_tse']}")
    print(f"p_fbk: {pts_df.at[0,'p_fbk']}")
    print(f"p_fbe: {pts_df.at[0,'p_fbe']}")
    print(f"p_tk_s: {pts_df.at[0,'p_tk_s']}")
    print(f"p_te_s: {pts_df.at[0,'p_te_s']}")
    print(f"p_tk_r: {pts_df.at[0,'p_tk_r']}")
    print(f"p_te_r: {pts_df.at[0,'p_te_r']}")
    print(f"p_good_pass: {pts_df.at[0,'p_good_pass']}")
    print(f"p_bad_pass: {pts_df.at[0,'p_bad_pass']}")
    print(f"p_serves: {pts_df.at[0,'p_serves']}")
    print(f"o_tsa: {pts_df.at[0,'o_tsa']}")
    print(f"o_tse: {pts_df.at[0,'o_tse']}")
    print(f"o_fbk: {pts_df.at[0,'o_fbk']}")
    print(f"o_fbe: {pts_df.at[0,'o_fbe']}")
    print(f"o_tk_s: {pts_df.at[0,'o_tk_s']}")
    print(f"o_te_s: {pts_df.at[0,'o_te_s']}")
    print(f"o_tk_r: {pts_df.at[0,'o_tk_r']}")
    print(f"o_te_r: {pts_df.at[0,'o_te_r']}")
    print(f"o_good_pass: {pts_df.at[0,'o_good_pass']}")
    print(f"o_bad_pass: {pts_df.at[0,'o_bad_pass']}")
    print(f"o_serves: {pts_df.at[0,'o_serves']}")
    print(f"pts_total: {pts_df.at[0,'pts_total']}")
  
  return pts_df

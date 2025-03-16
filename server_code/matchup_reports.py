import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from pair_functions import *
import scipy.stats as stats
import pandas as pd

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
@anvil.server.callable
def matchup_outcome_rpt( disp_league, disp_gender, disp_year, pair_a, pair_b ):

  matchup_df = matchup_outcome_df(disp_league, disp_gender, disp_year, pair_a, pair_b)
  matchup_outcome_mkdn = pd.DataFrame.to_markdown( matchup_df, index=False )
  return matchup_outcome_mkdn, '', ''

@anvil.server.callable
def matchup_scouting_rpt( disp_league, disp_gender, disp_year, pair_a, pair_b ):
  #
  # quick report with match outcome, match along the net and  match up on all 45 werves (times 4!)
  #

  matchup_df = matchup_outcome_df(disp_league, disp_gender, disp_year, pair_a, pair_b)
  matchup_outcome_mkdn = pd.DataFrame.to_markdown( matchup_df, index=False )

  matchup_df = matchup_net(disp_league, disp_gender, disp_year, pair_a, pair_b)
  matchup_net_mkdn = pd.DataFrame.to_markdown( matchup_df, index=False )

  matchup_df = matchup_45_serves(disp_league, disp_gender, disp_year, pair_a, pair_b)
  matchup_45_serves_mkdn = pd.DataFrame.to_markdown( matchup_df, index=False )
  
  return matchup_outcome_mkdn, matchup_net_mkdn, matchup_45_serves_mkdn

def matchup_outcome_df(disp_league, disp_gender, disp_year, pair_a, pair_b ):
  #
  #.  Report to return the predicted outcome of two pairs
  #
  #
  a1_matchup, a2_matchup, b1_matchup, b2_matchup = matchup_pair_data(disp_league,disp_gender,disp_year,pair_a,pair_b)

  player_a1, player_a2 = pair_players(pair_a)
  player_b1, player_b2 = pair_players(pair_b)
  
  # lastly, make a quick dataframe then convert it to markdown to return to display
  match_up_dict = {
                ' ':['Ace %','Err %','Expected','Points A Serving','Points B Serving','Point Ratio','Predicted Score'],
                pair_a:['','','','','',0,0],
                player_a1:[0,0,0,0,0,' ',' '],
                player_a2:[0,0,0,0,0,' ',' '],
                pair_b:['','','','','',0,0],
                player_b1:[0,0,0,0,0,' ',' '],
                player_b2:[0,0,0,0,0,' ',' ']
  }
  match_up_df = pd.DataFrame.from_dict( match_up_dict)

  # now populate the dataframe (row, column)
  match_up_df.iloc[0,2] = float('{:.2}'.format(a1_matchup[2])) # ace
  match_up_df.iloc[0,3] = float('{:.2}'.format(a2_matchup[2]))
  match_up_df.iloc[0,5] = float('{:.2}'.format(b1_matchup[2]))
  match_up_df.iloc[0,6] = float('{:.2}'.format(b2_matchup[2]))
  match_up_df.iloc[1,2] = float('{:.2}'.format(a1_matchup[3])) # error
  match_up_df.iloc[1,3] = float('{:.2}'.format(a2_matchup[3]))
  match_up_df.iloc[1,5] = float('{:.2}'.format(b1_matchup[3]))
  match_up_df.iloc[1,6] = float('{:.2}'.format(b2_matchup[3]))
  match_up_df.iloc[2,2] = float('{:.2}'.format(a1_matchup[4])) # expected
  match_up_df.iloc[2,3] = float('{:.2}'.format(a2_matchup[4]))
  match_up_df.iloc[2,5] = float('{:.2}'.format(b1_matchup[4]))
  match_up_df.iloc[2,6] = float('{:.2}'.format(b2_matchup[4]))
  match_up_df.iloc[3,2] = float('{:.2}'.format(a1_matchup[5])) # points on A serve
  match_up_df.iloc[3,3] = float('{:.2}'.format(a2_matchup[5]))
  match_up_df.iloc[3,5] = float('{:.2}'.format(b1_matchup[5]))
  match_up_df.iloc[3,6] = float('{:.2}'.format(b2_matchup[5]))
  match_up_df.iloc[4,2] = float('{:.2}'.format(a1_matchup[6])) # points on B Serve
  match_up_df.iloc[4,3] = float('{:.2}'.format(a2_matchup[6]))
  match_up_df.iloc[4,5] = float('{:.2}'.format(b1_matchup[6]))
  match_up_df.iloc[4,6] = float('{:.2}'.format(b2_matchup[6]))
  match_up_df.iloc[5,1] = float('{:.2}'.format(a1_matchup[9])) # point Ratio A
  match_up_df.iloc[5,4] = float('{:.2}'.format(b1_matchup[9])) # point ratio b
  match_up_df.iloc[6,1] = float('{:.2}'.format(a1_matchup[9]*(21/max(a1_matchup[9],b1_matchup[9])))) # points for A
  match_up_df.iloc[6,4] = float('{:.2}'.format(b1_matchup[9]*(21/max(a1_matchup[9],b1_matchup[9])))) # points for B
  
  return match_up_df

  
@anvil.server.callable
def matchup_pair_data(disp_league, disp_gender, disp_year, pair_a, pair_b):
  #
  # pull data from the pair data file for pair a and b
  # return the matchup vector(s)
  #
  # matchup vector:
  #. 0 - Player
  #. 1 - pair
  #. 2 - ace %
  #. 3 - err %
  #. 4 - expected
  #. 5 - % of points on A serve
  #. 6 - % of points on B Serve
  #. 7 - % of serves by this player
  #. 8 - point ratio (this will be the same for both a1 and a2 (or b1 abd b2))
  
  # fetch the pair_data and pair_data_stats files
  pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
  player_a1, player_a2 = pair_players( pair_a )
  player_b1, player_b2 = pair_players( pair_b )

  # get the row for pair_a and pair_b
  if pair_data_df['pair'].isin([pair_a]).any():
    pair_a1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a1) ].index[0]
    pair_a2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_a, '', '', ''
  if pair_data_df['pair'].isin([pair_b]).any():
    pair_b1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b1) ].index[0]
    pair_b2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_b, '', '', ''
  
  # create the working data in a vector
  #. 0 - Player
  #. 1 - pair
  #. 2 - ace %
  #. 3 - err %
  #. 4 - expected
  #. 5 - % of points on A serve
  #. 6 - % of points on B Serve
  #. 7 - % of serve receives by this player
  #. 8 - % of serves made by this player
  #. 9 - point ratio (this will be the same for both a1 and a2 (or b1 abd b2))
  pair_a1_matchup = [0,0,0,0,0,0,0,0,0,0]
  pair_a2_matchup = [0,0,0,0,0,0,0,0,0,0]
  pair_b1_matchup = [0,0,0,0,0,0,0,0,0,0]
  pair_b2_matchup = [0,0,0,0,0,0,0,0,0,0]

  # store the basic info into working vectors
  pair_a1_matchup[0] = pair_a
  pair_a1_matchup[1] = player_a1
  pair_a1_matchup[2] = pair_data_df.at[pair_a1_index,'srv_ace_per']
  pair_a1_matchup[3] = pair_data_df.at[pair_a1_index,'srv_err_per']
  pair_a1_matchup[4] = pair_data_df.at[pair_a1_index,'expected']/100
  pair_a1_matchup[7] = pair_data_df.at[pair_a1_index,'fbhe_n']/(pair_data_df.at[pair_a1_index,'fbhe_n']+pair_data_df.at[pair_a2_index,'fbhe_n'])
  pair_a1_matchup[8] = pair_data_df.at[pair_a1_index,'srv_n']/(pair_data_df.at[pair_a1_index,'srv_n']+pair_data_df.at[pair_a2_index,'srv_n'])
  #pair_a1_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_a1_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  #print(f" pair a1 matchup data: {pair_a1_matchup}")
  
  pair_a2_matchup[0] = pair_a
  pair_a2_matchup[1] = player_a2
  pair_a2_matchup[2] = pair_data_df.at[pair_a2_index,'srv_ace_per']
  pair_a2_matchup[3] = pair_data_df.at[pair_a2_index,'srv_err_per']
  pair_a2_matchup[4] = pair_data_df.at[pair_a2_index,'expected']/100
  pair_a2_matchup[7] = pair_data_df.at[pair_a2_index,'fbhe_n']/(pair_data_df.at[pair_a1_index,'fbhe_n']+pair_data_df.at[pair_a2_index,'fbhe_n'])
  pair_a2_matchup[8] = pair_data_df.at[pair_a2_index,'srv_n']/(pair_data_df.at[pair_a1_index,'srv_n']+pair_data_df.at[pair_a2_index,'srv_n'])
  #pair_a2_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_a2_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  #print(f" pair a2 matchup data: {pair_a2_matchup}")

  pair_b1_matchup[0] = pair_b
  pair_b1_matchup[1] = player_b1
  pair_b1_matchup[2] = pair_data_df.at[pair_b1_index,'srv_ace_per']
  pair_b1_matchup[3] = pair_data_df.at[pair_b1_index,'srv_err_per']
  pair_b1_matchup[4] = pair_data_df.at[pair_b1_index,'expected']/100
  pair_b1_matchup[7] = pair_data_df.at[pair_b1_index,'fbhe_n']/(pair_data_df.at[pair_b1_index,'fbhe_n']+pair_data_df.at[pair_b2_index,'fbhe_n'])
  pair_b1_matchup[8] = pair_data_df.at[pair_b1_index,'srv_n']/(pair_data_df.at[pair_b1_index,'srv_n']+pair_data_df.at[pair_b2_index,'srv_n'])
  #pair_b1_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_b1_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  #print(f" pair b1 matchup data: {pair_b1_matchup}")
  
  pair_b2_matchup[0] = pair_b
  pair_b2_matchup[1] = player_b2
  pair_b2_matchup[2] = pair_data_df.at[pair_b2_index,'srv_ace_per']
  pair_b2_matchup[3] = pair_data_df.at[pair_b2_index,'srv_err_per']
  pair_b2_matchup[4] = pair_data_df.at[pair_b2_index,'expected']/100
  pair_b2_matchup[7] = pair_data_df.at[pair_b2_index,'fbhe_n']/(pair_data_df.at[pair_b1_index,'fbhe_n']+pair_data_df.at[pair_b2_index,'fbhe_n'])
  pair_b2_matchup[8] = pair_data_df.at[pair_b2_index,'srv_n']/(pair_data_df.at[pair_b1_index,'srv_n']+pair_data_df.at[pair_b2_index,'srv_n'])
  #pair_b2_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_b2_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  #print(f" pair b2 matchup data: {pair_b2_matchup}")
  
  # calculate poiints, simple method, just using stats

  # first calculate a few things:
  a_ace_per = pair_a1_matchup[2]*pair_a1_matchup[8] + pair_a2_matchup[2]*pair_a2_matchup[8] # aces* pecent of serves for a1 and a2
  a_err_per = pair_a1_matchup[3]*pair_a1_matchup[8] + pair_a2_matchup[3]*pair_a2_matchup[8] # errors * percent of serves, for a1 and a2
  a_in_per = 1 - a_ace_per - a_err_per
  a1_rcv_per = pair_a1_matchup[7]/(pair_a1_matchup[7]+pair_a2_matchup[7])
  a1_srv_per = pair_a1_matchup[8]/(pair_a1_matchup[8]+pair_a2_matchup[8])
  a2_rcv_per = pair_a2_matchup[7]/(pair_a1_matchup[7]+pair_a2_matchup[7])
  a2_srv_per = pair_a2_matchup[8]/(pair_a1_matchup[8]+pair_a2_matchup[8])
  #print(f"Pair A Values: ace per:{a_ace_per}, Err Per:{a_err_per},A in Per:{a_in_per},A1 receive:{a1_rcv_per},A1 Serve:{a1_srv_per},A2 Receive:{a2_rcv_per},A2 Serve:{a2_srv_per}")
  
  b_ace_per = pair_b1_matchup[2]*pair_b1_matchup[8] + pair_b2_matchup[2]*pair_b2_matchup[8] # aces* pecent of serves for a1 and a2
  b_err_per = pair_b1_matchup[3]*pair_b1_matchup[8] + pair_b2_matchup[3]*pair_b2_matchup[8] # errors * percent of serves, for a1 and a2
  b_in_per = 1 - b_ace_per - b_err_per
  b1_rcv_per = pair_b1_matchup[7]/(pair_b1_matchup[7]+pair_b2_matchup[7])
  b1_srv_per = pair_b1_matchup[8]/(pair_b1_matchup[8]+pair_b2_matchup[8])
  b2_rcv_per = pair_b2_matchup[7]/(pair_b1_matchup[7]+pair_b2_matchup[7])
  b2_srv_per = pair_b2_matchup[8]/(pair_b1_matchup[8]+pair_b2_matchup[8])
  #print(f"Pair B Values: ace per:{b_ace_per}, Err Per:{b_err_per},B in Per:{b_in_per},B1 receive:{b1_rcv_per},B1 Serve:{b1_srv_per},B2 Receive:{b2_rcv_per},B2 Serve:{b2_srv_per}")
  
  # points on A serves: a points = a's ace percent + expected value of B * % of serves in play by A
  # points when A serves = aces + (1-b's expected value) * (percent of in serves)
  # points for A  = aces + ( (1-b1 expected)(b1 serve receive percent)+(1-b2 expected)(b2 serve receive percent)) * ( A1 total serves * (1-(aces+errors))
  pair_a1_matchup[5] = ( pair_a1_matchup[2] + (1-(pair_b1_matchup[4]*b1_rcv_per+pair_b2_matchup[4]*b2_rcv_per)) * (1-(pair_a1_matchup[2]+ pair_a1_matchup[3])))*pair_a1_matchup[8]
  pair_a2_matchup[5] = ( pair_a2_matchup[2] + (1-(pair_b1_matchup[4]*b1_rcv_per+pair_b2_matchup[4]*b2_rcv_per)) * (1-(pair_a2_matchup[2]+ pair_a2_matchup[3])))*pair_a2_matchup[8]
                                                                                                                
  # ppints on A serving: b points = err percet + (1-expected)*# of serves in play
  # points for B when A serves = errors + expected * percent of serves
  #
  pair_b1_matchup[5] = ( a_err_per + pair_b1_matchup[4]*a_in_per ) * b1_rcv_per
  pair_b2_matchup[5] = ( a_err_per + pair_b2_matchup[4]*a_in_per ) * b2_rcv_per

  # points on B serves: a points = a's ace percent + expected value of B * % of serves in play by A
  # points for B on B serves = aces + ( (1-b1 expected)(b1 serve receive percent)+(1-b2 expected)(b2 serve receive percent)) * ( A1 total serves * (1-(aces+errors))
  pair_b1_matchup[6] = ( pair_b1_matchup[2] + (1-(pair_a1_matchup[4]*a1_rcv_per+pair_a2_matchup[4]*a2_rcv_per)) * (1-(pair_b1_matchup[2]+ pair_b1_matchup[3])))*pair_b1_matchup[8]
  pair_b2_matchup[6] = ( pair_b2_matchup[2] + (1-(pair_a1_matchup[4]*a1_rcv_per+pair_a2_matchup[4]*a2_rcv_per)) * (1-(pair_b2_matchup[2]+ pair_b2_matchup[3])))*pair_b2_matchup[8]

  # ppints on B serving: b points = err percet + (1-expected)*# of serves in play
  # points for B when A serves = errors + expected * percent of serves
  #
  pair_a1_matchup[6] = ( b_err_per + pair_a1_matchup[4]*b_in_per ) * a1_rcv_per
  pair_a2_matchup[6] = ( b_err_per + pair_a2_matchup[4]*b_in_per ) * a2_rcv_per

  # predicted outcome
  # total A points:
  total_a_pts = pair_a1_matchup[5]+pair_a1_matchup[6]+pair_a2_matchup[5]+pair_a2_matchup[6]
  total_b_pts = pair_b1_matchup[5]+pair_b1_matchup[6]+pair_b2_matchup[5]+pair_b2_matchup[6]
  point_ratio_a = total_a_pts/(total_a_pts+total_b_pts)
  pair_a1_matchup[9] = point_ratio_a
  pair_a2_matchup[9] = point_ratio_a
  point_ratio_b = 1 - point_ratio_a
  pair_b1_matchup[9] = point_ratio_b
  pair_b2_matchup[9] = point_ratio_b

  #print(f"Pair Matchups: Pair A1: {pair_a1_matchup}, Pair A2: {pair_a2_matchup}, Pair B1: {pair_b1_matchup}, Pair B2: {pair_b2_matchup}")

  return pair_a1_matchup, pair_a2_matchup, pair_b1_matchup, pair_b2_matchup

@anvil.server.callable
def matchup_net(disp_league, disp_gender, disp_year, pair_a, pair_b):
  #
  # we matchup pair a vs pair b along the net
  #
  #
  # pull data from the pair data file for pair a and b
  # return the matchup vector(s)
  #
  # matchup dataframe:
  #. 0 - Serving Player
  #. 1 - Receiving Player
  #. 2 - Zone ( 1 - 5)
  #. 3 - Serving Player, Opp FBHE 
  #. 4 - Opp fbhe percentile
  #. 5 - Receiving Player, FBHE
  #. 6 - Receiving Player, Percentile
  #. 7 - Serve percentile - receive percentile
  matchup_dict = {
    'srv_player':'','rcv_player':'','zone':0,'opp_fbhe':0, 'opp_per':0,'fbhe':0,'fbhe_per':0,'per_diff':0
  }
  matchup_df = pd.DataFrame( matchup_dict, index=[0])
  print(f'matchup dataframe, 1-5 along net, empty:{matchup_df}')
  
  # fetch the pair_data and pair_data_stats files
  pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
  player_a1, player_a2 = pair_players( pair_a )
  player_b1, player_b2 = pair_players( pair_b )
  #print(f"pair stats: {pair_stats_df}")

  # get the row for pair_a and pair_b
  if pair_data_df['pair'].isin([pair_a]).any():
    pair_a1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a1) ].index[0]
    pair_a2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_a
  if pair_data_df['pair'].isin([pair_b]).any():
    pair_b1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b1) ].index[0]
    pair_b2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_b

  index = 0
  for playera in [player_a1, player_a2]:
    for playerb in [player_b1,player_b2]:
      for zone in [1,2,3,4,5]:
        
        # append a new row:
        matchup_df.loc[len(matchup_df)] = matchup_dict
        
        # store players and zones:
        matchup_df.iloc[index,0] = playera
        matchup_df.iloc[index,1] = playerb
        matchup_df.iloc[index,2] = zone

        # now store FBHE and OPP FBHE
        pa_data_index = pair_a1_index if playera == player_a1 else pair_a2_index
        pb_data_index = pair_b1_index if playerb == player_b1 else pair_b2_index
        opp_var = 'opp_fbhe'+str(zone)
        fbhe_var = 'fbhe'+str(zone)
        #print(f"opp_var: {opp_var}, fbhe_var : {fbhe_var} match_up dataframe: {matchup_df}")
        matchup_df.iloc[index,3] = pair_data_df.loc[pa_data_index,opp_var]
        matchup_df.iloc[index,5] = pair_data_df.loc[pb_data_index,fbhe_var]

        # calcalute the percentiles
        #print(f" stats file opp mean and stdev: {pair_stats_df.at[0,'opp_fbhe1_mean']}, {pair_stats_df.at[0,'opp_fbhe1_stdev']} FBHE {pair_stats_df[0,'fbhe1_mean']}, {pair_stats_df[0,'fbhe1_stdev']}" )
        matchup_df.iloc[index,4] = 1 - stats.norm.cdf( (matchup_df.iloc[index,3]-pair_stats_df.at[0,opp_var+'_mean'])/ pair_stats_df.at[0,opp_var+'_stdev'] )
        matchup_df.iloc[index,6] = stats.norm.cdf( (matchup_df.iloc[index,5]-pair_stats_df.at[0,fbhe_var+'_mean'])/ pair_stats_df.at[0,fbhe_var+'_stdev'] )
        matchup_df.iloc[index,7] = matchup_df.iloc[index,4] - matchup_df.iloc[index,6]

        index = index + 1
        
  print(f"match_up dataframe: {matchup_df}")
  # before we return, sort hte dataframe by the difference, ascending
  matchup_df = matchup_df.sort_values(by='per_diff', ascending=False)
  
  return matchup_df

@anvil.server.callable
def matchup_45_serves(disp_league, disp_gender, disp_year, pair_a, pair_b):
  #
  # we matchup pair a vs pair b across all 45 serve locations
  #
  #
  # pull data from the pair data file for pair a and b
  # return the matchup vector(s)
  #
  # matchup dataframe:
  #. 0 - Serving Player
  #. 1 - Receiving Player
  #. 2 - Serve From Zone ( 1, 3, 5 )
  #. 3 - Serve to Zone net ( 1,2,3,4,5)
  #. 4 - Serve to Zone Depth (c,d,e)
  #. 5 - Serving Player, Opp FBHE 
  #. 6 - Opp fbhe percentile
  #. 7 - Receiving Player, FBHE
  #. 8 - Receiving Player, Percentile
  #. 9 - Serve percentile - receive percentile
  
  matchup_dict = {
    'srv_player':'', 'rcv_player':'','srv_fr':0,'srv_to_net':0,'srv_to_depth':'','opp_fbhe':0, 'opp_per':0,'fbhe':0,'fbhe_per':0,'per_diff':0
  }
  matchup_df = pd.DataFrame( matchup_dict, index=[0])
  
  # fetch the pair_data and pair_data_stats files
  pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
  player_a1, player_a2 = pair_players( pair_a )
  player_b1, player_b2 = pair_players( pair_b )

  # get the row for pair_a and pair_b
  if pair_data_df['pair'].isin([pair_a]).any():
    pair_a1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a1) ].index[0]
    pair_a2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_a
  if pair_data_df['pair'].isin([pair_b]).any():
    pair_b1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b1) ].index[0]
    pair_b2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b2) ].index[0]
  else:
    return 'Pair not found in pair data:'+pair_b

  index = 0
  for playera in [player_a1, player_a2]:
    for playerb in [player_b1,player_b2]:
      # save the player index for player a and b in the pair_data table
      pa_data_index = pair_a1_index if playera == player_a1 else pair_a2_index
      pb_data_index = pair_b1_index if playerb == player_b1 else pair_b2_index

      # loop over 45 potential serves, from and to
      for srv_fr_zone in [1,3,5]:
        for srv_to_zone_net in [1,2,3,4,5]:
          for srv_to_zone_depth in ['c','d','e']:

            # append a new row:
            matchup_df.loc[len(matchup_df)] = matchup_dict
        
            # store players and zones:
            matchup_df.iloc[index,0] = playera
            matchup_df.iloc[index,1] = playerb
            matchup_df.iloc[index,2] = str(srv_fr_zone)
            matchup_df.iloc[index,3] = str(srv_to_zone_net)
            matchup_df.iloc[index,4] = str(srv_to_zone_depth)
            
            # now store FBHE and OPP FBHE
            opp_var = 'opp_fbhe_'+str(srv_fr_zone)+'_'+str(srv_to_zone_net)+str(srv_to_zone_depth)
            fbhe_var = 'fbhe_'+str(srv_fr_zone)+'_'+str(srv_to_zone_net)+str(srv_to_zone_depth)
            print(f" Indexes: {index}, pa_data_index {pa_data_index}, Opp var: {opp_var} ")
            matchup_df.iloc[index,5] = pair_data_df.at[pa_data_index,opp_var]
            matchup_df.iloc[index,7] = pair_data_df.at[pb_data_index,fbhe_var]

            # calcaulte the percentiles
            matchup_df.iloc[index,6] = 1 - stats.norm.cdf( (matchup_df.iloc[index,5]-pair_stats_df.at[0,opp_var+'_mean'])/ pair_stats_df.at[0,opp_var+'_stdev'] )
            matchup_df.iloc[index,8] = stats.norm.cdf( (matchup_df.iloc[index,7]-pair_stats_df.at[0,fbhe_var+'_mean'])/ pair_stats_df.at[0,fbhe_var+'_stdev'] )
            matchup_df.iloc[index,9] = matchup_df.iloc[index,6] - matchup_df.iloc[index,8]

            index = index + 1

  print(f"match_up dataframe: {matchup_df}")
  # before we return, sort hte dataframe by the difference, ascending
  matchup_df = matchup_df.sort_values(by='per_diff', ascending=False)

  return matchup_df
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

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
@anvil.server.callable
def matchup_outcome( disp_league, disp_gender, disp_year, pair_a, pair_b ):
  #
  #.  Report to return the predicted outcome of two pairs
  #
  #

  # fetch the pair_data and pair_data_stats files
  pair_data_df, pair_stats_df = get_pair_data( disp_league, disp_gender, disp_year)
  player_a1, player_a2 = pair_players( pair_a )
  player_b1, player_b2 = pair_players( pair_b )

  # get the row for pair_a and pair_b
  pair_a1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a1) ].index[0]
  pair_a2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_a) & (pair_data_df['player'] == player_a2) ].index[0]
  pair_b1_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b1) ].index[0]
  pair_b2_index = pair_data_df.loc[ (pair_data_df['pair'] == pair_b) & (pair_data_df['player'] == player_b2) ].index[0]
  
  # create the workign data in a vector
  #.  0 = pair
  #.  1 = player
  #.  2 = ace %
  #   3 = err %
  #   4 = expected
  #.  5 = expected percentile
  #.  6 = expected percentile difference : this is the difference between A and B's expected value percetaile'
  #.  7 == expected percentile predicted
  pair_a1_matchup = [0,0,0,0,0,0]
  pair_a2_matchup = [0,0,0,0,0,0]
  pair_b1_matchup = [0,0,0,0,0,0]
  pair_b2_matchup = [0,0,0,0,0,0]

  # store the basic info into working vectors
  pair_a1_matchup[0] = pair_a
  pair_a1_matchup[1] = player_a1
  pair_a1_matchup[2] = pair_data_df.at[pair_a1_index,'srv_ace_per']
  pair_a1_matchup[3] = pair_data_df.at[pair_a1_index,'srv_err_per']
  pair_a1_matchup[4] = pair_data_df.at[pair_a1_index,'expected']/100
  pair_a1_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_a1_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  print(f" pair a1 matchup data: {pair_a1_matchup}")
  
  pair_a2_matchup[0] = pair_a
  pair_a2_matchup[1] = player_a2
  pair_a2_matchup[2] = pair_data_df.at[pair_a2_index,'srv_ace_per']
  pair_a2_matchup[3] = pair_data_df.at[pair_a2_index,'srv_err_per']
  pair_a2_matchup[4] = pair_data_df.at[pair_a2_index,'expected']/100
  pair_a2_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_a2_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  print(f" pair a2 matchup data: {pair_a2_matchup}")

  pair_b1_matchup[0] = pair_b
  pair_b1_matchup[1] = player_b1
  pair_b1_matchup[2] = pair_data_df.at[pair_b1_index,'srv_ace_per']
  pair_b1_matchup[3] = pair_data_df.at[pair_b1_index,'srv_err_per']
  pair_b1_matchup[4] = pair_data_df.at[pair_b1_index,'expected']/100
  pair_b1_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_b1_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  print(f" pair b1 matchup data: {pair_b1_matchup}")
  
  pair_b2_matchup[0] = pair_b
  pair_b2_matchup[1] = player_b2
  pair_b2_matchup[2] = pair_data_df.at[pair_b2_index,'srv_ace_per']
  pair_b2_matchup[3] = pair_data_df.at[pair_b2_index,'srv_err_per']
  pair_b2_matchup[4] = pair_data_df.at[pair_b2_index,'expected']/100
  pair_b2_matchup[5] = stats.norm.cdf( (pair_data_df.at[pair_b2_index,'expected']-pair_stats_df.at[0,'expected_mean'])/pair_stats_df.at[0,'expected_mean'] )
  print(f" pair b2 matchup data: {pair_b2_matchup}")
  
  # calculate poiints, simple method, just using stats:
  # points on A serves: a points = a's ace percent + expected value of B * % of serves in play by A
  # points for A when serving = aces + 1-opponent's expected*serves A puts in play (1-(aces+errors))
  points_a1_a_srv = pair_a1_matchup[2] + (1 - (pair_b1_matchup[4]+pair_b2_matchup[4])/2 ) * (1 - ( pair_a1_matchup[2]+pair_a1_matchup[3] ))
  points_a2_a_srv = pair_a2_matchup[2] + (1 - (pair_b1_matchup[4]+pair_b2_matchup[4])/2 ) * (1 - ( pair_a2_matchup[2]+pair_a2_matchup[3] ))
  # ppints on A serving: b points = err percet + (1-expected)*# of serves in play
  # b poits = A's sere errors + expected value * A's serves in play
  points_b1_a_srv = (pair_a1_matchup[3]+pair_a2_matchup[3])/2 + (pair_b1_matchup[4] * (1 - ( pair_a1_matchup[2]+pair_a1_matchup[3] )) )
  points_b2_a_srv = (pair_a1_matchup[3]+pair_a2_matchup[3])/2 + (pair_b2_matchup[4] * (1 - ( pair_a1_matchup[2]+pair_a1_matchup[3] )) )
  print(f"A Serving, Points: A1 : {points_a1_a_srv}, A2, {points_a2_a_srv}, B1, {points_b1_a_srv}, B2 {points_b2_a_srv}")

  # points on B serves: a points = a's ace percent + expected value of B * % of serves in play by A
  # points for A when serving = aces + 1-opponent's expected*serves A puts in play (1-(aces+errors))
  points_b1_b_srv = pair_b1_matchup[2] + (1 - (pair_a1_matchup[4]+pair_a2_matchup[4])/2 ) * (1 - ( pair_b1_matchup[2]+pair_b1_matchup[3] ))
  points_b2_b_srv = pair_b2_matchup[2] + (1 - (pair_a1_matchup[4]+pair_a2_matchup[4])/2 ) * (1 - ( pair_b2_matchup[2]+pair_b2_matchup[3] ))
  points_a1_b_srv = (pair_b1_matchup[3]+pair_b2_matchup[3])/2 + (pair_a1_matchup[4] * (1 - ( pair_b1_matchup[2]+pair_b1_matchup[3] )) )
  points_a2_b_srv = (pair_b1_matchup[3]+pair_b2_matchup[3])/2 + (pair_a2_matchup[4] * (1 - ( pair_b1_matchup[2]+pair_b1_matchup[3] )) )
  print(f"A Serving, Points: A1 : {points_a1_b_srv}, A2, {points_a2_b_srv}, B1, {points_b1_b_srv}, B2 {points_b2_b_srv}" )

  # predicted outcome
  # total A points:
  total_a_pts = points_a1_a_srv + points_a2_a_srv + points_a1_b_srv + points_a2_b_srv
  total_b_pts = points_b1_a_srv + points_b2_a_srv + points_b1_b_srv + points_b2_b_srv
  point_ratio_a = total_a_pts/(total_a_pts+total_b_pts)
  point_ratio_b = 1 - point_ratio_a
  print(f"A points = {total_a_pts}, B points = {total_b_pts}, Point Ratio A = {point_ratio_a}, Point ratio B={point_ratio_b}")

  # predicted outcome
  winner_per = max(point_ratio_a,point_ratio_b)
  num_pts = 21/winner_per
  predicted_a_pts = num_pts * point_ratio_a
  predicted_b_pts = num_pts * point_ratio_b
  print(f"Number of Points: {num_pts}, A Points: {predicted_a_pts}, B Points: {predicted_b_pts}")

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

  # now populate the dataframe
  match_up_df.at['Ace %',player_a1] = float('{:.3}'.format(pair_a1_matchup[2]))
  match_up_df.at['Ace %',player_a2] = pair_a2_matchup[2]
  match_up_df.at['Ace %',player_b1] = pair_b1_matchup[2]
  match_up_df.at['Ace %',player_b2] = pair_b2_matchup[2]
  match_up_df.loc[1,1] = pair_a1_matchup[3]
  match_up_df.loc[2,1] = pair_a2_matchup[3]
  match_up_df.loc[4,1] = pair_b1_matchup[3]
  match_up_df.loc[5,1] = pair_b2_matchup[3]
  match_up_df.iloc[1,2] = pair_a1_matchup[4]
  match_up_df.iloc[2,2] = pair_a2_matchup[4]
  match_up_df.iloc[4,2] = pair_b1_matchup[4]
  match_up_df.iloc[5,2] = pair_b2_matchup[4]
  match_up_df.at[1,3] = points_a1_a_srv
  match_up_df.at[2,3] = points_a2_a_srv
  match_up_df.at[4,3] = points_b1_a_srv
  match_up_df.at[5,3] = points_b2_a_srv
  match_up_df.at[1,4] = points_a1_b_srv
  match_up_df.at[2,4] = points_a2_b_srv
  match_up_df.at[4,4] = points_b1_b_srv
  match_up_df.at[5,4] = points_b2_b_srv
  match_up_df.at[0,5] = point_ratio_a
  match_up_df.at[3,5] = point_ratio_b
  match_up_df.at[0,5] = predicted_a_pts
  match_up_df.at[3,5] = predicted_b_pts

  # coonvert to markdown
  matchup_outcome_mkdn = pd.DataFrame.to_markdown( match_up_df, index=False )
  
  return matchup_outcome_mkdn

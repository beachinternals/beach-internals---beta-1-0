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
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#
@anvil.server.callable
def check_user_team():
  # check if the user has a team assigned, maybe doubel check that all temas are valid
  team_list = ['Internals','Visitor','BallTime'
              'FSU',
              'LMU'
              ]

  user_row = anvil.users.get_user()
  if user_row:
    current_team = anvil.users.get_user()['team']
    if current_team in team_list:
      # all is good
      print(f"Current user is on a valid team: {current_team}")
    elif not current_team:
      # if blank, set to 'Visitor', and set a defulat set of league values
      user_row['team'] = 'Visitor'
      user_row['def_league'] = "FIVB"
      user_row['def_gender'] = "W"
      user_row['def_year'] = "2024"
      # send me an email about this
      anvil.email.send(from_name = "Beach Internals Support", 
                 to = "beachinternals@gmail.com",
                 subject = "New User Assigned to Visitor",
                 text = "A new user has been assigned to team Visitor :"+user_row['email'])
    else:
      # so we have an issue
      print(f"Appears we have a team with an invalid team: {current_team}")
      # send me an email about this
      anvil.email.send(from_name = "Beach Internals Support", 
                 to = "beachinternals@gmail.com",
                 subject = "User wiht Invalide Team",
                 text = "A user has an Invlaid Team :"+current_team)
      # set it all to blanks to eliminate future crashing
      user_row['team'] = 'Visitor'
      user_row['def_league'] = "FIVB"
      user_row['def_gender'] = "W"
      user_row['def_year'] = "2024"

  return True
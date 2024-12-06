import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta

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

# background task to check for user login's the last 24 hours and email me the list
@anvil.server.background_task
def chk_user_logins():

  # Get the current datetime
  now = datetime.now()

  # Calculate the time 24 hours ago
  twenty_four_hours_ago = now - timedelta(days=1)
  
  # check for logins in the last 245 hours
  user_r = app_tables.users.search(last_login > twenty_four_hours_ago)

  
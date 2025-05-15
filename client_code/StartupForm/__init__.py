from ._anvil_designer import StartupFormTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class StartupForm(StartupFormTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    url_params = anvil.js.window.location.search
    params = anvil.js.window.URLSearchParams.new(url_params)
    form_name = params.get('form')
    report_id = params.get('report_id')
    if form_name == 'DisplayForm' and report_id:
    open_form('DisplayForm', report_id=report_id)
else:
  open_form('Form1')
    # Any code you write here will run before the form opens.

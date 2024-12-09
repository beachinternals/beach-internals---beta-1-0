from ._anvil_designer import PDF_RptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
from anvil.tables import app_tables
import anvil.tables.query as q
import anvil.pdf
import anvil.media

class PDF_Rpt(PDF_RptTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

    

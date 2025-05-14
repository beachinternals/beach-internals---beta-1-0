from ._anvil_designer import popupformTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class popupform(popupformTemplate):
  def __init__(self, display_data=None, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    self.label_1.text = display_data or "No data provided"

    #------------------------------
    if report_id:
      # Fetch report data from server
      data = anvil.server.call('get_report_data', report_id)

      if data:
        # Set labels
        self.label_images_1.text = data['labels'][0]
        self.label_df_1.text = data['labels'][1]
        self.label_df_2.text = data['labels'][2]
        self.label_images_2.text = data['labels'][3]

        # Set first set of images
        for img_data in data['images']:
          img = Image(source=img_data)
          img.align = 'center'
          self.first_images_flow_panel.add_component(img)

          # Set dataframes in data grids
          if len(data['dataframes']) > 0:
            self.data_grid_1.repeating_panel_1.items = data['dataframes'][0].to_dict('records')
            if len(data['dataframes']) > 1:
              self.data_grid_2.repeating_panel_1.items = data['dataframes'][1].to_dict('records')

              # Set second set of images
              for img_data in data['more_images']:
                img = Image(source=img_data)
                img.align = 'center'
                self.second_images_flow_panel.add_component(img)
        #--------------------------------------------------


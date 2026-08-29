from ._anvil_designer import rpt_mgr_row_templateTemplate
from anvil import *
import anvil.users


class rpt_mgr_row_template(rpt_mgr_row_templateTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.update_display()

  def update_display(self):
    # INTERNALS sees every team's reports (list_rpt_mgr_reports), so show
    # which team each row belongs to. Other teams only ever see their own rows.
    user = anvil.users.get_user(allow_remembered=True)
    if user and user['team'] == 'INTERNALS':
      self.team_label.text = self.item['team'] or ''
      self.team_label.visible = True
    else:
      self.team_label.visible = False

    # Fall back to the legacy 'Report Description' column for rows created
    # before this admin UI existed -- those rows never got 'report_description'
    # populated, so they'd otherwise show a blank title here.
    self.description_label.text = self.item['report_description'] or self.item['Report Description'] or ''
    self.rpt_type_label.text = self.item['rpt_type'] or ''
    self.dow_label.text = self.item['dow'] or ''
    self.status_label.text = 'Active' if self.item['active'] == 'Yes' else 'Inactive'

  def edit_link_click(self, **event_args):
    """Bubble the 'edit this report' request up to the parent form"""
    self.parent.raise_event('x-edit-report', item=self.item)

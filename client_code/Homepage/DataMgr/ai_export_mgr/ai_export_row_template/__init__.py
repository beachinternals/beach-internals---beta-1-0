from ._anvil_designer import ai_export_row_templateTemplate
from anvil import *
import anvil.users


class ai_export_row_template(ai_export_row_templateTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.update_display()

  def update_display(self):
    # INTERNALS sees every team's exports (list_ai_exports), so show which
    # team each row belongs to. Other teams only ever see their own rows.
    user = anvil.users.get_user(allow_remembered=True)
    if user and user['team'] == 'INTERNALS':
      self.team_label.text = self.item['team'] or ''
      self.team_label.visible = True
    else:
      self.team_label.visible = False

    self.league_label.text = self.item['league'] or ''
    self.note_label.text = self.item['Note'] or ''
    self.dow_label.text = self.item['dow'] or ''
    self.status_label.text = 'Disabled' if self.item['disabled'] else 'Enabled'

  def edit_link_click(self, **event_args):
    """Bubble the 'edit this export' request up to the parent form"""
    self.parent.raise_event('x-edit-export', item=self.item)

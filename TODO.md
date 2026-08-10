# TODO

## Open items

- [ ] **Error density percentile is hardcoded to 0.**
  `calc_error_density_obj()` in `server_code/server_functions.py:992` returns a player's error density (errors per point) but always sets `percentile: 0` instead of a real ranking (see line 1081).
  Elsewhere (e.g. `reports_player.py`), percentiles are computed by comparing a player's stat to the league/tournament mean and stdev via the `calculate_percentile()` helper in `server_functions.py:2522`. This function never got wired up to do the same — it needs a league/tournament mean & stdev for error density, then a call to that helper (or equivalent) to replace the placeholder.

- [ ] **`pass_ea` (and possibly other `*_ea` metrics) producing extreme values.**
  Noted in `server_code/generate_player_metrics_json_server.py:330`. Needs investigation into why the pass expected-value metric (and related `*_ea` metrics) can come out as outliers/extreme values.

- [ ] **General cosmetic improvements to the UI.**
  Tighten up spacing/layout across the app, and check formatting/responsiveness on iPhone (mobile widths).

- [ ] **`master_player` data table — add full CRUD UI.**
  The `master_player` table (`app_tables.master_player`) is currently only read from and written to programmatically (e.g. `client_code/Homepage/DataMgr/btd_import/__init__.py`, `server_code/import_csv_file.py`). There's no form for a user to add, rename, or delete a player record directly — build one.

- [ ] **`ai_export_manager` — build a management UI.**
  `server_code/ai_export_manager.py` has no client-facing form. Add a UI to add, modify, and delete its records.

- [ ] **`rpt_manager` — build a management UI.**
  `server_code/rpt_manager.py` has no client-facing form. Add a UI to add, modify, and delete its records.

- [ ] **Stress test report generation.**
  Exercise the report-generation paths under load/volume to check performance and stability.

- [ ] **Expand help/FAQ content.**
  `client_code/Homepage/HelpPage/__init__.py` currently only has two hardcoded topic links (`video_upload_help`, `running_report_help`) — needs a proper FAQ/help section with more topics.
  Open design question: some help content is specific to this app, while other content is general enough to be shared across the AI app, this app, and the www app. Need to decide how that shared content is stored/managed so all three can access it without duplicating it (e.g. a shared content source vs. copy-pasted per app).

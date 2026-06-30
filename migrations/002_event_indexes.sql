CREATE INDEX IF NOT EXISTS idx_stela_events_created_at
  ON stela_events (created_at);

CREATE INDEX IF NOT EXISTS idx_stela_events_run_created_at
  ON stela_events (run_id, created_at);

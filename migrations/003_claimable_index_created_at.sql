DROP INDEX IF EXISTS idx_stela_runs_claimable;

CREATE INDEX IF NOT EXISTS idx_stela_runs_claimable
  ON stela_runs (status, scheduled_at, created_at)
  WHERE status IN ('pending', 'sleeping', 'running');

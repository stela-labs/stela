CREATE TABLE IF NOT EXISTS stela_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES stela_runs(id) ON DELETE CASCADE,
  signal_name VARCHAR(255) NOT NULL,
  payload JSONB,
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_stela_signals_pending
  ON stela_signals (run_id, signal_name)
  WHERE status = 'pending';

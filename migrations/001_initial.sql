CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS stela_migrations (
  version VARCHAR(255) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stela_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_name VARCHAR(255) NOT NULL,
  idempotency_key VARCHAR(255),
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  input JSONB NOT NULL DEFAULT '{}',
  output JSONB,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  worker_id VARCHAR(255),
  worker_lease_expires_at TIMESTAMPTZ,
  UNIQUE(workflow_name, idempotency_key)
);

CREATE TABLE IF NOT EXISTS stela_steps (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES stela_runs(id) ON DELETE CASCADE,
  step_name VARCHAR(255) NOT NULL,
  step_type VARCHAR(50) NOT NULL DEFAULT 'step',
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  output JSONB,
  error TEXT,
  attempt INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  scheduled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  UNIQUE(run_id, step_name)
);

CREATE TABLE IF NOT EXISTS stela_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES stela_runs(id) ON DELETE CASCADE,
  event_type VARCHAR(50) NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stela_runs_claimable
  ON stela_runs (status, scheduled_at)
  WHERE status IN ('pending', 'sleeping', 'running');

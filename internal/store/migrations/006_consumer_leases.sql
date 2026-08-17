-- Current workspace-scoped ownership for checkpointed consumers. The fence
-- is an epoch, not a timestamp: every takeover increments it so stale work
-- fails closed even when an old process continues after expiry.
CREATE TABLE IF NOT EXISTS fornix.consumer_leases (
  workspace_id TEXT NOT NULL,
  consumer_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, consumer_id),
  CONSTRAINT consumer_lease_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT consumer_lease_consumer_nonempty CHECK (length(consumer_id) > 0),
  CONSTRAINT consumer_lease_owner_nonempty CHECK (length(owner_id) > 0),
  CONSTRAINT consumer_lease_fence_positive CHECK (fence > 0)
);

CREATE INDEX IF NOT EXISTS consumer_leases_expiry_idx
  ON fornix.consumer_leases (workspace_id, lease_until)
  WHERE released_at IS NULL;

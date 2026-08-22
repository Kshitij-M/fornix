-- Durable, workspace-scoped repository change proposals and applications.
-- Postgres owns proposal identity, approval state, fencing admission, and
-- audit history. The filesystem remains an explicit external effect boundary.

CREATE TABLE IF NOT EXISTS fornix.change_proposals (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  packet_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  agent_run_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  repository TEXT NOT NULL,
  source JSONB NOT NULL,
  budgets JSONB NOT NULL,
  approval_mode TEXT NOT NULL,
  diff_artifact_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT change_proposals_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT change_proposals_id_nonempty CHECK (length(id) > 0),
  CONSTRAINT change_proposals_request_nonempty CHECK (length(request_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT change_proposals_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND packet_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT change_proposals_status_valid CHECK (status IN ('proposed','awaiting_approval','approved','rejected','applying','applied','conflicted','failed','cancelled','expired','recovery_required')),
  CONSTRAINT change_proposals_approval_valid CHECK (approval_mode IN ('automatic','required','denied')),
  CONSTRAINT change_proposals_fence_nonnegative CHECK (task_fence >= 0),
  CONSTRAINT change_proposals_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(source::text) <= 1048576 AND octet_length(budgets::text) <= 8192),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS change_proposals_workspace_status_idx
  ON fornix.change_proposals(workspace_id, status, updated_at DESC, id);
CREATE INDEX IF NOT EXISTS change_proposals_workspace_packet_idx
  ON fornix.change_proposals(workspace_id, packet_hash, id);

ALTER TABLE fornix.change_proposals
  ADD COLUMN IF NOT EXISTS expected_tree_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE fornix.change_proposals
  ADD CONSTRAINT change_proposals_tree_hash_shape
  CHECK (expected_tree_hash = '' OR expected_tree_hash ~ '^[0-9a-f]{64}$');

CREATE TABLE IF NOT EXISTS fornix.change_operations (
  workspace_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  operation_type TEXT NOT NULL,
  path TEXT NOT NULL,
  destination TEXT NOT NULL DEFAULT '',
  expected_hash TEXT NOT NULL DEFAULT '',
  expected_mode INTEGER NOT NULL DEFAULT 0,
  new_content_hash TEXT NOT NULL DEFAULT '',
  new_content_artifact_id BIGINT,
  new_byte_size BIGINT NOT NULL DEFAULT 0,
  result_hash TEXT NOT NULL DEFAULT '',
  new_mode INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, proposal_id, operation_id),
  CONSTRAINT change_operations_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  CONSTRAINT change_operations_ordinal_valid CHECK (ordinal > 0),
  CONSTRAINT change_operations_type_valid CHECK (operation_type IN ('create_file','replace_file','delete_file','rename_file','chmod_file')),
  CONSTRAINT change_operations_path_nonempty CHECK (length(path) > 0 AND length(path) <= 4096),
  CONSTRAINT change_operations_hash_shape CHECK (
    (expected_hash = '' OR expected_hash ~ '^[0-9a-f]{64}$') AND
    (new_content_hash = '' OR new_content_hash ~ '^[0-9a-f]{64}$') AND
    (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT change_operations_sizes_nonnegative CHECK (new_byte_size >= 0 AND expected_mode >= 0 AND new_mode >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS change_operations_proposal_ordinal_idx
  ON fornix.change_operations(workspace_id, proposal_id, ordinal);
CREATE INDEX IF NOT EXISTS change_operations_workspace_path_idx
  ON fornix.change_operations(workspace_id, path, proposal_id);

CREATE TABLE IF NOT EXISTS fornix.change_approvals (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  packet_hash TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  idempotency_key TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT change_approvals_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  CONSTRAINT change_approvals_hash_shape CHECK (packet_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT change_approvals_decision_valid CHECK (decision IN ('approved','rejected')),
  CONSTRAINT change_approvals_identity_nonempty CHECK (length(id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT change_approvals_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(reason) <= 8192),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS change_approvals_workspace_proposal_idx
  ON fornix.change_approvals(workspace_id, proposal_id, created_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.change_applications (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  packet_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL,
  expected_tree_hash TEXT NOT NULL DEFAULT '',
  result_tree_hash TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  conflict JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_artifact_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT change_applications_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  CONSTRAINT change_applications_hash_shape CHECK (
    packet_hash ~ '^[0-9a-f]{64}$' AND
    (expected_tree_hash = '' OR expected_tree_hash ~ '^[0-9a-f]{64}$') AND
    (result_tree_hash = '' OR result_tree_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT change_applications_status_valid CHECK (status IN ('applying','applied','conflicted','failed','cancelled','recovery_required')),
  CONSTRAINT change_applications_identity_nonempty CHECK (length(id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT change_applications_fence_nonnegative CHECK (task_fence >= 0),
  CONSTRAINT change_applications_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(conflict::text) <= 32768 AND octet_length(failure::text) <= 32768),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS change_applications_workspace_status_idx
  ON fornix.change_applications(workspace_id, status, updated_at DESC, id);
CREATE INDEX IF NOT EXISTS change_applications_workspace_proposal_idx
  ON fornix.change_applications(workspace_id, proposal_id, created_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.change_transitions (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  application_id TEXT NOT NULL DEFAULT '',
  from_status TEXT NOT NULL DEFAULT '',
  to_status TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT change_transitions_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  CONSTRAINT change_transitions_identity_nonempty CHECK (length(workspace_id) > 0 AND length(proposal_id) > 0 AND length(to_status) > 0 AND length(request_id) > 0),
  CONSTRAINT change_transitions_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(reason) <= 8192)
);

CREATE INDEX IF NOT EXISTS change_transitions_workspace_proposal_idx
  ON fornix.change_transitions(workspace_id, proposal_id, id);

CREATE TABLE IF NOT EXISTS fornix.change_artifact_links (
  workspace_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  application_id TEXT NOT NULL DEFAULT '',
  artifact_id BIGINT NOT NULL,
  role TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, proposal_id, application_id, artifact_id, role),
  CONSTRAINT change_artifact_links_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  CONSTRAINT change_artifact_links_role_nonempty CHECK (length(role) > 0)
);

CREATE INDEX IF NOT EXISTS change_artifact_links_workspace_artifact_idx
  ON fornix.change_artifact_links(workspace_id, artifact_id, proposal_id);

CREATE OR REPLACE FUNCTION fornix.reject_change_history_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix repository change history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS change_operations_append_only ON fornix.change_operations;
CREATE TRIGGER change_operations_append_only
  BEFORE UPDATE OR DELETE ON fornix.change_operations
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_change_history_mutation();
DROP TRIGGER IF EXISTS change_approvals_append_only ON fornix.change_approvals;
CREATE TRIGGER change_approvals_append_only
  BEFORE UPDATE OR DELETE ON fornix.change_approvals
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_change_history_mutation();
DROP TRIGGER IF EXISTS change_transitions_append_only ON fornix.change_transitions;
CREATE TRIGGER change_transitions_append_only
  BEFORE UPDATE OR DELETE ON fornix.change_transitions
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_change_history_mutation();
DROP TRIGGER IF EXISTS change_artifact_links_append_only ON fornix.change_artifact_links;
CREATE TRIGGER change_artifact_links_append_only
  BEFORE UPDATE OR DELETE ON fornix.change_artifact_links
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_change_history_mutation();

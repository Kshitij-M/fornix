CREATE SCHEMA IF NOT EXISTS fabric;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS public.coord_messages (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  sender TEXT NOT NULL,
  recipient TEXT NOT NULL,
  subject TEXT,
  body TEXT,
  host TEXT NOT NULL DEFAULT '',
  origin_host TEXT NOT NULL DEFAULT 'local'
);

ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS sender TEXT NOT NULL DEFAULT '';
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS recipient TEXT NOT NULL DEFAULT '';
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS subject TEXT;
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS body TEXT;
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT '';
ALTER TABLE public.coord_messages ADD COLUMN IF NOT EXISTS origin_host TEXT NOT NULL DEFAULT 'local';

CREATE INDEX IF NOT EXISTS coord_messages_ts_idx ON public.coord_messages (ts DESC);
CREATE INDEX IF NOT EXISTS coord_messages_origin_idx ON public.coord_messages (origin_host, id);

CREATE TABLE IF NOT EXISTS fabric.memos (
  id BIGSERIAL PRIMARY KEY,
  title TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'general',
  tags TEXT[] NOT NULL DEFAULT '{}',
  sha256 TEXT NOT NULL,
  tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title,'')), 'A') ||
    setweight(to_tsvector('english', content), 'B')
  ) STORED,
  embedding vector(768),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at TIMESTAMPTZ,
  CONSTRAINT memos_sha256_uniq UNIQUE (sha256)
);

ALTER TABLE fabric.memos ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE fabric.memos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE fabric.memos ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS memos_tsv_idx ON fabric.memos USING gin (tsv);
CREATE INDEX IF NOT EXISTS memos_type_idx ON fabric.memos (type) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS memos_created_idx ON fabric.memos (created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS memos_embedding_ivf ON fabric.memos USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS fabric.symbols (
  id BIGSERIAL PRIMARY KEY,
  repo TEXT NOT NULL,
  file_path TEXT NOT NULL,
  symbol_name TEXT NOT NULL,
  symbol_kind TEXT NOT NULL,
  language TEXT NOT NULL,
  line_start INT NOT NULL,
  line_end INT NOT NULL,
  signature TEXT,
  docstring TEXT,
  embedding vector(768),
  sha256 TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at TIMESTAMPTZ,
  CONSTRAINT symbols_uniq UNIQUE (repo, file_path, symbol_name, symbol_kind)
);

CREATE TABLE IF NOT EXISTS fabric.symbol_edges (
  src_id BIGINT NOT NULL REFERENCES fabric.symbols(id) ON DELETE CASCADE,
  dst_id BIGINT NOT NULL REFERENCES fabric.symbols(id) ON DELETE CASCADE,
  edge_kind TEXT NOT NULL,
  PRIMARY KEY (src_id, dst_id, edge_kind)
);

CREATE INDEX IF NOT EXISTS symbols_repo_idx ON fabric.symbols (repo) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS symbols_name_idx ON fabric.symbols (symbol_name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS symbols_file_idx ON fabric.symbols (repo, file_path) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS symbols_emb_ivf ON fabric.symbols USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS fabric.sessions (
  id TEXT PRIMARY KEY,
  host TEXT NOT NULL,
  capabilities TEXT[] NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'idle',
  current_task_id BIGINT,
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fabric.tasks (
  id BIGSERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  brief TEXT NOT NULL,
  required_capabilities TEXT[] NOT NULL DEFAULT '{}',
  assigned_session TEXT REFERENCES fabric.sessions(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  result TEXT,
  created_by TEXT NOT NULL,
  origin_host TEXT NOT NULL DEFAULT 'local',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

ALTER TABLE fabric.tasks ADD COLUMN IF NOT EXISTS origin_host TEXT NOT NULL DEFAULT 'local';

CREATE INDEX IF NOT EXISTS sessions_heartbeat_idx ON fabric.sessions (last_heartbeat DESC);
CREATE INDEX IF NOT EXISTS tasks_status_idx ON fabric.tasks (status, created_at);
CREATE INDEX IF NOT EXISTS tasks_assigned_idx ON fabric.tasks (assigned_session) WHERE status IN ('claimed','in_progress');

CREATE TABLE IF NOT EXISTS fabric.federation_peers (
  id TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  bearer_token TEXT NOT NULL,
  last_pull_at TIMESTAMPTZ,
  last_pull_high_water BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fabric.router_observations (
  id BIGSERIAL PRIMARY KEY,
  request_hash TEXT NOT NULL,
  task_category TEXT NOT NULL,
  model_id TEXT NOT NULL,
  cost_usd NUMERIC NOT NULL,
  latency_ms INT NOT NULL,
  outcome TEXT NOT NULL DEFAULT 'unknown',
  outcome_score REAL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS router_obs_category_model_idx
  ON fabric.router_observations (task_category, model_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS fabric.chunks (
  id BIGSERIAL PRIMARY KEY,
  source_path TEXT NOT NULL,
  source_range TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
  embedding vector(768),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chunks_sha256_uniq UNIQUE (content_sha256)
);

CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON fabric.chunks USING gin (tsv);
CREATE INDEX IF NOT EXISTS chunks_source_idx ON fabric.chunks (source_path);
CREATE INDEX IF NOT EXISTS chunks_created_idx ON fabric.chunks (created_at DESC);
CREATE INDEX IF NOT EXISTS chunks_emb_ivf ON fabric.chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- API key storage

CREATE TABLE IF NOT EXISTS api_keys (

  id BIGSERIAL PRIMARY KEY,

  key_prefix VARCHAR(20) NOT NULL,

  key_hash TEXT NOT NULL UNIQUE,

  owner_name VARCHAR(100),

  label VARCHAR(100),

  status VARCHAR(20) NOT NULL DEFAULT 'active',

  expires_at TIMESTAMPTZ,

  minute_limit INT NOT NULL DEFAULT 60,

  daily_limit INT NOT NULL DEFAULT 1000,

  last_used_at TIMESTAMPTZ,

  last_ip INET,

  last_user_agent TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  revoked_at TIMESTAMPTZ

);



CREATE INDEX IF NOT EXISTS idx_api_keys_prefix

ON api_keys(key_prefix);



CREATE INDEX IF NOT EXISTS idx_api_keys_status

ON api_keys(status);





-- Usage logging

CREATE TABLE IF NOT EXISTS usage_logs (

  id BIGSERIAL PRIMARY KEY,

  api_key_id BIGINT REFERENCES api_keys(id) ON DELETE SET NULL,

  feature_code VARCHAR(50) NOT NULL,

  endpoint VARCHAR(255) NOT NULL,

  method VARCHAR(10) NOT NULL,

  status_code INT,

  response_time_ms INT,

  ip_address INET,

  user_agent TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);



CREATE INDEX IF NOT EXISTS idx_usage_logs_api_key_id

ON usage_logs(api_key_id);



CREATE INDEX IF NOT EXISTS idx_usage_logs_feature_code

ON usage_logs(feature_code);



CREATE INDEX IF NOT EXISTS idx_usage_logs_created_at

ON usage_logs(created_at DESC);





-- Active session lock (single active client per API key)

CREATE TABLE IF NOT EXISTS active_sessions (

  id BIGSERIAL PRIMARY KEY,

  api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,

  session_token TEXT NOT NULL UNIQUE,

  client_id TEXT NOT NULL,

  ip_address INET,

  user_agent TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  expires_at TIMESTAMPTZ NOT NULL

);



CREATE INDEX IF NOT EXISTS idx_active_sessions_api_key_id

ON active_sessions(api_key_id);



CREATE INDEX IF NOT EXISTS idx_active_sessions_expires_at

ON active_sessions(expires_at);

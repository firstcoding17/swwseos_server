# swwseos_server

PostgreSQL-based API key auth, session lock, statistics, viz, ML, and MCP bridge server.

## Environment variables

Server/database:

- `PORT`: HTTP port. Default `5000`
- `DB_HOST`: PostgreSQL host
- `DB_PORT`: PostgreSQL port
- `DB_NAME`: PostgreSQL database name
- `DB_USER`: PostgreSQL user
- `DB_PASSWORD`: PostgreSQL password
- `DB_SSL`: set `true` when SSL is required
- `DATABASE_URL`: optional single-string PostgreSQL connection URL
- `SESSION_TTL_SECONDS`: session expiry window for API auth heartbeat. Default `300`
- `PYTHON_BIN`: optional python executable path
- `ANTHROPIC_API_KEY`: optional Claude API key for MCP chat planner
- `CLAUDE_MODEL`: optional Claude model override
- `CLAUDE_MAX_TOKENS`: optional Claude response token limit
- `MCP_INTERNAL_BASE`: optional internal base URL override for MCP tool proxying

Important:

- Do not commit `.env`
- Do not commit raw issued API key values
- Do not hardcode database passwords

## Auth flow

1. Create tables with `sql/init_api_auth.sql`
2. Issue an API key with `npm run create:api-key`
3. Client calls `GET /auth/verify` with:
   - `X-API-Key`
   - `X-Client-Id`
4. Read `X-Session-Token` from the response header
5. Send all three headers on protected routes
6. Call `POST /auth/heartbeat` every 30-60 seconds
7. Call `POST /auth/logout` on logout/exit when possible

## MCP / Claude

- Basic MCP tool routes work without a separate MCP vendor key.
- If you want Claude-backed MCP chat, set `ANTHROPIC_API_KEY` in the server `.env`.
- That key is read in [`services/services/claudeClient.js`](/c:/Users/user/Desktop/NGNL/swwseos_server/services/services/claudeClient.js#L5).
- If `ANTHROPIC_API_KEY` is empty, Claude chat is treated as not configured.

## SQL bootstrap

```bash
psql -U <user> -d <db> -f sql/init_api_auth.sql
```

## Run

```bash
npm install
npm start
```

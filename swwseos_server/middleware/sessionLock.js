const {
  cleanupExpiredSessions,
  createSession,
  findActiveSession,
  serializeSession,
  touchSessionById,
} = require('../services/authSessions');

function isVerifyRequest(req) {
  return String(req.originalUrl || '').startsWith('/auth/verify');
}

module.exports = async function sessionLock(req, res, next) {
  try {
    const apiKeyId = req.apiKey?.id;
    const clientId = String(req.header('X-Client-Id') || '').trim();
    const sessionToken = String(req.header('X-Session-Token') || '').trim();

    if (!clientId) {
      return res.status(400).json({
        ok: false,
        code: 'CLIENT_ID_REQUIRED',
        error: 'X-Client-Id header is required',
      });
    }

    await cleanupExpiredSessions(apiKeyId);
    const activeSession = await findActiveSession(apiKeyId);

    if (isVerifyRequest(req)) {
      if (!activeSession) {
        const created = await createSession({
          apiKeyId,
          clientId,
          ipAddress: req.ip,
          userAgent: req.get('user-agent'),
        });
        req.sessionInfo = serializeSession(created);
        res.set('X-Session-Token', created.session_token);
        return next();
      }

      if (String(activeSession.client_id) !== clientId) {
        return res.status(409).json({
          ok: false,
          code: 'SESSION_LOCKED',
          error: 'This API key is already being used by another client',
        });
      }

      const touched = await touchSessionById(activeSession.id, {
        ipAddress: req.ip,
        userAgent: req.get('user-agent'),
      });
      req.sessionInfo = serializeSession(touched);
      res.set('X-Session-Token', touched.session_token);
      return next();
    }

    if (!activeSession) {
      return res.status(401).json({
        ok: false,
        code: 'SESSION_REQUIRED',
        error: 'No active session found. Verify the API key first.',
      });
    }

    if (String(activeSession.client_id) !== clientId) {
      return res.status(409).json({
        ok: false,
        code: 'SESSION_LOCKED',
        error: 'This API key is already being used by another client',
      });
    }

    if (!sessionToken) {
      return res.status(401).json({
        ok: false,
        code: 'SESSION_TOKEN_REQUIRED',
        error: 'X-Session-Token header is required',
      });
    }

    if (String(activeSession.session_token) !== sessionToken) {
      return res.status(401).json({
        ok: false,
        code: 'SESSION_TOKEN_INVALID',
        error: 'Session token is invalid',
      });
    }

    req.sessionInfo = serializeSession(activeSession);
    return next();
  } catch (error) {
    console.error('sessionLock error:', error);
    return res.status(500).json({
      ok: false,
      code: 'SESSION_LOCK_ERROR',
      error: 'Internal server error',
    });
  }
};

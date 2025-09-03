require('dotenv').config();    
const express = require('express');
const http = require('http');
const cors = require('cors');


// 모듈 가져오기
const pythonRoutes = require('./routes/python'); // REST API 라우트
const statRouter = require('./routes/stat'); // 이름 통일


const { initializeWebSocket } = require('./services/socket'); // WebSocket 초기화

const app = express();
const server = http.createServer(app);


// ✅ CORS 설정
app.use(cors({
  origin: '*', // Vue.js가 실행되는 모든 도메인 허용
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'X-API-Key']
}));

app.use(express.json({limit: '20mb'})); // JSON 요청 처리

function apiKeyGuard(req, res, next) {
  const allowed = (process.env.ALLOWED_API_KEYS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
  const key = req.header('X-API-Key') || req.query.api_key;
  if (!key || !allowed.includes(key)) {
    return res.status(401).json({ ok: false, error: 'Invalid or missing API key' });
  }
  next();
}

app.get('/auth/verify', apiKeyGuard, (req, res) => {
  res.json({ ok: true });
});

// (선택) 헬스체크
app.get('/healthz', (req, res) => res.send('ok'));

// ✅ REST API 라우트 연결
app.use('/api', apiKeyGuard, pythonRoutes);



app.use('/stat', apiKeyGuard, statRouter);

// ✅ WebSocket 초기화
initializeWebSocket(server);


const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`✅ Server is running on http://localhost:${PORT}`);
});

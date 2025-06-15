const express = require('express');
const http = require('http');
const cors = require('cors');

// 모듈 가져오기
const pythonRoutes = require('./routes/python'); // REST API 라우트
const { initializeWebSocket } = require('./services/socket'); // WebSocket 초기화

const app = express();
const server = http.createServer(app);


// ✅ CORS 설정
app.use(cors({
  origin: '*', // Vue.js가 실행되는 모든 도메인 허용
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type']
}));

app.use(express.json()); // JSON 요청 처리

// ✅ REST API 라우트 연결
app.use('/api', pythonRoutes);

// ✅ WebSocket 초기화
initializeWebSocket(server);

// ✅ 서버 실행
const PORT = 5000;
server.listen(PORT, () => {
  console.log(`✅ Server is running on http://localhost:${PORT}`);
});
app.post("/api/generate-graph", (req, res) => {
  console.log("✅ 직접 등록된 generate-graph 라우트 작동");
  res.json({ image: "test" });
});
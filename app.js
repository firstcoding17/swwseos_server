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
  allowedHeaders: ['Content-Type']
}));

app.use(express.json()); // JSON 요청 처리

// ✅ REST API 라우트 연결
app.use('/api', pythonRoutes);



app.use('/stat', statRouter);

// ✅ WebSocket 초기화
initializeWebSocket(server);


const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`✅ Server is running on http://localhost:${PORT}`);
});

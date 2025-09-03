const { Server } = require('socket.io');

exports.initializeWebSocket = (server) => {
  const io = new Server(server, {
    cors: {
      origin: '*', // 모든 도메인에서 WebSocket 허용 (보안 강화 필요 시 특정 도메인만 허용)
      methods: ['GET', 'POST']
    }
  });

  io.on('connection', (socket) => {
    console.log('🔗 WebSocket Client connected');

    socket.on('message', (data) => {
      console.log('📨 Message from client:', data);
      socket.emit('server-message', { reply: 'Message received' });
    });

    socket.on('disconnect', () => {
      console.log('❌ WebSocket Client disconnected');
    });
  });

  console.log('✅ WebSocket Server started');
  return io;
};

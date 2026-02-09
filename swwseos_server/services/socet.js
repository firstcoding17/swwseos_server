const { Server } = require('socket.io');

exports.initializeWebSocket = (server) => {
  const io = new Server(server, {
    cors: {
      origin: '*', // Allow WebSocket from all origins
      methods: ['GET', 'POST']
    }
  });

  io.on('connection', (socket) => {
    console.log('WebSocket client connected');

    socket.on('message', (data) => {
      console.log('Message from client:', data);
      socket.emit('server-message', { reply: 'Message received' });
    });

    socket.on('disconnect', () => {
      console.log('WebSocket client disconnected');
    });
  });

  console.log('WebSocket server started');
  return io;
};

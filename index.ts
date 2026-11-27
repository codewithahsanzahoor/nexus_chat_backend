import http from 'http';
import dotenv from 'dotenv';
import { Server } from 'socket.io';
import app from './app.js';
import connectDB from './config/db.js';
import User from './models/User.js';

dotenv.config();

connectDB();

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// Map to track user sockets - userId -> Set of socketIds
const onlineUsers = new Map<string, Set<string>>();

io.on('connection', (socket) => {
  console.log('New client connected:', socket.id);

  socket.on('join_user', async (userId) => {
    if (!userId) return;
    socket.data.userId = userId;

    // Add socket to user's socket set
    if (!onlineUsers.has(userId)) {
      onlineUsers.set(userId, new Set());
    }
    onlineUsers.get(userId)!.add(socket.id);

    // Only update DB if this is the first connection
    const socketCount = onlineUsers.get(userId)!.size;
    if (socketCount === 1) {
      try {
        await User.findByIdAndUpdate(userId, { status: 'online' });
        io.emit('user_status_changed', { userId, status: 'online' });
      } catch (err) {
        console.error('Error updating status:', err);
      }
    }

    console.log(`User ${userId} is online with socket ${socket.id} (total sockets: ${socketCount})`);
  });

  socket.on('join_conversation', (conversationId) => {
    socket.join(conversationId);
    console.log(`Socket ${socket.id} joined conversation: ${conversationId}`);
  });

  socket.on('send_message', (data) => {
    console.log('Message received on server:', data);
    // Emit to all users in the conversation room
    io.to(data.conversationId).emit('receive_message', data);
  });

  socket.on('typing', async (data) => {
    const userId = socket.data.userId;
    if (userId) {
      try {
        await User.findByIdAndUpdate(userId, { status: 'typing' });
        io.emit('user_status_changed', { userId, status: 'typing' });
      } catch (err) {
        console.error('Error updating typing status:', err);
      }
    }
    socket.to(data.conversationId).emit('user_typing', data);
  });

  socket.on('stop_typing', async (data) => {
    const userId = socket.data.userId;
    if (userId) {
      try {
        await User.findByIdAndUpdate(userId, { status: 'online' });
        io.emit('user_status_changed', { userId, status: 'online' });
      } catch (err) {
        console.error('Error updating stop typing status:', err);
      }
    }
    socket.to(data.conversationId).emit('user_stop_typing', data);
  });

  socket.on('disconnect', async () => {
    const userId = socket.data.userId;
    if (userId) {
      const userSockets = onlineUsers.get(userId);
      if (userSockets) {
        userSockets.delete(socket.id);

        // Only set offline if this was the last socket
        if (userSockets.size === 0) {
          onlineUsers.delete(userId);
          try {
            await User.findByIdAndUpdate(userId, { status: 'offline', lastSeen: new Date() });
            io.emit('user_status_changed', { userId, status: 'offline' });
          } catch (err) {
            console.error('Error updating status:', err);
          }
          console.log(`User ${userId} went offline (no active sockets)`);
        } else {
          // Still has active sockets, ensure status is online (not typing from disconnected socket)
          try {
            await User.findByIdAndUpdate(userId, { status: 'online' });
            io.emit('user_status_changed', { userId, status: 'online' });
          } catch (err) {
            console.error('Error updating status:', err);
          }
          console.log(`User ${userId} still has ${userSockets.size} active socket(s)`);
        }
      }
      console.log(`Socket ${socket.id} disconnected for user ${userId}`);
    }
  });
});

const PORT = process.env.PORT || 5000;

server.listen(PORT, () => {
  console.log(`Server running in ${process.env.NODE_ENV || 'development'} mode on port ${PORT}`);
});
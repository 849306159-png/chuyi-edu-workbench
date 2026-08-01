/**
 * 楚怡教育工作台 - 后端服务
 * Express + Socket.IO + JSON文件持久化
 * 支持多用户实时协同、数据持久化、在线状态追踪、编辑锁
 */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

const DATA_FILE = path.join(__dirname, 'data.json');
const PORT = process.env.PORT || 3000;

// ==================== 数据持久化 ====================
function loadState() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function saveState(state) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), 'utf8');
}

let appState = loadState();

// ==================== 中间件 ====================
app.use(express.json({ limit: '50mb' }));
app.use(express.static(__dirname));

// ==================== REST API ====================

// 获取完整状态
app.get('/api/state', (req, res) => {
  res.json({ state: appState, hasData: !!appState });
});

// 全量同步（客户端发送完整状态）
app.post('/api/sync', (req, res) => {
  const newState = req.body;
  if (!newState || typeof newState !== 'object') {
    return res.status(400).json({ error: 'Invalid state data' });
  }
  appState = newState;
  saveState(appState);
  // 广播给所有其他客户端
  io.emit('state', appState);
  res.json({ success: true, timestamp: Date.now() });
});

// 健康检查
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    connections: io.engine.clientsCount,
    hasData: !!appState,
    dataKeys: appState ? Object.keys(appState) : []
  });
});

// 重置数据（清空服务端数据，下次客户端连接时会用其内联数据初始化）
app.post('/api/reset', (req, res) => {
  appState = null;
  try { fs.unlinkSync(DATA_FILE); } catch {}
  io.emit('reset');
  res.json({ success: true });
});

// ==================== Socket.IO 实时通信 ====================
const onlineUsers = new Map(); // socketId -> {id, name, joinedAt}
const editLocks = new Map();   // `${entity}:${id}` -> {socketId, name, timestamp}

io.on('connection', (socket) => {
  console.log(`[连接] ${socket.id} | 当前在线: ${io.engine.clientsCount}`);

  // 发送当前状态给新客户端
  socket.emit('state', appState);

  // 发送当前在线用户列表
  onlineUsers.set(socket.id, {
    id: socket.id,
    name: '匿名用户',
    joinedAt: Date.now()
  });
  broadcastPresence();

  // 发送当前编辑锁列表
  socket.emit('locks', Array.from(editLocks.entries()).map(([k, v]) => ({
    key: k, ...v
  })));

  // 用户设置名称
  socket.on('set-name', (name) => {
    const user = onlineUsers.get(socket.id);
    if (user) {
      user.name = name || '匿名用户';
      broadcastPresence();
    }
  });

  // 编辑锁 - 锁定某条记录
  socket.on('lock-edit', (info) => {
    const key = `${info.entity}:${info.id}`;
    const existing = editLocks.get(key);
    // 如果已被其他用户锁定，通知失败
    if (existing && existing.socketId !== socket.id) {
      socket.emit('lock-failed', {
        key,
        entity: info.entity,
        id: info.id,
        lockedBy: existing.name
      });
      return;
    }
    const user = onlineUsers.get(socket.id);
    editLocks.set(key, {
      socketId: socket.id,
      name: user?.name || '匿名',
      timestamp: Date.now()
    });
    socket.broadcast.emit('lock-edit', {
      entity: info.entity,
      id: info.id,
      by: user?.name || '匿名'
    });
  });

  // 编辑锁 - 解锁
  socket.on('unlock-edit', (info) => {
    const key = `${info.entity}:${info.id}`;
    const lock = editLocks.get(key);
    if (lock && lock.socketId === socket.id) {
      editLocks.delete(key);
      socket.broadcast.emit('unlock-edit', {
        entity: info.entity,
        id: info.id
      });
    }
  });

  // 断开连接
  socket.on('disconnect', () => {
    console.log(`[断开] ${socket.id} | 当前在线: ${io.engine.clientsCount}`);
    onlineUsers.delete(socket.id);
    // 清除该用户的所有编辑锁
    for (const [key, lock] of editLocks.entries()) {
      if (lock.socketId === socket.id) {
        editLocks.delete(key);
        io.emit('unlock-edit', { key, entity: key.split(':')[0], id: key.split(':')[1] });
      }
    }
    broadcastPresence();
  });
});

function broadcastPresence() {
  const users = Array.from(onlineUsers.values());
  io.emit('presence', users);
}

// ==================== 启动服务器 ====================
server.listen(PORT, () => {
  console.log('========================================');
  console.log('  楚怡教育工作台 - 服务器已启动');
  console.log('========================================');
  console.log(`  访问地址: http://localhost:${PORT}`);
  console.log(`  健康检查: http://localhost:${PORT}/api/health`);
  console.log(`  数据文件: ${DATA_FILE}`);
  console.log(`  数据状态: ${appState ? '已加载' : '空（等待客户端初始化）'}`);
  console.log('========================================');
});

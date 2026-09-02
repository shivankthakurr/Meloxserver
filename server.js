/**
 * Listen Together — WebSocket sync server
 * Compatible with the melox-style client protocol (JSON messages).
 *
 * Message shape (both directions):
 *   { "type": "<message_type>", "payload": { ... } }
 */

const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 8080;

// Plain HTTP server underneath — needed so Render/Railway see a real web
// service (health checks) and so we have a URL to self-ping.
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Listen Together server is alive\n');
});

const wss = new WebSocket.Server({ server: httpServer });

httpServer.listen(PORT, () => {
  console.log(`Listen Together server running on ws://0.0.0.0:${PORT}`);
});

// ---- Keep-alive self-ping ------------------------------------------------
// Render's free tier spins the service down after ~15 min of no HTTP traffic.
// Render auto-sets RENDER_EXTERNAL_URL to the public https URL of this
// service, so we ping ourselves every 5 min to stay awake. Works the same
// way on Railway/Fly if you set SELF_URL manually as an env var there.
const SELF_URL = process.env.RENDER_EXTERNAL_URL || process.env.SELF_URL || null;
const KEEP_ALIVE_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

if (SELF_URL) {
  setInterval(() => {
    fetch(SELF_URL)
      .then(() => console.log(`[keep-alive] pinged ${SELF_URL}`))
      .catch((err) => console.error('[keep-alive] ping failed', err.message));
  }, KEEP_ALIVE_INTERVAL_MS);
  console.log(`[keep-alive] self-ping enabled every 5 min -> ${SELF_URL}`);
} else {
  console.log('[keep-alive] SELF_URL/RENDER_EXTERNAL_URL not set — self-ping disabled. ' +
    'On Render this is set automatically; on other hosts set SELF_URL env var to your public https URL.');
}

// ---- In-memory state ----------------------------------------------------
// rooms: roomCode -> RoomState
// sessions: sessionToken -> { roomCode, userId }
const rooms = new Map();
const sessions = new Map();

// Give every connected socket an id we can look up later
function attach(ws, userId, roomCode) {
  ws.userId = userId;
  ws.roomCode = roomCode;
}

function genRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no confusing chars
  let code;
  do {
    code = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, type, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, payload: payload ?? null }));
  }
}

function broadcast(room, type, payload, excludeUserId = null) {
  for (const user of room.users) {
    if (user.userId === excludeUserId) continue;
    if (user.socket && user.socket.readyState === WebSocket.OPEN) {
      send(user.socket, type, payload);
    }
  }
}

function publicRoomState(room) {
  return {
    room_code: room.roomCode,
    host_id: room.hostId,
    users: room.users.map(u => ({
      user_id: u.userId,
      username: u.username,
      is_host: u.userId === room.hostId,
      is_connected: u.isConnected,
      avatar_index: u.avatarIndex || 0,
    })),
    current_track: room.currentTrack,
    is_playing: room.isPlaying,
    position: room.position,
    last_update: room.lastUpdate,
    volume: room.volume,
    queue: room.queue,
  };
}

function findRoomByUser(userId) {
  for (const room of rooms.values()) {
    if (room.users.some(u => u.userId === userId)) return room;
  }
  return null;
}

// ---- Connection handling --------------------------------------------------
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, 'error', { code: 'BAD_JSON', message: 'Invalid message' });
    }

    const { type, payload } = msg;

    try {
      handleMessage(ws, type, payload || {});
    } catch (err) {
      console.error('Error handling message', type, err);
      send(ws, 'error', { code: 'SERVER_ERROR', message: String(err.message || err) });
    }
  });

  ws.on('close', () => handleDisconnect(ws));
});

// Ping clients every 30s to detect dead connections
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// ---- Message router --------------------------------------------------
function handleMessage(ws, type, payload) {
  switch (type) {
    case 'create_room': return onCreateRoom(ws, payload);
    case 'join_room': return onJoinRoom(ws, payload);
    case 'leave_room': return onLeaveRoom(ws);
    case 'approve_join': return onApproveJoin(ws, payload);
    case 'reject_join': return onRejectJoin(ws, payload);
    case 'playback_action': return onPlaybackAction(ws, payload);
    case 'kick_user': return onKickUser(ws, payload);
    case 'transfer_host': return onTransferHost(ws, payload);
    case 'request_sync': return onRequestSync(ws);
    case 'reconnect': return onReconnect(ws, payload);
    case 'chat': return onChat(ws, payload);
    case 'ping': return send(ws, 'pong', {});
    default:
      send(ws, 'error', { code: 'UNKNOWN_TYPE', message: `Unknown type: ${type}` });
  }
}

// ---- Handlers --------------------------------------------------------
function onCreateRoom(ws, payload) {
  const userId = uuidv4();
  const roomCode = genRoomCode();
  const sessionToken = uuidv4();

  const room = {
    roomCode,
    hostId: userId,
    users: [{ userId, username: payload.username, avatarIndex: payload.avatar_index || 0, isConnected: true, socket: ws }],
    currentTrack: null,
    isPlaying: false,
    position: 0,
    lastUpdate: Date.now(),
    volume: 1.0,
    queue: [],
  };

  rooms.set(roomCode, room);
  sessions.set(sessionToken, { roomCode, userId });
  attach(ws, userId, roomCode);

  send(ws, 'room_created', { room_code: roomCode, user_id: userId, session_token: sessionToken });
}

function onJoinRoom(ws, payload) {
  const room = rooms.get((payload.room_code || '').toUpperCase());
  if (!room) return send(ws, 'error', { code: 'ROOM_NOT_FOUND', message: 'Room does not exist' });

  const userId = uuidv4();
  attach(ws, userId, room.roomCode);
  ws.pendingUsername = payload.username;
  ws.pendingAvatar = payload.avatar_index || 0;

  const host = room.users.find(u => u.userId === room.hostId);
  if (host && host.socket) {
    send(host.socket, 'join_request', {
      user_id: userId,
      username: payload.username,
      avatar_index: payload.avatar_index || 0,
    });
  }
}

function onApproveJoin(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return;

  // Find the pending guest socket (must have same userId assigned during join_room)
  const guestWs = [...wss.clients].find(c => c.userId === payload.user_id);
  if (!guestWs) return;

  const sessionToken = uuidv4();
  const user = { userId: payload.user_id, username: guestWs.pendingUsername, avatarIndex: guestWs.pendingAvatar || 0, isConnected: true, socket: guestWs };
  room.users.push(user);
  sessions.set(sessionToken, { roomCode: room.roomCode, userId: payload.user_id });

  send(guestWs, 'join_approved', {
    room_code: room.roomCode,
    user_id: payload.user_id,
    session_token: sessionToken,
    state: publicRoomState(room),
  });

  broadcast(room, 'user_joined', { user_id: user.userId, username: user.username, avatar_index: user.avatarIndex }, user.userId);
}

function onRejectJoin(ws, payload) {
  const guestWs = [...wss.clients].find(c => c.userId === payload.user_id);
  if (guestWs) send(guestWs, 'join_rejected', { reason: payload.reason || 'Rejected by host' });
}

function onPlaybackAction(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return; // only host drives playback

  const action = payload.action;
  const now = Date.now();

  switch (action) {
    case 'play':
      room.isPlaying = true;
      room.position = payload.position ?? room.position;
      break;
    case 'pause':
      room.isPlaying = false;
      room.position = payload.position ?? room.position;
      break;
    case 'seek':
      room.position = payload.position ?? room.position;
      break;
    case 'change_track':
      room.currentTrack = payload.track_info || null;
      room.isPlaying = false;
      room.position = 0;
      if (payload.queue) room.queue = payload.queue;
      break;
    case 'queue_add':
      if (payload.track_info) {
        if (payload.insert_next) room.queue.unshift(payload.track_info);
        else room.queue.push(payload.track_info);
      }
      break;
    case 'queue_remove':
      room.queue = room.queue.filter(t => t.id !== payload.track_id);
      break;
    case 'queue_clear':
      room.queue = [];
      break;
    case 'set_volume':
      if (typeof payload.volume === 'number') room.volume = Math.min(1, Math.max(0, payload.volume));
      break;
  }

  room.lastUpdate = now;

  // Rebroadcast to everyone else, stamping our own server_time for
  // latency compensation on the receiving client.
  broadcast(room, 'sync_playback', { ...payload, server_time: now }, ws.userId);
}

function onKickUser(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return;

  const kicked = room.users.find(u => u.userId === payload.user_id);
  room.users = room.users.filter(u => u.userId !== payload.user_id);
  if (kicked && kicked.socket) send(kicked.socket, 'kicked', { reason: payload.reason || 'Removed by host' });
  broadcast(room, 'user_left', { user_id: payload.user_id, username: kicked?.username });
}

function onTransferHost(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return;
  const newHost = room.users.find(u => u.userId === payload.new_host_id);
  if (!newHost) return;
  room.hostId = newHost.userId;
  broadcast(room, 'host_changed', { new_host_id: newHost.userId, new_host_name: newHost.username });
}

function onRequestSync(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  send(ws, 'sync_state', {
    current_track: room.currentTrack,
    is_playing: room.isPlaying,
    position: room.position,
    last_update: room.lastUpdate,
    queue: room.queue,
    volume: room.volume,
  });
}

function onReconnect(ws, payload) {
  const session = sessions.get(payload.session_token);
  if (!session) return send(ws, 'error', { code: 'INVALID_SESSION', message: 'Session expired' });

  const room = rooms.get(session.roomCode);
  if (!room) return send(ws, 'error', { code: 'ROOM_GONE', message: 'Room no longer exists' });

  attach(ws, session.userId, room.roomCode);
  const user = room.users.find(u => u.userId === session.userId);
  if (user) {
    user.socket = ws;
    user.isConnected = true;
  }

  send(ws, 'reconnected', {
    room_code: room.roomCode,
    user_id: session.userId,
    state: publicRoomState(room),
    is_host: room.hostId === session.userId,
  });
  broadcast(room, 'user_reconnected', { user_id: session.userId, username: user?.username }, session.userId);
}

function onChat(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const user = room.users.find(u => u.userId === ws.userId);
  broadcast(room, 'chat', {
    user_id: ws.userId,
    username: user?.username,
    message: payload.message,
    timestamp: Date.now(),
    reply_to: payload.reply_to || null,
  });
}

function onLeaveRoom(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const user = room.users.find(u => u.userId === ws.userId);
  room.users = room.users.filter(u => u.userId !== ws.userId);
  broadcast(room, 'user_left', { user_id: ws.userId, username: user?.username });
  cleanupRoomIfEmpty(room);
}

function handleDisconnect(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const user = room.users.find(u => u.userId === ws.userId);
  if (user) {
    user.isConnected = false;
    broadcast(room, 'user_disconnected', { user_id: ws.userId, username: user.username }, ws.userId);
  }
  cleanupRoomIfEmpty(room);
}

function cleanupRoomIfEmpty(room) {
  const anyoneConnected = room.users.some(u => u.isConnected);
  if (!anyoneConnected) {
    // Give a grace period before deleting, in case someone reconnects
    setTimeout(() => {
      const still = rooms.get(room.roomCode);
      if (still && !still.users.some(u => u.isConnected)) {
        rooms.delete(room.roomCode);
        console.log(`Room ${room.roomCode} cleaned up (empty).`);
      }
    }, 5 * 60 * 1000); // 5 min grace period
  }
}

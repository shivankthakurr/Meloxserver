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
// suggestions: suggestionId -> { roomCode, fromUserId, fromUsername, trackInfo }
const suggestions = new Map();

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

// Expire rooms that were created but never joined by a guest within 15 min.
// Rooms that a guest has ever joined (hasBeenJoined) are never auto-expired
// this way — they only clean up via the empty-room grace period below.
const ROOM_JOIN_EXPIRY_MS = 15 * 60 * 1000;
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (!room.hasBeenJoined && (now - room.createdAt) >= ROOM_JOIN_EXPIRY_MS) {
      const host = room.users.find(u => u.userId === room.hostId);
      if (host && host.socket) {
        send(host.socket, 'room_expired', { room_code: room.roomCode, reason: 'No one joined within 15 minutes' });
      }
      rooms.delete(room.roomCode);
      for (const [id, s] of suggestions) {
        if (s.roomCode === room.roomCode) suggestions.delete(id);
      }
      console.log(`Room ${room.roomCode} expired (unused for 15+ min).`);
    }
  }
}, 60 * 1000); // check every minute

// ---- Message router --------------------------------------------------
function handleMessage(ws, type, payload) {
  switch (type) {
    case 'create_room': return onCreateRoom(ws, payload);
    case 'join_room': return onJoinRoom(ws, payload);
    case 'leave_room': return onLeaveRoom(ws);
    case 'approve_join': return onApproveJoin(ws, payload);
    case 'reject_join': return onRejectJoin(ws, payload);
    case 'playback_action': return onPlaybackAction(ws, payload);
    case 'buffer_ready': return onBufferReady(ws, payload);
    case 'kick_user': return onKickUser(ws, payload);
    case 'transfer_host': return onTransferHost(ws, payload);
    case 'request_sync': return onRequestSync(ws);
    case 'reconnect': return onReconnect(ws, payload);
    case 'chat': return onChat(ws, payload);
    case 'suggest_track': return onSuggestTrack(ws, payload);
    case 'approve_suggestion': return onApproveSuggestion(ws, payload);
    case 'reject_suggestion': return onRejectSuggestion(ws, payload);
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
    hasBeenJoined: false, // becomes true forever once a guest is approved in — code is single-use
    createdAt: Date.now(),
    // buffer sync state (used while waiting for guests to buffer a newly changed track)
    bufferTrackId: null,
    bufferWaitingFor: null, // Set of userIds we're still waiting on
    bufferTimeoutHandle: null,
  };

  rooms.set(roomCode, room);
  sessions.set(sessionToken, { roomCode, userId });
  attach(ws, userId, roomCode);

  send(ws, 'room_created', { room_code: roomCode, user_id: userId, session_token: sessionToken });
}

const MAX_ROOM_USERS = 2; // host + 1 guest

function onJoinRoom(ws, payload) {
  const room = rooms.get((payload.room_code || '').toUpperCase());
  if (!room) return send(ws, 'error', { code: 'ROOM_NOT_FOUND', message: 'Room does not exist' });

  // One-time code: once a guest has ever been approved into this room, the
  // code stops accepting fresh joins — even if that guest later disconnects
  // or leaves. Reconnects use session tokens via `reconnect`, not this path.
  if (room.hasBeenJoined) {
    return send(ws, 'join_rejected', { reason: 'This room is no longer accepting new members' });
  }

  if (room.users.length >= MAX_ROOM_USERS) {
    return send(ws, 'join_rejected', { reason: 'Room is full' });
  }

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
  room.hasBeenJoined = true; // code is now permanently used up, no new joins via join_room
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
      startBufferWait(room, payload.track_info?.id || null);
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

const BUFFER_WAIT_TIMEOUT_MS = 10 * 1000; // don't let one slow guest freeze the room forever

function startBufferWait(room, trackId) {
  // Clear any previous pending wait (a new track change supersedes it)
  if (room.bufferTimeoutHandle) {
    clearTimeout(room.bufferTimeoutHandle);
    room.bufferTimeoutHandle = null;
  }

  if (!trackId) {
    room.bufferTrackId = null;
    room.bufferWaitingFor = null;
    return;
  }

  // Wait on every connected non-host user (guests) to confirm they've buffered
  const waitingIds = room.users
    .filter(u => u.userId !== room.hostId && u.isConnected)
    .map(u => u.userId);

  if (waitingIds.length === 0) {
    // No guests to wait for — nothing to do
    room.bufferTrackId = null;
    room.bufferWaitingFor = null;
    return;
  }

  room.bufferTrackId = trackId;
  room.bufferWaitingFor = new Set(waitingIds);

  broadcast(room, 'buffer_wait', { track_id: trackId, waiting_for: waitingIds });

  room.bufferTimeoutHandle = setTimeout(() => {
    if (room.bufferTrackId === trackId) {
      console.log(`Room ${room.roomCode}: buffer wait timed out for track ${trackId}, forcing buffer_complete`);
      finishBufferWait(room, trackId);
    }
  }, BUFFER_WAIT_TIMEOUT_MS);
}

function finishBufferWait(room, trackId) {
  if (room.bufferTimeoutHandle) {
    clearTimeout(room.bufferTimeoutHandle);
    room.bufferTimeoutHandle = null;
  }
  room.bufferTrackId = null;
  room.bufferWaitingFor = null;
  broadcast(room, 'buffer_complete', { track_id: trackId });
}

function onBufferReady(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  if (!room.bufferWaitingFor || room.bufferTrackId !== payload.track_id) return; // stale/unknown wait

  room.bufferWaitingFor.delete(ws.userId);

  if (room.bufferWaitingFor.size === 0) {
    finishBufferWait(room, payload.track_id);
  }
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

function onSuggestTrack(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  if (ws.userId === room.hostId) return; // host doesn't suggest to itself

  const suggester = room.users.find(u => u.userId === ws.userId);
  if (!suggester || !payload.track_info) return;

  const suggestionId = uuidv4();
  suggestions.set(suggestionId, {
    roomCode: room.roomCode,
    fromUserId: ws.userId,
    fromUsername: suggester.username,
    trackInfo: payload.track_info,
  });

  const host = room.users.find(u => u.userId === room.hostId);
  if (host && host.socket) {
    send(host.socket, 'suggestion_received', {
      suggestion_id: suggestionId,
      from_user_id: ws.userId,
      from_username: suggester.username,
      track_info: payload.track_info,
    });
  }
}

function onApproveSuggestion(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return; // only host approves

  const suggestion = suggestions.get(payload.suggestion_id);
  if (!suggestion || suggestion.roomCode !== room.roomCode) return;
  suggestions.delete(payload.suggestion_id);

  // Add the suggested track to the queue
  room.queue.push(suggestion.trackInfo);
  room.lastUpdate = Date.now();

  broadcast(room, 'suggestion_approved', {
    suggestion_id: payload.suggestion_id,
    track_info: suggestion.trackInfo,
  });
  send(ws, 'suggestion_approved', {
    suggestion_id: payload.suggestion_id,
    track_info: suggestion.trackInfo,
  });

  // Let everyone know the queue changed too
  broadcast(room, 'sync_playback', {
    action: 'queue_add',
    track_info: suggestion.trackInfo,
    server_time: Date.now(),
  }, ws.userId);
}

function onRejectSuggestion(ws, payload) {
  const room = rooms.get(ws.roomCode);
  if (!room || room.hostId !== ws.userId) return; // only host rejects

  const suggestion = suggestions.get(payload.suggestion_id);
  if (!suggestion || suggestion.roomCode !== room.roomCode) return;
  suggestions.delete(payload.suggestion_id);

  const reasonPayload = { suggestion_id: payload.suggestion_id, reason: payload.reason || 'Rejected by host' };

  broadcast(room, 'suggestion_rejected', reasonPayload);
  send(ws, 'suggestion_rejected', reasonPayload);
}

function onLeaveRoom(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const user = room.users.find(u => u.userId === ws.userId);
  room.users = room.users.filter(u => u.userId !== ws.userId);
  broadcast(room, 'user_left', { user_id: ws.userId, username: user?.username });
  releaseBufferWaitFor(room, ws.userId);
  cleanupRoomIfEmpty(room);
}

// If someone we were waiting on for buffering leaves/disconnects, don't let
// the rest of the room hang until the timeout — release them immediately.
function releaseBufferWaitFor(room, userId) {
  if (!room.bufferWaitingFor || !room.bufferWaitingFor.has(userId)) return;
  room.bufferWaitingFor.delete(userId);
  if (room.bufferWaitingFor.size === 0) {
    finishBufferWait(room, room.bufferTrackId);
  }
}

function handleDisconnect(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const user = room.users.find(u => u.userId === ws.userId);
  if (user) {
    user.isConnected = false;
    broadcast(room, 'user_disconnected', { user_id: ws.userId, username: user.username }, ws.userId);
  }
  releaseBufferWaitFor(room, ws.userId);
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
        for (const [id, s] of suggestions) {
          if (s.roomCode === room.roomCode) suggestions.delete(id);
        }
        console.log(`Room ${room.roomCode} cleaned up (empty).`);
      }
    }, 5 * 60 * 1000); // 5 min grace period
  }
}

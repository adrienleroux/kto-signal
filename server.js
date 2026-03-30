const http = require("http");
const WebSocket = require("ws");

const PORT = parseInt(process.env.PORT || "3001", 10);
const MAX_PLAYERS = 4;

// ── HTTP server (satisfies cPanel health check) ────────────────────
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("kto-signal ok");
});

httpServer.listen(PORT, () => {
  console.log(`[signal] listening on port ${PORT}`);
});

// ── state ──────────────────────────────────────────────────────────
const rooms = new Map();
let nextPeerId = 1;
let nextRoomId = 1;

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  ws.peerId = null;
  ws.roomId = null;
  ws.isAlive = true;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleMessage(ws, msg);
  });

  ws.on("close", () => handleDisconnect(ws));
  ws.on("error", () => handleDisconnect(ws));
});

// ── keepalive ──────────────────────────────────────────────────────
const PING_INTERVAL = 30000;
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL);

// ── message router ─────────────────────────────────────────────────
function handleMessage(ws, msg) {
  switch (msg.type) {
    case "list_rooms":  return sendRoomList(ws);
    case "create_room": return createRoom(ws, msg);
    case "join_room":   return joinRoom(ws, msg);
    case "leave_room":  return leaveRoom(ws);
    case "offer":       // fall-through
    case "answer":      // fall-through
    case "ice":         return relayToTarget(ws, msg);
    default: break;
  }
}

// ── room management ────────────────────────────────────────────────
function createRoom(ws, msg) {
  if (ws.roomId !== null) leaveRoom(ws);

  const roomId = String(nextRoomId++);
  const peerId = nextPeerId++;

  ws.peerId = peerId;
  ws.roomId = roomId;

  const room = {
    id: roomId,
    name: String(msg.name || "Room " + roomId).substring(0, 32),
    host_ws: ws,
    players: new Map([[peerId, ws]]),
  };
  rooms.set(roomId, room);

  send(ws, { type: "room_created", room_id: roomId, peer_id: peerId });
  broadcastRoomList();
}

function joinRoom(ws, msg) {
  if (ws.roomId !== null) leaveRoom(ws);

  const roomId = String(msg.room_id || "");
  const room = rooms.get(roomId);
  if (!room) return send(ws, { type: "error", message: "Room not found" });
  if (room.players.size >= MAX_PLAYERS) return send(ws, { type: "error", message: "Room full" });

  const peerId = nextPeerId++;
  ws.peerId = peerId;
  ws.roomId = roomId;
  room.players.set(peerId, ws);

  // tell the joiner about all existing peers
  const existingPeers = [];
  for (const [id] of room.players) {
    if (id !== peerId) existingPeers.push(id);
  }
  send(ws, { type: "room_joined", room_id: roomId, peer_id: peerId, peers: existingPeers });

  // tell every existing peer about the new joiner
  for (const [id, sock] of room.players) {
    if (id !== peerId) {
      send(sock, { type: "peer_joined", peer_id: peerId });
    }
  }

  broadcastRoomList();
}

function leaveRoom(ws) {
  const roomId = ws.roomId;
  if (roomId === null) return;

  const room = rooms.get(roomId);
  ws.roomId = null;
  const peerId = ws.peerId;
  ws.peerId = null;

  if (!room) return;
  room.players.delete(peerId);

  if (ws === room.host_ws || room.players.size === 0) {
    // host left → close entire room
    for (const [, sock] of room.players) {
      send(sock, { type: "room_closed" });
      sock.roomId = null;
      sock.peerId = null;
    }
    rooms.delete(roomId);
  } else {
    // non-host left → notify remaining
    for (const [, sock] of room.players) {
      send(sock, { type: "peer_left", peer_id: peerId });
    }
  }

  broadcastRoomList();
}

// ── WebRTC relay ───────────────────────────────────────────────────
function relayToTarget(ws, msg) {
  const room = rooms.get(ws.roomId);
  if (!room) return;

  const targetId = msg.target_peer_id;
  const target = room.players.get(targetId);
  if (!target) return;

  send(target, {
    type: msg.type,            // "offer" | "answer" | "ice"
    from_peer_id: ws.peerId,
    sdp: msg.sdp,              // for offer/answer
    candidate: msg.candidate,  // for ice
  });
}

// ── room list ──────────────────────────────────────────────────────
function sendRoomList(ws) {
  send(ws, { type: "room_list", rooms: buildRoomList() });
}

function broadcastRoomList() {
  const payload = JSON.stringify({ type: "room_list", rooms: buildRoomList() });
  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(payload);
  });
}

function buildRoomList() {
  const list = [];
  for (const [, room] of rooms) {
    list.push({
      id: room.id,
      name: room.name,
      players: room.players.size,
      max_players: MAX_PLAYERS,
    });
  }
  return list;
}

// ── helpers ────────────────────────────────────────────────────────
function handleDisconnect(ws) {
  if (ws.roomId !== null) leaveRoom(ws);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

module.exports = httpServer;

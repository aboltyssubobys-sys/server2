const http = require("http");
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");
const crypto = require("crypto");

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || "change-me";
const HOST_PUBLIC_URL = process.env.HOST_PUBLIC_URL || "";
const CHUNK_SIZE = 500;
const MAX_SNAPSHOT_SIZE = 100 * 1024 * 1024; // 100 MB

const rooms = new Map();
const activeSessions = new Map(); // sessionId -> { userId, lobbyId, expires }

function getRoom(lobbyId) {
  if (!rooms.has(lobbyId)) {
    rooms.set(lobbyId, { clients: new Map(), snapshot: [], history: [], hostUserId: null, createdAt: Date.now() });
  }
  return rooms.get(lobbyId);
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, {"Content-Type":"application/json"});
    res.end(JSON.stringify({ok:true, rooms:rooms.size, sessions:activeSessions.size, ts:Date.now()}));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: MAX_SNAPSHOT_SIZE });

// 🟢 Rate limiting (simple per-IP)
const ipConnections = new Map();
function checkRateLimit(ip) {
  const now = Date.now();
  const conns = (ipConnections.get(ip) || []).filter(t => now - t < 60_000);
  if (conns.length >= 30) return false;
  conns.push(now);
  ipConnections.set(ip, conns);
  return true;
}

function broadcast(room, msg, except = null) {
  const data = JSON.stringify(msg);
  for (const [ws] of room.clients) {
    if (ws !== except && ws.readyState === 1) {
      try { ws.send(data); } catch(e) {}
    }
  }
}
function sendPlayers(room) {
  const players = [];
  for (const [, info] of room.clients) players.push({ userId: info.userId, userName: info.userName, isHost: info.isHost });
  broadcast(room, { type: "players", players });
}
function sendSnapshotChunks(ws, room) {
  for (let i = 0; i < room.snapshot.length; i += CHUNK_SIZE) {
    try { ws.send(JSON.stringify({ type: "snapshot_chunk", chunk: room.snapshot.slice(i, i+CHUNK_SIZE), index: i, total: room.snapshot.length })); } catch(e) {}
  }
  if (room.history.length > 0) try { ws.send(JSON.stringify({ type: "history", data: room.history })); } catch(e) {}
}

wss.on("connection", (ws, req) => {
  const ip = req.socket.remoteAddress;
  if (!checkRateLimit(ip)) { ws.close(1008, "Rate limit"); return; }

  let lobbyId = null, myUserId = null, myUserName = null, myIsHost = false;
  let sessionToken = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch(e) { return; }
    if (!msg || typeof msg !== "object") return;

    // 🟢 Auth handshake
    if (msg.type === "hello") {
      try {
        const decoded = jwt.verify(msg.token, JWT_SECRET);
        // 🟢 Validate expiry and one-time session
        if (decoded.exp && Date.now() / 1000 > decoded.exp) throw new Error("Token expired");
        lobbyId = decoded.lobbyId;
        myUserId = Number(decoded.userId) || Number(msg.userId);
        myUserName = decoded.userName || msg.userName || String(myUserId);
        myIsHost = !!decoded.isHost;
        sessionToken = msg.token;
        activeSessions.set(sessionToken, { userId: myUserId, lobbyId, expires: Date.now() + 3600_000 });
      } catch(e) {
        try { ws.send(JSON.stringify({ type: "error", message: "Invalid token" })); } catch(x) {}
        try { ws.close(1008, "Invalid token"); } catch(x) {}
        return;
      }
      const room = getRoom(lobbyId);
      room.clients.set(ws, { userId: myUserId, userName: myUserName, isHost: myIsHost });
      if (!room.hostUserId) room.hostUserId = myIsHost ? myUserId : null;
      try { ws.send(JSON.stringify({ type: "welcome", lobbyId, snapshotSize: room.snapshot.length, players: [...room.clients.values()].map(v=>({userId:v.userId,userName:v.userName,isHost:v.isHost})) })); } catch(e) {}
      sendSnapshotChunks(ws, room);
      sendPlayers(room);
      return;
    }

    if (!lobbyId) return;
    const room = getRoom(lobbyId);

    // 🟢 Verify session still valid
    if (!activeSessions.has(sessionToken)) { ws.close(1008, "Session expired"); return; }

    if (msg.type === "request_snapshot") { sendSnapshotChunks(ws, room); return; }
    if (msg.type === "snapshot") {
      if (myIsHost && Array.isArray(msg.data)) { room.snapshot = msg.data; room.history = []; console.log(`[Snapshot] ${lobbyId}: ${room.snapshot.length}`); }
      return;
    }
    if (msg.type === "delta") {
      // 🟢 Sanitize: only allow owner or host to send certain kinds? (Basic validation)
      if (msg.kind && msg.kind.match(/^[a-z_]+$/) && msg.userId === myUserId) {
        if (msg.kind !== "camera") { room.history.push(msg); if (room.history.length > 5000) room.history.shift(); }
        broadcast(room, msg, ws);
      }
      return;
    }
    if (msg.type === "heartbeat") { try { ws.send(JSON.stringify({type:"heartbeat_ack",ts:Date.now()})); } catch(e){} return; }
    if (msg.type === "chat") { broadcast(room, {type:"chat",userId:myUserId,userName:myUserName,text:String(msg.text||"").slice(0,1000),ts:Date.now()}, null); return; }
    if (msg.type === "kick") {
      if (!myIsHost) return;
      const target = Number(msg.targetUserId);
      for (const [client, info] of room.clients) {
        if (info.userId === target) {
          try { client.send(JSON.stringify({type:"kicked"})); } catch(e){}
          try { client.close(); } catch(e){}
          room.clients.delete(client);
        }
      }
      sendPlayers(room);
      return;
    }
    if (msg.type === "leave") {
      const leavingInfo = room.clients.get(ws);
      const wasHost = leavingInfo && leavingInfo.isHost;
      room.clients.delete(ws);
      activeSessions.delete(sessionToken);
      if (wasHost) {
        broadcast(room, {type:"host_left"});
        setTimeout(() => {
          for (const [c] of room.clients) try { c.send(JSON.stringify({type:"host_left"})); c.close(); } catch(e){}
          room.clients.clear();
          rooms.delete(lobbyId);
        }, 1000);
      }
      sendPlayers(room);
      try { ws.close(); } catch(e){}
      return;
    }
  });

  ws.on("close", () => {
    if (lobbyId) {
      const room = rooms.get(lobbyId);
      if (room) {
        const leavingInfo = room.clients.get(ws);
        const wasHost = leavingInfo && leavingInfo.isHost;
        room.clients.delete(ws);
        if (sessionToken) activeSessions.delete(sessionToken);
        if (wasHost && room.clients.size > 0) broadcast(room, {type:"host_left"});
        sendPlayers(room);
        if (room.clients.size === 0) setTimeout(() => { const r = rooms.get(lobbyId); if (r && r.clients.size === 0) rooms.delete(lobbyId); }, 60_000);
      }
    }
  });
});

// 🟢 Periodic cleanup of expired sessions & empty rooms
setInterval(() => {
  const now = Date.now();
  for (const [token, s] of activeSessions) if (s.expires < now) activeSessions.delete(token);
  for (const [id, room] of rooms) if (room.clients.size === 0 && now - room.createdAt > 3600_000) rooms.delete(id);
  // Keep-alive
  const url = HOST_PUBLIC_URL ? HOST_PUBLIC_URL + "/health" : `http://localhost:${PORT}/health`;
  fetch(url).catch(() => {});
}, 10_000);

server.listen(PORT, () => console.log(`[CollabSync] Host server listening ${PORT}`));

const http = require("http");
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || "change-me";
const HOST_PUBLIC_URL = process.env.HOST_PUBLIC_URL || "";
const CHUNK_SIZE = 500;

const rooms = new Map();
function getRoom(lobbyId) {
  if (!rooms.has(lobbyId)) rooms.set(lobbyId, { clients: new Map(), snapshot: [], history: [], hostUserId: null });
  return rooms.get(lobbyId);
}
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, ts: Date.now() }));
    return;
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 200 * 1024 * 1024 });

function broadcast(room, msg, except = null) {
  const data = JSON.stringify(msg);
  for (const [ws] of room.clients) {
    if (ws !== except && ws.readyState === 1) try { ws.send(data); } catch(e) {}
  }
}
function sendPlayers(room) {
  const players = [];
  for (const [, info] of room.clients) players.push({ userId: info.userId, userName: info.userName, isHost: info.isHost });
  broadcast(room, { type: "players", players });
}
function sendSnapshotChunks(ws, room) {
  for (let i = 0; i < room.snapshot.length; i += CHUNK_SIZE) {
    const chunk = room.snapshot.slice(i, i + CHUNK_SIZE);
    try { ws.send(JSON.stringify({ type: "snapshot_chunk", chunk, index: i, total: room.snapshot.length })); } catch(e) {}
  }
  if (room.history.length > 0) try { ws.send(JSON.stringify({ type: "history", data: room.history })); } catch(e) {}
}

wss.on("connection", (ws) => {
  let lobbyId = null, myUserId = null, myUserName = null, myIsHost = false;
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch(e) { return; }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "hello") {
      try {
        const decoded = jwt.verify(msg.token, JWT_SECRET);
        lobbyId = decoded.lobbyId;
        myUserId = Number(decoded.userId) || Number(msg.userId);
        myUserName = decoded.userName || msg.userName || String(myUserId);
        myIsHost = !!decoded.isHost;
      } catch(e) {
        try { ws.send(JSON.stringify({ type: "error", message: "bad token" })); } catch(x) {}
        try { ws.close(); } catch(x) {}
        return;
      }
      const room = getRoom(lobbyId);
      room.clients.set(ws, { userId: myUserId, userName: myUserName, isHost: myIsHost });
      if (!room.hostUserId) room.hostUserId = myIsHost ? myUserId : null;
      try {
        ws.send(JSON.stringify({
          type: "welcome",
          lobbyId,
          snapshotSize: room.snapshot.length,
          historySize: room.history.length,
          players: [...room.clients.values()].map(v => ({ userId: v.userId, userName: v.userName, isHost: v.isHost })),
        }));
      } catch(e) {}
      sendSnapshotChunks(ws, room);
      sendPlayers(room);
      return;
    }
    if (!lobbyId) return;
    const room = getRoom(lobbyId);
    if (msg.type === "request_snapshot") {
      sendSnapshotChunks(ws, room);
      return;
    }
    if (msg.type === "snapshot") {
      if (myIsHost && Array.isArray(msg.data)) {
        room.snapshot = msg.data;
        room.history = [];
        console.log(`[Snapshot] ${lobbyId}: ${room.snapshot.length}`);
      }
      return;
    }
    if (msg.type === "delta") {
      if (msg.kind !== "camera") {
        room.history.push(msg);
        if (room.history.length > 5000) room.history.shift();
      }
      broadcast(room, msg, ws);
      return;
    }
    if (msg.type === "heartbeat") { try { ws.send(JSON.stringify({ type: "heartbeat_ack", ts: Date.now() })); } catch(e) {} return; }
    if (msg.type === "chat") { broadcast(room, { type: "chat", userId: myUserId, userName: myUserName, text: String(msg.text || "").slice(0, 1000), ts: Date.now() }, null); return; }
    if (msg.type === "kick") {
      if (!myIsHost) return;
      const target = Number(msg.targetUserId);
      for (const [client, info] of room.clients) {
        if (info.userId === target) {
          try { client.send(JSON.stringify({ type: "kicked" })); } catch(e) {}
          try { client.close(); } catch(e) {}
          room.clients.delete(client);
        }
      }
      sendPlayers(room);
      return;
    }
    if (msg.type === "leave") {
      room.clients.delete(ws);
      sendPlayers(room);
      try { ws.close(); } catch(e) {}
      return;
    }
  });
  ws.on("close", () => {
    if (lobbyId) {
      const room = rooms.get(lobbyId);
      if (room) {
        room.clients.delete(ws);
        sendPlayers(room);
        if (room.clients.size === 0) setTimeout(() => {
          const r = rooms.get(lobbyId);
          if (r && r.clients.size === 0) rooms.delete(lobbyId);
        }, 60_000);
      }
    }
  });
});
setInterval(() => {
  const url = HOST_PUBLIC_URL ? HOST_PUBLIC_URL + "/health" : `http://localhost:${PORT}/health`;
  fetch(url).catch(() => {});
}, 10_000);
server.listen(PORT, () => console.log(`[CollabSync] listening ${PORT}`));

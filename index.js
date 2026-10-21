const http = require("http");
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || "change-me";
const HOST_PUBLIC_URL = process.env.HOST_PUBLIC_URL || "";

// ============================================================
// ХРАНИЛИЩЕ КОМНАТ
// ============================================================
// room = {
//   clients: Map<ws, {userId, userName, isHost}>,
//   snapshot: [],
//   history: [],
//   hostUserId: number | null,
// }
const rooms = new Map();

function getRoom(lobbyId) {
  if (!rooms.has(lobbyId)) {
    rooms.set(lobbyId, {
      clients: new Map(),
      snapshot: [],
      history: [],
      hostUserId: null,
    });
  }
  return rooms.get(lobbyId);
}

// ============================================================
// HTTP-СЕРВЕР (для /health и keep-alive)
// ============================================================
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, ts: Date.now() }));
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});

// ============================================================
// WEBSOCKET-СЕРВЕР
// ============================================================
const wss = new WebSocketServer({ server, path: "/ws" });

function broadcast(room, msg, except = null) {
  const data = JSON.stringify(msg);
  for (const [ws] of room.clients) {
    if (ws !== except && ws.readyState === 1) {
      try { ws.send(data); } catch (e) { /* ignore */ }
    }
  }
}

function sendPlayers(room) {
  const players = [];
  for (const [, info] of room.clients) {
    players.push({
      userId: info.userId,
      userName: info.userName,
      isHost: info.isHost,
    });
  }
  broadcast(room, { type: "players", players });
}

wss.on("connection", (ws, req) => {
  let lobbyId = null;
  let myUserId = null;
  let myUserName = null;
  let myIsHost = false;

  console.log("[WS] New connection from", req.socket.remoteAddress);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }
    if (!msg || typeof msg !== "object") return;

    // ------------------------------------------------------------
    // HELLO — авторизация по JWT
    // ------------------------------------------------------------
    if (msg.type === "hello") {
      try {
        const decoded = jwt.verify(msg.token, JWT_SECRET);
        lobbyId = decoded.lobbyId;
        myUserId = Number(decoded.userId) || Number(msg.userId);
        myUserName = decoded.userName || msg.userName || String(myUserId);
        myIsHost = !!decoded.isHost;
      } catch (e) {
        console.log("[WS] Auth error:", e.message);
        try {
          ws.send(JSON.stringify({ type: "error", message: "bad token: " + e.message }));
        } catch {}
        try { ws.close(); } catch {}
        return;
      }

      const room = getRoom(lobbyId);
      room.clients.set(ws, {
        userId: myUserId,
        userName: myUserName,
        isHost: myIsHost,
      });
      if (!room.hostUserId) {
        room.hostUserId = myIsHost ? myUserId : null;
      }

      console.log(
        `[WS] User ${myUserName} (${myUserId}) joined room ${lobbyId}. Host=${myIsHost}. Clients=${room.clients.size}.`
      );

      try {
        ws.send(JSON.stringify({
          type: "welcome",
          lobbyId,
          snapshot: room.snapshot,
          history: room.history,
          players: [...room.clients.values()].map((v) => ({
            userId: v.userId,
            userName: v.userName,
            isHost: v.isHost,
          })),
        }));
      } catch (e) {
        console.log("[WS] welcome send error:", e.message);
      }

      sendPlayers(room);
      return;
    }

    // Все остальные сообщения — только для авторизованных
    if (!lobbyId) return;
    const room = getRoom(lobbyId);

    // ------------------------------------------------------------
    // SNAPSHOT — от хоста (полное состояние)
    // ------------------------------------------------------------
    if (msg.type === "snapshot") {
      if (myIsHost && Array.isArray(msg.data)) {
        room.snapshot = msg.data;
        room.history = [];
        console.log(
          `[WS] Snapshot received for room ${lobbyId}: ${room.snapshot.length} items.`
        );
      } else {
        console.log(
          `[WS] Snapshot rejected (not host or invalid data). User=${myUserName}, isHost=${myIsHost}`
        );
      }
      return;
    }

    // ------------------------------------------------------------
    // DELTA — изменения
    // ------------------------------------------------------------
    if (msg.type === "delta") {
      room.history.push(msg);
      if (room.history.length > 5000) room.history.shift();
      broadcast(room, msg, ws);
      return;
    }

    // ------------------------------------------------------------
    // HEARTBEAT — пинг
    // ------------------------------------------------------------
    if (msg.type === "heartbeat") {
      try {
        ws.send(JSON.stringify({ type: "heartbeat_ack", ts: Date.now() }));
      } catch {}
      return;
    }

    // ------------------------------------------------------------
    // CHAT
    // ------------------------------------------------------------
    if (msg.type === "chat") {
      const payload = {
        type: "chat",
        userId: myUserId,
        userName: myUserName,
        text: String(msg.text || "").slice(0, 1000),
        ts: Date.now(),
      };
      broadcast(room, payload, null);
      return;
    }

    // ------------------------------------------------------------
    // KICK — только хост
    // ------------------------------------------------------------
    if (msg.type === "kick") {
      if (!myIsHost) {
        console.log(`[WS] Kick denied: user ${myUserName} is not host.`);
        return;
      }
      const target = Number(msg.targetUserId);
      if (!target) return;
      console.log(`[WS] Host ${myUserName} kicked user ${target} in room ${lobbyId}.`);
      for (const [client, info] of room.clients) {
        if (info.userId === target) {
          try { client.send(JSON.stringify({ type: "kicked" })); } catch {}
          try { client.close(); } catch {}
          room.clients.delete(client);
        }
      }
      sendPlayers(room);
      return;
    }

    // ------------------------------------------------------------
    // LEAVE
    // ------------------------------------------------------------
    if (msg.type === "leave") {
      room.clients.delete(ws);
      console.log(`[WS] User ${myUserName} left room ${lobbyId}.`);
      sendPlayers(room);
      try { ws.close(); } catch {}
      return;
    }
  });

  ws.on("close", () => {
    if (lobbyId) {
      const room = rooms.get(lobbyId);
      if (room) {
        room.clients.delete(ws);
        console.log(
          `[WS] Disconnected: ${myUserName} from room ${lobbyId}. Clients left: ${room.clients.size}.`
        );
        sendPlayers(room);

        // Если комната пуста — через 60 сек удалить
        if (room.clients.size === 0) {
          setTimeout(() => {
            const r = rooms.get(lobbyId);
            if (r && r.clients.size === 0) {
              rooms.delete(lobbyId);
              console.log(`[WS] Room ${lobbyId} deleted (empty for 60s).`);
            }
          }, 60_000);
        }
      }
    }
  });

  ws.on("error", (err) => {
    console.log("[WS] Socket error:", err.message);
  });
});

// ============================================================
// KEEP-ALIVE — каждые 10 секунд пингуем свой /health,
// чтобы Render не засыпал
// ============================================================
setInterval(() => {
  const url = HOST_PUBLIC_URL
    ? HOST_PUBLIC_URL + "/health"
    : `http://localhost:${PORT}/health`;
  fetch(url).catch(() => {});
}, 10_000);

// ============================================================
// СТАРТ
// ============================================================
server.listen(PORT, () => {
  console.log(`[CollabSync] Host server listening on port ${PORT}`);
  console.log(`[CollabSync] WS endpoint: /ws`);
  console.log(`[CollabSync] Health endpoint: /health`);
});

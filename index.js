const http = require("http");
const { WebSocketServer } = require("ws");
const jwt = require("jsonwebtoken");

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || "change-me-securely";
const HOST_PUBLIC_URL = process.env.HOST_PUBLIC_URL || "";

const CHUNK_SIZE = 200;
const CHUNK_DELAY_MS = 60;
const MAX_HISTORY = 8000; // Защита от перегрузки памяти

const rooms = new Map();

function getRoom(lobbyId) {
    if (!rooms.has(lobbyId)) {
        rooms.set(lobbyId, { clients: new Map(), snapshot: [], history: [], hostUserId: null, lastHostPing: Date.now() });
    }
    return rooms.get(lobbyId);
}

const server = http.createServer((req, res) => {
    if (req.url === "/health") { 
        res.writeHead(200, {"Content-Type":"application/json"}); 
        res.end(JSON.stringify({ok:true, rooms:rooms.size, ts:Date.now()})); 
        return; 
    }
    res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 50 * 1024 * 1024 }); // 50MB limit

function broadcast(room, msg, except=null) {
    const data = JSON.stringify(msg);
    for (const [ws] of room.clients) {
        if (ws !== except && ws.readyState === 1) {
            try { ws.send(data); } catch(e){}
        }
    }
}

function sendPlayers(room) {
    const players = [];
    for (const [, info] of room.clients) {
        players.push({userId:info.userId, userName:info.userName, isHost:info.isHost, roles: info.roles||{}});
    }
    broadcast(room, {type:"players", players});
}

async function sendSnapshotChunks(ws, room) {
    for (let i = 0; i < room.snapshot.length; i += CHUNK_SIZE) {
        try { 
            ws.send(JSON.stringify({
                type:"snapshot_chunk", 
                chunk:room.snapshot.slice(i, i+CHUNK_SIZE), 
                index:i, 
                total:room.snapshot.length
            })); 
        } catch(e){ return; }
        await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
    }
    if (room.history.length > 0) {
        try { ws.send(JSON.stringify({type:"history", data:room.history})); } catch(e){}
    }
}

// Защита от фантомных лобби: проверка хоста каждые 10 секунд
setInterval(() => {
    const now = Date.now();
    for (const [lobbyId, room] of rooms.entries()) {
        if (room.hostUserId) {
            let hostOnline = false;
            for (const [, info] of room.clients) {
                if (info.userId === room.hostUserId && info.isHost) {
                    hostOnline = true;
                    break;
                }
            }
            if (!hostOnline && (now - room.lastHostPing > 15000)) {
                console.log(`[Phantom Lobby] Host missing for ${lobbyId}, cleaning up`);
                broadcast(room, {type:"host_left"});
                setTimeout(() => {
                    for (const [c] of room.clients) { try { c.close(); } catch(e){} }
                    room.clients.clear();
                    rooms.delete(lobbyId);
                }, 1000);
            }
        }
    }
}, 10000);

wss.on("connection", (ws) => {
    let lobbyId=null, myUserId=null, myUserName=null, myIsHost=false;
    
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
                myIsHost = !!decoded.isHost; // Строго из JWT, нельзя подделать
            } catch(e) {
                try { ws.send(JSON.stringify({type:"error", message:"bad token"})); } catch(x){}
                try { ws.close(); } catch(x){}
                return;
            }
            
            const room = getRoom(lobbyId);
            
            // Защита: если это не хост, а хост еще не подключился или отключился
            if (!myIsHost) {
                if (!room.hostUserId) {
                    try { ws.send(JSON.stringify({type:"error", message:"waiting for host"})); } catch(e){}
                    ws.close();
                    return;
                }
                let hostOnline = false;
                for (const [, info] of room.clients) {
                    if (info.userId === room.hostUserId && info.isHost) { hostOnline = true; break; }
                }
                if (!hostOnline) {
                    try { ws.send(JSON.stringify({type:"error", message:"host offline"})); } catch(e){}
                    ws.close();
                    return;
                }
            } else {
                room.hostUserId = myUserId;
                room.lastHostPing = Date.now();
            }
            
            room.clients.set(ws, { userId: myUserId, userName: myUserName, isHost: myIsHost, roles: {} });
            
            try {
                ws.send(JSON.stringify({
                    type:"welcome", lobbyId,
                    snapshotSize: room.snapshot.length,
                    players: [...room.clients.values()].map(v=>({userId:v.userId, userName:v.userName, isHost:v.isHost, roles: v.roles||{}})),
                }));
            } catch(e){}
            
            sendSnapshotChunks(ws, room).catch(()=>{});
            sendPlayers(room);
            return;
        }
        
        if (!lobbyId) return;
        const room = getRoom(lobbyId);
        const clientInfo = room.clients.get(ws);
        
        if (msg.type === "request_snapshot") { 
            sendSnapshotChunks(ws, room).catch(()=>{}); 
            return; 
        }
        
        if (msg.type === "roles") {
            if (clientInfo && msg.roles) { 
                clientInfo.roles = msg.roles; 
                sendPlayers(room); 
            }
            return;
        }
        
        if (msg.type === "snapshot") {
            // Только хост может отправлять полный снапшот
            if (myIsHost && Array.isArray(msg.data)) {
                room.snapshot = msg.data;
                room.history = [];
                console.log(`[Snapshot] ${lobbyId}: ${room.snapshot.length} items`);
            }
            return;
        }
        
        if (msg.type === "delta") {
            if (msg.kind !== "camera") { 
                room.history.push(msg); 
                if (room.history.length > MAX_HISTORY) room.history.shift(); // Защита от перегрузки
            }
            broadcast(room, msg, ws);
            return;
        }
        
        if (msg.type === "heartbeat") { 
            if (myIsHost) room.lastHostPing = Date.now();
            try { ws.send(JSON.stringify({type:"heartbeat_ack", ts:Date.now()})); } catch(e){} 
            return; 
        }
        
        if (msg.type === "chat") { 
            broadcast(room, {type:"chat", userId:myUserId, userName:myUserName, text:String(msg.text||"").slice(0,1000), ts:Date.now()}, null); 
            return; 
        }
        
        if (msg.type === "kick") {
            // Защита: кикать может только хост
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
            const wasHost = clientInfo && clientInfo.isHost;
            room.clients.delete(ws);
            
            if (wasHost) {
                broadcast(room, {type:"host_left"});
                setTimeout(() => {
                    for (const [c] of room.clients) { 
                        try { c.send(JSON.stringify({type:"host_left"})); c.close(); } catch(e){} 
                    }
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
                const info = room.clients.get(ws);
                const wasHost = info && info.isHost;
                room.clients.delete(ws);
                
                if (wasHost && room.clients.size > 0) {
                    broadcast(room, {type:"host_left"});
                    setTimeout(() => {
                        for (const [c] of room.clients) { 
                            try { c.send(JSON.stringify({type:"host_left"})); c.close(); } catch(e){} 
                        }
                        room.clients.clear();
                        rooms.delete(lobbyId);
                    }, 1000);
                }
                
                sendPlayers(room);
                if (room.clients.size === 0) {
                    setTimeout(() => { 
                        const r = rooms.get(lobbyId); 
                        if (r && r.clients.size === 0) rooms.delete(lobbyId); 
                    }, 60000);
                }
            }
        }
    });
});

setInterval(() => {
    const url = HOST_PUBLIC_URL ? HOST_PUBLIC_URL + "/health" : `http://localhost:${PORT}/health`;
    fetch(url).catch(()=>{});
}, 10000);

server.listen(PORT, () => console.log(`[CollabSync Host] listening ${PORT}`));

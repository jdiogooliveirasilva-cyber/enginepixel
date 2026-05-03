const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const path = require("path");
const { randomUUID } = require("crypto");

const app = express();
const port = process.env.PORT || 8080;

app.use(function(_req, res, next){
  res.setHeader("Access-Control-Allow-Origin", "*");
  next();
});

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "engine.html"));
});

app.get("/healthz", (_req, res) => {
  res.json({ status: "ok", rooms: rooms.size });
});

// Lista de salas para o lobby
app.get("/api/rooms", (_req, res) => {
  var list = Array.from(rooms.values()).map(function(r){
    return { name: r.name, players: r.players.size };
  });
  res.json(list);
});

const rooms = new Map();

function getOrCreateRoom(name) {
  if (!rooms.has(name)) rooms.set(name, { name, players: new Map(), state: null });
  return rooms.get(name);
}

function removePlayer(room, playerId) {
  const player = room.players.get(playerId);
  if (!player) return;
  room.players.delete(playerId);
  const msg = JSON.stringify({ type: "playerLeave", playerId });
  for (const p of room.players.values())
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg);
  if (room.players.size === 0) rooms.delete(room.name);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/api/net" });

setInterval(() => {
  for (const room of rooms.values())
    for (const p of room.players.values())
      if (p.ws.readyState === WebSocket.OPEN)
        p.ws.send(JSON.stringify({ type: "ping" }));
}, 25000);

wss.on("connection", (ws) => {
  const playerId = randomUUID();
  let currentRoom = null;
  let playerName = "Player";

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "join") {
      if (currentRoom) removePlayer(currentRoom, playerId);
      const roomName = msg.room || "default";
      playerName = msg.name || "Player";
      currentRoom = getOrCreateRoom(roomName);
      currentRoom.players.set(playerId, { id: playerId, name: playerName, ws });

      const others = Array.from(currentRoom.players.values())
        .filter(p => p.id !== playerId)
        .map(p => ({ id: p.id, name: p.name }));

      send(ws, { type: "welcome", id: playerId, room: roomName, players: others, state: currentRoom.state });

      const joinMsg = JSON.stringify({ type: "playerJoin", player: { id: playerId, name: playerName } });
      for (const p of currentRoom.players.values())
        if (p.id !== playerId && p.ws.readyState === WebSocket.OPEN) p.ws.send(joinMsg);
      return;
    }

    if (msg.type === "setState" && currentRoom) {
      currentRoom.state = msg.data !== undefined ? msg.data : null;
      const stateMsg = JSON.stringify({ type: "state", data: currentRoom.state, from: playerId, fromName: playerName });
      for (const p of currentRoom.players.values())
        if (p.id !== playerId && p.ws.readyState === WebSocket.OPEN) p.ws.send(stateMsg);
      return;
    }

    if (msg.type === "msg" && currentRoom) {
      const broadcast = JSON.stringify({ type: "msg", event: msg.event, data: msg.data ?? null, from: playerId, fromName: playerName });
      for (const p of currentRoom.players.values())
        if (p.id !== playerId && p.ws.readyState === WebSocket.OPEN) p.ws.send(broadcast);
      return;
    }
  });

  ws.on("close", () => { if (currentRoom) removePlayer(currentRoom, playerId); });
  ws.on("error", () => { if (currentRoom) removePlayer(currentRoom, playerId); });
});

httpServer.listen(port, () => {
  console.log("[PixelEngine] Servidor na porta " + port);
  console.log("[PixelEngine] Lobby em /api/rooms | WebSocket em /api/net");
});

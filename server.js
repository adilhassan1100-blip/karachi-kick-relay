// Karachi Kick Race relay: room codes + forwarding game packets between players.
// No dependencies (plain Node.js). Deploy anywhere that runs Node (Render, Railway, a VPS...).
//   node server.js            -> listens on $PORT or 8080
// Protocol
//   text  {"t":"host"}                    -> {"t":"hosted","code":"KHI7","id":1}
//   text  {"t":"join","code":"KHI7"}      -> {"t":"joined","id":N}   (host gets {"t":"peer","id":N})
//   binary [int32 LE target][u8 mode][payload]  -> forwarded as [int32 LE from][u8 mode][payload]
//          target 0 = everyone else in the room, negative = everyone except -target
"use strict";
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 8;
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const rooms = new Map(); // code -> { peers: Map(id -> client), next: 2 }

function newCode() {
  for (;;) {
    let c = "";
    for (let i = 0; i < 4; i++) c += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
}

// ---------------------------------------------------------------- minimal WebSocket framing
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) {
    head = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([head, payload]);
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = [];
    this.fragOp = 0;
    this.room = null;
    this.id = 0;
    this.alive = true;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }
  sendText(obj) { this.write(frame(0x1, Buffer.from(JSON.stringify(obj)))); }
  sendBinary(buf) { this.write(frame(0x2, buf)); }
  write(b) { if (this.alive) { try { this.socket.write(b); } catch (e) { this.onClose(); } } }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 8 * 1024 * 1024) { this.socket.destroy(); return; }
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = this.buf.subarray(off, off + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      this.buf = this.buf.subarray(need);
      if (op === 0x8) { this.write(frame(0x8, Buffer.alloc(0))); this.socket.end(); return; }
      if (op === 0x9) { this.write(frame(0xA, payload)); continue; }
      if (op === 0xA) continue;
      if (op === 0x0) { this.frag.push(payload); if (!fin) continue; payload = Buffer.concat(this.frag); this.frag = []; this.onMessage(this.fragOp, payload); continue; }
      if (!fin) { this.fragOp = op; this.frag = [payload]; continue; }
      this.onMessage(op, payload);
    }
  }
  onMessage(op, payload) {
    if (op === 0x1) {
      let m; try { m = JSON.parse(payload.toString()); } catch (e) { return; }
      if (m.t === "host" && !this.room) {
        const code = newCode();
        const room = { code, peers: new Map(), next: 2 };
        rooms.set(code, room);
        this.room = room; this.id = 1; room.peers.set(1, this);
        this.sendText({ t: "hosted", code, id: 1 });
      } else if (m.t === "join" && !this.room) {
        const room = rooms.get(String(m.code || "").toUpperCase());
        if (!room || !room.peers.has(1)) return this.sendText({ t: "error", msg: "Room nahi mila - code check karo" });
        if (room.peers.size >= MAX_PLAYERS) return this.sendText({ t: "error", msg: "Room full hai" });
        this.room = room; this.id = room.next++; room.peers.set(this.id, this);
        this.sendText({ t: "joined", id: this.id });
        room.peers.get(1).sendText({ t: "peer", id: this.id });
      }
    } else if (op === 0x2 && this.room && payload.length >= 5) {
      const target = payload.readInt32LE(0);
      const out = Buffer.from(payload);
      out.writeInt32LE(this.id, 0);
      for (const [pid, c] of this.room.peers) {
        if (pid === this.id) continue;
        if (target > 0 && pid !== target) continue;
        if (target < 0 && pid === -target) continue;
        c.sendBinary(out);
      }
    }
  }
  onClose() {
    if (!this.alive) return;
    this.alive = false;
    const room = this.room;
    if (!room) return;
    room.peers.delete(this.id);
    if (this.id === 1) {
      for (const c of room.peers.values()) { c.sendText({ t: "error", msg: "Host chala gaya" }); c.socket.end(); }
      rooms.delete(room.code);
    } else {
      const host = room.peers.get(1);
      if (host) host.sendText({ t: "left", id: this.id });
    }
  }
}

// The browser version of the game is served from ./web (index.html, index.wasm, index.pck ...).
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const WEB = path.join(__dirname, "web");
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".wasm": "application/wasm", ".pck": "application/octet-stream", ".png": "image/png" };
const gzCache = new Map();

const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || "/").split("?")[0]);
  if (p === "/status") {
    res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
    return res.end("Karachi Kick Race relay OK. Rooms: " + rooms.size + "\n");
  }
  if (p === "/") p = "/index.html";
  const f = path.join(WEB, p);
  if (!f.startsWith(WEB) || !fs.existsSync(f) || !fs.statSync(f).isFile()) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    return res.end("Not found. Game: / , server status: /status\n");
  }
  const headers = { "Content-Type": TYPES[path.extname(f)] || "application/octet-stream", "Cache-Control": "public, max-age=600" };
  const ext = path.extname(f);
  if ((ext === ".wasm" || ext === ".pck" || ext === ".js") && /gzip/.test(req.headers["accept-encoding"] || "")) {
    if (!gzCache.has(f)) gzCache.set(f, zlib.gzipSync(fs.readFileSync(f), { level: 6 }));
    headers["Content-Encoding"] = "gzip";
    res.writeHead(200, headers);
    return res.end(gzCache.get(f));
  }
  res.writeHead(200, headers);
  fs.createReadStream(f).pipe(res);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  new Client(socket);
});

server.listen(PORT, () => console.log("relay listening on " + PORT));

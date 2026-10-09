const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const port = process.env.PORT || 8080;
const ADMIN_KEY = process.env.ADMIN_KEY || "";
const ROOM_CODE = process.env.ROOM_CODE || "";
const RUTA = process.env.WS_RUTA || "/v3";   // el cliente debe conectar a esta ruta
const VERSION_MINIMA = 3;
const MAX_CLIENTES = 10;        // jugadores ya validados
const MAX_CONEXIONES = 30;      // conexiones totales
const MAX_POR_IP = 3;           // conexiones simultáneas por IP
const MAX_INTENTOS = 5;         // códigos incorrectos antes del bloqueo
const BLOQUEO_MS = 60000;

let siguienteId = 1;
const fallosPorIp = new Map();      // ip -> { n, hasta }
const conexionesPorIp = new Map();  // ip -> cantidad

function hash(t) { return crypto.createHash("sha256").update(String(t)).digest(); }
function iguales(a, b) { return crypto.timingSafeEqual(hash(a), hash(b)); }

function ipDe(req) {
  const f = req.headers["x-forwarded-for"];
  return f ? f.split(",")[0].trim() : req.socket.remoteAddress;
}

// Servidor HTTP normal (Render lo usa para comprobar que está vivo)
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 256 });

function rechazar(socket, codigo, texto) {
  socket.write("HTTP/1.1 " + codigo + " " + texto + "\r\nConnection: close\r\n\r\n");
  socket.destroy();
}

// Filtro previo: aquí se rechaza a los clientes viejos SIN crear conexión WebSocket
server.on("upgrade", (req, socket, head) => {
  socket.on("error", () => {});
  let ruta = "";
  try { ruta = new URL(req.url, "http://x").pathname; } catch {}
  if (ruta !== RUTA) return rechazar(socket, 403, "Forbidden");

  const ip = ipDe(req);
  const bloqueo = fallosPorIp.get(ip);
  if (bloqueo && bloqueo.hasta > Date.now()) return rechazar(socket, 429, "Too Many Requests");
  if ((conexionesPorIp.get(ip) || 0) >= MAX_POR_IP) return rechazar(socket, 429, "Too Many Requests");
  if (wss.clients.size >= MAX_CONEXIONES) return rechazar(socket, 503, "Service Unavailable");

  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.ip = ip;
    conexionesPorIp.set(ip, (conexionesPorIp.get(ip) || 0) + 1);
    wss.emit("connection", ws, req);
  });
});

function contarValidados() {
  let n = 0;
  for (const c of wss.clients) if (c.validado) n++;
  return n;
}

function enviarATodos(texto, excepto) {
  for (const c of wss.clients) {
    if (c !== excepto && c.validado && c.readyState === 1) c.send(texto);
  }
}

// Limpia registros vencidos cada 10 minutos
setInterval(() => {
  const ahora = Date.now();
  for (const [ip, r] of fallosPorIp) if (r.hasta < ahora && r.n === 0) fallosPorIp.delete(ip);
}, 600000);

wss.on("connection", (ws) => {
  ws.id = null;
  ws.versionOk = false;
  ws.validado = false;
  ws.ultimoMensaje = 0;
  ws.intentosFallidos = 0;   // intentos de la clave admin

  ws.on("error", (e) => console.log("Error de cliente:", e.message));

  // Si no manda un "hello" válido en 3 segundos, se le echa
  ws.timerHello = setTimeout(() => {
    if (!ws.versionOk) ws.close(4001, "Version vieja");
  }, 3000);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    // 1) Identificación de versión
    if (msg.type === "hello") {
      if (typeof msg.version !== "number" || msg.version < VERSION_MINIMA) {
        ws.close(4001, "Version vieja");
        return;
      }
      if (ws.versionOk) return;
      ws.versionOk = true;
      clearTimeout(ws.timerHello);

      if (!ROOM_CODE) {
        ws.send(JSON.stringify({ type: "error", msg: "Falta ROOM_CODE en Render" }));
        ws.close(4005, "Falta ROOM_CODE");
        return;
      }
      ws.timerAuth = setTimeout(() => {
        if (!ws.validado) ws.close(4003, "Tiempo agotado");
      }, 60000);
      ws.send(JSON.stringify({ type: "need_code" }));
      return;
    }

    // 2) Código de entrada
    if (msg.type === "auth") {
      if (!ws.versionOk || ws.validado) return;

      if (typeof msg.codigo !== "string" || !iguales(msg.codigo, ROOM_CODE)) {
        const reg = fallosPorIp.get(ws.ip) || { n: 0, hasta: 0 };
        reg.n++;
        const restantes = MAX_INTENTOS - reg.n;
        if (restantes <= 0) {
          reg.hasta = Date.now() + BLOQUEO_MS;
          reg.n = 0;
          fallosPorIp.set(ws.ip, reg);
          console.log("IP bloqueada por intentos:", ws.ip);
          ws.send(JSON.stringify({ type: "error", msg: "Demasiados intentos. Espera 1 minuto." }));
          ws.close(1008, "Demasiados intentos");
          return;
        }
        fallosPorIp.set(ws.ip, reg);
        ws.send(JSON.stringify({ type: "error", msg: "Código incorrecto. Intentos restantes: " + restantes }));
        return;
      }

      if (contarValidados() >= MAX_CLIENTES) {
        ws.send(JSON.stringify({ type: "error", msg: "Servidor lleno" }));
        ws.close(1013, "Servidor lleno");
        return;
      }

      ws.validado = true;
      clearTimeout(ws.timerAuth);
      fallosPorIp.delete(ws.ip);
      ws.id = siguienteId++;
      console.log("Cliente validado:", ws.id);
      ws.send(JSON.stringify({ type: "auth_ok", id: ws.id }));
      return;
    }

    // Todo lo demás exige haber puesto el código
    if (!ws.validado) return;

    // Reiniciar el servidor: requiere la clave de administrador
    if (msg.type === "reset") {
      console.log("Petición de reinicio del cliente", ws.id);
      if (!ADMIN_KEY) {
        ws.send(JSON.stringify({ type: "error", msg: "Reinicio desactivado: falta ADMIN_KEY en Render" }));
        return;
      }
      if (msg.clave !== ADMIN_KEY) {
        ws.intentosFallidos++;
        const restantes = 5 - ws.intentosFallidos;
        const texto = restantes > 0
          ? "Clave incorrecta. Intentos restantes: " + restantes
          : "Clave incorrecta. Te desconectamos.";
        ws.send(JSON.stringify({ type: "error", msg: texto }));
        if (restantes <= 0) ws.close(1008, "Demasiados intentos");
        return;
      }
      console.log("Reinicio aceptado");
      siguienteId = 1;
      for (const c of wss.clients) c.close(4000, "Servidor reiniciado");
      return;
    }

    // Posiciones
    if (msg.type !== "pos") return;
    const ahora = Date.now();
    if (ahora - ws.ultimoMensaje < 30) return;
    ws.ultimoMensaje = ahora;
    if (typeof msg.x !== "number" || typeof msg.y !== "number") return;
    if (!Number.isFinite(msg.x) || !Number.isFinite(msg.y)) return;

    enviarATodos(JSON.stringify({ type: "pos", id: ws.id, x: msg.x, y: msg.y }), ws);
  });

  ws.on("close", () => {
    clearTimeout(ws.timerHello);
    clearTimeout(ws.timerAuth);
    const n = (conexionesPorIp.get(ws.ip) || 1) - 1;
    if (n <= 0) conexionesPorIp.delete(ws.ip); else conexionesPorIp.set(ws.ip, n);
    if (ws.validado) enviarATodos(JSON.stringify({ type: "leave", id: ws.id }), ws);
  });
});

server.listen(port, () => {
  console.log("Servidor en puerto", port, "ruta", RUTA);
  console.log("Reinicio admin:", ADMIN_KEY ? "ACTIVADO" : "DESACTIVADO (falta ADMIN_KEY)");
  console.log("Código de entrada:", ROOM_CODE ? "ACTIVADO" : "FALTA ROOM_CODE (nadie podrá entrar)");
  console.log("VERSION 5 - filtro por ruta, versión mínima:", VERSION_MINIMA);
});

const { WebSocketServer } = require("ws");

const port = process.env.PORT || 8080;
const ADMIN_KEY = process.env.ADMIN_KEY || "";   // se define en Render
const MAX_CLIENTES = 10;
const wss = new WebSocketServer({ port, maxPayload: 256 });
let siguienteId = 1;

wss.on("connection", (ws) => {
  if (wss.clients.size > MAX_CLIENTES) {
    ws.close(1013, "Servidor lleno");
    return;
  }

  ws.id = siguienteId++;
  ws.ultimoMensaje = 0;
  ws.intentosFallidos = 0;
  console.log("Cliente conectado:", ws.id);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    // Reiniciar el servidor: requiere la clave de administrador
    if (msg.type === "reset") {
      if (!ADMIN_KEY || msg.clave !== ADMIN_KEY) {
        ws.intentosFallidos++;
        ws.send(JSON.stringify({ type: "error", msg: "Clave incorrecta" }));
        if (ws.intentosFallidos >= 5) ws.close(1008, "Demasiados intentos");
        return;
      }
      console.log("Reinicio pedido por el cliente", ws.id);
      siguienteId = 1;
      for (const c of wss.clients) c.close(4000, "Servidor reiniciado");
      return;
    }

    // Posiciones
    if (msg.type !== "pos") return;
    const ahora = Date.now();
    if (ahora - ws.ultimoMensaje < 30) return;   // límite de frecuencia
    ws.ultimoMensaje = ahora;
    if (typeof msg.x !== "number" || typeof msg.y !== "number") return;
    if (!Number.isFinite(msg.x) || !Number.isFinite(msg.y)) return;

    const salida = JSON.stringify({ type: "pos", id: ws.id, x: msg.x, y: msg.y });
    for (const c of wss.clients) {
      if (c !== ws && c.readyState === 1) c.send(salida);
    }
  });

  ws.on("close", () => {
    const salida = JSON.stringify({ type: "leave", id: ws.id });
    for (const c of wss.clients) {
      if (c.readyState === 1) c.send(salida);
    }
  });
});

console.log("Servidor en puerto", port);

const { WebSocketServer } = require("ws");

const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });
let siguienteId = 1;

wss.on("connection", (ws) => {
  ws.id = siguienteId++;
  console.log("Cliente conectado:", ws.id);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (msg.type !== "pos") return;

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

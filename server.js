    // Reiniciar el servidor: requiere la clave de administrador
    if (msg.type === "reset") {
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
      console.log("Reinicio pedido por el cliente", ws.id);
      siguienteId = 1;
      for (const c of wss.clients) c.close(4000, "Servidor reiniciado");
      return;
    }

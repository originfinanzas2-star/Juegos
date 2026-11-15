// Servidor de Misión Cumplida (Cloudflare Worker + Durable Object).
// Cada sala (código de 4 números) es un Durable Object que reenvía mensajes
// entre el anfitrión y los demás jugadores. La lógica del juego sigue en el móvil del anfitrión.
import { DurableObject } from "cloudflare:workers";

const ALIVE_MS = 45000;

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // Responde a los "ping" sin despertar el objeto (gratis y rápido).
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  live(tag, except) {
    return this.ctx.getWebSockets(tag).filter((w) => w !== except && w.readyState === 1);
  }

  alive(w) {
    const t = this.ctx.getWebSocketAutoResponseTimestamp(w);
    const last = t ? t.getTime() : (w.deserializeAttachment() || {}).t || 0;
    return Date.now() - last < ALIVE_MS;
  }

  liveHost(except) {
    return this.live("host", except).filter((w) => this.alive(w));
  }

  send(w, o) {
    try { w.send(JSON.stringify(o)); } catch (e) {}
  }

  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 426 });
    }
    const u = new URL(req.url);
    const role = u.searchParams.get("role");
    const cid = (u.searchParams.get("cid") || "x").slice(0, 40);
    const [cl, sv] = Object.values(new WebSocketPair());

    if (role === "h") {
      const other = this.liveHost().find((w) => (w.deserializeAttachment() || {}).cid !== cid);
      if (other) {
        // Ya hay otro anfitrión vivo con este código.
        this.ctx.acceptWebSocket(sv, ["x"]);
        this.send(sv, { ctl: "taken" });
        sv.close(4409, "taken");
      } else {
        // Un anfitrión que vuelve (misma id) sustituye al anterior.
        for (const w of this.live("host")) { try { w.close(4001, "replaced"); } catch (e) {} }
        this.ctx.acceptWebSocket(sv, ["host"]);
        sv.serializeAttachment({ role: "h", cid, t: Date.now() });
        for (const w of this.live("c")) this.send(w, { ctl: "hostup" });
      }
    } else {
      for (const w of this.live("c:" + cid)) { try { w.close(4001, "replaced"); } catch (e) {} }
      this.ctx.acceptWebSocket(sv, ["c", "c:" + cid]);
      sv.serializeAttachment({ role: "c", cid, t: Date.now() });
      if (!this.liveHost().length) {
        this.send(sv, { ctl: "nohost" });
        sv.close(4404, "nohost");
      }
    }
    return new Response(null, { status: 101, webSocket: cl });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > 200000) return;
    let d;
    try { d = JSON.parse(msg); } catch (e) { return; }
    const a = ws.deserializeAttachment() || {};
    if (a.role === "h") {
      if (!d || !d.to) return;
      for (const w of this.live("c:" + d.to)) this.send(w, d.m);
    } else if (a.role === "c") {
      const h = this.liveHost()[0];
      if (!h) { this.send(ws, { ctl: "nohost" }); return; }
      this.send(h, { from: a.cid, m: d });
    }
  }

  async webSocketClose(ws, code, reason) {
    try { ws.close(code, reason); } catch (e) {}
    this.gone(ws);
  }

  async webSocketError(ws) {
    this.gone(ws);
  }

  gone(ws) {
    const a = ws.deserializeAttachment() || {};
    if (a.role === "h") {
      if (!this.live("host", ws).length) {
        for (const w of this.live("c")) this.send(w, { ctl: "hostdown" });
      }
    } else if (a.role === "c") {
      // Solo avisa si no queda otra conexión viva de ese mismo jugador.
      if (!this.live("c:" + a.cid, ws).length) {
        const h = this.liveHost()[0];
        if (h) this.send(h, { ctl: "close", cid: a.cid });
      }
    }
  }
}

export default {
  async fetch(req, env) {
    const m = new URL(req.url).pathname.match(/^\/ws\/(\d{4})$/);
    if (!m) return new Response("Servidor de Misión Cumplida", { status: 200 });
    return env.ROOM.get(env.ROOM.idFromName(m[1])).fetch(req);
  },
};

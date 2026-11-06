// Party Server v2 — un solo archivo. Uso:  npm install && node server.js
// Variables de entorno (todas opcionales):
//   PORT=8080                       puerto (Render/Railway/Fly lo ponen solos)
//   PARTY_ADMINS=Nombre1,Nombre2    admins (por defecto MichuluToaster,nikkyfemboyrico)
//   PARTY_AUTH=mojang|off           verificar identidad con Mojang (por defecto mojang)
//   PARTY_DATA=./data               carpeta donde se guardan bans y jugadores conocidos
//   PARTY_MAX_SIZE=20               maximo de miembros por party
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "8080", 10);
const ADMIN_NAMES = (process.env.PARTY_ADMINS || "MichuluToaster,nikkyfemboyrico")
  .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
const AUTH_MODE = (process.env.PARTY_AUTH || "mojang").toLowerCase();
const DATA_DIR = process.env.PARTY_DATA || path.join(__dirname, "data");
const HARD_MAX = parseInt(process.env.PARTY_MAX_SIZE || "20", 10);
const INVITE_TTL = 60_000, REQUEST_TTL = 60_000, STALE_MS = 30_000;

fs.mkdirSync(DATA_DIR, { recursive: true });
const BANS_FILE = path.join(DATA_DIR, "bans.json");
const KNOWN_FILE = path.join(DATA_DIR, "players.json");
const load = (f, d) => { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return d; } };
const save = (f, v) => { try { fs.writeFileSync(f, JSON.stringify(v, null, 2)); } catch (e) { console.error("save", f, e.message); } };

let bans = load(BANS_FILE, {});     // uuid -> {name, reason, by, at}
let known = load(KNOWN_FILE, {});   // uuid -> name
const normUuid = u => String(u || "").replace(/-/g, "").toLowerCase();
const isAdminName = n => ADMIN_NAMES.includes(String(n || "").toLowerCase());

// ---- estado en memoria ----
const clients = new Map();   // uuid -> client
const parties = new Map();   // id -> party
const invites = new Map();   // id -> invite
const requests = new Map();  // id -> join request
const newId = () => crypto.randomBytes(6).toString("hex");

const send = (c, obj) => { if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); };
const notice = (c, text, level = "info") => send(c, { op: "notice", text, level });
const err = (c, code, text) => send(c, { op: "error", code, text: text || code });
const scope = c => c.server;  // las parties solo existen dentro del mismo servidor de Minecraft

function partyOf(c) { return c.partyId ? parties.get(c.partyId) : null; }
function isAdmin(c) { return c.verified && isAdminName(c.name); }

function publicPlayer(c) { return { uuid: c.uuid, name: c.name, dnd: c.dnd, inParty: !!c.partyId, admin: isAdminName(c.name) && c.verified }; }
function partyView(p) {
  return {
    id: p.id, leader: p.leader,
    leaderName: (clients.get(p.leader) || {}).name || known[p.leader] || "?",
    members: [...p.members].map(u => ({ uuid: u, name: (clients.get(u) || {}).name || known[u] || "?" })),
    limit: p.limit, open: p.open,
  };
}
function pushState(c) {
  const p = partyOf(c);
  send(c, {
    op: "state",
    party: p ? partyView(p) : null,
    invites: [...invites.values()].filter(i => i.to === c.uuid).map(i => ({ id: i.id, from: i.fromName, expiresAt: i.expiresAt })),
    requests: p && p.leader === c.uuid
      ? [...requests.values()].filter(r => r.partyId === p.id).map(r => ({ id: r.id, from: r.fromName, expiresAt: r.expiresAt }))
      : [],
  });
}
const pushParty = p => p.members.forEach(u => pushState(clients.get(u)));

function removeFromParty(c, why) {
  const p = partyOf(c); if (!p) return;
  p.members.delete(c.uuid); c.partyId = null;
  if (p.members.size === 0) {
    parties.delete(p.id);
    [...requests.values()].filter(r => r.partyId === p.id).forEach(r => requests.delete(r.id));
  } else {
    if (p.leader === c.uuid) p.leader = [...p.members][0];
    pushParty(p);
    p.members.forEach(u => notice(clients.get(u), `${c.name} ${why || "salió de la party"}`));
  }
  pushState(c);
}
function addToParty(p, c) {
  p.members.add(c.uuid); c.partyId = p.id;
  [...invites.values()].filter(i => i.to === c.uuid).forEach(i => invites.delete(i.id));
  [...requests.values()].filter(r => r.from === c.uuid).forEach(r => requests.delete(r.id));
  pushParty(p);
  p.members.forEach(u => u !== c.uuid && notice(clients.get(u), `${c.name} se unió a la party`));
}

// ---- verificación Mojang (opcional) ----
async function verifyMojang(c, hello) {
  if (AUTH_MODE === "off") return true; // modo pruebas: se confía en el nombre
  if (!hello.authOk) return false;
  try {
    const url = `https://sessionserver.mojang.com/session/minecraft/hasJoined?username=${encodeURIComponent(c.name)}&serverId=${c.challenge}`;
    const r = await fetch(url, { signal: AbortSignal.timeout(5000) });
    if (r.status !== 200) return false;
    const j = await r.json();
    return normUuid(j.id) === normUuid(c.uuid);
  } catch { return false; }
}

// ---- servidor HTTP + WS ----
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") { res.writeHead(200); return res.end("ok"); }
  res.writeHead(200, { "content-type": "text/plain" });
  res.end(`Party server v2 — ${clients.size} jugadores, ${parties.size} parties\n`);
});
const wss = new WebSocketServer({ server: httpServer, path: "/ws", maxPayload: 64 * 1024 });

wss.on("connection", ws => {
  const c = { ws, uuid: null, name: null, server: "default", challenge: crypto.randomBytes(12).toString("hex"),
    verified: false, dnd: false, partyId: null, stats: null, helloDone: false, last: Date.now(), msgs: 0 };
  send(c, { op: "challenge", serverId: c.challenge, auth: AUTH_MODE });
  const rl = setInterval(() => { c.msgs = 0; }, 1000);

  ws.on("message", async raw => {
    if (++c.msgs > 60) return;
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    c.last = Date.now();
    try {
      if (m.op === "hello") return await onHello(c, m);
      if (!c.helloDone) return;
      onMessage(c, m);
    } catch (e) { console.error("handler", m.op, e); }
  });
  ws.on("close", () => { clearInterval(rl); onClose(c); });
  ws.on("error", () => {});
});

async function onHello(c, m) {
  if (c.helloDone) return;
  const name = String(m.player || "").slice(0, 16);
  const uuid = normUuid(m.uuid);
  if (!/^[A-Za-z0-9_]{1,16}$/.test(name) || uuid.length !== 32) { return c.ws.close(4000, "bad hello"); }
  c.name = name; c.uuid = uuid;
  c.server = String(m.mcServer || "default").slice(0, 100).toLowerCase();
  c.dnd = !!m.dnd;
  c.verified = await verifyMojang(c, m);
  if (bans[uuid]) {
    send(c, { op: "banned", reason: bans[uuid].reason || "" });
    return c.ws.close(4003, "banned");
  }
  const old = clients.get(uuid);
  if (old) { send(old, { op: "error", code: "replaced" }); old.ws.close(4001, "replaced"); onClose(old, true); }
  c.helloDone = true;
  clients.set(uuid, c);
  known[uuid] = name; save(KNOWN_FILE, known);
  send(c, { op: "welcome", uuid, name, admin: isAdmin(c), verified: c.verified, maxSize: HARD_MAX });
  pushState(c);
}

function onClose(c, replaced) {
  if (!c.helloDone) return;
  if (clients.get(c.uuid) === c) { removeFromParty(c, "se desconectó"); clients.delete(c.uuid); }
  [...invites.values()].filter(i => i.from === c.uuid || i.to === c.uuid).forEach(i => invites.delete(i.id));
  [...requests.values()].filter(r => r.from === c.uuid).forEach(r => requests.delete(r.id));
}

function findByName(name) {
  const n = String(name || "").toLowerCase();
  for (const c of clients.values()) if (c.name.toLowerCase() === n) return c;
  return null;
}
function uuidByName(name) {
  const n = String(name || "").toLowerCase();
  for (const [u, nm] of Object.entries(known)) if (nm.toLowerCase() === n) return u;
  return null;
}

function onMessage(c, m) {
  switch (m.op) {
    case "stats": {
      const n = v => (typeof v === "number" && isFinite(v) ? v : 0);
      c.stats = { x: n(m.x), y: n(m.y), z: n(m.z), dim: String(m.dim || "").slice(0, 80), h: n(m.h), m: Math.max(1, n(m.m)), t: Date.now() };
      return;
    }
    case "set_dnd": c.dnd = !!m.dnd; return;
    case "list_players": {
      const q = String(m.query || "").toLowerCase();
      const list = [...clients.values()].filter(o => o !== c && o.server === scope(c) && (!q || o.name.toLowerCase().includes(q)))
        .slice(0, 100).map(publicPlayer);
      return send(c, { op: "players", list });
    }
    case "list_parties": {
      const list = [...parties.values()].filter(p => (clients.get(p.leader) || {}).server === scope(c)).slice(0, 100).map(partyView);
      return send(c, { op: "parties", list });
    }
    case "invite": {
      const to = clients.get(normUuid(m.toUuid));
      let p = partyOf(c);
      if (!to || to.server !== scope(c)) return err(c, "player_missing", "Jugador no encontrado");
      if (to.partyId) return err(c, "target_in_party", "Ese jugador ya está en una party");
      if (to.dnd) return err(c, "dnd", "Ese jugador no acepta invitaciones");
      if (p && p.leader !== c.uuid) return err(c, "not_leader", "Solo el líder puede invitar");
      if (p && p.members.size >= p.limit) return err(c, "party_full", "Party llena");
      if ([...invites.values()].some(i => i.from === c.uuid && i.to === to.uuid)) return err(c, "invite_exists", "Ya invitaste a ese jugador");
      const inv = { id: newId(), from: c.uuid, fromName: c.name, to: to.uuid, expiresAt: Date.now() + INVITE_TTL };
      invites.set(inv.id, inv);
      pushState(to); notice(c, `Invitación enviada a ${to.name}`);
      return;
    }
    case "invite_response": {
      const inv = invites.get(String(m.id));
      if (!inv || inv.to !== c.uuid) return;
      invites.delete(inv.id);
      const from = clients.get(inv.from);
      if (!m.accepted) { pushState(c); if (from) notice(from, `${c.name} rechazó tu invitación`); return; }
      if (!from) { pushState(c); return err(c, "party_missing", "El que invitó ya no está"); }
      if (c.partyId) { pushState(c); return err(c, "already_in_party", "Ya estás en una party"); }
      let p = partyOf(from);
      if (!p) { p = { id: newId(), leader: from.uuid, members: new Set(), limit: HARD_MAX, open: true }; parties.set(p.id, p); addToParty(p, from); }
      if (p.members.size >= p.limit) { pushState(c); return err(c, "party_full", "Party llena"); }
      addToParty(p, c);
      return;
    }
    case "request_join": {
      const p = parties.get(String(m.partyId));
      if (!p) return err(c, "party_missing", "Party no encontrada");
      if (c.partyId) return err(c, "already_in_party", "Ya estás en una party");
      if (!p.open) return err(c, "requests_closed", "La party está cerrada");
      if (p.members.size >= p.limit) return err(c, "party_full", "Party llena");
      if ([...requests.values()].some(r => r.from === c.uuid && r.partyId === p.id)) return err(c, "request_exists", "Ya tienes una solicitud pendiente");
      const r = { id: newId(), from: c.uuid, fromName: c.name, partyId: p.id, expiresAt: Date.now() + REQUEST_TTL };
      requests.set(r.id, r);
      const leader = clients.get(p.leader); if (leader) { pushState(leader); notice(leader, `${c.name} quiere unirse a tu party`); }
      return notice(c, "Solicitud enviada");
    }
    case "join_response": {
      const r = requests.get(String(m.id)); const p = r && parties.get(r.partyId);
      if (!r || !p || p.leader !== c.uuid) return;
      requests.delete(r.id);
      const who = clients.get(r.from);
      if (!m.accepted) { pushState(c); if (who) notice(who, "Tu solicitud fue rechazada"); return; }
      if (!who || who.partyId) return pushState(c);
      if (p.members.size >= p.limit) { pushState(c); return err(c, "party_full", "Party llena"); }
      addToParty(p, who);
      return;
    }
    case "kick": {
      const p = partyOf(c); const t = clients.get(normUuid(m.uuid));
      if (!p || p.leader !== c.uuid || !t || t.partyId !== p.id || t === c) return;
      notice(t, "Fuiste expulsado de la party", "warn");
      removeFromParty(t, "fue expulsado");
      return;
    }
    case "leave": return removeFromParty(c);
    case "set_limit": {
      const p = partyOf(c); if (!p || p.leader !== c.uuid) return err(c, "not_leader", "Solo el líder");
      p.limit = Math.max(Math.max(1, p.members.size), Math.min(HARD_MAX, parseInt(m.limit, 10) || HARD_MAX));
      return pushParty(p);
    }
    case "set_open": {
      const p = partyOf(c); if (!p || p.leader !== c.uuid) return err(c, "not_leader", "Solo el líder");
      p.open = !!m.open; return pushParty(p);
    }
    case "admin_ban": case "admin_unban": case "admin_list": return onAdmin(c, m);
  }
}

function onAdmin(c, m) {
  if (!isAdmin(c)) {
    return err(c, "not_admin", c.verified || AUTH_MODE === "off"
      ? "No eres admin del mod"
      : "No pude verificar tu cuenta con Mojang, no puedes usar comandos de admin");
  }
  if (m.op === "admin_list") {
    const entries = Object.values(bans).map(b => b.name + (b.reason ? ` (${b.reason})` : ""));
    return notice(c, entries.length ? "Baneados: " + entries.join(", ") : "No hay usuarios baneados");
  }
  const name = String(m.name || "");
  const uuid = (findByName(name) || {}).uuid || uuidByName(name);
  if (m.op === "admin_ban") {
    if (!uuid) return err(c, "player_missing", `No conozco a "${name}" (nunca se conectó a este servidor de party)`);
    if (isAdminName(known[uuid])) return err(c, "protected", "No puedes banear a un admin");
    bans[uuid] = { name: known[uuid] || name, reason: String(m.reason || "").slice(0, 120), by: c.name, at: Date.now() };
    save(BANS_FILE, bans);
    const t = clients.get(uuid);
    if (t) { removeFromParty(t, "fue baneado"); send(t, { op: "banned", reason: bans[uuid].reason }); t.ws.close(4003, "banned"); }
    return notice(c, `${bans[uuid].name} fue baneado del mod`);
  }
  if (m.op === "admin_unban") {
    if (!uuid || !bans[uuid]) return err(c, "not_banned", `"${name}" no está baneado`);
    const nm = bans[uuid].name; delete bans[uuid]; save(BANS_FILE, bans);
    return notice(c, `${nm} fue desbaneado`);
  }
}

// limpieza de invitaciones/solicitudes vencidas + difusión de stats
setInterval(() => {
  const now = Date.now();
  let changed = new Set();
  for (const i of [...invites.values()]) if (i.expiresAt < now) { invites.delete(i.id); changed.add(i.to); }
  for (const r of [...requests.values()]) if (r.expiresAt < now) {
    requests.delete(r.id); const p = parties.get(r.partyId); if (p) changed.add(p.leader);
  }
  changed.forEach(u => pushState(clients.get(u)));
  for (const c of clients.values()) if (now - c.last > 90_000) c.ws.terminate();
}, 5000);

setInterval(() => {
  for (const p of parties.values()) {
    const players = {};
    for (const u of p.members) {
      const mc = clients.get(u); if (!mc || !mc.stats) continue;
      players[u] = { ...mc.stats, stale: Date.now() - mc.stats.t > STALE_MS };
    }
    const msg = { op: "stats_batch", players };
    for (const u of p.members) send(clients.get(u), msg);
  }
}, 250);

if (require.main === module) {
  httpServer.listen(PORT, () => {
    console.log(`Party server escuchando en :${PORT}  (ws://HOST:${PORT}/ws)`);
    console.log(`Admins: ${ADMIN_NAMES.join(", ")} | verificación Mojang: ${AUTH_MODE}`);
  });
}
module.exports = { httpServer, wss };

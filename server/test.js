// Prueba de humo: PARTY_AUTH=off PORT=18080 node test.js
process.env.PARTY_AUTH = "off"; process.env.PORT = "18080";
process.env.PARTY_DATA = require("os").tmpdir() + "/party-test-" + Date.now();
const WebSocket = require("ws"); const assert = require("assert");
const { httpServer } = require("./server.js");
const U = n => n.padStart(32, "0");
function client(name, uuid) {
  return new Promise(res => {
    const ws = new WebSocket("ws://127.0.0.1:18080/ws"); const c = { ws, got: [], name };
    ws.on("message", d => { const m = JSON.parse(d); c.got.push(m);
      if (m.op === "challenge") ws.send(JSON.stringify({ op: "hello", player: name, uuid, mcServer: "s1", authOk: false }));
      if (m.op === "welcome") res(c); if (m.op === "banned") res(c); });
    c.send = o => ws.send(JSON.stringify(o));
    c.last = op => [...c.got].reverse().find(m => m.op === op);
  });
}
const wait = ms => new Promise(r => setTimeout(r, ms));
httpServer.listen(18080, async () => {
  const a = await client("MichuluToaster", U("a")), b = await client("Bob", U("b")), n = await client("nikkyfemboyrico", U("c"));
  assert(a.last("welcome").admin && !b.last("welcome").admin);
  a.send({ op: "invite", toUuid: U("b") }); await wait(100);
  const inv = b.last("state").invites[0]; assert(inv);
  b.send({ op: "invite_response", id: inv.id, accepted: true }); await wait(100);
  assert.equal(a.last("state").party.members.length, 2);
  b.send({ op: "stats", x: 1, y: 64, z: 3, dim: "minecraft:overworld", h: 14, m: 40 }); await wait(400);
  assert.equal(a.last("stats_batch").players[U("b")].h, 14);
  b.send({ op: "admin_ban", name: "MichuluToaster" }); await wait(100);
  assert.equal(b.last("error").code, "not_admin");
  a.send({ op: "admin_ban", name: "Bob", reason: "test" }); await wait(200);
  assert(b.last("banned")); assert.equal(a.last("state").party.members.length, 1);
  const b2 = await client("Bob", U("b")); assert(b2.last("banned"));
  n.send({ op: "admin_unban", name: "Bob" }); await wait(100);
  const b3 = await client("Bob", U("b")); assert(b3.last("welcome"));
  a.send({ op: "admin_ban", name: "nikkyfemboyrico" }); await wait(100);
  assert.equal(a.last("error").code, "protected");
  console.log("TODAS LAS PRUEBAS OK"); process.exit(0);
});

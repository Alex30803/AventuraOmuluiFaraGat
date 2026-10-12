// Server pentru "Aventura omului fără gât": servește pagina și sincronizează camerele prin WebSocket.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  fs.readFile(path.join(__dirname, 'index.html'), (err, buf) => {
    if (err) { res.writeHead(500); res.end('index.html lipsește'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 1 << 19 });
const store = new Map(); // cale -> {data, t}
let nid = 1;

const valid = p => typeof p === 'string' && /^games\/[A-Z0-9]{4}(\/seats\/[0-9])?$/.test(p);
const colOf = p => p.slice(0, p.lastIndexOf('/'));
const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const all = (m, except) => wss.clients.forEach(c => { if (c !== except) send(c, m); });
const peers = () => { const a = []; wss.clients.forEach(c => { if (c.pres) a.push({ peer: c.id, presence: c.pres }); }); return a; };
const colDocs = p => { const r = []; for (const [k, v] of store) if (colOf(k) === p) r.push({ id: k.slice(p.length + 1), data: v.data }); return r; };

function notify(p, data) {
  const id = p.slice(p.lastIndexOf('/') + 1), c = colOf(p);
  wss.clients.forEach(ws => {
    if (ws.subs.has('d:' + p)) send(ws, { t: 'doc', path: p, data });
    if (ws.subs.has('c:' + c)) send(ws, { t: 'chg', col: c, id, data });
  });
}

wss.on('connection', ws => {
  ws.id = 'p' + nid++; ws.subs = new Set(); ws.alive = true; ws.pres = null;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'set': if (valid(m.path) && m.data && typeof m.data === 'object') { store.set(m.path, { data: m.data, t: Date.now() }); notify(m.path, m.data); } break;
      case 'del': if (valid(m.path)) { store.delete(m.path); notify(m.path, null); } break;
      case 'subdoc': if (valid(m.path)) { ws.subs.add('d:' + m.path); const v = store.get(m.path); send(ws, { t: 'doc', path: m.path, data: v ? v.data : null }); } break;
      case 'subcol': if (valid(m.path + '/0') || /^games\/[A-Z0-9]{4}\/seats$/.test(m.path)) { ws.subs.add('c:' + m.path); send(ws, { t: 'col', path: m.path, docs: colDocs(m.path) }); } break;
      case 'emit': if (typeof m.topic === 'string' && m.topic.length < 30) all({ t: 'msg', topic: m.topic, data: m.data, peer: ws.id }, ws); break;
      case 'pres': ws.pres = m.data && typeof m.data === 'object' ? m.data : null; all({ t: 'peers', peers: peers() }); break;
    }
  });
  ws.on('close', () => all({ t: 'peers', peers: peers() }));
});

// detectează conexiunile moarte (după ~15-30 s)
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 15000);
// șterge camerele nefolosite de peste 12 ore
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of store) {
    if (/^games\/[A-Z0-9]{4}$/.test(k) && now - v.t > 43200000) {
      store.delete(k);
      for (const k2 of [...store.keys()]) if (k2.startsWith(k + '/')) store.delete(k2);
    }
  }
}, 600000);

server.listen(PORT, () => console.log('Aventura pornită pe portul ' + PORT));

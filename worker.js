// 원고 전체를 브라우저에 올려 두고 검색하는 작업자
let N = 0, CH = 20, SPK = [], EPDATE = {}, loaded = 0;
const DATA = {};            // ep id -> [[t, spk, text], ...]
const chunkP = {};          // chunk -> promise
function b64(s) { const bin = atob(s.trim()); const u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i); return u; }
async function gunzipJSON(res) {
  const buf = new Uint8Array(await res.arrayBuffer());
  if (buf[0] === 0x1f && buf[1] === 0x8b) {   // 아직 압축된 상태면 직접 풀기
    const ds = new Blob([buf]).stream().pipeThrough(new DecompressionStream('gzip'));
    return JSON.parse(await new Response(ds).text());
  }
  return JSON.parse(new TextDecoder().decode(buf));
}
function loadChunk(k) {
  if (!chunkP[k]) chunkP[k] = (async () => {
    const r = await fetch(`c/${String(k).padStart(3, '0')}.gz`);
    if (!r.ok) throw new Error('chunk ' + k);
    const d = await gunzipJSON(r);
    for (const id in d) DATA[id] = d[id];
    loaded++; postMessage({ type: 'progress', loaded, total: N });
  })();
  return chunkP[k];
}
async function loadAll() {
  const order = [...Array(N).keys()];
  let i = 0;
  const run = async () => { while (i < order.length) { const k = order[i++]; try { await loadChunk(k) } catch (e) { delete chunkP[k]; postMessage({ type: 'error', msg: String(e) }) } } };
  await Promise.all([run(), run(), run(), run()]);
  postMessage({ type: 'ready' });
}
function terms(q) {
  const pos = [], neg = [];
  for (const p of (q.match(/"[^"]+"|-?\S+/g) || [])) {
    if (p.startsWith('"')) { const w = p.replace(/"/g, '').toLowerCase(); if (w) pos.push(w); }
    else if (p.startsWith('-')) { const w = p.slice(1).toLowerCase(); if (w) neg.push(w); }
    else pos.push(p.toLowerCase());
  }
  return [pos, neg];
}
function search({ q, speaker, from, to }) {
  const [pos, neg] = terms(q); const out = [];
  if (!pos.length) return out;
  const si = speaker ? SPK.indexOf(speaker) : -1;
  if (speaker && si < 0) return out;
  const ids = Object.keys(DATA).map(Number).sort((a, b) => a - b);
  for (const id of ids) {
    const d = EPDATE[id] || '';
    if (from && d < from) continue; if (to && d > to + '~') continue;
    const L = DATA[id];
    for (let i = 0; i < L.length; i++) {
      const l = L[i]; if (si >= 0 && l[1] !== si) continue;
      const t = l[2].toLowerCase(); let ok = true;
      for (const w of pos) if (!t.includes(w)) { ok = false; break; }
      if (!ok) continue;
      for (const w of neg) if (t.includes(w)) { ok = false; break; }
      if (ok) { out.push([id, i, l[0], l[1], l[2]]); if (out.length >= 20000) return out; }
    }
  }
  return out;
}
onmessage = async (e) => {
  const m = e.data;
  if (m.type === 'init') { N = m.nchunks; CH = m.chunk; SPK = m.spk; EPDATE = m.epdate; loadAll(); }
  else if (m.type === 'search') { postMessage({ type: 'result', id: m.id, hits: search(m), complete: loaded >= N }); }
  else if (m.type === 'episode') {
    try { await loadChunk(Math.floor((m.ep - 1) / CH)); postMessage({ type: 'episode', id: m.id, lines: DATA[m.ep] || [] }); }
    catch (err) { postMessage({ type: 'episode', id: m.id, error: String(err) }); }
  }
};

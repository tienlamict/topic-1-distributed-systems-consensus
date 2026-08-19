'use strict';

// Demo Raft — bộ replay chạy hoàn toàn phía browser.
//
// Server chạy trọn vẹn mô phỏng rồi trả về cả trace một lần. Ở đây ta dựng lại
// trạng thái cluster tại thời điểm T bất kỳ bằng cách áp dụng tuần tự các
// record có t <= T. Nhờ vậy tua ngược cũng nhanh như tua xuôi.

const $ = (s) => document.querySelector(s);
const SVGNS = 'http://www.w3.org/2000/svg';

const MSG_COLOR = {
  RequestVote:        '#60a5fa',
  RequestVoteReply:   '#a78bfa',
  AppendEntries:      '#34d399',
  AppendEntriesReply: '#14b8a6',
};
const MSG_GLYPH = {
  RequestVote: 'RV', RequestVoteReply: 'RV↩',
  AppendEntries: 'AE', AppendEntriesReply: 'AE↩',
};
const STATE_COLOR = { follower: '#64748b', candidate: '#f59e0b', leader: '#22c55e' };
const GROUP_COLOR = ['#22d3ee', '#e879f9', '#facc15', '#fb923c'];

const S = {
  scenarios: [], scenario: null, result: null,
  trace: [], msgs: [], duration: 0, nodes: [],
  T: 0, playing: false, speed: 1,
  cursor: 0, lastT: -1,
  st: {},           // id -> snapshot hiện tại
  groups: {},       // id -> nhóm partition
  events: [], note: null,
  live: { elections: 0, leaderChanges: 0, acked: 0, maxTerm: 0 },
  selected: null,
  pos: {},          // id -> {x,y}
  segs: {},         // id -> các đoạn trạng thái, cho timeline
  marks: [],        // mốc sự kiện trên timeline
};

// ───────────────────────────────────────────────────────── khởi động

async function boot() {
  S.scenarios = await (await fetch('api/scenarios')).json();
  const sel = $('#scenario');
  sel.innerHTML = S.scenarios.map((s) => `<option value="${s.id}">${s.title}</option>`).join('');
  sel.onchange = () => { buildParams(); run(); };
  $('#run').onclick = run;
  $('#seed').onchange = run;

  $('#play').onclick = () => (S.playing ? pause() : play());
  $('#rewind').onclick = () => { setT(0); pause(); };
  $('#step').onclick = stepToNextEvent;
  $('#seek').oninput = (e) => { pause(); setT(+e.target.value); };
  $('#speed').onchange = (e) => (S.speed = +e.target.value);

  document.addEventListener('keydown', (e) => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;
    if (e.code === 'Space') { e.preventDefault(); S.playing ? pause() : play(); }
    if (e.code === 'ArrowRight') { e.preventDefault(); pause(); stepToNextEvent(); }
    if (e.code === 'ArrowLeft') { e.preventDefault(); pause(); setT(Math.max(0, S.T - 100)); }
  });

  buildParams();
  await run();
  requestAnimationFrame(frame);
}

function currentScenario() {
  return S.scenarios.find((s) => s.id === $('#scenario').value);
}

function buildParams() {
  const sc = currentScenario();
  const box = $('#params');
  box.innerHTML = '';
  (sc.params || []).forEach((p) => {
    const el = document.createElement('div');
    el.className = 'param';
    el.innerHTML = `
      <div class="top"><span class="lbl">${p.label}</span><span class="val" id="v_${p.key}">${p.default}</span></div>
      <input type="range" id="p_${p.key}" min="${p.min}" max="${p.max}" step="${p.step}" value="${p.default}">
      <div class="help">${p.help || ''}</div>`;
    box.appendChild(el);
    const input = el.querySelector('input');
    input.oninput = () => { $(`#v_${p.key}`).textContent = input.value; };
    input.onchange = run;
  });
}

// ───────────────────────────────────────────────────────── chạy kịch bản

async function run() {
  const sc = currentScenario();
  S.scenario = sc;
  $('#ref').textContent = sc.ref;
  $('#brief').textContent = sc.brief;
  $('#status').textContent = 'đang chạy mô phỏng…';

  const q = new URLSearchParams({ scenario: sc.id, seed: $('#seed').value });
  (sc.params || []).forEach((p) => q.set('p_' + p.key, $(`#p_${p.key}`).value));

  const res = await (await fetch('api/run?' + q)).json();
  if (res.error) { $('#status').textContent = res.error; return; }

  S.result = res;
  S.trace = res.trace || [];
  S.duration = res.duration;
  S.nodes = res.nodes;
  S.selected = S.selected && S.nodes.includes(S.selected) ? S.selected : S.nodes[0];

  S.msgs = S.trace.filter((r) => r.kind === 'msg');
  const dropped = new Set(S.trace.filter((r) => r.kind === 'drop').map((r) => r.mid));
  S.msgs.forEach((m) => (m.lost = dropped.has(m.mid)));

  $('#seek').max = S.duration;
  $('#status').textContent = `${S.trace.length.toLocaleString('vi')} record`;

  layoutNodes();
  buildStaticSVG();
  buildSegments();
  renderMetrics();
  setT(0);
  play();
}

// ───────────────────────────────────────────────────────── replay

function blankState() {
  const st = {};
  S.nodes.forEach((id) => {
    st[id] = { state: 'follower', term: 0, votedFor: '', commitIndex: 0, alive: true,
               log: [], timerStart: 0, timerDeadline: 0, votes: [] };
  });
  return st;
}

function resetDerived() {
  S.st = blankState();
  S.groups = {}; S.nodes.forEach((id) => (S.groups[id] = 0));
  S.events = []; S.note = null;
  S.live = { elections: 0, leaderChanges: 0, acked: 0, maxTerm: 0 };
  S.cursor = 0; S.lastT = 0;
}

function apply(r) {
  switch (r.kind) {
    case 'node': {
      const prev = S.st[r.node];
      const s = r.state;
      // Go marshal slice rỗng thành null — chuẩn hoá về mảng để phần vẽ
      // không phải kiểm tra null ở mọi chỗ.
      s.log = s.log || [];
      s.votes = s.votes || [];
      if (prev) {
        // Đếm cả trường hợp candidate hết giờ rồi mở election MỚI (state
        // không đổi nhưng term tăng) — quan trọng ở S2 khi split vote lặp lại.
        if (s.state === 'candidate' && (prev.state !== 'candidate' || s.term > prev.term)) S.live.elections++;
        if (s.state === 'leader' && prev.state !== 'leader') S.live.leaderChanges++;
      }
      S.st[r.node] = s;
      if (s.term > S.live.maxTerm) S.live.maxTerm = s.term;
      break;
    }
    case 'note':
      S.note = r;
      break;
    case 'ack':
      S.live.acked++;
      S.events.push(r);
      break;
    case 'log': case 'truncate':
      S.events.push(r);
      break;
    case 'fault':
      if (r.data && r.data.groups) S.groups = r.data.groups;
      S.events.push(r);
      break;
  }
}

function setT(t) {
  t = Math.max(0, Math.min(S.duration, t));
  if (t < S.lastT || S.lastT < 0) resetDerived();
  while (S.cursor < S.trace.length && S.trace[S.cursor].t <= t) apply(S.trace[S.cursor++]);
  S.T = t; S.lastT = t;
}

function play() { if (S.T >= S.duration) setT(0); S.playing = true; $('#play').textContent = '⏸'; }
function pause() { S.playing = false; $('#play').textContent = '▶'; }

const STEP_KINDS = new Set(['note', 'fault', 'ack', 'truncate', 'log']);
function stepToNextEvent() {
  for (const r of S.trace) {
    if (r.t > S.T + 0.5 && STEP_KINDS.has(r.kind)) { setT(r.t); return; }
  }
  setT(S.duration);
}

let lastFrame = 0;
function frame(ts) {
  if (S.playing) {
    const dt = Math.min(120, ts - lastFrame);
    setT(S.T + dt * S.speed);
    if (S.T >= S.duration) pause();
  }
  lastFrame = ts;
  render();
  requestAnimationFrame(frame);
}

// ───────────────────────────────────────────────────────── dựng SVG tĩnh

const W = 820, H = 560, CX = 410, CY = 250, R = 175, NR = 30;

function layoutNodes() {
  const n = S.nodes.length;
  S.pos = {};
  S.nodes.forEach((id, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    S.pos[id] = { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) };
  });
}

function el(tag, attrs, parent) {
  const e = document.createElementNS(SVGNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}

let gEdges, gMsgs, gNodes, gBanner, nodeEls = {}, msgEls = {};

function buildStaticSVG() {
  const box = $('#canvas');
  box.innerHTML = '';
  const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'xMidYMid meet' }, box);

  gEdges = el('g', {}, svg);
  gMsgs = el('g', {}, svg);
  gNodes = el('g', {}, svg);
  gBanner = el('text', { x: CX, y: 26, 'text-anchor': 'middle', class: 'banner' }, svg);

  // Cạnh giữa mọi cặp node.
  gEdges.innerHTML = '';
  S.edgeEls = {};
  for (let i = 0; i < S.nodes.length; i++) {
    for (let j = i + 1; j < S.nodes.length; j++) {
      const a = S.pos[S.nodes[i]], b = S.pos[S.nodes[j]];
      S.edgeEls[S.nodes[i] + '|' + S.nodes[j]] =
        el('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, class: 'edge' }, gEdges);
    }
  }

  nodeEls = {}; msgEls = {};
  gNodes.innerHTML = ''; gMsgs.innerHTML = '';

  S.nodes.forEach((id) => {
    const p = S.pos[id];
    const g = el('g', { class: 'nodeg', transform: `translate(${p.x},${p.y})` }, gNodes);
    g.onclick = () => { S.selected = id; };

    const groupRing = el('circle', { r: NR + 13, fill: 'none', 'stroke-width': 2,
      'stroke-dasharray': '3 4', stroke: 'none' }, g);
    // Vòng nền của timer + vòng tiến trình (xoay -90° để bắt đầu từ 12h).
    const timerBg = el('circle', { r: NR + 7, fill: 'none', stroke: '#1a2338',
      'stroke-width': 3, transform: 'rotate(-90)' }, g);
    const timer = el('circle', { r: NR + 7, fill: 'none', 'stroke-width': 3,
      'stroke-linecap': 'round', transform: 'rotate(-90)' }, g);

    const disc = el('circle', { r: NR, fill: '#64748b', stroke: '#0a0e1a', 'stroke-width': 2 }, g);
    const label = el('text', { class: 'label', 'text-anchor': 'middle', y: 0 }, g);
    const sub = el('text', { class: 'sub', 'text-anchor': 'middle', y: 13 }, g);
    const term = el('text', { class: 'term', 'text-anchor': 'middle', y: -NR - 16 }, g);
    const strip = el('g', { transform: `translate(0,${NR + 16})` }, g);

    nodeEls[id] = { g, disc, label, sub, term, timer, timerBg, strip, groupRing, sig: '' };
  });
}

// ───────────────────────────────────────────────────────── vẽ mỗi frame

const TIMER_C = 2 * Math.PI * (NR + 7);

function render() {
  renderCluster();
  renderMessages();
  renderNote();
  renderEvents();
  renderInspector();
  renderTimelinePlayhead();
  $('#clock').textContent = `t = ${Math.round(S.T).toLocaleString('vi')} ms`;
  $('#seek').value = S.T;
  $('#metrics') && renderLiveMetrics();
}

function renderCluster() {
  const partitioned = new Set(Object.values(S.groups)).size > 1;
  gBanner.textContent = partitioned ? '✂  NETWORK PARTITION' : '';

  // Cạnh: ẩn nét khi hai đầu khác nhóm hoặc có node chết.
  for (let i = 0; i < S.nodes.length; i++) {
    for (let j = i + 1; j < S.nodes.length; j++) {
      const a = S.nodes[i], b = S.nodes[j];
      const line = S.edgeEls[a + '|' + b];
      const cut = S.groups[a] !== S.groups[b] || !S.st[a].alive || !S.st[b].alive;
      line.setAttribute('class', cut ? 'edge cut' : 'edge');
    }
  }

  S.nodes.forEach((id) => {
    const s = S.st[id], e = nodeEls[id];
    const color = s.alive ? STATE_COLOR[s.state] : '#3b0d0d';

    e.disc.setAttribute('fill', color);
    e.disc.setAttribute('stroke', S.selected === id ? '#e2e8f0' : '#0a0e1a');
    e.disc.setAttribute('stroke-width', S.selected === id ? 3 : 2);

    e.label.textContent = s.alive ? id : '✕';
    e.label.setAttribute('fill', s.alive ? '#fff' : '#ef4444');
    e.sub.textContent = s.alive ? (s.state === 'leader' ? 'LEADER' : s.state === 'candidate' ? 'CAND' : '') : 'CRASH';
    e.term.textContent = `term ${s.term}` + (s.votedFor ? ` · vote→${s.votedFor}` : '');

    // Vòng cung đếm ngược election timeout — thứ làm randomized timeout
    // trở nên hiển nhiên mà không cần giải thích.
    const span = s.timerDeadline - s.timerStart;
    if (s.alive && span > 0 && S.T >= s.timerStart) {
      const p = Math.max(0, Math.min(1, (S.T - s.timerStart) / span));
      e.timerBg.setAttribute('stroke', '#1a2338');
      e.timer.setAttribute('stroke', p > 0.8 ? '#f87171' : '#334d7a');
      e.timer.setAttribute('stroke-dasharray', `${p * TIMER_C} ${TIMER_C}`);
    } else {
      e.timerBg.setAttribute('stroke', 'none');
      e.timer.setAttribute('stroke-dasharray', `0 ${TIMER_C}`);
    }

    const partitioned2 = new Set(Object.values(S.groups)).size > 1;
    e.groupRing.setAttribute('stroke', partitioned2 ? GROUP_COLOR[S.groups[id] % GROUP_COLOR.length] : 'none');

    // Dải log dưới mỗi node, vẽ lại khi có thay đổi.
    const sig = `${s.log.length}|${s.commitIndex}|${s.log.map((x) => x.term).join(',')}`;
    if (sig !== e.sig) {
      e.sig = sig;
      e.strip.innerHTML = '';
      const show = s.log.slice(-12);
      const off = s.log.length - show.length;
      const cw = 12, total = show.length * cw;
      show.forEach((entry, k) => {
        const idx = off + k + 1;
        const committed = idx <= s.commitIndex;
        const x = -total / 2 + k * cw;
        el('rect', { x, y: 0, width: cw - 2, height: 15, rx: 2,
          fill: committed ? '#22c55e' : '#475569',
          stroke: committed ? '#065f46' : '#334155' }, e.strip);
        const t = el('text', { x: x + (cw - 2) / 2, y: 11, 'text-anchor': 'middle',
          'font-size': 9, fill: committed ? '#052e16' : '#cbd5e1', 'font-weight': 700 }, e.strip);
        t.textContent = entry.term;
      });
    }
  });
}

function renderMessages() {
  const live = new Set();
  // Chỉ quét cửa sổ quanh T — message bay tối đa vài chục ms.
  for (const m of S.msgs) {
    if (m.deliver < S.T) continue;
    if (m.t > S.T) break;
    live.add(m.mid);

    const a = S.pos[m.from], b = S.pos[m.to];
    if (!a || !b) continue;
    const p = (S.T - m.t) / Math.max(1, m.deliver - m.t);
    // Message bị mất thì dừng lại giữa đường rồi biến mất.
    const q = m.lost ? p * 0.55 : p;
    const x = a.x + (b.x - a.x) * q, y = a.y + (b.y - a.y) * q;

    let e = msgEls[m.mid];
    if (!e) {
      const g = el('g', { class: 'msg' }, gMsgs);
      const c = el('circle', { r: 8.5, stroke: '#0a0e1a', 'stroke-width': 1.5 }, g);
      const t = el('text', { 'text-anchor': 'middle', y: 2.8 }, g);
      t.textContent = MSG_GLYPH[m.type] || '?';
      e = msgEls[m.mid] = { g, c, t };
      c.setAttribute('fill', MSG_COLOR[m.type] || '#94a3b8');
    }
    e.g.setAttribute('transform', `translate(${x},${y})`);
    e.g.setAttribute('opacity', m.lost ? String(Math.max(0, 1 - p * 1.6)) : '1');
  }
  for (const mid in msgEls) {
    if (!live.has(+mid)) { msgEls[mid].g.remove(); delete msgEls[mid]; }
  }
}

function renderNote() {
  const box = $('#note');
  if (!S.note) { box.className = 'note'; box.innerHTML = '<span class="placeholder">—</span>'; return; }
  box.className = 'note ' + (S.note.level || 'info');
  box.textContent = S.note.text;
}

let lastEvCount = -1;
function renderEvents() {
  if (S.events.length === lastEvCount) return;
  lastEvCount = S.events.length;
  const list = $('#evlist');
  list.innerHTML = S.events.slice(-250).map((e) =>
    `<div class="ev ${e.level || 'info'}"><span class="t">${Math.round(e.t)}</span><span>${escapeHTML(e.text || '')}</span></div>`
  ).join('');
  const box = $('#events');
  box.scrollTop = box.scrollHeight;
}

function escapeHTML(s) {
  return s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

let lastInspSig = '';
function renderInspector() {
  const id = S.selected;
  if (!id || !S.st[id]) return;
  const s = S.st[id];
  const sig = `${id}|${s.state}|${s.term}|${s.votedFor}|${s.commitIndex}|${s.log.length}|${s.alive}|${(s.votes || []).join()}`;
  if (sig === lastInspSig) return;
  lastInspSig = sig;

  const rows = [
    ['state', s.alive ? s.state : 'CRASHED'],
    ['currentTerm', s.term],
    ['votedFor', s.votedFor || '—'],
    ['commitIndex', s.commitIndex],
    ['log length', s.log.length],
  ];
  if (s.state === 'candidate') rows.push(['phiếu đã nhận', `${(s.votes || []).length}/${S.nodes.length} (${(s.votes || []).join(', ')})`]);
  if (s.state === 'leader' && s.matchIndex) rows.push(['matchIndex', s.matchIndex.join(', ')]);
  if (s.state === 'leader' && s.nextIndex) rows.push(['nextIndex', s.nextIndex.join(', ')]);

  let html = `<h3>Node inspector — ${id}</h3><div class="kv">` +
    rows.map(([k, v]) => `<span class="k">${k}</span><span class="v">${escapeHTML(String(v))}</span>`).join('') +
    '</div>';

  if (s.log.length) {
    html += '<table class="logtable"><tr><th>idx</th><th>term</th><th>command</th><th></th></tr>' +
      s.log.map((e, i) => {
        const idx = i + 1, done = idx <= s.commitIndex;
        return `<tr class="${done ? 'committed' : 'pending'}"><td>${idx}</td><td>${e.term}</td>` +
               `<td>${escapeHTML(e.cmd || '')}</td><td>${done ? '■ committed' : '□ chưa'}</td></tr>`;
      }).join('') + '</table>';
  }
  $('#inspector').innerHTML = html;
}

// ───────────────────────────────────────────────────────── metrics

function renderMetrics() { renderLiveMetrics(true); }

let lastMetricSig = '';
function renderLiveMetrics() {
  const m = S.result.metrics;
  const sig = `${S.live.elections}|${S.live.leaderChanges}|${S.live.acked}|${S.live.maxTerm}`;
  if (sig === lastMetricSig) return;
  lastMetricSig = sig;

  const first = m.firstLeaderAt < 0
    ? '<span class="v bad">KHÔNG BAO GIỜ</span>'
    : `<span class="v">${m.firstLeaderAt} ms</span>`;

  $('#metrics').innerHTML = `
    <span class="k">— tới thời điểm hiện tại —</span><span class="v"></span>
    <span class="k">election đã mở</span><span class="v">${S.live.elections}</span>
    <span class="k">lần đổi leader</span><span class="v">${S.live.leaderChanges}</span>
    <span class="k">term cao nhất</span><span class="v">${S.live.maxTerm}</span>
    <span class="k">write đã ack</span><span class="v good">${S.live.acked}</span>
    <span class="k" style="padding-top:8px">— cả lượt chạy —</span><span class="v"></span>
    <span class="k">leader đầu tiên</span>${first}
    <span class="k">tổng thời gian không leader</span><span class="v">${m.leaderlessMs} ms</span>
    <span class="k">client write gửi</span><span class="v">${m.writesSubmitted}</span>
    <span class="k">→ được ack</span><span class="v good">${m.writesAcked}</span>
    <span class="k">→ bị từ chối</span><span class="v">${m.writesRejected}</span>
    <span class="k">→ bị cắt bỏ (chưa từng ack)</span><span class="v ${m.writesLost ? 'bad' : ''}">${m.writesLost}</span>
    <span class="k">message gửi / mất</span><span class="v">${m.messagesSent} / ${m.messagesLost}</span>`;
}

// ───────────────────────────────────────────────────────── timeline

const TLW = 1000, ROW = 15;

function buildSegments() {
  S.segs = {}; S.marks = [];
  const cur = {};
  S.nodes.forEach((id) => { cur[id] = { t0: 0, state: 'follower', alive: true }; S.segs[id] = []; });

  for (const r of S.trace) {
    if (r.kind === 'fault') { S.marks.push({ t: r.t, level: r.level }); continue; }
    if (r.kind !== 'node') continue;
    const c = cur[r.node];
    const st = r.state.state, alive = r.state.alive;
    if (st !== c.state || alive !== c.alive) {
      if (r.t > c.t0) S.segs[r.node].push({ t0: c.t0, t1: r.t, state: c.state, alive: c.alive });
      cur[r.node] = { t0: r.t, state: st, alive };
    }
  }
  S.nodes.forEach((id) => S.segs[id].push({ t0: cur[id].t0, t1: S.duration, state: cur[id].state, alive: cur[id].alive }));

  const h = S.nodes.length * ROW + 16;
  const svg = $('#timeline');
  svg.setAttribute('viewBox', `0 0 ${TLW} ${h}`);
  svg.style.height = h + 'px';
  svg.innerHTML = '';

  const sx = (t) => (t / S.duration) * (TLW - 40) + 34;

  S.nodes.forEach((id, i) => {
    const y = i * ROW + 2;
    const lb = el('text', { x: 0, y: y + 9, class: 'tl-label' }, svg);
    lb.textContent = id;
    S.segs[id].forEach((sg) => {
      el('rect', { x: sx(sg.t0), y, width: Math.max(1, sx(sg.t1) - sx(sg.t0)), height: ROW - 4, rx: 2,
        fill: sg.alive ? STATE_COLOR[sg.state] : '#3b0d0d',
        opacity: sg.state === 'follower' && sg.alive ? 0.45 : 0.95 }, svg);
    });
  });

  S.marks.forEach((mk) => {
    el('line', { x1: sx(mk.t), y1: 0, x2: sx(mk.t), y2: S.nodes.length * ROW,
      stroke: mk.level === 'good' ? '#34d399' : '#f87171', 'stroke-width': 1, opacity: 0.7 }, svg);
  });

  S.playhead = el('line', { x1: 34, y1: 0, x2: 34, y2: S.nodes.length * ROW + 4,
    stroke: '#e2e8f0', 'stroke-width': 1.5 }, svg);

  svg.onclick = (ev) => {
    const r = svg.getBoundingClientRect();
    const frac = ((ev.clientX - r.left) / r.width * TLW - 34) / (TLW - 40);
    pause(); setT(frac * S.duration);
  };
}

function renderTimelinePlayhead() {
  if (!S.playhead) return;
  const x = (S.T / S.duration) * (TLW - 40) + 34;
  S.playhead.setAttribute('x1', x);
  S.playhead.setAttribute('x2', x);
}

boot();

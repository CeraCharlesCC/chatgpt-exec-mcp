import { Prism } from './vendor/prism.js';
import {
  $, esc, interacting, syncList, updateTimes, time, syncOptions,
  clearSelection, setPointerTarget, clearPointerTarget, hasPointerTarget
} from './dom.js';
import { eventPools, isMessageEvent, eventIdentities } from './model/events.js';
import { messages } from './model/messages.js';

const state = {data: null, view: 'activity', filter: 'all', pool: '', agent: '', session: '', format: true, group: false, refreshing: false, refreshEpoch: 0, clearing: false, acking: new Set()};
function toast(msg, bad = false) {
  const t = $('#toast');
  t.textContent = msg;
  t.className = 'toast show' + (bad ? ' bad' : '');
  clearTimeout(t._x);
  t._x = setTimeout(() => t.className = 'toast', 2200);
}
function setCover(open) {
  if (open && state.clearing) return;
  $('#clearControl').classList.toggle('cover-open', open);
  const cover = $('#clearCover'), button = $('#clearHistory');
  cover.setAttribute('aria-expanded', String(open));
  cover.setAttribute('aria-label', open ? 'Close safety cover' : 'Open safety cover');
  button.disabled = !open || state.clearing;
  if (!open && document.activeElement === button) {
    (cover.disabled ? $('#menuClose') : cover).focus();
  }
  $('#clearHint').textContent = state.clearing ? 'Clearing history…' :
    open ? 'Press CLEAR to erase history.' : 'Lift the cover to clear history.';
}
function setMenu(open) {
  $('#menuToggle').setAttribute('aria-expanded', String(open));
  $('#menuToggle').setAttribute('aria-label', open ? 'Close console menu' : 'Open console menu');
  $('#consoleMenu').classList.toggle('open', open);
  $('#consoleMenu').inert = !open;
  $('#menuBackdrop').classList.toggle('open', open);
  $('main').inert = open;
  if (!open) setCover(false);
  (open ? $('#menuClose') : $('#menuToggle')).focus();
}
function connection(connected) {
  const node = $('#connectionStatus'), label = connected ? 'Connected' : 'Disconnected';
  node.dataset.connection = connected ? 'connected' : 'disconnected';
  node.setAttribute('aria-label', label);
  node.title = label;
}
async function clearHistory() {
  if (state.clearing || $('#clearCover').getAttribute('aria-expanded') !== 'true') return;
  state.clearing = true;
  // Discard any snapshot requested before the clear, and pause polling until it finishes.
  state.refreshEpoch++;
  $('#clearCover').disabled = true;
  $('#clearControl').classList.add('pressing');
  setCover(false);
  setTimeout(() => $('#clearControl').classList.remove('pressing'), 180);
  try {
    const r = await fetch('/_admin/api/clear', {method: 'POST', headers: {'content-type': 'application/json'}, body: '{}'});
    if (!r.ok) throw new Error(await r.text());
    if (state.data) {
      state.data.events = [];
      state.data.admin_messages = [];
    }
    clearSelection();
    render();
    $('#clearHint').textContent = 'History cleared.';
    toast('Activity, MCP log and admin inbox cleared');
  } catch (e) {
    $('#clearHint').textContent = 'Clear failed. Lift the cover to retry.';
    toast(e.message, true);
  } finally {
    state.clearing = false;
    $('#clearCover').disabled = false;
    await refresh();
  }
}

function renderPools() {
  const root = $('#pools'), pools = state.data?.pools || [];
  if (!interacting(root)) {
    // Pool wrappers and agent rows are keyed separately, so changing a lease
    // does not replace the pool title or a focused Terminate button.
    syncList(root, pools, p => p.name, p => `<div class="pool-title"><button class="pool-filter" data-pool="${esc(p.name)}" aria-pressed="false" title="Show activity from ${esc(p.name)}">${esc(p.name)}</button><span class="muted pool-count"></span></div><div class="pool-agents"></div>`, 'No active agents', 'pool');
    for (const box of root.children) {
      const p = pools.find(p => p.name === box.dataset.key);
      if (!p) continue;
      const button = box.querySelector('.pool-filter');
      button.setAttribute('aria-pressed', String(state.pool === p.name));
      box.querySelector('.pool-count').textContent = p.agents.length;
      syncList(box.querySelector('.pool-agents'), p.agents, a => a.agent, a => `<div><div class="agent-name"><span class="tiny-dot"></span><strong>${esc(a.agent)}</strong></div><div class="agent-meta">seen <span data-clock="ago"></span> · lease <span data-clock="left"></span></div></div><button class="danger" data-pool="${esc(p.name)}" data-agent="${esc(a.agent)}">Terminate</button>`, '', 'agent');
      for (const row of box.querySelector('.pool-agents').children) {
        const agent = p.agents.find(a => a.agent === row.dataset.key);
        if (!agent) continue;
        row.querySelector('[data-clock=ago]').dataset.time = agent.last_seen_ms;
        row.querySelector('[data-clock=left]').dataset.time = agent.expires_ms;
      }
    }
    updateTimes(root);
  }
  renderTargets();
}
function renderTargets() {
  const pools = state.data?.pools || [], ps = $('#poolSelect'), ts = $('#targetSelect');
  syncOptions(ps, pools.map(p => ({value: p.name, label: p.name})), ps.value);
  const p = pools.find(x => x.name === ps.value);
  syncOptions(ts, ['global', ...(p?.agents || []).map(a => a.agent)].map(value => ({value, label: value})), ts.value);
}
function renderInbox() {
  const root = $('#inbox'), messages = state.data?.admin_messages || [];
  if (interacting(root)) return;
  syncList(root, messages, m => m.message_id, m => `<div class="message-head"><span>${esc(m.from)} · ${esc(m.pool)}</span>${time(m.created_ms, '')}</div><div style="white-space:pre-wrap;overflow-wrap:anywhere">${esc(m.message)}</div><div class="message-actions"><button class="ack" data-message-id="${esc(m.message_id)}" title="ACKed messages stay visible until history is cleared or the server restarts">ACK</button></div>`, 'No messages', 'message');
  for (const row of root.children) {
    const message = messages.find(m => m.message_id === row.dataset.key);
    if (!message) continue;
    const button = row.querySelector('.ack'), pending = state.acking.has(message.message_id);
    const label = message.acknowledged ? 'ACKed' : pending ? 'ACKing…' : 'ACK';
    if (button.textContent !== label) button.textContent = label;
    button.disabled = !!message.acknowledged || pending;
  }
}
function renderFilters(events, starts, finishes, conversation) {
  const names = new Set((state.data?.pools || []).map(p => p.name));
  const agents = new Set(), sessions = new Set();
  for (const p of state.data?.pools || []) {
    if (!state.pool || p.name === state.pool) for (const a of p.agents) {
      agents.add(a.agent);
      sessions.add(a.session);
    }
  }
  for (const e of events) {
    const pools = eventPools(e, starts);
    for (const pool of pools) names.add(pool);
    if (!state.pool || pools.has(state.pool)) {
      if (e.session) sessions.add(e.session);
      for (const a of eventIdentities(e, starts, finishes)) {
        if (!state.pool || a.pool === state.pool) agents.add(a.agent);
      }
    }
  }
  for (const m of state.data?.admin_messages || []) names.add(m.pool);
  for (const m of conversation) if (!state.pool || m.pool === state.pool) {
    for (const agent of m.agents) agents.add(agent);
    for (const session of m.sessions) sessions.add(session);
  }
  if (state.pool) names.add(state.pool);
  if (state.agent) agents.add(state.agent);
  if (state.session) sessions.add(state.session);
  const options = (values, label) => [{value: '', label}, ...[...values].filter(Boolean).sort().map(value => ({value, label: value}))];
  syncOptions($('#activityPool'), options(names, 'All pools'), state.pool);
  syncOptions($('#activityAgent'), options(agents, 'All agents'), state.agent);
  syncOptions($('#activitySession'), options(sessions, 'All sessions'), state.session);
}
const jsonText = value => JSON.stringify(value, null, 2) ?? 'null';
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const highlightCache = new Map();
function highlight(text, language) {
  // Large previews remain complete, but avoid expensive tokenization on every poll.
  if (!language || text.length > 65536) return esc(text);
  const key = language + ':' + text;
  if (highlightCache.has(key)) return highlightCache.get(key);
  try {
    const html = Prism.highlight(text, Prism.languages[language], language);
    if (highlightCache.size >= 128) highlightCache.delete(highlightCache.keys().next().value);
    highlightCache.set(key, html);
    return html;
  }
  catch { return esc(text); }
}
function codeBlock(label, text, language = '', error = false, original = null) {
  return `<section class="code-block${error ? ' tool-error' : ''}" data-copy-text="${esc(JSON.stringify(original ?? text))}"><div class="code-head"><span>${esc(label)}</span><span class="code-actions"><button type="button" data-code-action="expand" aria-expanded="false" aria-label="Expand ${esc(label)}">Expand</button><button type="button" data-code-action="copy" aria-label="Copy ${esc(label)}">Copy</button></span></div><pre><code${language ? ` class="language-${language}"` : ''}>${highlight(text, language)}</code></pre></section>`;
}
function outputBlock(output) {
  // Copy retains the exact output, including original JSON whitespace.
  let text = output, language = '';
  if (output.trim() && output.length <= 65536) {
    try { text = jsonText(JSON.parse(output)); language = 'json'; } catch {}
  }
  return codeBlock('Output', text, language, false, language ? output : null);
}
function toolDetail(e) {
  const d = e.detail;
  if (!state.format) return codeBlock('Raw JSON', jsonText(d));
  if (!record(d) || !['tool.start', 'tool.finish', 'tool.error'].includes(e.kind)) {
    return codeBlock('Details', jsonText(d), 'json');
  }
  let html = `<div class="tool-name">${esc(d.tool || e.kind)}${d.start_event_id !== undefined ? ` <span class="chip">Start event: ${esc(d.start_event_id)}</span>` : ''}</div>`;
  const rest = {...d};
  delete rest.tool;
  delete rest.start_event_id;
  if (e.kind === 'tool.start' && record(d.arguments)) {
    const args = {...d.arguments};
    if (typeof args.cmd === 'string') {
      html += codeBlock('Command', args.cmd, 'bash');
      delete args.cmd;
    }
    if (Object.keys(args).length) html += codeBlock('Arguments', jsonText(args), 'json');
    delete rest.arguments;
  } else if (e.kind !== 'tool.start' && record(d.result)) {
    const result = {...d.result}, meta = [];
    if (result.exit_code !== undefined && result.exit_code !== null) {
      meta.push(`<span class="chip${result.exit_code !== 0 ? ' failed' : ''}">Exit code: ${esc(result.exit_code)}</span>`);
      delete result.exit_code;
    }
    if (typeof result.call_wall_time_seconds === 'number') {
      meta.push(`<span class="chip">Execution time: ${esc(result.call_wall_time_seconds)} s</span>`);
      delete result.call_wall_time_seconds;
    }
    if (result.session_id !== undefined && result.session_id !== null) {
      meta.push(`<span class="chip">Process session: ${esc(result.session_id)}</span>`);
      delete result.session_id;
    }
    if (meta.length) html += `<div class="tool-meta">${meta.join('')}</div>`;
    if (result.error !== undefined) {
      html += codeBlock('Error', typeof result.error === 'string' ? result.error : jsonText(result.error), '', true);
      delete result.error;
    }
    if (typeof result.output === 'string') {
      html += outputBlock(result.output);
      delete result.output;
    }
    if (Object.keys(result).length) html += codeBlock('Result details', jsonText(result), 'json', e.kind === 'tool.error');
    delete rest.result;
  }
  // Keep unfamiliar fields and correlation IDs available without duplicating output.
  if (Object.keys(rest).length) html += codeBlock('Event details', jsonText(rest), 'json', e.kind === 'tool.error');
  return html;
}
function eventMarkup(e) {
  const body = toolDetail(e);
  if (state.view === 'logs') {
    return `<div class="logmeta${e.kind === 'tool.error' ? ' lvl-error' : ''}">${new Date(e.timestamp_ms).toLocaleTimeString()} [${esc(e.level)}] ${esc(e.kind)}${e.session ? ' session=' + esc(e.session) : ''}</div>${body}`;
  }
  return `<article class="event lvl-${esc(e.level)}"><div class="event-head"><span class="event-kind">${esc(e.kind)}</span>${time(e.timestamp_ms)}</div>${e.agents?.length ? `<div class="chips">${e.agents.map(a => `<span class="chip">${esc(a.pool)}/${esc(a.agent)}</span>`).join('')}</div>` : ''}${body}</article>`;
}
function groupMarkup(item) {
  if (!item.start) return eventMarkup(item);
  const finish = item.finish;
  return `<article class="event${finish?.kind === 'tool.error' ? ' lvl-error' : ''}"><details class="tool-group" open><summary>${esc(item.start.detail?.tool || 'Tool call')}<span class="chip">${finish ? esc(finish.kind) : 'In progress'}</span></summary><div data-group-part="start">${eventMarkup(item.start)}</div><div data-group-part="finish">${finish ? eventMarkup(finish) : ''}</div></details></article>`;
}
function updateEvent(node, html) {
  const group = node.querySelector('.tool-group');
  if (!group) {
    node.innerHTML = html;
    node.querySelectorAll('.tool-group summary, [data-group-part]').forEach(part => { part._markup = part.innerHTML; });
    return;
  }
  const template = document.createElement('template');
  template.innerHTML = html;
  node.firstElementChild.className = template.content.firstElementChild.className;
  // Completing a group must preserve its open state and the command's DOM/scroll.
  for (const selector of ['summary', '[data-group-part="start"]', '[data-group-part="finish"]']) {
    const current = group.querySelector(selector), next = template.content.querySelector(selector);
    if (current._markup !== next.innerHTML) {
      current.innerHTML = next.innerHTML;
      current._markup = next.innerHTML;
    }
  }
}
function messageMarkup(m) {
  return `<article class="event lvl-${esc(m.level || 'info')}"><div class="event-head"><span class="event-kind">${esc(m.from)} → ${esc(m.to)}</span>${time(m.timestamp_ms)}</div><div class="chips"><span class="chip">${esc(m.pool)}</span><span class="chip">${esc(m.kind)}</span>${m.message_id ? `<span class="chip">${esc(m.message_id)}</span>` : ''}${m.in_reply_to ? `<span class="chip">reply to ${esc(m.in_reply_to)}</span>` : ''}</div><div class="message-body">${esc(m.message)}</div><div class="message-status">${esc(m.status)}</div></article>`;
}
function updateMessage(node, html) {
  if (!node.firstElementChild) {
    node.innerHTML = html;
    return;
  }
  const template = document.createElement('template');
  template.innerHTML = html;
  node.firstElementChild.className = template.content.firstElementChild.className;
  // Delivery updates must not replace a body that the user has scrolled.
  for (const selector of ['.event-head', '.chips', '.message-body', '.message-status']) {
    const current = node.querySelector(selector), next = template.content.querySelector(selector);
    if (current.innerHTML !== next.innerHTML) current.innerHTML = next.innerHTML;
  }
}
function renderEvents() {
  const root = $('#events'), events = state.data?.events || [];
  const grouping = state.group && state.format && state.view === 'activity' && state.filter === 'all';
  $('#groupTools').disabled = !state.format || state.view !== 'activity' || state.filter !== 'all';
  const starts = new Map(events.filter(e => e.kind === 'tool.start').map(e => [e.id, e]));
  const finishes = new Map(events.filter(e => e.detail?.start_event_id).map(e => [e.detail.start_event_id, e]));
  const conversationRows = messages(events, finishes, state.data?.pools || [], state.data?.admin_messages || []);
  renderFilters(events, starts, finishes, conversationRows);
  if (interacting(root)) return;
  const viewKey = JSON.stringify([state.view, state.filter, state.pool, state.agent, state.session, state.format, grouping]);
  if (root._viewKey !== viewKey) root.scrollTop = 0;
  root._viewKey = viewKey;
  const conversation = state.view === 'activity' && state.filter === 'messages';
  const all = conversation ? conversationRows : [...events].reverse();
  const shown = all.filter(item => {
    if (conversation) return (!state.pool || item.pool === state.pool) &&
      (!state.agent || item.agents.includes(state.agent)) &&
      (!state.session || item.sessions.includes(state.session));
    return (!state.pool || eventPools(item, starts).has(state.pool)) &&
      (!state.agent || eventIdentities(item, starts, finishes).some(a =>
        a.agent === state.agent && (!state.pool || a.pool === state.pool))) &&
      (!state.session || item.session === state.session) &&
      (state.filter !== 'messages' || isMessageEvent(item, starts));
  });
  let rows = shown;
  if (grouping) {
    const visible = new Set(shown.map(e => e.id));
    const paired = new Map(shown.filter(e => ['tool.finish', 'tool.error'].includes(e.kind) && visible.has(e.detail?.start_event_id))
      .map(e => [e.detail.start_event_id, e]));
    rows = shown.filter(e => !paired.has(e.id)).map(e => {
      const start = starts.get(e.detail?.start_event_id);
      if (start && paired.get(start.id) === e) return {key: `group:${start.id}`, start, finish: e};
      if (e.kind === 'tool.start') return {key: `group:${e.id}`, start: e};
      return e;
    });
  }
  syncList(root, rows, item => `${state.view}:${conversation ? 'message:' : 'event:'}${item.key || item.id}`,
    conversation ? messageMarkup : grouping ? groupMarkup : eventMarkup,
    state.pool || state.agent || state.session || state.filter !== 'all' ? 'No entries match these filters' : 'No activity yet',
    state.view === 'logs' ? 'logline' : '', conversation ? updateMessage : updateEvent);
  $('#filterCount').textContent = `${shown.length} / ${all.length} ${conversation ? 'messages' : 'events'}${grouping ? ` · ${rows.length} cards` : ''}`;
}
function render() {
  renderPools();
  renderInbox();
  renderEvents();
}
async function terminate(pool, agent) {
  if (!confirm(`Terminate ${agent} from ${pool}?`)) return;
  try {
    const r = await fetch('/_admin/api/terminate', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({pool, agent})});
    if (!r.ok) throw new Error(await r.text());
    toast('Membership terminated');
    await refresh();
  } catch (e) { toast(e.message, true); }
}
async function send() {
  const pool = $('#poolSelect').value, target = $('#targetSelect').value, message = $('#message').value;
  if (!pool || !target || !message.trim()) return toast('Pool, target and message are required', true);
  try {
    const r = await fetch('/_admin/api/send', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({pool, target, message})});
    if (!r.ok) throw new Error(await r.text());
    const x = await r.json();
    $('#message').value = '';
    toast(`Queued for ${(x.recipients || []).length} recipient(s)`);
    await refresh();
  } catch (e) { toast(e.message, true); }
}
async function ackMessage(messageId) {
  if (state.acking.has(messageId)) return;
  state.acking.add(messageId);
  renderInbox();
  try {
    const r = await fetch('/_admin/api/ack', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({message_id: messageId})});
    if (!r.ok) throw new Error(await r.text());
    if (!(await r.json()).acknowledged) throw new Error('Message is no longer available');
    const message = state.data?.admin_messages.find(m => m.message_id === messageId);
    if (message) message.acknowledged = true;
    toast('ACKed; retained until clear or restart');
  } catch (e) { toast(e.message, true); }
  finally { state.acking.delete(messageId); renderInbox(); }
}
async function refresh() {
  // Never let a slow poll overlap another request and overwrite newer data.
  if (state.refreshing || state.clearing) return;
  state.refreshing = true;
  const epoch = state.refreshEpoch;
  try {
    const r = await fetch('/_admin/api/snapshot', {cache: 'no-store'});
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    if (epoch !== state.refreshEpoch) return;
    state.data = data;
    connection(true);
    $('#status').textContent = `v${state.data.version} · up ${Math.floor(state.data.uptime_seconds / 60)}m`;
    render();
  } catch (e) {
    if (epoch !== state.refreshEpoch) return;
    connection(false);
    $('#status').textContent = 'disconnected';
    toast(e.message, true);
  } finally { state.refreshing = false; }
}
$('#menuToggle').onclick = () => setMenu($('#menuToggle').getAttribute('aria-expanded') !== 'true');
$('#menuClose').onclick = $('#menuBackdrop').onclick = () => setMenu(false);
$('#clearCover').onclick = () => setCover($('#clearCover').getAttribute('aria-expanded') !== 'true');
$('#clearHistory').onclick = clearHistory;
document.addEventListener('keydown', event => {
  if ($('#menuToggle').getAttribute('aria-expanded') !== 'true') return;
  if (event.key === 'Escape') { event.preventDefault(); setMenu(false); }
  if (event.key === 'Tab') {
    const buttons = [...$('#consoleMenu').querySelectorAll('button:not(:disabled)'), $('#menuToggle')];
    const next = buttons.indexOf(document.activeElement) + (event.shiftKey ? -1 : 1);
    event.preventDefault();
    buttons[(next + buttons.length) % buttons.length].focus();
  }
});
$('#poolSelect').onchange = renderTargets;
$('#send').onclick = send;
$('#inbox').onclick = event => {
  const button = event.target.closest('button.ack');
  if (button && !button.disabled) ackMessage(button.dataset.messageId);
};
$('#pools').onclick = event => {
  const button = event.target.closest('button');
  if (button?.classList.contains('danger')) terminate(button.dataset.pool, button.dataset.agent);
  if (button?.classList.contains('pool-filter')) {
    selectPool(state.pool === button.dataset.pool ? '' : button.dataset.pool);
  }
};
$('#activityFilter').onchange = event => { state.filter = event.target.value; clearSelection(); renderEvents(); };
function selectPool(pool) {
  clearSelection();
  state.pool = pool;
  state.agent = '';
  state.session = '';
  render();
}
$('#activityPool').onchange = event => selectPool(event.target.value);
$('#activityAgent').onchange = event => { state.agent = event.target.value; clearSelection(); renderEvents(); };
$('#activitySession').onchange = event => { state.session = event.target.value; clearSelection(); renderEvents(); };
$('#formatLogs').onchange = event => { state.format = event.target.checked; clearSelection(); renderEvents(); };
$('#wrapLogs').onchange = event => $('#events').classList.toggle('wrap', event.target.checked);
$('#groupTools').onchange = event => { state.group = event.target.checked; clearSelection(); renderEvents(); };
$('#events').onclick = async event => {
  const button = event.target.closest('button[data-code-action]');
  if (!button) return;
  const block = button.closest('.code-block');
  if (button.dataset.codeAction === 'expand') {
    const expanded = block.classList.toggle('expanded');
    button.setAttribute('aria-expanded', String(expanded));
    const label = block.querySelector('.code-head > span').textContent;
    button.setAttribute('aria-label', `${expanded ? 'Collapse' : 'Expand'} ${label}`);
    button.textContent = expanded ? 'Collapse' : 'Expand';
  } else {
    try {
      await navigator.clipboard.writeText(JSON.parse(block.dataset.copyText));
      toast('Copied');
    } catch { toast('Copy failed. Select the text and copy it manually.', true); }
  }
};
document.querySelectorAll('.tab').forEach(button => button.onclick = () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x === button));
  state.view = button.dataset.view;
  clearSelection();
  renderEvents();
});
document.addEventListener('pointerdown', event => { setPointerTarget(event.target); });
function endPointer() { clearPointerTarget(); render(); }
document.addEventListener('pointerup', endPointer);
document.addEventListener('pointercancel', endPointer);
window.addEventListener('blur', () => { clearPointerTarget(); });
// Apply deferred rows as soon as a selection is cleared, without waiting for a poll.
document.addEventListener('selectionchange', () => {
  if (window.getSelection()?.isCollapsed && !hasPointerTarget()) render();
});
refresh();
setInterval(refresh, 1500);

export { refresh, clearHistory, state };

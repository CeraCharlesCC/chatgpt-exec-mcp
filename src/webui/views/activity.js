import { $, esc, interacting, syncList, time, syncOptions, clearSelection } from '../dom.js';
import { eventPools, isMessageEvent, eventIdentities, pageEventContext } from '../model/events.js';
import { messages } from '../model/messages.js';
import { toolDetail, handleCodeAction } from './tool-output.js';

function renderFilters(state, events, starts, finishes, conversation) {
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
function eventMarkup(e, state) {
  const body = toolDetail(e, state.format);
  if (state.view === 'logs') {
    return `<div class="logmeta${e.kind === 'tool.error' ? ' lvl-error' : ''}">${new Date(e.timestamp_ms).toLocaleTimeString()} [${esc(e.level)}] ${esc(e.kind)}${e.session ? ' session=' + esc(e.session) : ''}</div>${body}`;
  }
  return `<article class="event lvl-${esc(e.level)}"><div class="event-head"><span class="event-kind">${esc(e.kind)}</span>${time(e.timestamp_ms)}</div>${e.agents?.length ? `<div class="chips">${e.agents.map(a => `<span class="chip">${esc(a.pool)}/${esc(a.agent)}</span>`).join('')}</div>` : ''}${body}</article>`;
}
function groupMarkup(item, state) {
  if (!item.start) return eventMarkup(item, state);
  const finish = item.finish;
  return `<article class="event${finish?.kind === 'tool.error' ? ' lvl-error' : ''}"><details class="tool-group" open><summary>${esc(item.start.detail?.tool || 'Tool call')}<span class="chip">${finish ? esc(finish.kind) : 'In progress'}</span></summary><div data-group-part="start">${eventMarkup(item.start, state)}</div><div data-group-part="finish">${finish ? eventMarkup(finish, state) : ''}</div></details></article>`;
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
export function renderEvents(state) {
  const root = $('#events'), events = state.data?.events || [];
  $('#activityPagination').hidden = !state.data ||
    (state.activityPageIndex === 0 && state.activityNextBeforeId === null);
  $('#activityPageLabel').textContent = `Page ${state.activityPageIndex + 1}${state.activityLoading ? ' · loading…' : ''}`;
  $('#activityNewer').disabled = state.activityLoading || state.activityPageIndex === 0;
  $('#activityOlder').disabled = state.activityLoading || state.activityNextBeforeId === null;
  const grouping = state.group && state.format && state.view === 'activity' && state.filter === 'all';
  $('#groupTools').disabled = !state.format || state.view !== 'activity' || state.filter !== 'all';
  const {starts, finishes, messageEvents} = pageEventContext(events, state.data?.correlated_events);
  const conversationRows = messages(messageEvents, finishes, state.data?.pools || [],
    state.data?.admin_messages || [], starts);
  renderFilters(state, events, starts, finishes, conversationRows);
  if (interacting(root)) return;
  const viewKey = JSON.stringify([state.view, state.filter, state.pool, state.agent, state.session, state.format, grouping, state.activityPageIndex, state.activityCursors[state.activityPageIndex]]);
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
    conversation ? messageMarkup : grouping ? item => groupMarkup(item, state) : item => eventMarkup(item, state),
    state.activityLoading ? 'Loading activity…' : state.pool || state.agent || state.session || state.filter !== 'all' ? 'No entries match these filters' : 'No activity yet',
    state.view === 'logs' ? 'logline' : '', conversation ? updateMessage : updateEvent);
  $('#filterCount').textContent = `${shown.length} / ${all.length} ${conversation ? 'messages' : 'events'}${grouping ? ` · ${rows.length} cards` : ''}`;
}

export function bindActivity({state, render, selectPool, toast, changeActivityPage}) {
  $('#activityNewer').onclick = () => changeActivityPage(-1);
  $('#activityOlder').onclick = () => changeActivityPage(1);
  $('#activityFilter').onchange = event => { state.filter = event.target.value; clearSelection(); renderEvents(state); };
  $('#activityPool').onchange = event => selectPool(event.target.value);
  $('#activityAgent').onchange = event => { state.agent = event.target.value; clearSelection(); renderEvents(state); };
  $('#activitySession').onchange = event => { state.session = event.target.value; clearSelection(); renderEvents(state); };
  $('#formatLogs').onchange = event => { state.format = event.target.checked; clearSelection(); renderEvents(state); };
  $('#wrapLogs').onchange = event => $('#events').classList.toggle('wrap', event.target.checked);
  $('#groupTools').onchange = event => { state.group = event.target.checked; clearSelection(); renderEvents(state); };
  $('#events').onclick = event => handleCodeAction(event, toast);
  document.querySelectorAll('.tab').forEach(button => button.onclick = () => {
    document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x === button));
    state.view = button.dataset.view;
    clearSelection();
    renderEvents(state);
  });
}

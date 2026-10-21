// Event transport compatibility lives here, at the browser boundary. A v1
// lifecycle is authoritative even if legacy detail fields disagree. Historic
// unversioned events are mapped once to the same semantic shape.
export function normalizeEvent(event) {
  if (!event || event.semantic) return event;
  const detail = event.detail || {};
  const typed = event.schema_version === 1;
  const lifecycle = typed ? event.lifecycle : null;
  const result = detail.result;
  const legacySend = !typed && event.kind === 'tool.start' && detail.tool === 'pool_send' &&
    typeof detail.arguments?.message === 'string' && !detail.arguments.action;
  const message = typed ? (lifecycle?.type === 'message_send' &&
    typeof lifecycle.message?.body === 'string' ? {
      pool: lifecycle.message.pool, target: lifecycle.message.target,
      message: lifecycle.message.body,
      from: lifecycle.message.from, message_id: lifecycle.message.message_id,
      recipients: lifecycle.message.recipients, error: lifecycle.message.error,
      status: lifecycle.message.status,
    } : null) : (legacySend ? detail.arguments : null);
  const completion = !typed && ['tool.finish', 'tool.error'].includes(event.kind) ? {
    from: result?.value?.assigned_agent, recipients: result?.value?.recipients,
    message_id: result?.value?.message_id, error: result?.error || result?.value?.error,
    status: event.kind === 'tool.error' ? 'failed' : 'queued',
  } : null;
  return {...event, semantic: {
    startId: typed ? lifecycle?.start_event_id : detail.start_event_id,
    tool: typed ? lifecycle?.tool : detail.tool,
    pools: event.kind === 'admin.message.send' ? [detail.pool] :
      typed ? [lifecycle?.pool] : [detail.pool, detail.arguments?.pool],
    message, completion,
    messages: typed ? lifecycle?.messages || [] :
      result?.messages || result?.value?.messages || [],
    deliveryReceipts: typed ? lifecycle?.delivery_receipts || [] :
      result?.delivery_receipts || result?.value?.delivery_receipts || [],
  }};
}

export const normalizeEvents = events => (events || []).map(normalizeEvent);
export const eventStartId = e => normalizeEvent(e)?.semantic.startId;
export const eventTool = e => normalizeEvent(e)?.semantic.tool;
export const messageSend = e => normalizeEvent(e)?.semantic.message || null;
export const messageCompletion = e => normalizeEvent(e)?.semantic.completion || null;
export const eventMessages = e => normalizeEvent(e)?.semantic.messages || [];
export const deliveryReceipts = e => normalizeEvent(e)?.semantic.deliveryReceipts || [];

export function eventPools(event, starts) {
  const e = normalizeEvent(event);
  const pools = new Set((e.agents || []).map(a => a.pool));
  const start = normalizeEvent(starts.get(eventStartId(e)));
  const names = [...e.semantic.pools, ...(start?.semantic.pools || []),
    messageSend(e)?.pool, messageSend(start)?.pool];
  for (const name of names) if (name) pools.add(name);
  for (const m of eventMessages(e)) pools.add(m.pool);
  for (const receipt of deliveryReceipts(e)) pools.add(receipt.pool);
  return pools;
}

export function isMessageEvent(e, starts) {
  if (e.kind === 'admin.message.send') return true;
  const start = e.kind === 'tool.start' ? e : starts.get(eventStartId(e));
  return Boolean(messageSend(e) || messageSend(start) || eventMessages(e).length);
}

export function eventIdentities(e, starts, finishes) {
  const start = e.kind === 'tool.start' ? e : starts.get(eventStartId(e));
  const finish = e.kind === 'tool.start' ? finishes.get(e.id) : e;
  const agents = [...(e.agents || []), ...(start?.agents || []), ...(finish?.agents || [])];
  const send = messageSend(start) || messageSend(e) || messageSend(finish);
  const completed = messageSend(finish) || messageCompletion(finish);
  if (send?.pool) {
    const from = completed?.from || send.from;
    if (from) agents.push({pool: send.pool, agent: from});
    if (send.target && send.target !== 'global') agents.push({pool: send.pool, agent: send.target});
    for (const agent of completed?.recipients || []) agents.push({pool: send.pool, agent});
  }
  if (e.kind === 'admin.message.send') {
    agents.push({pool: e.detail.pool, agent: 'admin'});
    if (e.detail.target !== 'global') agents.push({pool: e.detail.pool, agent: e.detail.target});
  }
  for (const m of eventMessages(e)) {
    agents.push({pool: m.pool, agent: m.from}, {pool: m.pool, agent: m.to});
  }
  for (const receipt of deliveryReceipts(e)) {
    agents.push({pool: receipt.pool, agent: receipt.to});
  }
  return agents;
}

// Correlated events are lookup context, never an additional history page. A
// selected finish can borrow its off-page start for legacy message projection;
// a selected start borrows an off-page finish only for status and identities.
export function pageEventContext(events, correlatedEvents = []) {
  const selected = normalizeEvents(events), correlated = normalizeEvents(correlatedEvents);
  const selectedIds = new Set(selected.map(e => e.id));
  const selectedStarts = new Set(selected.filter(e => e.kind === 'tool.start').map(e => e.id));
  const selectedFinishes = new Set(selected.map(eventStartId).filter(id => id != null));
  const includedIds = new Set(selectedIds), counterparts = [];
  for (const e of correlated) {
    if (includedIds.has(e.id)) continue;
    if (e.kind === 'tool.start' && !selectedFinishes.has(e.id)) continue;
    if (['tool.finish', 'tool.error'].includes(e.kind) && !selectedStarts.has(eventStartId(e))) continue;
    if (!['tool.start', 'tool.finish', 'tool.error'].includes(e.kind)) continue;
    includedIds.add(e.id);
    counterparts.push(e);
  }
  const related = [...selected, ...counterparts];
  const starts = new Map(related.filter(e => e.kind === 'tool.start').map(e => [e.id, e]));
  const finishes = new Map(related.filter(e =>
    ['tool.finish', 'tool.error'].includes(e.kind) && eventStartId(e) != null)
    .map(e => [eventStartId(e), e]));
  const extraStarts = counterparts.filter(e => e.kind === 'tool.start' &&
    !selected.some(e => eventStartId(e) === e.id && messageSend(e)));
  return {starts, finishes, messageEvents: [...selected, ...extraStarts]};
}

export function hasPendingSends(events, correlatedEvents = []) {
  const selected = normalizeEvents(events);
  const {finishes} = pageEventContext(selected, correlatedEvents);
  return selected.some(e => e.kind === 'tool.start' && messageSend(e) &&
    !finishes.has(e.id) && messageSend(e)?.status !== 'queued' && messageSend(e)?.status !== 'failed');
}

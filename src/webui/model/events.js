// Versioned event fields are authoritative for newly written events. Persisted
// pre-v1 history retains only the loose detail payload, read below as a fallback.
const lifecycle = e => e?.schema_version === 1 ? e.lifecycle : null;

export const eventStartId = e => e?.schema_version === 1 ?
  lifecycle(e)?.start_event_id : e?.detail?.start_event_id;

export function messageSend(e) {
  if (!e) return null;
  if (e.schema_version === 1) {
    const value = lifecycle(e);
    if (value?.type !== 'message_send' || !value.message ||
      typeof value.message.body !== 'string') return null;
    const m = value.message;
    return {pool: m.pool, target: m.target, message: m.body, in_reply_to: m.in_reply_to,
      from: m.from, message_id: m.message_id, recipients: m.recipients,
      error: m.error, status: m.status};
  }
  if (e.kind !== 'tool.start' || e.detail?.tool !== 'pool_send' ||
    typeof e.detail.arguments?.message !== 'string' || e.detail.arguments.action) return null;
  return e.detail.arguments;
}

export function peerMessages(e) {
  if (e.schema_version === 1) return lifecycle(e)?.peer_messages || [];
  const result = e.detail?.result;
  return result?.peer_messages || (e.detail?.tool === 'pool_wait' ? result?.messages : null) || [];
}

export function deliveryReceipts(e) {
  return e.schema_version === 1 ? lifecycle(e)?.delivery_receipts || [] :
    e.detail?.result?.delivery_receipts || [];
}

export function eventPools(e, starts) {
  const pools = new Set((e.agents || []).map(a => a.pool));
  const start = starts.get(eventStartId(e));
  const names = e.kind === 'admin.message.send' ? [e.detail?.pool] : e.schema_version === 1 ?
    [lifecycle(e)?.pool, lifecycle(start)?.pool, messageSend(e)?.pool, messageSend(start)?.pool] :
    [e.detail?.pool, e.detail?.arguments?.pool, start?.detail?.arguments?.pool];
  for (const name of names) if (name) pools.add(name);
  for (const m of peerMessages(e)) pools.add(m.pool);
  for (const receipt of deliveryReceipts(e)) pools.add(receipt.pool);
  return pools;
}

export function isMessageEvent(e, starts) {
  if (e.kind === 'admin.message.send') return true;
  const start = e.kind === 'tool.start' ? e : starts.get(eventStartId(e));
  return Boolean(messageSend(e) || messageSend(start) || peerMessages(e).length);
}

export function eventIdentities(e, starts, finishes) {
  const start = e.kind === 'tool.start' ? e : starts.get(eventStartId(e));
  const finish = e.kind === 'tool.start' ? finishes.get(e.id) : e;
  const agents = [...(e.agents || []), ...(start?.agents || []), ...(finish?.agents || [])];
  const send = messageSend(start) || messageSend(e) || messageSend(finish);
  const completed = messageSend(finish);
  const value = finish?.schema_version === 1 ? null : finish?.detail?.result?.value;
  if (send?.pool) {
    const from = completed?.from || send.from || value?.assigned_agent;
    if (from) agents.push({pool: send.pool, agent: from});
    if (send.target && send.target !== 'global') agents.push({pool: send.pool, agent: send.target});
    for (const agent of completed?.recipients || value?.recipients || []) {
      agents.push({pool: send.pool, agent});
    }
  }
  if (e.kind === 'admin.message.send') {
    agents.push({pool: e.detail.pool, agent: 'admin'});
    if (e.detail.target !== 'global') agents.push({pool: e.detail.pool, agent: e.detail.target});
  }
  for (const m of peerMessages(e)) {
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
  const selectedIds = new Set(events.map(e => e.id));
  const selectedStarts = new Set(events.filter(e => e.kind === 'tool.start').map(e => e.id));
  const selectedFinishes = new Set(events.map(eventStartId).filter(id => id != null));
  const includedIds = new Set(selectedIds), counterparts = [];
  for (const e of correlatedEvents || []) {
    if (includedIds.has(e.id)) continue;
    if (e.kind === 'tool.start' && !selectedFinishes.has(e.id)) continue;
    if (['tool.finish', 'tool.error'].includes(e.kind) && !selectedStarts.has(eventStartId(e))) continue;
    if (!['tool.start', 'tool.finish', 'tool.error'].includes(e.kind)) continue;
    includedIds.add(e.id);
    counterparts.push(e);
  }
  const related = [...events, ...counterparts];
  const starts = new Map(related.filter(e => e.kind === 'tool.start').map(e => [e.id, e]));
  const finishes = new Map(related.filter(e =>
    ['tool.finish', 'tool.error'].includes(e.kind) && eventStartId(e) != null)
    .map(e => [eventStartId(e), e]));
  const extraStarts = counterparts.filter(e => e.kind === 'tool.start' &&
    !events.some(selected => eventStartId(selected) === e.id && messageSend(selected)));
  return {starts, finishes, messageEvents: [...events, ...extraStarts]};
}

export function hasPendingSends(events, correlatedEvents = []) {
  const {finishes} = pageEventContext(events, correlatedEvents);
  return events.some(e => e.kind === 'tool.start' && messageSend(e) &&
    !finishes.has(e.id) && messageSend(e)?.status !== 'queued' && messageSend(e)?.status !== 'failed');
}

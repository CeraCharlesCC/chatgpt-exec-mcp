export function eventPools(e, starts) {
  const pools = new Set((e.agents || []).map(a => a.pool));
  for (const name of [e.detail?.pool, e.detail?.arguments?.pool,
    starts.get(e.detail?.start_event_id)?.detail?.arguments?.pool]) {
    if (name) pools.add(name);
  }
  for (const m of e.detail?.result?.peer_messages || []) pools.add(m.pool);
  return pools;
}
export function isMessageEvent(e, starts) {
  const start = e.kind === 'tool.start' ? e : starts.get(e.detail?.start_event_id);
  return e.kind === 'admin.message.send' ||
    (start?.detail?.tool === 'pool_send' && typeof start.detail.arguments?.message === 'string' && !start.detail.arguments.action) ||
    (e.detail?.result?.peer_messages || []).length > 0;
}
export function eventIdentities(e, starts, finishes) {
  const start = e.kind === 'tool.start' ? e : starts.get(e.detail?.start_event_id);
  const finish = e.kind === 'tool.start' ? finishes.get(e.id) : e;
  const agents = [...(e.agents || []), ...(start?.agents || []), ...(finish?.agents || [])];
  const a = start?.detail?.arguments, value = finish?.detail?.result?.value;
  if (a?.pool && value?.assigned_agent) agents.push({pool: a.pool, agent: value.assigned_agent});
  if (a?.pool && a.target && a.target !== 'global') agents.push({pool: a.pool, agent: a.target});
  if (a?.pool) for (const agent of value?.recipients || []) agents.push({pool: a.pool, agent});
  if (e.kind === 'admin.message.send') {
    agents.push({pool: e.detail.pool, agent: 'admin'});
    if (e.detail.target !== 'global') agents.push({pool: e.detail.pool, agent: e.detail.target});
  }
  for (const m of e.detail?.result?.peer_messages || []) {
    agents.push({pool: m.pool, agent: m.from}, {pool: m.pool, agent: m.to});
  }
  return agents;
}

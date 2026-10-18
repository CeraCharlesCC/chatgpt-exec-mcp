export function messages(events, finishes, pools, adminMessages) {
  const rows = [], received = new Set();
  const identities = (pools || []).flatMap(p => p.agents);
  function receive(m, timestamp_ms, session, recipient) {
    const to = recipient || m.to;
    const key = JSON.stringify([m.pool, m.message_id, session || to]);
    if (received.has(key)) return;
    received.add(key);
    rows.push({...m, to, key: 'receive:' + key, timestamp_ms, kind: 'message.receive',
      status: to === 'admin' ? 'Received' : 'Offered in tool response',
      agents: [m.from, to], sessions: session ? [session] : []});
  }
  for (const e of events) {
    const d = e.detail || {};
    if (e.kind === 'tool.start' && d.tool === 'pool_send' && typeof d.arguments?.message === 'string' && !d.arguments.action) {
      const a = d.arguments, finish = finishes.get(e.id), result = finish?.detail?.result;
      const from = result?.value?.assigned_agent ||
        [...(finish?.agents || []), ...(e.agents || [])].find(x => x.pool === a.pool)?.agent ||
        identities.find(x => x.pool === a.pool && x.session === e.session)?.agent || 'Agent';
      const failed = finish?.kind === 'tool.error';
      const agents = [from, a.target, ...(result?.value?.recipients || [])];
      rows.push({...a, key: 'send:' + e.id, from, to: a.target, timestamp_ms: e.timestamp_ms,
        kind: 'message.send', level: failed ? 'error' : 'info', message_id: result?.value?.message_id,
        agents, sessions: e.session ? [e.session] : [], recipient_count: (result?.value?.recipients || []).length,
        status: !finish ? 'Sending…' : failed ? `Failed: ${result?.error || 'Send failed'}` :
          `Queued for ${(result?.value?.recipients || []).length} recipient(s)`});
    }
    if (e.kind === 'admin.message.send') {
      const agents = ['admin', d.target, ...(e.agents || []).filter(a => a.pool === d.pool).map(a => a.agent)];
      rows.push({...d, key: 'admin:' + e.id, from: 'admin', to: d.target,
        timestamp_ms: e.timestamp_ms, kind: 'message.send', agents, sessions: [], recipient_count: d.delivery_count,
        status: `Queued for ${d.delivery_count} recipient(s)`});
    }
    for (const m of d.result?.peer_messages || []) {
      // Older snapshots use to=global. Event identities retain the recipient
      // even after that member has left or its name has been reused.
      const recipient = m.to === 'global' ? (e.agents || []).find(a => a.pool === m.pool)?.agent : m.to;
      receive(m, e.timestamp_ms, e.session, recipient);
    }
  }
  for (const m of adminMessages || []) receive({...m, to: 'admin'}, m.created_ms);
  // Keep sends before receipts at the same timestamp in chronological order.
  return coalesceMessages(rows.sort((a, b) => a.timestamp_ms - b.timestamp_ms ||
    Number(a.kind === 'message.receive') - Number(b.kind === 'message.receive'))).reverse();
}
export function coalesceMessages(rows) {
  const result = [], merged = new Map();
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    let send;
    if (row.kind === 'message.receive' && row.message_id) {
      // Measure the gap before folding rows, so repeated deliveries cannot
      // keep an old send eligible indefinitely. Five intervening rows stay separate.
      for (let j = i - 1; j >= Math.max(0, i - 5); j--) {
        const candidate = rows[j];
        if (candidate.kind === 'message.send' && candidate.level !== 'error' &&
          candidate.pool === row.pool && candidate.message_id === row.message_id && candidate.from === row.from) {
          send = candidate;
          break;
        }
      }
    }
    if (!send) {
      result.push({...row});
      continue;
    }
    let message = merged.get(send.key);
    if (!message) {
      message = result.find(m => m.key === send.key);
      message.kind = 'message.receive';
      message.deliveries = [];
      merged.set(send.key, message);
    }
    message.deliveries.push(row);
    message.agents = [...new Set([...message.agents, ...row.agents])];
    message.sessions = [...new Set([...message.sessions, ...row.sessions])];
    const notes = message.deliveries.map(m => {
      const sameName = message.deliveries.filter(d => d.to === m.to).length > 1;
      const recipient = m.to + (sameName && m.sessions.length ? ` (${m.sessions.join(', ')})` : '');
      return m.to === 'admin' ? 'Received' : `${m.status} to ${recipient}`;
    });
    const pending = send.recipient_count - new Set(message.deliveries.map(m => m.to)).size;
    if (pending > 0) notes.push(`Queued for ${pending} recipient(s)`);
    message.status = notes.join(' · ');
  }
  return result;
}

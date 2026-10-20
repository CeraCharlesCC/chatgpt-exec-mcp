import { eventStartId, messageSend, peerMessages } from './events.js';

export function messages(events, finishes, pools, adminMessages, starts = new Map()) {
  const rows = [], received = new Set();
  const identities = (pools || []).flatMap(p => p.agents);
  const selectedStarts = new Set(events.filter(e => e.kind === 'tool.start' && messageSend(e)).map(e => e.id));
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
    const send = messageSend(e);
    // A v1 finish is self-contained and may be the only selected page event.
    // If its start is on the same page, render that single stable send row.
    if (send && (e.kind === 'tool.start' ||
      (e.schema_version === 1 && ['tool.finish', 'tool.error'].includes(e.kind) &&
        !selectedStarts.has(eventStartId(e))))) {
      const startId = e.kind === 'tool.start' ? e.id : eventStartId(e) ?? e.id;
      const origin = e.kind === 'tool.start' ? e : starts.get(startId) || e;
      const finish = e.kind === 'tool.start' ? finishes.get(e.id) : e;
      const completed = messageSend(finish);
      const result = finish?.schema_version === 1 ? null : finish?.detail?.result;
      const payload = completed || send;
      const pool = send.pool || payload.pool;
      const recipients = completed?.recipients ?? result?.value?.recipients ?? [];
      const from = completed?.from || send.from || result?.value?.assigned_agent ||
        [...(finish?.agents || []), ...(e.agents || [])].find(x => x.pool === pool)?.agent ||
        identities.find(x => x.pool === pool && x.session === origin.session)?.agent || 'Agent';
      const status = completed?.status || (finish?.kind === 'tool.error' ? 'failed' : finish ? 'queued' : 'sending');
      const failed = status === 'failed';
      const agents = [from, send.target, ...recipients];
      rows.push({...send, pool, key: 'send:' + startId, from, to: send.target,
        timestamp_ms: origin.timestamp_ms, kind: 'message.send', level: failed ? 'error' : 'info',
        message_id: completed?.message_id ?? result?.value?.message_id,
        agents, sessions: [...new Set([origin.session, e.session].filter(Boolean))],
        recipient_count: recipients.length,
        status: status === 'sending' ? 'Sending…' : failed ?
          `Failed: ${completed?.error || result?.error || 'Send failed'}` :
          `Queued for ${recipients.length} recipient(s)`});
    }
    if (e.kind === 'admin.message.send') {
      const agents = ['admin', d.target, ...(e.agents || []).filter(a => a.pool === d.pool).map(a => a.agent)];
      rows.push({...d, key: 'admin:' + e.id, from: 'admin', to: d.target,
        timestamp_ms: e.timestamp_ms, kind: 'message.send', agents, sessions: [], recipient_count: d.delivery_count,
        status: `Queued for ${d.delivery_count} recipient(s)`});
    }
    for (const m of peerMessages(e)) {
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

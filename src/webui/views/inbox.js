import { $, esc, interacting, syncList, time } from '../dom.js';

export function renderInbox(state) {
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

export function bindInbox(ackMessage) {
  $('#inbox').onclick = event => {
    const button = event.target.closest('button.ack');
    if (button && !button.disabled) ackMessage(button.dataset.messageId);
  };
}

import { $, clearSelection, setPointerTarget, clearPointerTarget, hasPointerTarget } from './dom.js';
import { state, attachPaginationState } from './state.js';
import { fetchSnapshot, setCsrfToken, sendAdminMessage, terminateAgent, ackAdminMessage, clearAdminHistory } from './api.js';
import { createActivityController } from './controllers/activity.js';
import { createInboxController } from './controllers/inbox.js';
import { toast, setCover, connection, bindMenu } from './views/menu.js';
import { renderEvents, bindActivity } from './views/activity.js';
import { renderPools, bindPools } from './views/pools.js';
import { renderInbox, bindInbox } from './views/inbox.js';
const activity = createActivityController();
const inbox = createInboxController();
attachPaginationState(activity, inbox);

async function clearHistory() {
  if (state.clearing || $('#clearCover').getAttribute('aria-expanded') !== 'true') return;
  state.clearing = true;
  // Discard any snapshot requested before the clear, and pause polling until it finishes.
  inbox.invalidate();
  // A decompression request may still be in flight when CLEAR completes.
  activity.invalidate();
  $('#clearCover').disabled = true;
  $('#clearControl').classList.add('pressing');
  setCover(false, state);
  setTimeout(() => $('#clearControl').classList.remove('pressing'), 180);
  try {
    const r = await clearAdminHistory();
    if (!r.ok) throw new Error(await r.text());
    if (state.data) {
      state.data.events = [];
      state.data.correlated_events = [];
      state.data.admin_messages = [];
      state.data.admin_messages_next_cursor = null;
    }
    inbox.reset();
    activity.reset();
    clearSelection();
    render();
    $('#clearHint').textContent = 'History cleared.';
    toast('Activity, MCP log and admin inbox cleared');
  } catch (e) {
    // A failed clear still invalidated in-flight page loads; reload the latest
    // activity on the next snapshot rather than retaining a loading indicator.
    activity.reset();
    $('#clearHint').textContent = 'Clear failed. Lift the cover to retry.';
    toast(e.message, true);
  } finally {
    state.clearing = false;
    $('#clearCover').disabled = false;
    await refresh();
  }
}

function render() {
  renderPools(state);
  renderInbox(state);
  renderEvents(state);
}
async function terminate(pool, agent) {
  if (!confirm(`Terminate ${agent} from ${pool}?`)) return;
  try {
    const r = await terminateAgent(pool, agent);
    if (!r.ok) throw new Error(await r.text());
    toast('Membership terminated');
    await refresh();
  } catch (e) { toast(e.message, true); }
}
async function send() {
  const pool = $('#poolSelect').value, target = $('#targetSelect').value, message = $('#message').value;
  if (!pool || !target || !message.trim()) return toast('Pool, target and message are required', true);
  try {
    const r = await sendAdminMessage(pool, target, message);
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
  renderInbox(state);
  try {
    const r = await ackAdminMessage(messageId);
    if (!r.ok) throw new Error(await r.text());
    if (!(await r.json()).acknowledged) throw new Error('Message is no longer available');
    const message = state.data?.admin_messages.find(m => m.message_id === messageId);
    if (message) message.acknowledged = true;
    toast('ACKed; retained until clear or restart');
  } catch (e) { toast(e.message, true); }
  finally { state.acking.delete(messageId); renderInbox(state); }
}

function changePage(direction) {
  if (state.clearing || !inbox.navigate(direction, state.data)) return;
  clearSelection();
  renderInbox(state);
  if (inbox.refreshing) inbox.requestRefresh();
  else void refresh();
}

async function changeActivityPage(direction) {
  if (state.clearing) return;
  await activity.navigate(direction, () => state.data, {
    invalidateSnapshot: () => inbox.invalidate(),
    requestSnapshot: () => inbox.refreshing ? inbox.requestRefresh() : void refresh(),
    onChange: () => { clearSelection(); renderEvents(state); },
    onError: error => toast(error.message, true),
    isClearing: () => state.clearing,
  });
}
async function refresh() {
  // Never let a slow poll overlap another request and overwrite newer data.
  if (state.clearing || inbox.refreshing) return;
  const epoch = inbox.beginRefresh();
  try {
    const r = await fetchSnapshot(inbox.cursor);
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    if (!inbox.isCurrent(epoch)) return;
    activity.acceptSnapshot(data, state.data);
    setCsrfToken(data.csrf_token);
    state.data = data;
    inbox.loaded();
    connection(true);
    $('#status').textContent = `v${state.data.version} · up ${Math.floor(state.data.uptime_seconds / 60)}m`;
    render();
    await activity.reconcile(state.data, {
      isSnapshotCurrent: () => inbox.isCurrent(epoch),
      isClearing: () => state.clearing,
      onChange: () => renderEvents(state),
    });
  } catch (e) {
    if (!inbox.isCurrent(epoch)) return;
    inbox.failed();
    activity.failedSnapshot();
    connection(false);
    $('#status').textContent = 'disconnected';
    toast(e.message, true);
  } finally {
    if (inbox.endRefresh(state.clearing)) void refresh();
  }
}
bindMenu({state, clearHistory});
bindPools({state, terminate, selectPool});
bindInbox({ackMessage, changePage});
bindActivity({state, render, selectPool, toast, changeActivityPage});
$('#send').onclick = send;
function selectPool(pool) {
  clearSelection();
  state.pool = pool;
  state.agent = '';
  state.session = '';
  render();
}
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

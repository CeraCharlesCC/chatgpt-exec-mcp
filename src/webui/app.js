import { $, clearSelection, setPointerTarget, clearPointerTarget, hasPointerTarget } from './dom.js';
import { state } from './state.js';
import { fetchSnapshot, fetchEvents, setCsrfToken, sendAdminMessage, terminateAgent, ackAdminMessage, clearAdminHistory } from './api.js';
import { toast, setCover, connection, bindMenu } from './views/menu.js';
import { renderEvents, bindActivity } from './views/activity.js';
import { renderPools, bindPools } from './views/pools.js';
import { renderInbox, bindInbox } from './views/inbox.js';
import { hasPendingSends } from './model/events.js';

const correlatedEvents = data => Array.isArray(data?.correlated_events) ? data.correlated_events.slice(0, 200) : [];

async function clearHistory() {
  if (state.clearing || $('#clearCover').getAttribute('aria-expanded') !== 'true') return;
  state.clearing = true;
  // Discard any snapshot requested before the clear, and pause polling until it finishes.
  state.refreshEpoch++;
  // A decompression request may still be in flight when CLEAR completes.
  state.activityEpoch++;
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
    state.inboxCursors = [null];
    state.inboxPageIndex = 0;
    state.inboxLoading = false;
    state.activityCursors = [null];
    state.activityPageIndex = 0;
    state.activityNextBeforeId = null;
    state.activityLoading = false;
    state.refreshPending = false;
    clearSelection();
    render();
    $('#clearHint').textContent = 'History cleared.';
    toast('Activity, MCP log and admin inbox cleared');
  } catch (e) {
    // A failed clear still invalidated in-flight page loads; reload the latest
    // activity on the next snapshot rather than retaining a loading indicator.
    state.activityCursors = [null];
    state.activityPageIndex = 0;
    state.activityNextBeforeId = null;
    state.activityLoading = false;
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
  if (state.clearing || state.inboxLoading || !state.data) return;
  if (direction === 1) {
    const cursor = state.data.admin_messages_next_cursor;
    if (!cursor) return;
    state.inboxCursors = state.inboxCursors.slice(0, state.inboxPageIndex + 1);
    state.inboxCursors.push(cursor);
    state.inboxPageIndex++;
  } else if (direction === -1 && state.inboxPageIndex > 0) {
    state.inboxPageIndex--;
  } else return;
  state.refreshEpoch++;
  state.inboxLoading = true;
  state.data.admin_messages = [];
  state.data.admin_messages_next_cursor = null;
  clearSelection();
  renderInbox(state);
  if (state.refreshing) state.refreshPending = true;
  else void refresh();
}

async function changeActivityPage(direction) {
  if (state.clearing || state.activityLoading || !state.data) return;
  const previous = {
    cursors: state.activityCursors,
    index: state.activityPageIndex,
    next: state.activityNextBeforeId,
    events: state.data.events,
    correlated: state.data.correlated_events,
  };
  if (direction === 1 && state.activityNextBeforeId !== null) {
    state.activityCursors = state.activityCursors.slice(0, state.activityPageIndex + 1);
    state.activityCursors.push(state.activityNextBeforeId);
    state.activityPageIndex++;
  } else if (direction === -1 && state.activityPageIndex > 0) {
    state.activityPageIndex--;
  } else return;

  // Keep just the selected page in browser state, even while loading.
  state.data.events = [];
  state.data.correlated_events = [];
  state.activityNextBeforeId = null;
  state.activityLoading = true;
  const epoch = ++state.activityEpoch;
  // A snapshot from before this click must not paint over the new page.
  state.refreshEpoch++;
  clearSelection();
  renderEvents(state);
  if (state.activityPageIndex === 0) {
    // The current page comes from the lightweight snapshot endpoint.
    if (state.refreshing) state.refreshPending = true;
    else void refresh();
    return;
  }

  try {
    const response = await fetchEvents(state.activityCursors[state.activityPageIndex]);
    if (!response.ok) throw new Error(await response.text());
    const page = await response.json();
    if (epoch !== state.activityEpoch || state.clearing) return;
    if (!Array.isArray(page.events)) throw new Error('Invalid activity page response');
    state.data.events = page.events.slice(-200);
    state.data.correlated_events = correlatedEvents(page);
    state.activityNextBeforeId = page.has_more ? page.next_before_id ?? null : null;
    state.activityLoading = false;
    renderEvents(state);
  } catch (error) {
    if (epoch !== state.activityEpoch || state.clearing) return;
    state.activityCursors = previous.cursors;
    state.activityPageIndex = previous.index;
    state.activityNextBeforeId = previous.next;
    state.data.events = previous.events;
    state.data.correlated_events = previous.correlated;
    state.activityLoading = false;
    renderEvents(state);
    toast(error.message, true);
  }
}

async function reconcileArchivedActivity(refreshEpoch) {
  // A delayed completion can arrive while a reader stays on an older page.
  // Re-fetch only that page while it contains an unresolved send; this is one
  // bounded lookup, never a download of unrelated history.
  if (state.activityPageIndex === 0 || state.activityLoading ||
    !hasPendingSends(state.data?.events || [], state.data?.correlated_events)) return;
  const index = state.activityPageIndex, activityEpoch = state.activityEpoch;
  const cursor = state.activityCursors[index];
  if (cursor == null) return;
  try {
    const response = await fetchEvents(cursor);
    if (!response.ok) return;
    const page = await response.json();
    if (refreshEpoch !== state.refreshEpoch || activityEpoch !== state.activityEpoch ||
      state.clearing || state.activityPageIndex !== index || !Array.isArray(page.events)) return;
    state.data.events = page.events.slice(-200);
    state.data.correlated_events = correlatedEvents(page);
    state.activityNextBeforeId = page.has_more ? page.next_before_id ?? null : null;
    renderEvents(state);
  } catch {
    // Transient archival lookup failures leave the current page intact; retry
    // on a later snapshot without clearing the reader's position or scroll.
  }
}
async function refresh() {
  // Never let a slow poll overlap another request and overwrite newer data.
  if (state.refreshing || state.clearing) return;
  state.refreshing = true;
  const epoch = state.refreshEpoch;
  try {
    const r = await fetchSnapshot(state.inboxCursors[state.inboxPageIndex]);
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    if (epoch !== state.refreshEpoch) return;
    if (state.activityPageIndex === 0) {
      data.events = (data.events || []).slice(-200);
      data.correlated_events = correlatedEvents(data);
      const hasMore = data.events_has_more ?? data.events.length === 200;
      state.activityNextBeforeId = hasMore ? data.events_next_before_id ?? data.events[0]?.id ?? null : null;
      state.activityLoading = false;
    } else {
      // Poll live pool/inbox data without replacing the selected archive page.
      data.events = state.data?.events || [];
      data.correlated_events = state.data?.correlated_events || [];
    }
    setCsrfToken(data.csrf_token);
    state.data = data;
    state.inboxLoading = false;
    connection(true);
    $('#status').textContent = `v${state.data.version} · up ${Math.floor(state.data.uptime_seconds / 60)}m`;
    render();
    await reconcileArchivedActivity(epoch);
  } catch (e) {
    if (epoch !== state.refreshEpoch) return;
    state.inboxLoading = false;
    if (state.activityPageIndex === 0) state.activityLoading = false;
    connection(false);
    $('#status').textContent = 'disconnected';
    toast(e.message, true);
  } finally {
    state.refreshing = false;
    if (state.refreshPending && !state.clearing) {
      state.refreshPending = false;
      void refresh();
    }
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

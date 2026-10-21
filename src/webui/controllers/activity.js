import { fetchEvents } from '../api.js';
import { normalizeEvents, hasPendingSends } from '../model/events.js';

const correlatedEvents = page => normalizeEvents(Array.isArray(page?.correlated_events) ?
  page.correlated_events.slice(0, 200) : []);

function normalizePage(page) {
  if (!Array.isArray(page.events)) throw new Error('Invalid activity page response');
  return {
    events: normalizeEvents(page.events.slice(-200)),
    correlated: correlatedEvents(page),
    next: page.has_more ? page.next_before_id ?? null : null,
  };
}

// Activity page requests have their own invalidation generation, independent
// from snapshot/inbox requests. History clear invalidates both controllers.
export function createActivityController() {
  let cursors = [null], pageIndex = 0, nextBeforeId = null, loading = false, epoch = 0;
  return {
    get cursors() { return cursors; },
    get pageIndex() { return pageIndex; },
    get nextBeforeId() { return nextBeforeId; },
    get loading() { return loading; },
    get epoch() { return epoch; },
    invalidate() { ++epoch; },
    reset() {
      ++epoch;
      cursors = [null];
      pageIndex = 0;
      nextBeforeId = null;
      loading = false;
    },
    acceptSnapshot(data, previousData) {
      if (pageIndex === 0) {
        data.events = normalizeEvents((data.events || []).slice(-200));
        data.correlated_events = correlatedEvents(data);
        const hasMore = data.events_has_more ?? data.events.length === 200;
        nextBeforeId = hasMore ? data.events_next_before_id ?? data.events[0]?.id ?? null : null;
        loading = false;
      } else {
        // Poll current pool/inbox data without replacing the selected archive.
        data.events = previousData?.events || [];
        data.correlated_events = previousData?.correlated_events || [];
      }
    },
    failedSnapshot() { if (pageIndex === 0) loading = false; },
    async navigate(direction, getData, {invalidateSnapshot, requestSnapshot, onChange, onError, isClearing}) {
      const data = getData();
      if (loading || !data) return;
      const previous = {
        cursors, index: pageIndex, next: nextBeforeId,
        events: data.events, correlated: data.correlated_events,
      };
      if (direction === 1 && nextBeforeId !== null) {
        cursors = [...cursors.slice(0, pageIndex + 1), nextBeforeId];
        ++pageIndex;
      } else if (direction === -1 && pageIndex > 0) {
        --pageIndex;
      } else return;
      data.events = [];
      data.correlated_events = [];
      nextBeforeId = null;
      loading = true;
      const requestEpoch = ++epoch;
      invalidateSnapshot();
      onChange();
      if (pageIndex === 0) {
        requestSnapshot();
        return;
      }
      try {
        const response = await fetchEvents(cursors[pageIndex]);
        if (!response.ok) throw new Error(await response.text());
        const page = await response.json();
        if (requestEpoch !== epoch || isClearing()) return;
        const normalized = normalizePage(page);
        // Polling can replace the snapshot while this page request is pending.
        const currentData = getData();
        currentData.events = normalized.events;
        currentData.correlated_events = normalized.correlated;
        nextBeforeId = normalized.next;
        loading = false;
        onChange();
      } catch (error) {
        if (requestEpoch !== epoch || isClearing()) return;
        cursors = previous.cursors;
        pageIndex = previous.index;
        nextBeforeId = previous.next;
        const currentData = getData();
        currentData.events = previous.events;
        currentData.correlated_events = previous.correlated;
        loading = false;
        onChange();
        onError(error);
      }
    },
    async reconcile(data, {isSnapshotCurrent, isClearing, onChange}) {
      // One bounded lookup for a selected archive page with an unresolved send.
      // Do not project off-page events into the selected history page.
      if (pageIndex === 0 || loading ||
        !hasPendingSends(data?.events || [], data?.correlated_events)) return;
      const index = pageIndex, requestEpoch = epoch, cursor = cursors[index];
      if (cursor == null) return;
      try {
        const response = await fetchEvents(cursor);
        if (!response.ok) return;
        const page = await response.json();
        if (!isSnapshotCurrent() || requestEpoch !== epoch || isClearing() ||
          pageIndex !== index || !Array.isArray(page.events)) return;
        const normalized = normalizePage(page);
        data.events = normalized.events;
        data.correlated_events = normalized.correlated;
        nextBeforeId = normalized.next;
        onChange();
      } catch {
        // Retry transient archive failures during a later snapshot.
      }
    },
  };
}

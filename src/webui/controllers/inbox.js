// Snapshot transport and inbox pagination share one request generation: a poll
// started for one cursor must never publish into a later inbox page.
export function createInboxController() {
  let cursors = [null], pageIndex = 0, loading = false;
  let refreshEpoch = 0, refreshing = false, refreshPending = false;
  return {
    get cursors() { return cursors; },
    get pageIndex() { return pageIndex; },
    get loading() { return loading; },
    get refreshing() { return refreshing; },
    get refreshPending() { return refreshPending; },
    get refreshEpoch() { return refreshEpoch; },
    get cursor() { return cursors[pageIndex]; },
    invalidate() { ++refreshEpoch; },
    reset() {
      ++refreshEpoch;
      cursors = [null];
      pageIndex = 0;
      loading = false;
      refreshPending = false;
    },
    navigate(direction, data) {
      if (loading || !data) return false;
      if (direction === 1 && data.admin_messages_next_cursor) {
        cursors = [...cursors.slice(0, pageIndex + 1), data.admin_messages_next_cursor];
        ++pageIndex;
      } else if (direction === -1 && pageIndex > 0) {
        --pageIndex;
      } else return false;
      ++refreshEpoch;
      loading = true;
      data.admin_messages = [];
      data.admin_messages_next_cursor = null;
      return true;
    },
    beginRefresh() {
      if (refreshing) return null;
      refreshing = true;
      return refreshEpoch;
    },
    isCurrent(epoch) { return refreshEpoch === epoch; },
    requestRefresh() { refreshPending = true; },
    loaded() { loading = false; },
    failed() { loading = false; },
    endRefresh(clearing) {
      refreshing = false;
      if (refreshPending && !clearing) {
        refreshPending = false;
        return true;
      }
      return false;
    },
  };
}

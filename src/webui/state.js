export const state = {data: null, view: 'activity', filter: 'all', pool: '', agent: '', session: '', format: true, group: false, clearing: false, acking: new Set()};

// Keep the existing read-only view/test surface while request and pagination
// state lives exclusively in its respective controller.
export function attachPaginationState(activity, inbox) {
  const selectors = {
    inboxCursors: () => inbox.cursors, inboxPageIndex: () => inbox.pageIndex,
    inboxLoading: () => inbox.loading, refreshing: () => inbox.refreshing,
    refreshPending: () => inbox.refreshPending, refreshEpoch: () => inbox.refreshEpoch,
    activityCursors: () => activity.cursors, activityPageIndex: () => activity.pageIndex,
    activityNextBeforeId: () => activity.nextBeforeId,
    activityLoading: () => activity.loading, activityEpoch: () => activity.epoch,
  };
  for (const [name, get] of Object.entries(selectors)) {
    Object.defineProperty(state, name, {get, enumerable: true});
  }
}

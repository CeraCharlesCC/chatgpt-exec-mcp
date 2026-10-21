const {test, before} = require('node:test');
const assert = require('node:assert/strict');

let createActivityController, createInboxController;
before(async () => {
  ({createActivityController} = await import('../../src/webui/controllers/activity.js'));
  ({createInboxController} = await import('../../src/webui/controllers/inbox.js'));
});

const event = (id, kind = 'tool.start', detail = {}) => ({id, kind, detail});
const pendingSend = () => event(10, 'tool.start', {
  tool: 'pool_send', arguments: {pool: 'alpha', target: 'Bob', message: 'hello'},
});
const sendFinish = () => event(11, 'tool.finish', {
  tool: 'pool_send', start_event_id: 10,
  result: {value: {message_id: 'sent', recipients: ['Bob']}},
});
const response = page => ({ok: true, json: async () => page});
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function activityHarness(fetchEvents) {
  const activity = createActivityController({fetchEvents});
  let data = {events: [event(201)], correlated_events: [],
    events_has_more: true, events_next_before_id: 201};
  activity.acceptSnapshot(data);
  const calls = {changes: 0, invalidations: 0, snapshots: 0, errors: []};
  const hooks = {
    invalidateSnapshot: () => ++calls.invalidations,
    requestSnapshot: () => ++calls.snapshots,
    onChange: () => ++calls.changes,
    onError: error => calls.errors.push(error),
    isClearing: () => false,
  };
  return {activity, hooks, calls, getData: () => data,
    setData: next => { data = next; },
    navigate: direction => activity.navigate(direction, () => data, hooks)};
}

test('activity snapshots bound and normalize transport events and choose history cursors', () => {
  const activity = createActivityController();
  const data = {events: Array.from({length: 250}, (_, i) => event(i + 1)),
    correlated_events: Array.from({length: 250}, (_, i) => event(i + 501))};
  activity.acceptSnapshot(data);
  assert.deepEqual(data.events.map(e => e.id), Array.from({length: 200}, (_, i) => i + 51));
  assert.equal(data.correlated_events.length, 200);
  assert.ok(data.events.every(e => e.semantic));
  assert.ok(data.correlated_events.every(e => e.semantic));
  assert.equal(activity.nextBeforeId, 51, 'legacy snapshots fall back to the oldest retained ID');
  activity.acceptSnapshot({events: [event(10)], events_has_more: true, events_next_before_id: 7});
  assert.equal(activity.nextBeforeId, 7);
  activity.acceptSnapshot({events: [event(10)], events_has_more: false, events_next_before_id: 7});
  assert.equal(activity.nextBeforeId, null, 'an exhausted snapshot ignores a supplied cursor');
});

test('activity navigation commits into the latest snapshot while polling retains selected history', async () => {
  const request = deferred();
  const seen = [];
  const h = activityHarness(cursor => { seen.push(cursor); return request.promise; });
  const navigation = h.navigate(1);
  assert.equal(h.activity.loading, true);
  assert.equal(h.activity.pageIndex, 1);
  assert.deepEqual(h.getData().events, []);
  assert.equal(h.calls.invalidations, 1);
  const poll = {events: [event(999)], correlated_events: [event(998)], pools: ['updated pool']};
  h.activity.acceptSnapshot(poll, h.getData());
  h.setData(poll);
  assert.deepEqual(poll.events, []);
  request.resolve(response({events: [event(100)], correlated_events: [event(101)],
    has_more: true, next_before_id: 100}));
  await navigation;
  assert.deepEqual(seen, [201]);
  assert.deepEqual(poll.events.map(e => e.id), [100]);
  assert.ok(poll.events[0].semantic);
  assert.deepEqual(poll.correlated_events.map(e => e.id), [101]);
  assert.deepEqual(poll.pools, ['updated pool']);
  assert.equal(h.activity.loading, false);
  assert.equal(h.activity.nextBeforeId, 100);
  const nextPoll = {events: [event(1000)], correlated_events: []};
  h.activity.acceptSnapshot(nextPoll, poll);
  assert.equal(nextPoll.events, poll.events);
  assert.equal(nextPoll.correlated_events, poll.correlated_events);
});

test('activity navigation guards duplicate loads and requests a fresh snapshot on returning to page one', async () => {
  const request = deferred();
  let requests = 0;
  const h = activityHarness(() => { ++requests; return request.promise; });
  await h.navigate(-1);
  assert.equal(h.calls.changes, 0);
  const navigation = h.navigate(1);
  await h.navigate(1);
  assert.equal(requests, 1);
  request.resolve(response({events: [event(100)], has_more: false}));
  await navigation;
  await h.navigate(1);
  assert.equal(h.activity.pageIndex, 1);
  await h.navigate(-1);
  assert.equal(h.activity.pageIndex, 0);
  assert.equal(h.activity.loading, true);
  assert.equal(h.calls.snapshots, 1);
  assert.equal(requests, 1, 'the recent page uses snapshot polling');
  h.activity.failedSnapshot();
  assert.equal(h.activity.loading, false);
});

for (const failure of ['http', 'malformed', 'network']) {
  test(`activity ${failure} failures roll navigation back without losing the selected page`, async () => {
    const h = activityHarness(async () => {
      if (failure === 'network') throw new Error('offline');
      if (failure === 'http') return {ok: false, text: async () => 'server unavailable'};
      return response({events: null});
    });
    const previous = h.getData().events;
    await h.navigate(1);
    assert.equal(h.activity.pageIndex, 0);
    assert.deepEqual(h.activity.cursors, [null]);
    assert.equal(h.activity.nextBeforeId, 201);
    assert.equal(h.activity.loading, false);
    assert.equal(h.getData().events, previous);
    assert.equal(h.calls.errors.length, 1);
    assert.match(h.calls.errors[0].message, /offline|server unavailable|Invalid activity/);
  });
}

for (const failure of [false, true]) {
  test(`history reset rejects a late activity ${failure ? 'failure' : 'response'}`, async () => {
    const request = deferred();
    const h = activityHarness(() => request.promise);
    const navigation = h.navigate(1);
    h.activity.reset();
    const cleared = {events: [], correlated_events: []};
    h.setData(cleared);
    if (failure) request.reject(new Error('obsolete failure'));
    else request.resolve(response({events: [event(1)], has_more: true, next_before_id: 1}));
    await navigation;
    assert.equal(h.activity.pageIndex, 0);
    assert.deepEqual(h.activity.cursors, [null]);
    assert.equal(h.activity.nextBeforeId, null);
    assert.equal(h.activity.loading, false);
    assert.deepEqual(cleared.events, []);
    assert.deepEqual(h.calls.errors, []);
    assert.equal(h.calls.changes, 1, 'only navigation before clear rendered');
  });
}

test('archive reconciliation enriches pending sends without changing the selected event IDs', async () => {
  const requests = [];
  const start = pendingSend(), finish = sendFinish();
  const h = activityHarness(async cursor => {
    requests.push(cursor);
    return response({events: [start], correlated_events: requests.length > 1 ? [finish] : []});
  });
  await h.navigate(1);
  const hooks = {isSnapshotCurrent: () => true, isClearing: () => false, onChange: () => {}};
  await h.activity.reconcile(h.getData(), hooks);
  assert.deepEqual(requests, [201, 201]);
  assert.deepEqual(h.getData().events.map(e => e.id), [10]);
  assert.deepEqual(h.getData().correlated_events.map(e => e.id), [11]);
  await h.activity.reconcile(h.getData(), hooks);
  assert.equal(requests.length, 2, 'a completed send does not request more correlation');
});

for (const stale of ['snapshot', 'activity', 'clear']) {
  test(`archive reconciliation discards a response invalidated by ${stale}`, async () => {
    const request = deferred();
    let count = 0;
    const h = activityHarness(() => ++count === 1 ?
      response({events: [pendingSend()]}) : request.promise);
    await h.navigate(1);
    let current = true, clearing = false, changes = 0;
    const data = h.getData();
    const reconcile = h.activity.reconcile(data, {isSnapshotCurrent: () => current,
      isClearing: () => clearing, onChange: () => ++changes});
    if (stale === 'snapshot') current = false;
    if (stale === 'activity') h.activity.invalidate();
    if (stale === 'clear') clearing = true;
    request.resolve(response({events: [pendingSend()], correlated_events: [sendFinish()]}));
    await reconcile;
    assert.deepEqual(data.correlated_events, []);
    assert.equal(changes, 0);
  });
}

test('transient archive reconciliation failure leaves the selected history available for retry', async () => {
  let requests = 0;
  const h = activityHarness(async () => {
    if (++requests === 2) throw new Error('temporary failure');
    return response({events: [pendingSend()], correlated_events: requests > 2 ? [sendFinish()] : []});
  });
  await h.navigate(1);
  const previous = h.getData().events;
  const hooks = {isSnapshotCurrent: () => true, isClearing: () => false, onChange: () => {}};
  await h.activity.reconcile(h.getData(), hooks);
  assert.equal(h.getData().events, previous);
  await h.activity.reconcile(h.getData(), hooks);
  assert.equal(requests, 3);
  assert.deepEqual(h.getData().correlated_events.map(e => e.id), [11]);
});

test('inbox navigation invalidates in-flight polls and blocks overlapping cursor transitions', () => {
  const inbox = createInboxController();
  const data = {admin_messages: [{message_id: 'new'}], admin_messages_next_cursor: 'older'};
  const epoch = inbox.beginRefresh();
  assert.equal(inbox.beginRefresh(), null);
  assert.equal(inbox.navigate(1, data), true);
  assert.equal(inbox.cursor, 'older');
  assert.equal(inbox.pageIndex, 1);
  assert.equal(inbox.isCurrent(epoch), false);
  assert.equal(inbox.loading, true);
  assert.deepEqual(data.admin_messages, []);
  assert.equal(data.admin_messages_next_cursor, null);
  assert.equal(inbox.navigate(-1, data), false);
  inbox.loaded();
  assert.equal(inbox.navigate(-1, data), true);
  assert.equal(inbox.cursor, null);
  inbox.failed();
  assert.equal(inbox.loading, false);
  assert.equal(inbox.navigate(-1, data), false);
});

test('inbox refresh requests coalesce and clearing defers the queued refresh', () => {
  const inbox = createInboxController();
  inbox.beginRefresh();
  inbox.requestRefresh();
  inbox.requestRefresh();
  assert.equal(inbox.endRefresh(true), false);
  assert.equal(inbox.refreshPending, true);
  assert.equal(inbox.refreshing, false);
  const epoch = inbox.beginRefresh();
  assert.equal(inbox.endRefresh(false), true);
  assert.equal(inbox.refreshPending, false);
  assert.equal(inbox.endRefresh(false), false);
  inbox.invalidate();
  assert.equal(inbox.isCurrent(epoch), false);
});

test('inbox reset invalidates an in-flight poll and clears cursor history and queued work', () => {
  const inbox = createInboxController();
  inbox.navigate(1, {admin_messages_next_cursor: 'old'});
  const epoch = inbox.beginRefresh();
  inbox.requestRefresh();
  inbox.reset();
  assert.equal(inbox.isCurrent(epoch), false);
  assert.equal(inbox.pageIndex, 0);
  assert.deepEqual(inbox.cursors, [null]);
  assert.equal(inbox.loading, false);
  assert.equal(inbox.refreshPending, false);
  assert.equal(inbox.beginRefresh(), null, 'the stale transport still owns its refresh slot');
  assert.equal(inbox.endRefresh(false), false);
  assert.equal(inbox.beginRefresh(), inbox.refreshEpoch);
});

test('inbox revisiting an older page uses the latest cursor and discards the abandoned branch', () => {
  const inbox = createInboxController();
  assert.equal(inbox.navigate(1, null), false);
  assert.equal(inbox.navigate(1, {}), false);
  for (const cursor of ['page-2', 'page-3']) {
    assert.equal(inbox.navigate(1, {admin_messages_next_cursor: cursor}), true);
    inbox.loaded();
  }
  assert.equal(inbox.navigate(-1, {}), true);
  inbox.loaded();
  assert.equal(inbox.navigate(1, {admin_messages_next_cursor: 'replacement-page-3'}), true);
  assert.deepEqual(inbox.cursors, [null, 'page-2', 'replacement-page-3']);
  assert.equal(inbox.cursor, 'replacement-page-3');
});

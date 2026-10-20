const {test, before, after} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {createServer} = require('node:http');
const {join} = require('node:path');
const {chromium} = require('playwright');

const html = readFileSync(join(__dirname, '../../src/webui.html'));
const assetDir = join(__dirname, '../../src/webui');
const assets = new Map([
  ['/_admin/assets/styles.css', ['styles.css', 'text/css; charset=utf-8']],
  ['/_admin/assets/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/state.js', ['state.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/api.js', ['api.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/dom.js', ['dom.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/model/events.js', ['model/events.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/model/messages.js', ['model/messages.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/views/activity.js', ['views/activity.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/views/pools.js', ['views/pools.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/views/inbox.js', ['views/inbox.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/views/menu.js', ['views/menu.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/views/tool-output.js', ['views/tool-output.js', 'text/javascript; charset=utf-8']],
  ['/_admin/assets/vendor/prism.js', ['vendor/prism.js', 'text/javascript; charset=utf-8']],
]);
let browser, server, url;
before(async () => {
  browser = await chromium.launch({
    ...(process.env.WEBUI_BROWSER ? {executablePath: process.env.WEBUI_BROWSER} : {}),
    args: ['--no-sandbox'],
  });
  server = createServer((req, res) => {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    if (req.method === 'GET' && pathname === '/') {
      res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
      return res.end(html);
    }
    const asset = req.method === 'GET' ? assets.get(pathname) : undefined;
    if (asset) {
      res.writeHead(200, {
        'Content-Type': asset[1],
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
      });
      return res.end(readFileSync(join(assetDir, asset[0])));
    }
    res.writeHead(404, {'Content-Type': 'text/plain; charset=utf-8'});
    res.end('Not Found');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  url = `http://127.0.0.1:${server.address().port}`;
});
after(async () => {
  await browser?.close();
  if (server) await new Promise(resolve => server.close(resolve));
});

function fixture() {
  const now = Date.now();
  const agent = (pool, name, session) => ({pool, agent: name, session, last_seen_ms: now, expires_ms: now + 60000});
  const identity = (pool, name) => ({pool, agent: name});
  const event = (id, kind, detail, agents = [], session = 'chat-alpha') => ({
    id, kind, detail, agents, session, level: kind === 'tool.error' ? 'error' : 'info', timestamp_ms: now + id,
  });
  const lines = Array.from({length: 100}, (_, n) => `line ${n} with selectable text`).join('\n');
  const peer = {pool: 'beta', message_id: 'received-1', from: 'Bob', to: 'Alice', message: 'beta incoming\nsecond line'};
  return {
    version: 'test', uptime_seconds: 0, csrf_token: 'browser-test-csrf-token',
    pools: [{name: 'alpha', agents: [agent('alpha', 'Alice', 'chat-alpha')]},
      {name: 'beta', agents: [agent('beta', 'Alice', 'chat-beta'), agent('beta', 'Bob', 'chat-bob')]}],
    admin_messages: [{pool: 'old-pool', message_id: 'inbox-1', from: 'Former agent', message: '<script>unsafe()</script>\ninbox message', created_ms: now, acknowledged: false}],
    admin_messages_next_cursor: null,
    events: [
      event(1, 'tool.start', {tool: 'exec_command', arguments: {cmd: lines}}, [identity('alpha', 'Alice')]),
      event(2, 'tool.finish', {tool: 'exec_command', start_event_id: 1, result: {output: lines}}, [identity('alpha', 'Alice')]),
      event(3, 'tool.start', {tool: 'pool_send', arguments: {pool: 'beta', target: 'Bob', message: 'first send <b>plain text</b>', in_reply_to: 'earlier'}}, [], 'chat-beta'),
      event(4, 'tool.finish', {tool: 'pool_send', start_event_id: 3, result: {value: {assigned_agent: 'Alice', message_id: 'sent-1', recipients: ['Bob']}}}, [identity('beta', 'Alice')], 'chat-beta'),
      event(5, 'tool.finish', {tool: 'exec_command', result: {peer_messages: [peer]}}, [identity('alpha', 'Alice'), identity('beta', 'Alice')], 'chat-beta'),
      event(6, 'tool.finish', {tool: 'exec_command', result: {peer_messages: [peer]}}, [identity('beta', 'Alice')], 'chat-beta'),
      event(7, 'tool.start', {tool: 'pool_send', arguments: {pool: 'old-pool', action: 'leave'}}, [], 'old-chat'),
      event(8, 'tool.finish', {tool: 'pool_send', start_event_id: 7, result: {value: {}}}, [], 'old-chat'),
      event(9, 'admin.message.send', {pool: 'beta', target: 'global', message: 'admin broadcast', message_id: 'admin-1', delivery_count: 2}, [identity('beta', 'Alice'), identity('beta', 'Bob')], null),
      event(10, 'tool.start', {tool: 'pool_send', arguments: {pool: 'beta', target: 'missing', message: 'failed message'}}, [identity('beta', 'Alice')], 'chat-beta'),
      event(11, 'tool.error', {tool: 'pool_send', start_event_id: 10, result: {error: 'unknown target'}}, [identity('beta', 'Alice')], 'chat-beta'),
    ],
  };
}
async function open(t, data = fixture(), viewport = {width: 1440, height: 900}) {
  const context = await browser.newContext({viewport});
  t.after(() => context.close());
  // Native ES modules do not expose lexical bindings on window. Install only
  // test-harness references to the real module exports, including on reloads.
  await context.addInitScript(() => {
    document.addEventListener('DOMContentLoaded', async () => {
      const {refresh, clearHistory, state} = await import('/_admin/assets/app.js');
      Object.assign(window, {refresh, clearHistory, state});
    }, {once: true});
  });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, [], 'no browser exceptions'));
  await page.route('**/_admin/api/snapshot*', route => route.fulfill({json: data}));
  await page.goto(url);
  await page.waitForFunction(() => document.querySelector('#status').textContent.includes('vtest'));
  await page.waitForFunction(() => typeof refresh === 'function');
  // Manual refreshes let tests cross age boundaries without timing-dependent sleeps.
  await page.evaluate(() => { window.testClock = Date.now(); Date.now = () => window.testClock; });
  return {page, data, poll: async () => {
    await page.waitForFunction(() => typeof refresh === 'function');
    await page.evaluate(async () => { window.testClock += 2000; await refresh(); });
  }};
}

test('polls preserve nested scroll, focus and working pool/send interactions', async t => {
  const {page, data, poll} = await open(t);
  await page.selectOption('#poolSelect', 'beta');
  await page.selectOption('#targetSelect', 'Bob');
  await page.fill('#message', 'draft survives polls');
  await page.evaluate(() => {
    for (const id of [1, 2]) document.querySelector(`[data-key="activity:event:${id}"] pre`).scrollTop = 180;
    document.querySelector('.danger').focus();
  });
  for (let n = 0; n < 3; n++) {
    data.pools[0].agents[0].last_seen_ms += 1500;
    data.pools[0].agents[0].expires_ms += 1500;
    await poll();
  }
  assert.deepEqual(await page.evaluate(() => ({
    scroll: [1, 2].map(id => document.querySelector(`[data-key="activity:event:${id}"] pre`).scrollTop),
    focusedAgent: document.activeElement.dataset.agent,
    focusedPool: document.activeElement.dataset.pool,
  })), {scroll: [180, 180], focusedAgent: 'Alice', focusedPool: 'alpha'});
  assert.match(await page.locator('#inbox').innerText(), /Former agent/);
  assert.match(await page.locator('#inbox').innerText(), /inbox message/);
  assert.equal(await page.inputValue('#poolSelect'), 'beta');
  assert.equal(await page.inputValue('#targetSelect'), 'Bob');
  assert.equal(await page.inputValue('#message'), 'draft survives polls');

  let sent;
  await page.route('**/_admin/api/send', route => {
    assert.equal(route.request().headers()['x-webui-csrf-token'], 'browser-test-csrf-token');
    assert.equal(route.request().headers()['content-type'], 'application/json');
    sent = route.request().postDataJSON();
    return route.fulfill({json: {recipients: ['Bob']}});
  });
  await page.click('#send');
  await page.waitForFunction(() => document.querySelector('#toast').textContent.includes('Queued for 1'));
  assert.deepEqual(sent, {pool: 'beta', target: 'Bob', message: 'draft survives polls'});
  await page.click('.pool-filter[data-pool="alpha"]');
  assert.equal(await page.inputValue('#activityPool'), 'alpha');
  assert.equal(await page.locator('.pool-filter[data-pool="alpha"]').getAttribute('aria-pressed'), 'true');
  await page.selectOption('#poolSelect', 'alpha');
  assert.deepEqual(await page.locator('#targetSelect option').evaluateAll(nodes => nodes.map(node => node.value)), ['global', 'Alice']);
});

test('admin ACK keeps the message visible across polls and browser reloads', async t => {
  const {page, data, poll} = await open(t);
  data.admin_messages.push({...data.admin_messages[0], message_id: 'inbox-2', message: 'still unread'});
  await poll();
  const card = page.locator('#inbox [data-key="inbox-1"]'), button = card.locator('.ack');
  let complete;
  const requests = [];
  await page.route('**/_admin/api/ack', async route => {
    assert.equal(route.request().method(), 'POST');
    assert.equal(route.request().headers()['x-webui-csrf-token'], 'browser-test-csrf-token');
    requests.push(route.request().postDataJSON());
    await new Promise(resolve => { complete = resolve; });
    data.admin_messages[0].acknowledged = true;
    await route.fulfill({json: {acknowledged: true}});
  });
  await button.click();
  await page.waitForFunction(() => document.querySelector('#inbox .ack').textContent === 'ACKing…');
  assert.equal(await button.isDisabled(), true);
  await poll();
  assert.equal(await button.isDisabled(), true, 'polls keep an in-flight ACK disabled');
  complete();
  await page.waitForFunction(() => document.querySelector('#inbox .ack').textContent === 'ACKed');
  assert.deepEqual(requests, [{message_id: 'inbox-1'}]);
  assert.equal(await button.isDisabled(), true);
  assert.match(await card.innerText(), /<script>unsafe\(\)<\/script>\ninbox message/);
  assert.equal(await page.locator('#inbox .message').count(), 2);
  assert.equal(await page.locator('#inbox [data-key="inbox-2"] .ack').isEnabled(), true);
  await poll();
  assert.equal(await button.innerText(), 'ACKed');
  await page.reload();
  await page.waitForFunction(() => document.querySelector('#inbox .ack')?.textContent === 'ACKed');
  assert.equal(await page.locator('#inbox .message').count(), 2);
  assert.equal(await button.isDisabled(), true);
  assert.match(await card.innerText(), /<script>unsafe\(\)<\/script>\ninbox message/);
});

test('admin inbox cursor navigation reaches older unread messages and preserves ACKs', async t => {
  const {page, data, poll} = await open(t);
  const cursor = '1000:msg_00000000-0000-4000-8000-000000000000';
  data.admin_messages_next_cursor = cursor;
  const older = {
    ...data, admin_messages_next_cursor: null,
    admin_messages: [{...data.admin_messages[0], message_id: 'inbox-old', message: 'old unread message'}],
  };
  await poll();
  assert.equal(await page.locator('#inboxOlder').isEnabled(), true);
  assert.equal(await page.locator('#inboxNewer').isDisabled(), true);

  const seenCursors = [];
  await page.route('**/_admin/api/snapshot*', route => {
    const requested = new URL(route.request().url()).searchParams.get('cursor');
    seenCursors.push(requested);
    return route.fulfill({json: requested === cursor ? older : data});
  });
  await page.locator('#inboxOlder').click();
  await page.waitForFunction(() => document.querySelector('#inbox')?.textContent.includes('old unread message'));
  assert.equal(await page.locator('#inboxPageLabel').innerText(), 'Page 2');
  assert.equal(await page.locator('#inboxNewer').isEnabled(), true);
  assert.equal(await page.locator('#inboxOlder').isDisabled(), true);
  assert.ok(seenCursors.includes(cursor));

  await page.route('**/_admin/api/ack', route => {
    assert.equal(route.request().postDataJSON().message_id, 'inbox-old');
    older.admin_messages[0].acknowledged = true;
    return route.fulfill({json: {acknowledged: true}});
  });
  await page.locator('#inbox .ack').click();
  await page.waitForFunction(() => document.querySelector('#inbox .ack')?.textContent === 'ACKed');
  await poll();

  await page.locator('#inboxNewer').click();
  await page.waitForFunction(() => document.querySelector('#inbox')?.textContent.includes('inbox message'));
  assert.equal(await page.locator('#inboxPageLabel').innerText(), 'Page 1');
  await page.locator('#inboxOlder').click();
  await page.waitForFunction(() => document.querySelector('#inbox')?.textContent.includes('old unread message'));
  assert.equal(await page.locator('#inbox .ack').innerText(), 'ACKed');
});

test('activity pages keep only 200 events and survive polling, filtering, and Older/Newer navigation', async t => {
  const data = fixture();
  const sample = data.events[0];
  const event = id => ({...sample, id, kind: 'custom.event', timestamp_ms: sample.timestamp_ms + id,
    agents: [], detail: {sequence: id}});
  data.events = Array.from({length: 200}, (_, i) => event(i + 401));
  data.events_has_more = true;
  data.events_next_before_id = 401;
  const {page, poll} = await open(t, data);
  const secondPage = Array.from({length: 200}, (_, i) => event(i + 201));
  secondPage[0] = {...sample, id: 201, timestamp_ms: sample.timestamp_ms + 201};
  secondPage[1] = {...fixture().events[1], id: 202, timestamp_ms: sample.timestamp_ms + 202,
    detail: {...fixture().events[1].detail, start_event_id: 201}};
  secondPage[2] = {...fixture().events[2], id: 203, timestamp_ms: sample.timestamp_ms + 203};
  // Keep this pagination fixture settled: unresolved archived sends are tested
  // separately and legitimately trigger a bounded correlation re-fetch.
  secondPage[3] = {...fixture().events[3], id: 204, timestamp_ms: sample.timestamp_ms + 204,
    detail: {...fixture().events[3].detail, start_event_id: 203}};
  const thirdPage = Array.from({length: 200}, (_, i) => event(i + 1));
  const requests = [];
  await page.route('**/_admin/api/events*', route => {
    const params = new URL(route.request().url()).searchParams;
    const cursor = Number(params.get('before_id'));
    requests.push({cursor, limit: params.get('limit')});
    if (cursor === 401) return route.fulfill({json: {events: secondPage, next_before_id: 201, has_more: true}});
    if (cursor === 201) return route.fulfill({json: {events: thirdPage, next_before_id: null, has_more: false}});
    throw new Error(`unexpected cursor ${cursor}`);
  });

  assert.equal(await page.locator('#events > [data-key]').count(), 200);
  assert.equal(await page.locator('#activityOlder').isEnabled(), true);
  assert.equal(await page.locator('#activityNewer').isDisabled(), true);
  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  assert.equal(await page.locator('#activityPageLabel').innerText(), 'Page 2');
  assert.equal(await page.locator('#events > [data-key]').count(), 200);
  assert.equal(await page.locator('[data-key="activity:event:201"]').count(), 1);
  assert.equal(await page.locator('[data-key="activity:event:401"]').count(), 0);
  assert.equal(await page.evaluate(() => state.data.events.length), 200);

  // Normal polling updates the live sidebar without fetching or replacing the
  // archived page, even when new activity arrives on the latest page.
  data.events = [...data.events.slice(1), event(601)];
  data.admin_messages.push({...data.admin_messages[0], message_id: 'inbox-later', message: 'live inbox'});
  for (let i = 0; i < 3; i++) await poll();
  assert.equal(await page.locator('[data-key="activity:event:601"]').count(), 0);
  assert.equal(await page.locator('[data-key="activity:event:201"]').count(), 1);
  assert.equal(await page.locator('#inbox [data-key="inbox-later"]').count(), 1);
  assert.deepEqual(requests, [{cursor: 401, limit: '200'}]);

  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 2);
  assert.equal(await page.locator('#events > [data-key]').count(), 200);
  assert.equal(await page.locator('#activityOlder').isDisabled(), true);
  assert.equal(await page.locator('#activityNewer').isEnabled(), true);
  await page.locator('[data-view="logs"]').click();
  assert.equal(await page.locator('#events .logline').count(), 200);
  await page.locator('#activityNewer').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  assert.equal(await page.locator('#events .logline').count(), 200);
  await page.locator('[data-view="activity"]').click();
  await page.check('#groupTools');
  assert.equal(await page.locator('[data-key="activity:event:group:201"]').count(), 1);
  await page.selectOption('#activityFilter', 'messages');
  assert.ok(await page.locator('#events .message-body').count() >= 1, 'page filters still work');
  await page.selectOption('#activityFilter', 'all');
  await page.locator('#activityNewer').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 0);
  assert.equal(await page.locator('[data-key="activity:event:601"]').count(), 1);
  assert.equal(await page.locator('#events > [data-key]').count(), 200);
  assert.deepEqual(requests, [{cursor: 401, limit: '200'}, {cursor: 201, limit: '200'},
    {cursor: 401, limit: '200'}]);
});

test('a delayed off-page finish updates a selected send without adding raw events or replacing the message body', async t => {
  const data = fixture();
  const [start, finish] = data.events.slice(2, 4);
  data.events = [start];
  data.correlated_events = [];
  const {page, poll} = await open(t, data);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.match(await card.locator('.message-status').innerText(), /Sending/);
  const body = card.locator('.message-body');
  await body.evaluate(node => { node.scrollTop = 20; });

  data.correlated_events = [finish];
  await poll();
  assert.match(await card.locator('.message-status').innerText(), /Queued for 1 recipient/);
  assert.equal(await body.textContent(), start.detail.arguments.message);
  assert.equal(await page.evaluate(() => state.data.events.length), 1);
  assert.deepEqual(await page.evaluate(() => state.data.correlated_events.map(e => e.id)), [4]);
  await page.selectOption('#activityPool', 'beta');
  await page.selectOption('#activityAgent', 'Bob');
  await page.selectOption('#activitySession', 'chat-beta');
  assert.equal(await card.count(), 1, 'correlated finish provides recipient filter identity');
  await page.selectOption('#activityAgent', '');
  await page.selectOption('#activitySession', '');
  await page.selectOption('#activityFilter', 'all');
  await page.check('#groupTools');
  assert.equal(await page.locator('[data-key="activity:event:group:3"]').count(), 1);
  assert.match(await page.locator('.tool-group summary').innerText(), /In progress/);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 1, 'correlated finish never becomes a raw log event');
  assert.equal(await page.locator('[data-key="logs:event:4"]').count(), 0);
});

test('a finish-only page projects the off-page legacy send, while older page uses off-page completion', async t => {
  const data = fixture();
  const [start, finish] = data.events.slice(2, 4);
  const unrelated = {...data.events[5], id: 99};
  data.events = [finish];
  data.correlated_events = [start, unrelated];
  data.events_has_more = true;
  data.events_next_before_id = 4;
  const {page} = await open(t, data);
  await page.route('**/_admin/api/events*', route => route.fulfill({json: {
    events: [start], correlated_events: [finish, unrelated], next_before_id: null, has_more: false,
  }}));
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.equal(await card.count(), 1, 'finish-only page still displays original message');
  assert.match(await card.innerText(), /first send/);
  assert.match(await card.innerText(), /Queued for 1 recipient/);
  assert.equal(await page.locator('#events .message-body', {hasText: 'beta incoming'}).count(), 0,
    'unrelated correlated deliveries do not appear on this page');
  await page.selectOption('#activityPool', 'beta');
  await page.selectOption('#activityAgent', 'Bob');
  assert.equal(await card.count(), 1, 'finish inherits off-page start pool and recipient');
  await page.selectOption('#activityAgent', '');
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 1);
  assert.equal(await page.locator('[data-key="logs:event:3"]').count(), 0);

  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  assert.deepEqual(await page.evaluate(() => state.data.events.map(e => e.id)), [3]);
  await page.click('[data-view="activity"]');
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await card.count(), 1, 'start-only page projects completion from the other page');
  assert.match(await card.innerText(), /Queued for 1 recipient/);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 1);
  assert.equal(await page.locator('[data-key="logs:event:4"]').count(), 0);
});

test('an archived pending send reconciles a delayed finish on polling without navigation or idle refetches', async t => {
  const data = fixture();
  const [start, finish] = data.events.slice(2, 4);
  data.events = [{...data.events[0], id: 500, kind: 'unrelated.event'}];
  data.correlated_events = [];
  data.events_has_more = true;
  data.events_next_before_id = 500;
  const {page, poll} = await open(t, data);
  let completed = false, archiveRequests = 0;
  await page.route('**/_admin/api/events*', route => {
    archiveRequests++;
    return route.fulfill({json: {
      events: [start], correlated_events: completed ? [finish] : [],
      next_before_id: null, has_more: false,
    }});
  });
  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.match(await card.locator('.message-status').innerText(), /Sending/);

  completed = true;
  await poll();
  await page.waitForFunction(() => document.querySelector('[data-key="activity:message:send:3"] .message-status')?.textContent.includes('Queued'));
  assert.equal(await page.locator('#activityPageLabel').innerText(), 'Page 2');
  assert.ok(archiveRequests >= 2, 'poll fetched only the selected archived page to resolve pending send');
  const requestsWhenResolved = archiveRequests;
  await poll();
  assert.equal(archiveRequests, requestsWhenResolved, 'resolved archived page no longer needs polling');
  assert.deepEqual(await page.evaluate(() => state.data.events.map(e => e.id)), [3]);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 1);
});

test('v1 finish-only messages and receives use typed lifecycle fields instead of misleading detail', async t => {
  const data = fixture();
  const typedFinish = {
    ...data.events[3], schema_version: 1,
    detail: {tool: 'pool_send', start_event_id: 1000,
      result: {value: {assigned_agent: 'Ghost', message_id: 'wrong', recipients: ['Nobody']}}},
    lifecycle: {type: 'message_send', phase: 'finish', tool: 'pool_send', start_event_id: 3, pool: 'beta',
      message: {pool: 'beta', target: 'Bob', body: 'typed only <b>plain text</b>', from: 'Alice',
        in_reply_to: 'original', message_id: 'typed-1', recipients: ['Bob'], status: 'queued'},
      peer_messages: [{pool: 'beta', message_id: 'typed-2', from: 'Cara', to: 'Bob', message: 'typed incoming'}],
    },
  };
  data.events = [typedFinish];
  data.correlated_events = [];
  const {page, poll} = await open(t, data);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.equal(await card.count(), 1);
  assert.match(await card.innerText(), /Alice/);
  assert.match(await card.innerText(), /typed only <b>plain text<\/b>/);
  assert.match(await card.innerText(), /reply to original/);
  assert.match(await card.innerText(), /Queued for 1 recipient/);
  assert.match(await page.locator('#events').innerText(), /typed incoming/);
  assert.doesNotMatch(await page.locator('#events').innerText(), /Ghost|Nobody|wrong/);
  assert.equal(await page.locator('#events b').count(), 0);
  await page.selectOption('#activityPool', 'beta');
  await page.selectOption('#activityAgent', 'Alice');
  assert.equal(await card.count(), 1, 'typed sender identity survives agent filter');
  await page.selectOption('#activityAgent', 'Bob');
  assert.equal(await card.count(), 1, 'typed recipient identity survives agent filter');
  await page.selectOption('#activitySession', 'chat-beta');
  assert.equal(await card.count(), 1);
  await page.selectOption('#activitySession', '');

  data.events = [{...typedFinish, kind: 'tool.error', level: 'error',
    lifecycle: {...typedFinish.lifecycle, phase: 'error', message: {
      ...typedFinish.lifecycle.message, status: 'failed', error: 'target no longer exists',
    }}}];
  await poll();
  assert.match(await card.locator('.message-status').innerText(), /Failed: target no longer exists/);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 1);
});

test('CLEAR discards an in-flight archived reconciliation and its paired events', async t => {
  const data = fixture();
  const [start, finish] = data.events.slice(2, 4);
  data.events = [{...data.events[0], id: 500, kind: 'unrelated.event'}];
  data.correlated_events = [];
  data.events_has_more = true;
  data.events_next_before_id = 500;
  const {page} = await open(t, data);
  let calls = 0, release, markRequested;
  const gate = new Promise(resolve => { release = resolve; });
  const requested = new Promise(resolve => { markRequested = resolve; });
  await page.route('**/_admin/api/events*', async route => {
    calls++;
    if (calls === 1) return route.fulfill({json: {
      events: [start], correlated_events: [], next_before_id: null, has_more: false,
    }});
    markRequested();
    await gate;
    return route.fulfill({json: {
      events: [start], correlated_events: [finish], next_before_id: null, has_more: false,
    }});
  });
  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  await page.evaluate(() => { window.pendingRefresh = refresh(); });
  await requested;
  await page.route('**/_admin/api/clear', route => {
    data.events = [];
    data.correlated_events = [];
    data.events_has_more = false;
    data.events_next_before_id = null;
    data.admin_messages = [];
    return route.fulfill({json: {cleared_events: 10, cleared_admin_messages: 1}});
  });
  await page.locator('#menuToggle').click();
  await page.locator('#clearCover').click();
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => !state.clearing && state.activityPageIndex === 0);
  release();
  await page.evaluate(() => window.pendingRefresh);
  assert.deepEqual(await page.evaluate(() => ({
    events: state.data.events, correlated: state.data.correlated_events,
    page: state.activityPageIndex,
  })), {events: [], correlated: [], page: 0});
  assert.equal(await page.locator('#events > [data-key]').count(), 0);
});

test('stale snapshot cannot replace an older activity page', async t => {
  const data = fixture();
  data.events_has_more = true;
  data.events_next_before_id = 1;
  const {page} = await open(t, data);
  await page.route('**/_admin/api/events*', route => route.fulfill({json: {
    events: [{...data.events[0], id: 1, detail: {tool: 'exec_command', arguments: {cmd: 'old'}}}],
    next_before_id: null, has_more: false,
  }}));
  let release, markRequested;
  const gate = new Promise(resolve => { release = resolve; });
  const requested = new Promise(resolve => { markRequested = resolve; });
  await page.route('**/_admin/api/snapshot*', async route => {
    markRequested();
    await gate;
    return route.fulfill({json: data});
  });
  await page.evaluate(() => { window.pendingRefresh = refresh(); });
  await requested;
  await page.locator('#activityOlder').click();
  await page.waitForFunction(() => !state.activityLoading && state.activityPageIndex === 1);
  assert.equal(await page.locator('#events > [data-key]').count(), 1);
  release();
  await page.evaluate(() => window.pendingRefresh);
  assert.equal(await page.locator('#activityPageLabel').innerText(), 'Page 2');
  assert.equal(await page.locator('#events > [data-key]').count(), 1);
});

test('CLEAR discards an in-flight archived activity response and resets pagination', async t => {
  const data = fixture();
  data.events_has_more = true;
  data.events_next_before_id = 1;
  const {page} = await open(t, data);
  let release, markRequested;
  const gate = new Promise(resolve => { release = resolve; });
  const requested = new Promise(resolve => { markRequested = resolve; });
  await page.route('**/_admin/api/events*', async route => {
    markRequested();
    await gate;
    await route.fulfill({json: {events: [{...data.events[0], id: 1}], next_before_id: null, has_more: false}});
  });
  await page.locator('#activityOlder').click();
  await requested;
  assert.equal(await page.locator('#activityPageLabel').innerText(), 'Page 2 · loading…');
  await page.route('**/_admin/api/clear', route => {
    data.events = [];
    data.events_has_more = false;
    data.events_next_before_id = null;
    data.admin_messages = [];
    return route.fulfill({json: {cleared_events: 11, cleared_admin_messages: 1}});
  });
  await page.locator('#menuToggle').click();
  await page.locator('#clearCover').click();
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => !state.clearing && state.activityPageIndex === 0);
  release();
  await page.waitForFunction(() => !state.activityLoading);
  assert.equal(await page.locator('#events > [data-key]').count(), 0);
  assert.equal(await page.locator('#activityPagination').isHidden(), true);
  assert.deepEqual(await page.evaluate(() => state.activityCursors), [null]);
});

test('failed admin ACKs leave messages unread and allow a retry', async t => {
  const {page, data, poll} = await open(t);
  const button = page.locator('#inbox .ack');
  for (const response of [
    {status: 500, body: 'ACK failed'},
    {json: {acknowledged: false}},
  ]) {
    await page.route('**/_admin/api/ack', route => route.fulfill(response));
    await button.click();
    await page.waitForFunction(() => document.querySelector('#toast').classList.contains('bad'));
    assert.equal(await button.isEnabled(), true);
    assert.equal(await button.innerText(), 'ACK');
    assert.equal(data.admin_messages[0].acknowledged, false);
    assert.match(await page.locator('#inbox').innerText(), /inbox message/);
    await poll();
    await page.unroute('**/_admin/api/ack');
    await page.evaluate(() => { document.querySelector('#toast').className = 'toast'; });
  }
});

test('dragging tool.start/tool.finish headers across polls preserves only the selected text', async t => {
  const {page, data, poll} = await open(t);
  for (const id of [1, 2]) {
    const header = page.locator(`[data-key="activity:event:${id}"] .event-kind`);
    await header.scrollIntoViewIfNeeded();
    const range = await header.evaluate(node => {
      const range = document.createRange();
      range.setStart(node.firstChild, 0);
      range.setEnd(node.firstChild, 4);
      const r = range.getBoundingClientRect();
      return {x1: r.left + 0.5, x2: r.right - 0.5, y: r.top + r.height / 2};
    });
    await page.mouse.move(range.x1, range.y);
    await page.mouse.down();
    await page.mouse.move(range.x2, range.y, {steps: 5});
    await poll();
    await page.mouse.up();
    assert.equal(await page.evaluate(() => getSelection().toString()), 'tool');
    // Even retention eviction must wait until the user clears their selection.
    data.events = data.events.filter(e => e.id !== id);
    await poll();
    assert.equal(await page.evaluate(() => getSelection().toString()), 'tool');
    assert.equal(await header.count(), 1);
    await page.evaluate(() => getSelection().removeAllRanges());
    await page.waitForFunction(id => !document.querySelector(`[data-key="activity:event:${id}"]`), id);
  }
});

test('new events preserve the viewport anchor while reading older events', async t => {
  const {page, data, poll} = await open(t);
  const before = await page.evaluate(() => {
    const root = document.querySelector('#events');
    root.scrollTop = 220;
    const node = [...root.children].find(node => node.getBoundingClientRect().bottom > root.getBoundingClientRect().top);
    window.anchor = node;
    return node.getBoundingClientRect().top;
  });
  data.events.push({...data.events[10], id: 12, detail: {tool: 'exec_command', result: {output: 'new event'}}});
  await poll();
  const after = await page.evaluate(() => window.anchor.getBoundingClientRect().top);
  assert.ok(Math.abs(before - after) < 1, `anchor moved by ${after - before}px`);
});

test('messages show sent/received text, deduplicate deliveries and exclude pool leave', async t => {
  const {page} = await open(t);
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#events .event').count(), 5);
  const text = await page.locator('#events').innerText();
  assert.match(text, /Alice → Bob/);
  assert.match(text, /first send <b>plain text<\/b>/);
  assert.match(text, /reply to earlier/);
  assert.match(text, /Failed: unknown target/);
  assert.match(text, /beta incoming\nsecond line/);
  assert.match(text, /Former agent → admin/);
  assert.doesNotMatch(text, /operation|tool.start|output/);
  assert.equal(await page.locator('#events script, #events b').count(), 0, 'message text is escaped');
  assert.equal(await page.locator('#events .message-body', {hasText: 'beta incoming'}).count(), 1);
});

test('a nearby delivery updates the queued card in place and preserves body scroll', async t => {
  const data = fixture();
  const text = Array.from({length: 100}, (_, i) => `long message line ${i}`).join('\n');
  data.events[2].detail.arguments.message = text;
  const {page, poll} = await open(t, data);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.match(await card.innerText(), /Queued for 1 recipient/);
  await card.evaluate(node => {
    node.querySelector('.message-body').scrollTop = 180;
  });
  data.events.push({
    id: 12, kind: 'tool.finish', level: 'info', timestamp_ms: data.events[10].timestamp_ms + 1,
    session: 'chat-bob', agents: [{pool: 'beta', agent: 'Bob'}],
    detail: {tool: 'pool_wait', result: {reason: 'message', membership_changed: false, messages: [{
      pool: 'beta', message_id: 'sent-1', from: 'Alice', to: 'Bob', target: 'Bob', message: text,
    }]}},
  });
  await poll();
  assert.equal(await page.locator('#events .event').count(), 5);
  assert.match(await card.innerText(), /message.receive/);
  assert.match(await card.innerText(), /Offered in tool response to Bob/);
  assert.doesNotMatch(await card.innerText(), /Queued/);
  assert.equal(await card.locator('.message-body').evaluate(node => node.scrollTop), 180);
  assert.equal(await card.locator('.message-body').textContent(), text);
  assert.equal(await card.locator('[data-time]').getAttribute('data-time'), String(data.events[2].timestamp_ms));
  for (const session of ['chat-beta', 'chat-bob']) {
    await page.selectOption('#activitySession', session);
    assert.equal(await card.count(), 1, 'both sender and recipient session filters retain the message');
  }
  await page.selectOption('#activitySession', '');
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('[data-key="logs:event:12"]').count(), 1, 'raw delivery remains in MCP log');
});

test('admin inbox receipts merge even when send and receipt have the same timestamp', async t => {
  const data = fixture();
  data.events[2].detail.arguments.target = 'admin';
  data.events[3].detail.result.value.recipients = ['admin'];
  data.admin_messages = [{
    pool: 'beta', message_id: 'sent-1', from: 'Alice', message: data.events[2].detail.arguments.message,
    created_ms: data.events[2].timestamp_ms,
  }];
  const {page} = await open(t, data);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:send:3"]');
  assert.equal(await page.locator('#events .event').count(), 4);
  assert.equal(await card.locator('.message-status').innerText(), 'Received');
  assert.match(await card.innerText(), /Alice → admin/);
  assert.equal(await page.locator('#inbox .message').count(), 1, 'admin inbox is unchanged');
});

test('four intervening messages merge, while five or six keep delivery separate', async t => {
  for (const gap of [4, 5, 6]) await t.test(`${gap} intervening messages`, async t => {
    const data = fixture();
    data.events = data.events.slice(2, 4);
    data.admin_messages = [];
    const now = data.events[1].timestamp_ms;
    for (let i = 0; i < gap; i++) data.events.push({
      id: 10 + i, kind: 'admin.message.send', level: 'info', timestamp_ms: now + i + 1,
      agents: [], session: null, detail: {
        pool: 'beta', target: 'Bob', message: `intervening ${i}`, message_id: `gap-${i}`, delivery_count: 1,
      },
    });
    data.events.push({
      id: 30, kind: 'tool.finish', level: 'info', timestamp_ms: now + 30,
      agents: [{pool: 'beta', agent: 'Bob'}], session: 'chat-bob',
      detail: {tool: 'pool_members', result: {peer_messages: [{
        pool: 'beta', message_id: 'sent-1', from: 'Alice', to: 'Bob', message: data.events[0].detail.arguments.message,
      }]}},
    });
    const {page, poll} = await open(t, data);
    await page.selectOption('#activityFilter', 'messages');
    const merged = gap === 4;
    assert.equal(await page.locator('#events .event').count(), gap + (merged ? 1 : 2));
    const card = page.locator('[data-key="activity:message:send:3"]');
    assert.equal(await card.locator('.message-status').innerText(), merged ? 'Offered in tool response to Bob' : 'Queued for 1 recipient(s)');
    const keys = await page.locator('#events > [data-key]').evaluateAll(nodes => nodes.map(n => n.dataset.key));
    await poll();
    assert.deepEqual(await page.locator('#events > [data-key]').evaluateAll(nodes => nodes.map(n => n.dataset.key)), keys);
  });
});

test('a nearby broadcast tracks partial offers, deduplicates repeats and retains recipient sessions', async t => {
  const {page, data, poll} = await open(t);
  await page.selectOption('#activityFilter', 'messages');
  const card = page.locator('[data-key="activity:message:admin:9"]');
  const offer = (id, to, session) => ({
    id, kind: 'tool.finish', level: 'info', timestamp_ms: data.events[10].timestamp_ms + id,
    agents: [{pool: 'beta', agent: to}], session,
    detail: {tool: 'pool_members', result: {peer_messages: [{
      pool: 'beta', message_id: 'admin-1', from: 'admin', to, target: 'global', message: 'admin broadcast',
    }]}},
  });
  data.events.push(offer(12, 'Alice', 'chat-beta'), offer(13, 'Alice', 'chat-beta'));
  await poll();
  assert.equal(await page.locator('#events .event').count(), 5);
  assert.equal(await card.locator('.message-status').innerText(), 'Offered in tool response to Alice · Queued for 1 recipient(s)');
  data.events.push(offer(14, 'Bob', 'chat-bob'), offer(15, 'Alice', 'former-session'));
  await poll();
  const status = await card.locator('.message-status').innerText();
  assert.doesNotMatch(status, /Queued/);
  assert.match(status, /Alice \(chat-beta\)/);
  assert.match(status, /Bob/);
  assert.match(status, /Alice \(former-session\)/);
  assert.equal(await page.locator('#events .event').count(), 5);
  for (const session of ['chat-beta', 'chat-bob', 'former-session']) {
    await page.selectOption('#activitySession', session);
    assert.equal(await card.count(), 1);
  }
});

test('pool, agent and session filters compose and survive polling', async t => {
  const {page, poll} = await open(t);
  await page.selectOption('#activityPool', 'beta');
  assert.equal(await page.locator('#events > [data-key="activity:event:3"]').count(), 1, 'first send before membership is included');
  await page.selectOption('#activityAgent', 'Bob');
  await page.selectOption('#activitySession', 'chat-beta');
  const keys = await page.locator('#events > [data-key]').evaluateAll(nodes => nodes.map(n => n.dataset.key));
  assert.deepEqual(keys, [6, 5, 4, 3].map(id => `activity:event:${id}`));
  await poll();
  assert.equal(await page.inputValue('#activityPool'), 'beta');
  assert.equal(await page.inputValue('#activityAgent'), 'Bob');
  assert.equal(await page.inputValue('#activitySession'), 'chat-beta');
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#events .event').count(), 2);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 4);
  await page.click('[data-view="activity"]');
  await page.click('.pool-filter[data-pool="alpha"]');
  assert.equal(await page.inputValue('#activityPool'), 'alpha');
  assert.equal(await page.inputValue('#activityAgent'), '');
  assert.equal(await page.inputValue('#activitySession'), '');
  assert.equal(await page.locator('.pool-filter[data-pool="alpha"]').getAttribute('aria-pressed'), 'true');
  assert.match(await page.locator('#events').innerText(), /No entries match/);
});

test('broadcast offers remain visible per recipient session, including old snapshots', async t => {
  const data = fixture();
  const makeOffer = (id, to, session, recipient) => ({
    id, kind: 'tool.finish', level: 'info', timestamp_ms: Date.now() + id, session,
    agents: [{pool: 'beta', agent: recipient}],
    detail: {tool: 'pool_members', result: {peer_messages: [{
      pool: 'beta', message_id: 'broadcast', from: 'admin', to, target: 'global', message: 'shared broadcast',
    }]}},
  });
  data.events.push(
    makeOffer(12, 'Alice', 'chat-beta', 'Alice'),
    makeOffer(13, 'Alice', 'chat-beta', 'Alice'), // Re-offer to this session is deduplicated.
    makeOffer(14, 'Bob', 'chat-bob', 'Bob'),
    makeOffer(15, 'global', 'former-session', 'Alice'), // Same name, different recipient session.
  );
  const {page, poll} = await open(t, data);
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#events .message-body', {hasText: 'shared broadcast'}).count(), 3);
  for (const [session, recipient] of [['chat-beta', 'Alice'], ['chat-bob', 'Bob'], ['former-session', 'Alice']]) {
    await page.selectOption('#activitySession', session);
    const row = page.locator('#events .event', {hasText: 'shared broadcast'});
    assert.equal(await row.count(), 1);
    assert.match(await row.innerText(), new RegExp(`admin → ${recipient}`));
    assert.match(await row.innerText(), /Offered in tool response/);
    await poll();
    assert.equal(await row.count(), 1);
  }
  await page.selectOption('#activitySession', '');
  await page.selectOption('#activityAgent', 'Bob');
  assert.equal(await page.locator('#events .message-body', {hasText: 'shared broadcast'}).count(), 1);
});

test('historical pools, leave finishes and identical agent names stay correctly scoped', async t => {
  const {page} = await open(t);
  await page.selectOption('#activityPool', 'old-pool');
  assert.deepEqual(await page.locator('#events > [data-key]').evaluateAll(nodes => nodes.map(n => n.dataset.key)),
    ['activity:event:8', 'activity:event:7']);
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#events .message-body').count(), 1, 'admin inbox remains visible for an inactive pool');
  await page.selectOption('#activityPool', 'alpha');
  await page.selectOption('#activityAgent', 'Alice');
  assert.equal(await page.locator('#events .event').count(), 0, 'beta messages do not leak through the same agent name');
  await page.selectOption('#activityFilter', 'all');
  await page.selectOption('#activitySession', 'chat-alpha');
  assert.deepEqual(await page.locator('#events > [data-key]').evaluateAll(nodes => nodes.map(n => n.dataset.key)),
    ['activity:event:2', 'activity:event:1']);
});

test('MCP log nested scroll and text selections survive updates too', async t => {
  const {page, poll} = await open(t);
  await page.click('[data-view="logs"]');
  await page.evaluate(() => {
    const pre = document.querySelector('[data-key="logs:event:2"] pre');
    pre.scrollTop = 190;
  });
  await poll();
  assert.equal(await page.locator('[data-key="logs:event:2"] pre').evaluate(node => node.scrollTop), 190);
});

test('filters fit narrow screens without horizontal overflow', async t => {
  const {page} = await open(t, fixture(), {width: 390, height: 844});
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await page.selectOption('#activityPool', 'beta');
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
});


test('slow snapshot requests do not overlap and race newer snapshots', async t => {
  const {page, data} = await open(t);
  let release, requested, requests = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { requested = resolve; });
  await page.route('**/_admin/api/snapshot?all=false', async route => {
    requests++;
    requested();
    await gate;
    await route.fulfill({json: data});
  });
  await page.evaluate(() => { window.pendingRefresh = refresh(); });
  await started;
  await page.evaluate(async () => { await refresh(); });
  assert.equal(requests, 1);
  release();
  await page.evaluate(async () => { await window.pendingRefresh; });
  assert.equal(await page.evaluate(() => state.refreshing), false);
});

test('the header indicator follows connectivity and the drawer supports keyboard dismissal', async t => {
  const {page, data, poll} = await open(t);
  const toggle = page.locator('#menuToggle');
  assert.doesNotMatch(await page.locator('.top').innerText(), /debug console|up 0m/);
  assert.equal(await page.locator('.brand .dot').count(), 1);
  assert.equal(await page.locator('#connectionStatus').getAttribute('data-connection'), 'connected');
  await page.route('**/_admin/api/snapshot?all=false', route => route.fulfill({status: 503, body: 'offline'}));
  await poll();
  assert.equal(await page.locator('#connectionStatus').getAttribute('aria-label'), 'Disconnected');
  await page.route('**/_admin/api/snapshot?all=false', route => route.fulfill({json: data}));
  await poll();
  assert.equal(await page.locator('#connectionStatus').getAttribute('aria-label'), 'Connected');

  await toggle.focus();
  await page.keyboard.press('Enter');
  assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
  assert.equal(await page.evaluate(() => document.querySelector('main').inert), true);
  assert.equal(await page.evaluate(() => document.activeElement.id), 'menuClose');
  await page.keyboard.press('Tab');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'clearCover');
  await page.keyboard.press('Enter');
  assert.equal(await page.locator('#clearHistory').isEnabled(), true);
  await page.keyboard.press('Shift+Tab');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'clearHistory');
  await page.keyboard.press('Escape');
  assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'menuToggle');
  assert.equal(await page.evaluate(() => document.querySelector('main').inert), false);
  assert.equal(await page.locator('#clearHistory').isDisabled(), true);
  await toggle.click();
  await page.locator('#menuBackdrop').click({position: {x: 10, y: 10}});
  assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
});

test('the menu covers the admin column and fits mobile and short screens', async t => {
  for (const viewport of [{width: 1440, height: 900}, {width: 390, height: 844}, {width: 320, height: 480}]) {
    const {page} = await open(t, fixture(), viewport);
    await page.emulateMedia({reducedMotion: 'reduce'});
    await page.locator('#menuToggle').click();
    const menu = await page.locator('#consoleMenu').boundingBox();
    const admin = await page.locator('.right').boundingBox();
    assert.ok(menu.x >= 0 && menu.x + menu.width <= viewport.width);
    assert.ok(menu.y >= 52 && menu.y + menu.height <= viewport.height);
    if (viewport.width === 1440) {
      assert.equal(menu.x, admin.x);
      assert.equal(menu.width, admin.width);
    }
    await page.locator('#clearCover').click();
    await page.locator('#clearHistory').scrollIntoViewIfNeeded();
    const button = await page.locator('#clearHistory').boundingBox();
    assert.ok(button.y >= menu.y && button.y + button.height <= menu.y + menu.height);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  }
});

test('CLEAR requires an open cover, closes it on press and clears every history view', async t => {
  const {page, data, poll} = await open(t);
  await page.fill('#message', 'keep this draft');
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const requests = [];
  await page.route('**/_admin/api/clear', async route => {
    requests.push({method: route.request().method(), body: route.request().postDataJSON()});
    await gate;
    data.events = [];
    data.admin_messages = [];
    await route.fulfill({json: {cleared_events: 11, cleared_admin_messages: 1}});
  });
  await page.locator('#menuToggle').click();
  assert.equal(await page.locator('#clearHistory').isDisabled(), true);
  await page.evaluate(async () => { await clearHistory(); });
  assert.equal(requests.length, 0);
  await page.locator('#clearCover').click();
  await poll();
  assert.equal(await page.locator('#clearCover').getAttribute('aria-expanded'), 'true');
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => state.clearing);
  assert.equal(await page.locator('#clearCover').getAttribute('aria-expanded'), 'false');
  assert.equal(await page.locator('#clearCover').isDisabled(), true);
  assert.equal(await page.locator('#clearHistory').isDisabled(), true);
  await page.evaluate(async () => { await clearHistory(); await refresh(); });
  assert.deepEqual(requests, [{method: 'POST', body: {}}]);
  release();
  await page.waitForFunction(() => !state.clearing && document.querySelector('#clearHint').textContent === 'History cleared.');
  assert.equal(await page.locator('#events [data-key]').count(), 0);
  assert.equal(await page.locator('#inbox .message').count(), 0);
  await page.locator('#menuClose').click();
  assert.equal(await page.inputValue('#message'), 'keep this draft');
  assert.equal(await page.locator('#pools .agent').count(), 3);
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#events .event').count(), 0);
  await page.locator('[data-view="logs"]').click();
  assert.equal(await page.locator('#events .logline').count(), 0);
  await page.reload();
  await page.waitForFunction(() => document.querySelector('#status').textContent.includes('vtest'));
  assert.equal(await page.locator('#events [data-key]').count(), 0);
  assert.equal(await page.locator('#inbox .message').count(), 0);
  data.events.push({id: 12, kind: 'tool.start', level: 'info', timestamp_ms: Date.now(), agents: [], detail: {}});
  await poll();
  assert.equal(await page.locator('#events [data-key]').count(), 1);
});

test('a failed clear preserves history, closes the cover and allows retry', async t => {
  const {page, data} = await open(t);
  await page.route('**/_admin/api/clear', route => route.fulfill({status: 500, body: 'Clear failed'}));
  await page.locator('#menuToggle').click();
  await page.locator('#clearCover').click();
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => !state.clearing && document.querySelector('#toast').classList.contains('bad'));
  assert.equal(await page.locator('#events [data-key]').count(), 11);
  assert.equal(await page.locator('#inbox .message').count(), 1);
  assert.equal(await page.locator('#clearCover').getAttribute('aria-expanded'), 'false');
  assert.equal(await page.locator('#clearCover').isEnabled(), true);
  await page.route('**/_admin/api/clear', route => {
    data.events = [];
    data.admin_messages = [];
    return route.fulfill({json: {cleared_events: 11, cleared_admin_messages: 1}});
  });
  await page.locator('#clearCover').click();
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => !state.clearing && document.querySelector('#clearHint').textContent === 'History cleared.');
  assert.equal(await page.locator('#events [data-key]').count(), 0);
});

test('a snapshot requested before CLEAR cannot restore erased history', async t => {
  const {page, data} = await open(t);
  await page.locator('#menuToggle').click();
  await page.locator('#clearCover').click();
  let release, started;
  const gate = new Promise(resolve => { release = resolve; });
  const requested = new Promise(resolve => { started = resolve; });
  const stale = structuredClone(data);
  await page.route('**/_admin/api/snapshot?all=false', async route => {
    started();
    await gate;
    await route.fulfill({json: stale});
  });
  await page.evaluate(() => { window.pendingRefresh = refresh(); });
  await requested;
  await page.route('**/_admin/api/clear', route => {
    data.events = [];
    data.admin_messages = [];
    return route.fulfill({json: {cleared_events: 11, cleared_admin_messages: 1}});
  });
  await page.locator('#clearHistory').click();
  await page.waitForFunction(() => !state.clearing);
  assert.equal(await page.locator('#events [data-key]').count(), 0);
  release();
  await page.evaluate(async () => { await window.pendingRefresh; });
  assert.equal(await page.locator('#events [data-key]').count(), 0);
  assert.equal(await page.locator('#inbox .message').count(), 0);
  await page.route('**/_admin/api/snapshot?all=false', route => route.fulfill({json: data}));
});

test('formatted tools separate commands, arguments, results and errors safely in both views', async t => {
  const data = fixture();
  const command = 'printf "%s\\n" "$HOME" "<img src=x onerror=unsafe()>"';
  const output = '<script>unsafe()</script>\nsecond line & <b>literal</b>';
  data.events[0].detail.arguments = {cmd: command, workdir: '/tmp', tty: false};
  data.events[1].detail.result = {exit_code: 7, call_wall_time_seconds: 0.125, output, output_truncated: true};
  data.events.push({...data.events[10], id: 12, kind: 'custom.event', detail: {unfamiliar: '<svg onload=unsafe()>', count: 3}});
  const {page, poll} = await open(t, data);
  for (const view of ['activity', 'logs']) {
    await page.click(`[data-view="${view}"]`);
    const start = page.locator(`[data-key="${view}:event:1"]`);
    const finish = page.locator(`[data-key="${view}:event:2"]`);
    assert.equal(await start.locator('code.language-bash').textContent(), command);
    assert.ok(await start.locator('.token').count() > 0, 'Bash is highlighted');
    assert.deepEqual(JSON.parse(await start.locator('code.language-json').textContent()), {workdir: '/tmp', tty: false});
    assert.match(await finish.locator('.tool-meta').innerText(), /Exit code: 7.*Execution time: 0.125 s/s);
    assert.equal(await finish.locator('code').first().textContent(), output);
    assert.equal(await finish.locator('code').first().locator('.token').count(), 0, 'terminal output stays plain');
    assert.match(await finish.locator('code.language-json').first().textContent(), /"output_truncated": true/);
    assert.equal(await page.locator(`[data-key="${view}:event:11"] .tool-error code`).textContent(), 'unknown target');
    assert.ok(await page.locator(`[data-key="${view}:event:12"] .token.property`).count() > 0, 'unknown events use highlighted JSON');
    assert.equal(await page.locator('#events script, #events img, #events b, #events svg').count(), 0);
    await page.uncheck('#formatLogs');
    assert.deepEqual(JSON.parse(await finish.locator('code').textContent()), data.events[1].detail);
    assert.equal(await finish.locator('.token, .tool-meta').count(), 0);
    await poll();
    assert.equal(await page.isChecked('#formatLogs'), false);
    await page.check('#formatLogs');
  }
});

test('JSON output is pretty printed while copying preserves the original data', async t => {
  const data = fixture();
  const output = ' {"html":"<img src=x>","items":[true,null,42]}\r\n';
  data.events[1].detail.result = {output};
  data.events[0].detail.arguments.cmd = 'echo "<&>"\r\n';
  const {page} = await open(t, data);
  await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', {configurable: true, value: {
    writeText: async text => { window.copied = text; },
  }}));
  const finish = page.locator('[data-key="activity:event:2"]');
  assert.equal(await finish.locator('code').first().textContent(), JSON.stringify(JSON.parse(output), null, 2));
  assert.ok(await finish.locator('.token.boolean').count() > 0);
  await finish.getByRole('button', {name: 'Copy Output', exact: true}).click();
  assert.equal(await page.evaluate(() => window.copied), output);
  await page.locator('[data-key="activity:event:1"]').getByRole('button', {name: 'Copy Command', exact: true}).click();
  assert.equal(await page.evaluate(() => window.copied), data.events[0].detail.arguments.cmd);
  await page.uncheck('#formatLogs');
  await finish.getByRole('button', {name: 'Copy Raw JSON', exact: true}).click();
  assert.equal(await page.evaluate(() => window.copied), JSON.stringify(data.events[1].detail, null, 2));
});

test('plain, empty, invalid and large outputs retain their text without tokenization', async t => {
  const data = fixture();
  const outputs = ['', 'not JSON\n  indented\ttext', '{invalid JSON}', 'x'.repeat(65537)];
  data.events = outputs.map((output, i) => ({...data.events[1], id: i + 20, detail: {tool: 'exec_command', result: {output}}}));
  const {page} = await open(t, data);
  for (const [i, output] of outputs.entries()) {
    const code = page.locator(`[data-key="activity:event:${i + 20}"] code`);
    assert.equal(await code.textContent(), output);
    assert.equal(await code.locator('.token').count(), 0);
  }
});

test('wrap, expansion, nested scroll, focus and highlighted selection survive polls', async t => {
  const {page, data, poll} = await open(t);
  const start = page.locator('[data-key="activity:event:1"]');
  const pre = start.locator('pre');
  assert.equal(await pre.evaluate(node => getComputedStyle(node).whiteSpace), 'pre');
  await pre.evaluate(node => { node.scrollTop = 180; });
  await page.check('#wrapLogs');
  assert.equal(await pre.evaluate(node => node.scrollTop), 180);
  assert.equal(await pre.evaluate(node => getComputedStyle(node).whiteSpace), 'pre-wrap');
  const expand = start.getByRole('button', {name: 'Expand Command', exact: true});
  await expand.click();
  assert.equal(await pre.evaluate(node => getComputedStyle(node).maxHeight), 'none');
  await poll();
  assert.equal(await start.getByRole('button', {name: 'Collapse Command', exact: true}).getAttribute('aria-expanded'), 'true');
  assert.equal(await pre.locator('code').textContent(), data.events[0].detail.arguments.cmd);
  await start.getByRole('button', {name: 'Collapse Command', exact: true}).click();
  await pre.evaluate(node => { node.scrollTop = 160; });
  await start.getByRole('button', {name: 'Expand Command', exact: true}).focus();
  await poll();
  assert.equal(await pre.evaluate(node => node.scrollTop), 160);
  assert.equal(await page.evaluate(() => document.activeElement.getAttribute('aria-label')), 'Expand Command');
  await pre.locator('code').evaluate(node => {
    const range = document.createRange();
    range.selectNodeContents(node);
    getSelection().removeAllRanges(); getSelection().addRange(range);
    window.selectedCommand = getSelection().toString();
  });
  data.events.push({...data.events[10], id: 12});
  await poll();
  assert.equal(await page.evaluate(() => getSelection().toString() === window.selectedCommand), true);
  assert.equal(await page.locator('[data-key="activity:event:12"]').count(), 0, 'insertion waits for selection');
  await page.evaluate(() => getSelection().removeAllRanges());
  await page.waitForFunction(() => document.querySelector('[data-key="activity:event:12"]'));
  await page.click('[data-view="logs"]');
  assert.equal(await page.isChecked('#wrapLogs'), true);
});

test('groups update on completion without losing the command or reopening a collapsed card', async t => {
  const data = fixture();
  data.events = [data.events[0]];
  const {page, poll} = await open(t, data);
  await page.check('#groupTools');
  const card = page.locator('[data-key="activity:event:group:1"]');
  assert.match(await card.locator('summary').innerText(), /In progress/);
  await card.getByRole('button', {name: 'Expand Command', exact: true}).click();
  await card.locator('summary').click();
  data.events.push({...fixture().events[1], timestamp_ms: data.events[0].timestamp_ms + 100});
  await poll();
  assert.equal(await card.locator('.tool-group').getAttribute('open'), null);
  assert.equal(await card.locator('[data-group-part="start"] code').textContent(), data.events[0].detail.arguments.cmd);
  await card.locator('summary').click();
  assert.match(await card.locator('summary').innerText(), /tool.finish/);
  assert.equal(await card.getByRole('button', {name: 'Collapse Command', exact: true}).getAttribute('aria-expanded'), 'true');
  assert.equal(await card.locator('[data-group-part="finish"] code').first().textContent(), data.events[1].detail.result.output);
  assert.match(await page.locator('#filterCount').innerText(), /2 \/ 2 events · 1 cards/);
  await page.click('[data-view="logs"]');
  assert.equal(await page.locator('#events .logline').count(), 2);
  assert.equal(await page.locator('#groupTools').isDisabled(), true);
  await page.click('[data-view="activity"]');
  assert.equal(await card.count(), 1);
  data.events.shift(); // Retention may evict the start before its finish.
  await poll();
  assert.equal(await page.locator('[data-key="activity:event:2"]').count(), 1);
});

test('grouping keeps failed calls, orphan finishes and session filters accessible', async t => {
  const data = fixture();
  // Deliberately differing sessions must not pull a filtered event into a group.
  data.events[1].session = 'other-session';
  const {page} = await open(t, data);
  await page.check('#groupTools');
  assert.match(await page.locator('[data-key="activity:event:group:10"] summary').innerText(), /tool.error/);
  assert.match(await page.locator('[data-key="activity:event:group:10"] .tool-error').innerText(), /unknown target/);
  assert.equal(await page.locator('[data-key="activity:event:5"]').count(), 1, 'orphan result is visible');
  await page.selectOption('#activitySession', 'other-session');
  assert.equal(await page.locator('#events [data-key]').count(), 1);
  assert.equal(await page.locator('[data-key="activity:event:2"]').count(), 1);
  await page.selectOption('#activitySession', 'chat-alpha');
  assert.equal(await page.locator('[data-key="activity:event:group:1"] [data-group-part="finish"]').textContent(), '');
  await page.selectOption('#activityFilter', 'messages');
  assert.equal(await page.locator('#groupTools').isDisabled(), true);
  assert.equal(await page.locator('#events .tool-group').count(), 0);
});

test('copy failures are visible and long lines stay inside the viewer on mobile', async t => {
  const data = fixture();
  data.events[0].detail.arguments.cmd = 'echo ' + 'longargument'.repeat(100);
  const {page, poll} = await open(t, data, {width: 320, height: 568});
  await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', {configurable: true, value: {
    writeText: async () => { throw new Error('denied'); },
  }}));
  const start = page.locator('[data-key="activity:event:1"]');
  await start.getByRole('button', {name: 'Copy Command', exact: true}).click();
  assert.match(await page.locator('#toast.bad').innerText(), /Copy failed/);
  const pre = start.locator('pre');
  await pre.evaluate(node => { node.scrollLeft = 120; });
  await poll();
  assert.equal(await pre.evaluate(node => node.scrollLeft), 120);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await page.check('#wrapLogs');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
});

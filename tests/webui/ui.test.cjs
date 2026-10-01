const {test, before, after} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {createServer} = require('node:http');
const {chromium} = require('playwright');

const html = readFileSync(require('node:path').join(__dirname, '../../src/webui.html'));
let browser, server, url;
before(async () => {
  browser = await chromium.launch({
    ...(process.env.WEBUI_BROWSER ? {executablePath: process.env.WEBUI_BROWSER} : {}),
    args: ['--no-sandbox'],
  });
  server = createServer((req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.end(html);
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
    version: 'test', uptime_seconds: 0,
    pools: [{name: 'alpha', agents: [agent('alpha', 'Alice', 'chat-alpha')]},
      {name: 'beta', agents: [agent('beta', 'Alice', 'chat-beta'), agent('beta', 'Bob', 'chat-bob')]}],
    admin_messages: [{pool: 'old-pool', message_id: 'inbox-1', from: 'Former agent', message: '<script>unsafe()</script>\ninbox message', created_ms: now}],
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
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, [], 'no browser exceptions'));
  await page.route('**/_admin/api/snapshot', route => route.fulfill({json: data}));
  await page.goto(url);
  await page.waitForFunction(() => document.querySelector('#status').textContent.includes('vtest'));
  // Manual refreshes let tests cross age boundaries without timing-dependent sleeps.
  await page.evaluate(() => { window.testClock = Date.now(); Date.now = () => window.testClock; });
  return {page, data, poll: async () => {
    await page.evaluate(async () => { window.testClock += 2000; await refresh(); });
  }};
}

test('polls preserve tool.start/tool.finish nested scroll, DOM identity and focus', async t => {
  const {page, data, poll} = await open(t);
  await page.evaluate(() => {
    window.savedBodies = [1, 2].map(id => {
      const pre = document.querySelector(`[data-key="activity:event:${id}"] pre`);
      pre.scrollTop = 180;
      return pre;
    });
    window.poolTitle = document.querySelector('.pool-filter');
    window.inboxMessage = document.querySelector('#inbox .message');
    document.querySelector('.danger').focus();
    window.terminateButton = document.activeElement;
    window.poolOptions = document.querySelector('#poolSelect').firstElementChild;
  });
  for (let n = 0; n < 3; n++) {
    data.pools[0].agents[0].last_seen_ms += 1500;
    data.pools[0].agents[0].expires_ms += 1500;
    await poll();
  }
  assert.deepEqual(await page.evaluate(() => ({
    bodies: window.savedBodies.map((pre, index) => ({
      same: pre === document.querySelector(`[data-key="activity:event:${index + 1}"] pre`), scroll: pre.scrollTop,
    })),
    title: window.poolTitle === document.querySelector('.pool-filter'),
    inbox: window.inboxMessage === document.querySelector('#inbox .message'),
    focus: document.activeElement === window.terminateButton,
    options: window.poolOptions === document.querySelector('#poolSelect').firstElementChild,
  })), {bodies: [{same: true, scroll: 180}, {same: true, scroll: 180}], title: true, inbox: true, focus: true, options: true});
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
    window.sentCard = node;
    window.sentBody = node.querySelector('.message-body');
    window.sentBody.scrollTop = 180;
  });
  data.events.push({
    id: 12, kind: 'tool.finish', level: 'info', timestamp_ms: data.events[10].timestamp_ms + 1,
    session: 'chat-bob', agents: [{pool: 'beta', agent: 'Bob'}],
    detail: {tool: 'pool_members', result: {peer_messages: [{
      pool: 'beta', message_id: 'sent-1', from: 'Alice', to: 'Bob', target: 'Bob', message: text,
    }]}},
  });
  await poll();
  assert.equal(await page.locator('#events .event').count(), 5);
  assert.match(await card.innerText(), /message.receive/);
  assert.match(await card.innerText(), /Offered in tool response to Bob/);
  assert.doesNotMatch(await card.innerText(), /Queued/);
  assert.deepEqual(await card.evaluate(node => [
    node === window.sentCard, node.querySelector('.message-body') === window.sentBody, window.sentBody.scrollTop,
  ]), [true, true, 180]);
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
    window.logBody = pre;
  });
  await poll();
  assert.deepEqual(await page.evaluate(() => [window.logBody === document.querySelector('[data-key="logs:event:2"] pre'), window.logBody.scrollTop]), [true, 190]);
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
  await page.route('**/_admin/api/snapshot', async route => {
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

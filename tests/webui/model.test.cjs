const {test, before} = require('node:test');
const assert = require('node:assert/strict');

let eventPools, isMessageEvent, eventIdentities, messages, coalesceMessages;
before(async () => {
  ({eventPools, isMessageEvent, eventIdentities} = await import('../../src/webui/model/events.js'));
  ({messages, coalesceMessages} = await import('../../src/webui/model/messages.js'));
});

const event = (id, kind, detail = {}, other = {}) => ({
  id, kind, detail, timestamp_ms: id, agents: [], session: null, ...other,
});

test('four intervening rows can merge but five or more cannot', () => {
  const send = {
    key: 'send:1', kind: 'message.send', pool: 'alpha', message_id: 'msg-1', from: 'Alice',
    to: 'Bob', agents: ['Alice', 'Bob'], sessions: ['sender'], recipient_count: 1,
    status: 'Queued for 1 recipient(s)',
  };
  const receipt = {
    key: 'receive:1', kind: 'message.receive', pool: 'alpha', message_id: 'msg-1',
    from: 'Alice', to: 'Bob', agents: ['Alice', 'Bob'], sessions: ['recipient'],
    status: 'Offered in tool response',
  };
  for (const gap of [4, 5, 6]) {
    const intervening = Array.from({length: gap}, (_, i) => ({
      key: 'other:' + i, kind: 'message.send', pool: 'alpha', message_id: 'other:' + i,
    }));
    const result = coalesceMessages([send, ...intervening, receipt]);
    assert.equal(result.length, gap <= 4 ? gap + 1 : gap + 2, `gap=${gap}`);
    if (gap === 4) {
      assert.equal(result[0].key, send.key);
      assert.equal(result[0].kind, 'message.receive');
      assert.equal(result[0].deliveries.length, 1);
      assert.deepEqual(result[0].sessions, ['sender', 'recipient']);
      assert.equal(result[0].status, 'Offered in tool response to Bob');
    } else {
      assert.equal(result.at(-1).key, receipt.key);
      assert.equal(result[0].kind, 'message.send');
    }
  }
});

test('repeat receipts deduplicate within a session but retain different recipient sessions', () => {
  const peer = {pool: 'alpha', message_id: 'same', from: 'Alice', to: 'Bob', message: 'hello'};
  const events = [
    event(1, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-1'}),
    event(2, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-1'}),
    event(3, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-2'}),
  ];
  const result = messages(events, new Map(), [], []);
  assert.equal(result.length, 2);
  assert.deepEqual(result.map(m => m.sessions), [['bob-2'], ['bob-1']]);
  assert.equal(new Set(result.map(m => m.key)).size, 2);
  assert.deepEqual(result.map(m => m.to), ['Bob', 'Bob']);
});

test('multiple deliveries to a reused recipient name stay visible in a coalesced send', () => {
  const peer = {pool: 'alpha', message_id: 'same', from: 'Alice', to: 'Bob', message: 'hello'};
  const start = event(10, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'alpha', target: 'Bob', message: 'hello'},
  }, {agents: [{pool: 'alpha', agent: 'Alice'}], session: 'sender'});
  const finish = event(11, 'tool.finish', {
    start_event_id: 10, result: {value: {assigned_agent: 'Alice', message_id: 'same', recipients: ['Bob']}},
  });
  const receipts = [
    event(12, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-1'}),
    event(13, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-2'}),
    event(14, 'tool.finish', {result: {peer_messages: [peer]}}, {session: 'bob-1'}),
  ];
  const result = messages([start, finish, ...receipts], new Map([[10, finish]]), [], []);
  assert.equal(result.length, 1);
  assert.equal(result[0].deliveries.length, 2);
  assert.deepEqual(result[0].sessions, ['sender', 'bob-1', 'bob-2']);
  assert.match(result[0].status, /Bob \(bob-1\)/);
  assert.match(result[0].status, /Bob \(bob-2\)/);
});

test('legacy global receipts resolve the historical event recipient and pool', () => {
  const peer = {pool: 'old-pool', message_id: 'legacy', from: 'Alice', to: 'global', message: 'hi'};
  const events = [
    event(1, 'tool.finish', {result: {peer_messages: [peer]}}, {
      agents: [{pool: 'old-pool', agent: 'Bob'}], session: 'bob-session',
    }),
    event(2, 'tool.finish', {result: {peer_messages: [peer]}}, {
      agents: [{pool: 'old-pool', agent: 'Cara'}], session: 'cara-session',
    }),
  ];
  const result = messages(events, new Map(), [], []);
  assert.deepEqual(result.map(m => m.to), ['Cara', 'Bob']);
  assert.ok(result.every(m => m.pool === 'old-pool'));
  assert.deepEqual(result.map(m => m.sessions), [['cara-session'], ['bob-session']]);
});

test('equal timestamps still merge sends before receipts regardless of input ordering', () => {
  const start = event(1, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'alpha', target: 'Bob', message: 'hello'},
  }, {timestamp_ms: 100, agents: [{pool: 'alpha', agent: 'Alice'}], session: 'sender'});
  const finish = event(2, 'tool.finish', {
    start_event_id: 1,
    result: {
      value: {assigned_agent: 'Alice', message_id: 'msg-1', recipients: ['Bob']},
      peer_messages: [{pool: 'alpha', message_id: 'msg-1', from: 'Alice', to: 'Bob', message: 'hello'}],
    },
  }, {timestamp_ms: 100, session: 'bob'});
  const result = messages([finish, start], new Map([[1, finish]]), [], []);
  assert.equal(result.length, 1);
  assert.equal(result[0].key, 'send:1');
  assert.equal(result[0].kind, 'message.receive');
  assert.equal(result[0].deliveries.length, 1);
  assert.deepEqual(result[0].sessions, ['sender', 'bob']);
  assert.equal(result[0].status, 'Offered in tool response to Bob');
});

test('admin inbox receipts and pool-scoped agent identity fallbacks are pure inputs', () => {
  const start = event(1, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'beta', target: 'global', message: 'hello'},
  }, {session: 'shared-session'});
  const pools = [
    {name: 'alpha', agents: [{pool: 'alpha', agent: 'Wrong', session: 'shared-session'}]},
    {name: 'beta', agents: [{pool: 'beta', agent: 'Bob', session: 'shared-session'}]},
  ];
  const adminMessages = [{pool: 'archived', message_id: 'admin-1', from: 'Alice',
    message: 'admin message', created_ms: 2}];
  const result = messages([start], new Map(), pools, adminMessages);
  assert.equal(result.length, 2);
  assert.equal(result.find(m => m.kind === 'message.send').from, 'Bob');
  const receipt = result.find(m => m.kind === 'message.receive');
  assert.equal(receipt.pool, 'archived');
  assert.equal(receipt.to, 'admin');
  assert.equal(receipt.status, 'Received');
});

test('event models correlate start and finish pools, agents, recipients and leave actions', () => {
  const start = event(1, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'alpha', target: 'global', message: 'hello'},
  }, {agents: [{pool: 'alpha', agent: 'Alice'}]});
  const finish = event(2, 'tool.finish', {
    start_event_id: 1,
    result: {
      value: {assigned_agent: 'Alice', recipients: ['Bob', 'Cara']},
      peer_messages: [{pool: 'historic', from: 'Dana', to: 'Eve'}],
    },
  });
  const starts = new Map([[start.id, start]]);
  const finishes = new Map([[start.id, finish]]);
  assert.deepEqual([...eventPools(finish, starts)].sort(), ['alpha', 'historic']);
  assert.deepEqual(
    new Set(eventIdentities(start, starts, finishes).map(a => `${a.pool}:${a.agent}`)),
    new Set(['alpha:Alice', 'alpha:Bob', 'alpha:Cara']),
  );
  assert.deepEqual(
    new Set(eventIdentities(finish, starts, finishes).map(a => `${a.pool}:${a.agent}`)),
    new Set(['alpha:Alice', 'alpha:Bob', 'alpha:Cara', 'historic:Dana', 'historic:Eve']),
  );
  assert.equal(isMessageEvent(start, starts), true);
  assert.equal(isMessageEvent(finish, starts), true);
  assert.equal(isMessageEvent(event(3, 'admin.message.send'), starts), true);
  assert.equal(isMessageEvent(event(4, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'alpha', action: 'leave', message: 'not sent'},
  }), starts), false);
});

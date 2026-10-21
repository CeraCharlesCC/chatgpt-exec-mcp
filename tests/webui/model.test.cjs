const {test, before} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');

let eventPools, isMessageEvent, eventIdentities, pageEventContext, messages, coalesceMessages;
let normalizeEvents, eventStartId;
before(async () => {
  ({eventPools, isMessageEvent, eventIdentities, pageEventContext, normalizeEvents, eventStartId} = await import('../../src/webui/model/events.js'));
  ({messages, coalesceMessages} = await import('../../src/webui/model/messages.js'));
});

test('shared Rust serialized fixture normalizes versioned and historical sends at ingress', () => {
  const {events: wireEvents} = JSON.parse(readFileSync(join(__dirname, '../fixtures/activity-events.json')));
  const events = normalizeEvents(wireEvents);
  const [typedStart, typedFinish, legacyStart, legacyFinish] = events;
  assert.equal(eventStartId(typedFinish), 100, 'typed correlation overrides legacy detail');
  assert.deepEqual([...eventPools(typedFinish, new Map())], ['beta']);
  assert.deepEqual([...eventPools(legacyStart, new Map())], ['old']);
  assert.equal(events[0], normalizeEvents([events[0]])[0], 'an event is normalized only once');
  const typed = pageEventContext([typedFinish], [typedStart]);
  const typedRows = messages(typed.messageEvents, typed.finishes, [], [], typed.starts);
  assert.equal(typedRows.find(m => m.kind === 'message.send')?.message, 'typed body');
  assert.equal(typedRows.find(m => m.kind === 'message.send')?.message_id, 'message-100');
  assert.equal(typedRows.find(m => m.kind === 'message.receive')?.message, 'typed incoming');
  assert.equal(typedRows.find(m => m.kind === 'message.receive')?.sent_at_ms, 1001);
  assert.equal(typedFinish.semantic.deliveryReceipts[0].offered_at_ms, 1001);
  assert.deepEqual([...new Set(eventIdentities(typedFinish, typed.starts, typed.finishes)
    .filter(a => a.pool === 'beta').map(a => a.agent))].sort(), ['Alice', 'Bob', 'Cara']);
  const older = pageEventContext([legacyStart], [legacyFinish]);
  const oldRows = messages(older.messageEvents, older.finishes, [], [], older.starts);
  assert.equal(oldRows[0].message, 'legacy body');
  assert.equal(oldRows[0].message_id, 'legacy-201');
});

const event = (id, kind, detail = {}, other = {}) => ({
  id, kind, detail, timestamp_ms: id, agents: [], session: null, ...other,
});

test('legacy nested result communications and errors normalize without reconstructing delivery in views', () => {
  const peer = {pool: 'alpha', message_id: 'nested', from: 'Alice', to: 'Bob', message: 'nested offer'};
  const receipt = {pool: 'alpha', message_id: 'nested', to: 'Alice', status: 'offered'};
  const nested = event(92, 'tool.finish', {tool: 'pool_members', result: {value: {
    agents: ['Alice'], messages: [peer], delivery_receipts: [receipt],
  }}});
  assert.equal(isMessageEvent(nested, new Map()), true);
  assert.deepEqual([...eventPools(nested, new Map())], ['alpha']);
  assert.deepEqual([...new Set(eventIdentities(nested, new Map(), new Map()).map(a => a.agent))],
    ['Alice', 'Bob']);
  assert.deepEqual(messages([nested], new Map(), [], []).map(m => m.message), ['nested offer']);
  const failure = event(94, 'tool.error', {tool: 'pool_send', start_event_id: 93,
    result: {value: {error: 'recipient left'}}});
  const start = event(93, 'tool.start', {tool: 'pool_send', arguments: {
    pool: 'alpha', target: 'Bob', message: 'failed send',
  }});
  const rows = messages([start, failure], new Map([[93, failure]]), [], []);
  assert.equal(rows.find(m => m.kind === 'message.send')?.status, 'Failed: recipient left');
  const empty = event(96, 'tool.finish', {tool: 'pool_wait', result: {
    messages: [], value: {messages: [peer]},
  }});
  assert.deepEqual(messages([empty], new Map(), [], []), [], 'an explicit empty messages array is authoritative');
});

test('top-level messages from every response appear in conversations and receipts retain filter identities', () => {
  const peer = {pool: 'alpha', message_id: 'msg-wait', from: 'Alice', to: 'Bob', message: 'done'};
  for (const tool of ['exec_command', 'write_stdin', 'pool_members', 'pool_send', 'pool_wait']) {
    const received = event(1, 'tool.finish', {
      tool, result: {messages: [peer]},
    }, {session: 'bob-session'});
    assert.equal(isMessageEvent(received, new Map()), true, tool);
    assert.deepEqual([...eventPools(received, new Map())], ['alpha'], tool);
    assert.deepEqual(messages([received], new Map(), [], []).map(m => m.message), ['done'], tool);
    assert.deepEqual(eventIdentities(received, new Map(), new Map()), [
      {pool: 'alpha', agent: 'Alice'}, {pool: 'alpha', agent: 'Bob'},
    ], tool);
  }
  const receipt = event(2, 'tool.finish', {tool: 'exec_command', result: {
    delivery_receipts: [{pool: 'archived', to: 'Cara', message_id: 'old', status: 'offered'}],
  }});
  assert.deepEqual([...eventPools(receipt, new Map())], ['archived']);
  assert.deepEqual(eventIdentities(receipt, new Map(), new Map()), [{pool: 'archived', agent: 'Cara'}]);
  // Receipt-only calls stay in tool activity, with their structured details;
  // they are not fabricated peer messages or hidden from both views.
  assert.equal(isMessageEvent(receipt, new Map()), false);
  assert.deepEqual(messages([receipt], new Map(), [], []), []);
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
    event(1, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-1'}),
    event(2, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-1'}),
    event(3, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-2'}),
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
    event(12, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-1'}),
    event(13, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-2'}),
    event(14, 'tool.finish', {result: {messages: [peer]}}, {session: 'bob-1'}),
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
    event(1, 'tool.finish', {result: {messages: [peer]}}, {
      agents: [{pool: 'old-pool', agent: 'Bob'}], session: 'bob-session',
    }),
    event(2, 'tool.finish', {result: {messages: [peer]}}, {
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
      messages: [{pool: 'alpha', message_id: 'msg-1', from: 'Alice', to: 'Bob', message: 'hello'}],
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
      messages: [{pool: 'historic', from: 'Dana', to: 'Eve'}],
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

test('page correlation enriches selected sends without projecting off-page events or unrelated deliveries', () => {
  const start = event(10, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'beta', target: 'Bob', message: 'cross-page message'},
  }, {session: 'sender-session'});
  const finish = event(11, 'tool.finish', {
    tool: 'pool_send', start_event_id: 10,
    result: {value: {assigned_agent: 'Alice', message_id: 'msg-10', recipients: ['Bob']}},
  });
  const unrelated = event(12, 'tool.finish', {result: {messages: [{
    pool: 'beta', from: 'Ghost', to: 'Bob', message_id: 'ghost', message: 'not on this page',
  }]}});

  for (const [selected, counterparts] of [
    [[start], [finish, unrelated]],
    [[finish], [start, unrelated]],
  ]) {
    const context = pageEventContext(selected, counterparts);
    assert.deepEqual(context.messageEvents.map(e => e.id),
      selected[0] === start ? [start.id] : [finish.id, start.id],
      'the selected finish may pull its off-page start, but off-page finishes are context only');
    assert.equal(context.starts.get(start.id)?.id, start.id);
    assert.equal(context.finishes.get(start.id)?.id, finish.id);
    const rows = messages(context.messageEvents, context.finishes, [], []);
    assert.deepEqual(rows.map(row => row.message), ['cross-page message']);
    assert.match(rows[0].status, /Queued for 1 recipient/);
  }
});

test('v1 lifecycle sends are authoritative and finish-only events render without legacy detail or a correlated start', () => {
  const typedStart = event(40, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'wrong', target: 'Nobody', message: 'spoofed legacy detail'},
  }, {schema_version: 1, session: 'sender', lifecycle: {
    type: 'message_send', phase: 'start', tool: 'pool_send',
    message: {pool: 'beta', target: 'Bob', body: 'typed body', status: 'sending'},
  }});
  const typedFinish = event(41, 'tool.finish', {
    start_event_id: 999, result: {value: {message_id: 'wrong', recipients: ['Nobody']}},
  }, {schema_version: 1, session: 'recipient', lifecycle: {
    type: 'message_send', phase: 'finish', tool: 'pool_send', start_event_id: 40,
    message: {pool: 'beta', target: 'Bob', body: 'typed body',
      from: 'Alice', message_id: 'correct', recipients: ['Bob'], status: 'queued'},
  }});

  const finishOnly = pageEventContext([typedFinish], []);
  const shown = messages(finishOnly.messageEvents, finishOnly.finishes, [], [], finishOnly.starts);
  assert.equal(shown.length, 1);
  assert.equal(shown[0].key, 'send:40');
  assert.equal(shown[0].message, 'typed body');
  assert.equal(shown[0].from, 'Alice');
  assert.equal(shown[0].message_id, 'correct');
  assert.equal(shown[0].status, 'Queued for 1 recipient(s)');
  assert.deepEqual([...eventPools(typedFinish, finishOnly.starts)], ['beta']);
  assert.deepEqual(new Set(eventIdentities(typedFinish, finishOnly.starts, finishOnly.finishes)
    .map(a => a.agent)), new Set(['Alice', 'Bob']));

  const startOnly = pageEventContext([typedStart], [typedFinish]);
  assert.deepEqual(messages(startOnly.messageEvents, startOnly.finishes, [], [], startOnly.starts)
    .map(m => m.status), ['Queued for 1 recipient(s)']);
  assert.deepEqual([...eventPools(typedStart, startOnly.starts)], ['beta']);

  const failed = event(42, 'tool.error', {result: {value: {recipients: ['Nobody']}}}, {
    schema_version: 1, lifecycle: {type: 'message_send', phase: 'error', tool: 'pool_send',
      start_event_id: 40, message: {pool: 'beta', target: 'Bob', body: 'typed body',
        status: 'failed', error: 'recipient unavailable'}},
  });
  const failedRows = messages([failed], new Map([[40, failed]]), [], []);
  assert.equal(failedRows[0].status, 'Failed: recipient unavailable');
  assert.equal(failedRows[0].message, 'typed body');

  const legacyLookingV1 = event(50, 'tool.start', {
    tool: 'pool_send', arguments: {pool: 'wrong', target: 'Nobody', message: 'not authoritative'},
  }, {schema_version: 1, lifecycle: {type: 'tool', phase: 'start', tool: 'pool_send'}});
  assert.equal(isMessageEvent(legacyLookingV1, new Map()), false);
  assert.deepEqual(messages([legacyLookingV1], new Map(), [], []), []);
});

test('v1 typed peer messages and receipts supersede legacy detail for messages and filters', () => {
  const typedPeer = {pool: 'beta', message_id: 'received', from: 'Alice', to: 'Bob', message: 'typed received'};
  const legacyPeer = {pool: 'wrong', message_id: 'old', from: 'Ghost', to: 'Nobody', message: 'legacy received'};
  const e = event(61, 'tool.finish', {
    tool: 'pool_wait', result: {messages: [legacyPeer],
      delivery_receipts: [{pool: 'wrong', to: 'Nobody'}]},
  }, {schema_version: 1, session: 'bob-session', lifecycle: {
    type: 'tool', phase: 'finish', tool: 'pool_wait', start_event_id: 60,
    messages: [typedPeer], delivery_receipts: [{pool: 'beta', to: 'Cara'}],
  }});
  assert.deepEqual(messages([e], new Map(), [], []).map(m => m.message), ['typed received']);
  assert.deepEqual([...eventPools(e, new Map())], ['beta']);
  assert.deepEqual(new Set(eventIdentities(e, new Map(), new Map()).map(x => x.agent)),
    new Set(['Alice', 'Bob', 'Cara']));
  const noTypedPeers = {...e, lifecycle: {...e.lifecycle, messages: [], delivery_receipts: []}};
  assert.equal(isMessageEvent(noTypedPeers, new Map()), false);
  assert.deepEqual(messages([noTypedPeers], new Map(), [], []), []);
});

test('v1 non-message pool tools and leave events retain pool filters without parsing legacy arguments', () => {
  const waitStart = event(70, 'tool.start', {arguments: {pool: 'wrong'}}, {
    schema_version: 1, lifecycle: {type: 'tool', phase: 'start', tool: 'pool_wait', pool: 'beta'},
  });
  const waitFinish = event(71, 'tool.finish', {start_event_id: 999}, {
    schema_version: 1, lifecycle: {type: 'tool', phase: 'finish', tool: 'pool_wait',
      start_event_id: 70, pool: 'beta'},
  });
  const leave = event(72, 'tool.start', {tool: 'pool_send', arguments: {
    pool: 'wrong', action: 'leave', message: 'not a send',
  }}, {schema_version: 1, lifecycle: {type: 'tool', phase: 'start', tool: 'pool_send', pool: 'gamma'}});
  assert.deepEqual([...eventPools(waitStart, new Map())], ['beta']);
  assert.deepEqual([...eventPools(waitFinish, new Map())], ['beta']);
  assert.deepEqual([...eventPools(leave, new Map())], ['gamma']);
  assert.equal(isMessageEvent(leave, new Map()), false);
  assert.deepEqual(messages([leave], new Map(), [], []), []);

  // Admin messages have no tool lifecycle even when emitted with schema v1;
  // their detail payload is still the agreed source of pool identity.
  const admin = event(73, 'admin.message.send', {
    pool: 'old-admin-pool', target: 'global', message: 'announcement', delivery_count: 0,
  }, {schema_version: 1});
  assert.deepEqual([...eventPools(admin, new Map())], ['old-admin-pool']);
  assert.equal(messages([admin], new Map(), [], [])[0].pool, 'old-admin-pool');
});

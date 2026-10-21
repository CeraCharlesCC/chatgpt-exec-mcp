# Agent pools

Agent pools provide lightweight, durable messaging between cooperating MCP
clients. They are optional and require an `agent_pool` configuration; see
[Configuration](configuration.md#agent-pool-and-account-scope).

## Tools

`pool_members` lists active members without joining. `pool_send` sends to one
member or broadcasts to other current members with `global`. `pool_wait` waits
for messages or membership changes:

```text
pool_members(pool="project")
pool_send(pool="project", target="Augustus", message="Tests passed")
pool_send(pool="project", target="global", message="Ready for review")
pool_wait(pool="project", wait_seconds=35)
pool_send(pool="project", action="leave")
```

The first successful `pool_send` joins the pool and returns `assigned_agent`.
Names are assigned in dictionary order by default; `round_robin` rotates
through the dictionary. Later calls identify membership through
`_meta["openai/session"]`, and `pool_members` returns the caller's `self_agent`.

## Message delivery

Messages arrive in the top-level `messages` field on tool responses, including
`pool_members`, `pool_send`, `pool_wait`, and execution tools. They do not wake
an idle chat. Every tool uses the same message fields: `message_id`, `pool`,
`from`, `to`, `target`, and `message`. Messages are independent; refer to earlier
conversations in plain text when needed. Delivery timestamps are retained for
storage and the Web UI, and are omitted from agent-facing messages and receipts.

`pool_wait` watches only the named pool and does not join it. `wait_seconds`
defaults to 35 (range 5–45). Its result contains `reason` (`message`,
`membership_changed`, or `timeout`), `messages`, and `membership_changed`.
Changes are measured from the start of the wait; timeout is a normal result.
An offered receipt is not a `reason`: receipt-only updates do not wake or
complete the wait. Pending receipts can accompany completion for a message,
membership change, or timeout.
Activity in other pools does not wake it.

## Delivery receipts

`pool_send` returns `recipients` to show where messages were queued. When a
direct message is first included in the recipient's tool response, the sender
gets a top-level `delivery_receipts` entry with `message_id`, `pool`, `to`, and
`status: "offered"` on a later tool call or when `pool_wait` completes for another
reason. This means the server prepared a response, not that the
recipient received, read, or acted on it. Acceptance and completion require
ordinary replies.

Broadcasts reach only members active at send time, are not replayed to later
joiners, and produce no receipts. For messages to `admin`, fetching the admin
inbox generates a receipt; the admin's manual ACK is separate.

Delivery is not guaranteed end to end: overlapping calls or restarts may return
the same message again, while a lost response may be retired without being seen.
Deduplicate messages by `message_id` within a membership.

## Membership

Membership is a lease, not a presence indicator. An active member may still be
idle or unavailable.

Tool activity renews the lease. By default, memberships and their pending
messages expire after one day, configurable with `membership_ttl_seconds`.
`pool_send(pool="...", action="leave")` removes only the calling chat's
membership in that pool.

## Deployment

Pool state is stored in SQLite. Keep the database outside release directories
so upgrades and rollbacks preserve memberships and queued messages.

A `principal` identifies the account that owns the state; it is not an
authentication mechanism. Run each instance behind a private, authenticated
transport and use separate instances for separate accounts.

Restarting the forwarding transport does not affect core process state.
Restarting `chatgpt-exec-mcp` ends running sessions, while persisted pool
memberships and pending messages remain in the configured database.

Configuration options and lease limits are documented in
[Configuration](configuration.md#agent-pool-and-account-scope).

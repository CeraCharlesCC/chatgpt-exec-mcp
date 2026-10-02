# Agent pools

Agent pools provide lightweight, durable messaging between cooperating MCP
clients. They are optional and require an `agent_pool` configuration; see
[Configuration](configuration.md#agent-pool-and-account-scope).

## Tools

`pool_members` lists active members of a pool. It can also wait for a message or
membership change:

```text
pool_members(pool="project")
pool_members(pool="project", wait_seconds=35)
```

`pool_send` sends to one member or broadcasts to all other current members with
`global`:

```text
pool_send(pool="project", target="Augustus", message="Tests passed")
pool_send(pool="project", target="global", message="Ready for review")
pool_send(pool="project", action="leave")
```

The first send from a chat joins the pool and assigns an agent name. Later calls
identify that membership through `_meta["openai/session"]`.
The first send returns `assigned_agent`; after joining, `pool_members` returns
the caller's name as `self_agent`.

## Message delivery

Messages are returned as `peer_messages` on the recipient's next tool call.
They do not wake an idle chat.

Delivery is at least once. A message returned to a session is acknowledged by
that session's next tool call, so a message can be returned again if no later
call acknowledges it.

A send result includes `recipients`, which identifies the inboxes where the
message was queued. It is not confirmation that a recipient read, accepted, or
completed the request.

Broadcasts reach only members that are active when the message is sent. They are
not replayed to agents that join later.

Each peer message includes its `message_id`, pool, sender, actual recipient,
original target, send time, message text, and optional `in_reply_to`.

## Membership

Membership is a lease, not a presence indicator. An active member has an
unexpired lease; it may still be idle or unavailable.

Ordinary tool activity renews the lease. By default, inactive memberships and
their pending messages expire after one day. The lifetime is configurable with
`membership_ttl_seconds`.

`pool_send(pool="...", action="leave")` removes only the calling chat's
membership in that pool.

`wait_seconds` accepts 5–45 seconds. A waiting `pool_members` returns for a
message to the session in any joined pool, a membership change in the named
pool, or timeout. Waiting does not acknowledge messages by itself.

## Deployment

Pool state is stored in SQLite. Keep the database outside release directories so
upgrades and rollbacks do not discard memberships or queued messages.

A `principal` identifies the account that owns the pool state; it is not an
authentication mechanism. Run each instance behind a private, authenticated
transport and use separate instances for separate accounts.

Restarting the transport in front of the server does not affect core process
state. Restarting `chatgpt-exec-mcp` ends live PTYs, while persisted pool
memberships and pending messages remain in the configured database.

Configuration options, file permissions, lease limits, and agent-name rules are
documented in [Configuration](configuration.md#agent-pool-and-account-scope).

import { $, esc, interacting, syncList, updateTimes, syncOptions } from '../dom.js';

export function renderPools(state) {
  const root = $('#pools'), pools = state.data?.pools || [];
  if (!interacting(root)) {
    // Pool wrappers and agent rows are keyed separately, so changing a lease
    // does not replace the pool title or a focused Terminate button.
    syncList(root, pools, p => p.name, p => `<div class="pool-title"><button class="pool-filter" data-pool="${esc(p.name)}" aria-pressed="false" title="Show activity from ${esc(p.name)}">${esc(p.name)}</button><span class="muted pool-count"></span></div><div class="pool-agents"></div>`, 'No active agents', 'pool');
    for (const box of root.children) {
      const p = pools.find(p => p.name === box.dataset.key);
      if (!p) continue;
      const button = box.querySelector('.pool-filter');
      button.setAttribute('aria-pressed', String(state.pool === p.name));
      box.querySelector('.pool-count').textContent = p.agents.length;
      syncList(box.querySelector('.pool-agents'), p.agents, a => a.agent, a => `<div><div class="agent-name"><span class="tiny-dot"></span><strong>${esc(a.agent)}</strong></div><div class="agent-meta">seen <span data-clock="ago"></span> · lease <span data-clock="left"></span></div></div><button class="danger" data-pool="${esc(p.name)}" data-agent="${esc(a.agent)}">Terminate</button>`, '', 'agent');
      for (const row of box.querySelector('.pool-agents').children) {
        const agent = p.agents.find(a => a.agent === row.dataset.key);
        if (!agent) continue;
        row.querySelector('[data-clock=ago]').dataset.time = agent.last_seen_ms;
        row.querySelector('[data-clock=left]').dataset.time = agent.expires_ms;
      }
    }
    updateTimes(root);
  }
  renderTargets(state);
}
export function renderTargets(state) {
  const pools = state.data?.pools || [], ps = $('#poolSelect'), ts = $('#targetSelect');
  syncOptions(ps, pools.map(p => ({value: p.name, label: p.name})), ps.value);
  const p = pools.find(x => x.name === ps.value);
  syncOptions(ts, ['global', ...(p?.agents || []).map(a => a.agent)].map(value => ({value, label: value})), ts.value);
}

export function bindPools({state, terminate, selectPool}) {
  $('#poolSelect').onchange = () => renderTargets(state);
  $('#pools').onclick = event => {
    const button = event.target.closest('button');
    if (button?.classList.contains('danger')) terminate(button.dataset.pool, button.dataset.agent);
    if (button?.classList.contains('pool-filter')) {
      selectPool(state.pool === button.dataset.pool ? '' : button.dataset.pool);
    }
  };
}

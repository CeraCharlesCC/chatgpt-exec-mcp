export const $ = s => document.querySelector(s);
export const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
export function ago(ms) {
  const s = Math.floor(Math.max(0, Date.now() - ms) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  return Math.floor(s / 3600) + 'h ago';
}
export function left(ms) {
  const s = Math.max(0, Math.floor((ms - Date.now()) / 1000));
  if (s < 60) return s + 's remaining';
  if (s < 3600) return Math.floor(s / 60) + 'm remaining';
  return Math.floor(s / 3600) + 'h remaining';
}

// Keep immutable rows in the DOM. Replacing the list on every poll destroys
// nested scroll positions, focus and the browser's text-selection anchors.
let pointerTarget = null;
// Keep pointer interactions in the same module as the selection guard.
export function setPointerTarget(target) { pointerTarget = target; }
export function clearPointerTarget() { pointerTarget = null; }
export function hasPointerTarget() { return !!pointerTarget; }
export function interacting(root) {
  const selection = window.getSelection();
  return (pointerTarget && root.contains(pointerTarget)) ||
    (selection && !selection.isCollapsed &&
      (root.contains(selection.anchorNode) || root.contains(selection.focusNode)));
}
export function syncList(root, items, key, markup, emptyText, className = '', update = (node, html) => { node.innerHTML = html; }) {
  if (interacting(root)) return;
  const bounds = root.getBoundingClientRect();
  const anchor = root.scrollTop > 0 && [...root.children].find(node =>
    node.dataset.key && node.getBoundingClientRect().bottom > bounds.top);
  const anchorTop = anchor ? anchor.getBoundingClientRect().top : 0;
  const nodes = new Map([...root.children].map(node => [node.dataset.key, node]));
  const keep = new Set();
  let cursor = root.firstElementChild;
  for (const item of items) {
    const id = String(key(item));
    keep.add(id);
    let node = nodes.get(id);
    if (!node) {
      node = document.createElement('div');
      node.dataset.key = id;
    }
    const html = markup(item);
    if (node._markup !== html) {
      node.className = className;
      update(node, html);
      node._markup = html;
    }
    if (node !== cursor) root.insertBefore(node, cursor);
    cursor = node.nextElementSibling;
  }
  for (const node of [...root.children]) {
    if (!keep.has(node.dataset.key)) node.remove();
  }
  if (!items.length && !root.firstElementChild) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = emptyText;
    root.appendChild(empty);
  }
  updateTimes(root);
  if (anchor && root.contains(anchor)) {
    root.scrollTop += anchor.getBoundingClientRect().top - anchorTop;
  }
}
export function updateTimes(root) {
  if (interacting(root)) return;
  root.querySelectorAll('[data-time]').forEach(node => {
    const value = (node.dataset.clock === 'left' ? left : ago)(Number(node.dataset.time));
    if (node.textContent !== value) node.textContent = value;
  });
}
export function time(ms, className = 'event-time', clock = 'ago') {
  return `<span class="${className}" data-time="${Number(ms)}" data-clock="${clock}"></span>`;
}

export function syncOptions(select, values, selected) {
  // Native selects must also survive polling while their menu is open.
  const signature = JSON.stringify(values);
  if (select._options !== signature) {
    select.replaceChildren(...values.map(({value, label}) => new Option(label, value)));
    select._options = signature;
  }
  if (values.some(x => x.value === selected)) select.value = selected;
}

export function clearSelection() { window.getSelection()?.removeAllRanges(); }

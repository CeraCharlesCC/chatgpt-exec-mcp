import { $ } from '../dom.js';

export function toast(msg, bad = false) {
  const t = $('#toast');
  t.textContent = msg;
  t.className = 'toast show' + (bad ? ' bad' : '');
  clearTimeout(t._x);
  t._x = setTimeout(() => t.className = 'toast', 2200);
}
export function setCover(open, state) {
  if (open && state.clearing) return;
  $('#clearControl').classList.toggle('cover-open', open);
  const cover = $('#clearCover'), button = $('#clearHistory');
  cover.setAttribute('aria-expanded', String(open));
  cover.setAttribute('aria-label', open ? 'Close safety cover' : 'Open safety cover');
  button.disabled = !open || state.clearing;
  if (!open && document.activeElement === button) {
    (cover.disabled ? $('#menuClose') : cover).focus();
  }
  $('#clearHint').textContent = state.clearing ? 'Clearing history…' :
    open ? 'Press CLEAR to erase history.' : 'Lift the cover to clear history.';
}
export function setMenu(open, state) {
  $('#menuToggle').setAttribute('aria-expanded', String(open));
  $('#menuToggle').setAttribute('aria-label', open ? 'Close console menu' : 'Open console menu');
  $('#consoleMenu').classList.toggle('open', open);
  $('#consoleMenu').inert = !open;
  $('#menuBackdrop').classList.toggle('open', open);
  $('main').inert = open;
  if (!open) setCover(false, state);
  (open ? $('#menuClose') : $('#menuToggle')).focus();
}
export function connection(connected) {
  const node = $('#connectionStatus'), label = connected ? 'Connected' : 'Disconnected';
  node.dataset.connection = connected ? 'connected' : 'disconnected';
  node.setAttribute('aria-label', label);
  node.title = label;
}

export function bindMenu({state, clearHistory}) {
  $('#menuToggle').onclick = () => setMenu($('#menuToggle').getAttribute('aria-expanded') !== 'true', state);
  $('#menuClose').onclick = $('#menuBackdrop').onclick = () => setMenu(false, state);
  $('#clearCover').onclick = () => setCover($('#clearCover').getAttribute('aria-expanded') !== 'true', state);
  $('#clearHistory').onclick = clearHistory;
  document.addEventListener('keydown', event => {
    if ($('#menuToggle').getAttribute('aria-expanded') !== 'true') return;
    if (event.key === 'Escape') { event.preventDefault(); setMenu(false, state); }
    if (event.key === 'Tab') {
      const buttons = [...$('#consoleMenu').querySelectorAll('button:not(:disabled)'), $('#menuToggle')];
      const next = buttons.indexOf(document.activeElement) + (event.shiftKey ? -1 : 1);
      event.preventDefault();
      buttons[(next + buttons.length) % buttons.length].focus();
    }
  });
}

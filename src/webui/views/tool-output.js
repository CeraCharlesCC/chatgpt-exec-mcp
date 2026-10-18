import { Prism } from '../vendor/prism.js';
import { esc } from '../dom.js';

const jsonText = value => JSON.stringify(value, null, 2) ?? 'null';
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const highlightCache = new Map();
function highlight(text, language) {
  // Large previews remain complete, but avoid expensive tokenization on every poll.
  if (!language || text.length > 65536) return esc(text);
  const key = language + ':' + text;
  if (highlightCache.has(key)) return highlightCache.get(key);
  try {
    const html = Prism.highlight(text, Prism.languages[language], language);
    if (highlightCache.size >= 128) highlightCache.delete(highlightCache.keys().next().value);
    highlightCache.set(key, html);
    return html;
  }
  catch { return esc(text); }
}
function codeBlock(label, text, language = '', error = false, original = null) {
  return `<section class="code-block${error ? ' tool-error' : ''}" data-copy-text="${esc(JSON.stringify(original ?? text))}"><div class="code-head"><span>${esc(label)}</span><span class="code-actions"><button type="button" data-code-action="expand" aria-expanded="false" aria-label="Expand ${esc(label)}">Expand</button><button type="button" data-code-action="copy" aria-label="Copy ${esc(label)}">Copy</button></span></div><pre><code${language ? ` class="language-${language}"` : ''}>${highlight(text, language)}</code></pre></section>`;
}
function outputBlock(output) {
  // Copy retains the exact output, including original JSON whitespace.
  let text = output, language = '';
  if (output.trim() && output.length <= 65536) {
    try { text = jsonText(JSON.parse(output)); language = 'json'; } catch {}
  }
  return codeBlock('Output', text, language, false, language ? output : null);
}
export function toolDetail(e, format) {
  const d = e.detail;
  if (!format) return codeBlock('Raw JSON', jsonText(d));
  if (!record(d) || !['tool.start', 'tool.finish', 'tool.error'].includes(e.kind)) {
    return codeBlock('Details', jsonText(d), 'json');
  }
  let html = `<div class="tool-name">${esc(d.tool || e.kind)}${d.start_event_id !== undefined ? ` <span class="chip">Start event: ${esc(d.start_event_id)}</span>` : ''}</div>`;
  const rest = {...d};
  delete rest.tool;
  delete rest.start_event_id;
  if (e.kind === 'tool.start' && record(d.arguments)) {
    const args = {...d.arguments};
    if (typeof args.cmd === 'string') {
      html += codeBlock('Command', args.cmd, 'bash');
      delete args.cmd;
    }
    if (Object.keys(args).length) html += codeBlock('Arguments', jsonText(args), 'json');
    delete rest.arguments;
  } else if (e.kind !== 'tool.start' && record(d.result)) {
    const result = {...d.result}, meta = [];
    if (result.exit_code !== undefined && result.exit_code !== null) {
      meta.push(`<span class="chip${result.exit_code !== 0 ? ' failed' : ''}">Exit code: ${esc(result.exit_code)}</span>`);
      delete result.exit_code;
    }
    if (typeof result.call_wall_time_seconds === 'number') {
      meta.push(`<span class="chip">Execution time: ${esc(result.call_wall_time_seconds)} s</span>`);
      delete result.call_wall_time_seconds;
    }
    if (result.session_id !== undefined && result.session_id !== null) {
      meta.push(`<span class="chip">Process session: ${esc(result.session_id)}</span>`);
      delete result.session_id;
    }
    if (meta.length) html += `<div class="tool-meta">${meta.join('')}</div>`;
    if (result.error !== undefined) {
      html += codeBlock('Error', typeof result.error === 'string' ? result.error : jsonText(result.error), '', true);
      delete result.error;
    }
    if (typeof result.output === 'string') {
      html += outputBlock(result.output);
      delete result.output;
    }
    if (Object.keys(result).length) html += codeBlock('Result details', jsonText(result), 'json', e.kind === 'tool.error');
    delete rest.result;
  }
  // Keep unfamiliar fields and correlation IDs available without duplicating output.
  if (Object.keys(rest).length) html += codeBlock('Event details', jsonText(rest), 'json', e.kind === 'tool.error');
  return html;
}

export async function handleCodeAction(event, toast) {
  const button = event.target.closest('button[data-code-action]');
  if (!button) return;
  const block = button.closest('.code-block');
  if (button.dataset.codeAction === 'expand') {
    const expanded = block.classList.toggle('expanded');
    button.setAttribute('aria-expanded', String(expanded));
    const label = block.querySelector('.code-head > span').textContent;
    button.setAttribute('aria-label', `${expanded ? 'Collapse' : 'Expand'} ${label}`);
    button.textContent = expanded ? 'Collapse' : 'Expand';
  } else {
    try {
      await navigator.clipboard.writeText(JSON.parse(block.dataset.copyText));
      toast('Copied');
    } catch { toast('Copy failed. Select the text and copy it manually.', true); }
  }
}

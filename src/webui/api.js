// Requests return the original Response so app.js owns errors and UI state.
let csrfToken = '';
export const setCsrfToken = token => { csrfToken = token; };
export const fetchSnapshot = (cursor = null) => fetch(`/_admin/api/snapshot${cursor === null ? '' : `?${new URLSearchParams({cursor})}`}`, {cache: 'no-store'});
export const fetchEvents = (beforeId, limit = 200) => fetch(`/_admin/api/events?${new URLSearchParams({before_id: String(beforeId), limit: String(limit)})}`, {cache: 'no-store'});
const adminPost = (endpoint, body) => fetch(`/_admin/api/${endpoint}`, {method: 'POST', headers: {'content-type': 'application/json', 'x-webui-csrf-token': csrfToken}, body: JSON.stringify(body)});
export const sendAdminMessage = (pool, target, message) => adminPost('send', {pool, target, message});
export const terminateAgent = (pool, agent) => adminPost('terminate', {pool, agent});
export const ackAdminMessage = messageId => adminPost('ack', {message_id: messageId});
export const clearAdminHistory = () => adminPost('clear', {});

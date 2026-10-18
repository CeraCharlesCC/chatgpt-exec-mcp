// Requests return the original Response so app.js owns errors and UI state.
export const fetchSnapshot = () => fetch('/_admin/api/snapshot', {cache: 'no-store'});
export const sendAdminMessage = (pool, target, message) => fetch('/_admin/api/send', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({pool, target, message})});
export const terminateAgent = (pool, agent) => fetch('/_admin/api/terminate', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({pool, agent})});
export const ackAdminMessage = messageId => fetch('/_admin/api/ack', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({message_id: messageId})});
export const clearAdminHistory = () => fetch('/_admin/api/clear', {method: 'POST', headers: {'content-type': 'application/json'}, body: '{}'});

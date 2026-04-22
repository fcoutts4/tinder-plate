import { dispatch } from '../lib/db.js';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { action, payload } = req.body || {};

  if (!action) return res.status(400).json({ ok: false, error: 'No action specified' });

  try {
    const result = await dispatch(action, payload);
    return res.json(result);
  } catch (e) {
    console.error('Dispatch error:', action, e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}

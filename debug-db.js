// api/debug-db.js
import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

export default async function handler(req, res) {
  try {
    const now = await pool.query("select now() as now");
    const tables = await pool.query(`
      select table_schema, table_name
      from information_schema.tables
      where table_type = 'BASE TABLE'
      order by table_schema, table_name
      limit 50
    `);

    return res.status(200).json({
      ok: true,
      now: now.rows,
      tables: tables.rows
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e.message,
      stack: e.stack
    });
  }
}
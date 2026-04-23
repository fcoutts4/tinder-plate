/**
 * seed.mjs — Imports data from "Tinder Plate.xlsx" into Neon PostgreSQL.
 * Usage: node --env-file=.env.local scripts/seed.mjs
 */
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { neon } from '@neondatabase/serverless';
import xlsx from 'xlsx';
import bcrypt from 'bcryptjs';
import { randomUUID } from 'crypto';

function generateId() {
  return randomUUID().substring(0, 8).toUpperCase();
}

function slugify(str) {
  return String(str || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 40) || 'equipo';
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const XLSX_PATH = join(__dirname, '..', 'Tinder Plate.xlsx');

const sql = neon(process.env.DATABASE_URL);

// ── Helpers ────────────────────────────────────────────────
function norm(v) {
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

function normBool(v) {
  if (v === true || v === 1) return true;
  if (typeof v === 'string') {
    const s = v.trim().toLowerCase();
    if (['true', '1', 'si', 'sí', 'yes'].includes(s)) return true;
  }
  return false;
}

function normDate(v) {
  if (!v) return null;
  if (v instanceof Date) {
    if (isNaN(v.getTime())) return null;
    return v.toISOString().split('T')[0];
  }
  const s = String(v).trim();
  if (!s) return null;
  // Excel serial numbers are already converted to Date by xlsx when cellDates:true
  try {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];
  } catch {}
  return null;
}

function normInt(v) {
  const n = parseInt(v, 10);
  return isNaN(n) ? 0 : n;
}

function sheetToObjects(ws) {
  const rows = xlsx.utils.sheet_to_json(ws, { defval: '', raw: false });
  return rows;
}

// ── Create tables ──────────────────────────────────────────
async function createTables() {
  console.log('Creating tables...');

  // Tabla maestra de equipos
  await sql`
    CREATE TABLE IF NOT EXISTS equipos (
      id                TEXT PRIMARY KEY,
      slug              TEXT UNIQUE NOT NULL,
      nombre            TEXT NOT NULL DEFAULT 'Mi Equipo',
      password_hash     TEXT NOT NULL DEFAULT '',
      color             TEXT DEFAULT '#16a34a',
      logo              TEXT DEFAULT '',
      liga              TEXT DEFAULT '',
      planilla_v2       TEXT DEFAULT '',
      modalidad         TEXT DEFAULT '11',
      jugadores_por_lado INTEGER DEFAULT 11,
      activo            BOOLEAN DEFAULT true
    )`;

  await sql`CREATE TABLE IF NOT EXISTS config (clave TEXT PRIMARY KEY, valor TEXT)`;
  await sql`
    CREATE TABLE IF NOT EXISTS campeonatos (
      id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
      ano TEXT, temporada TEXT, tipo TEXT DEFAULT 'Liga', equipo_id TEXT
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS partidos (
      id TEXT PRIMARY KEY, id_campeonato TEXT,
      fecha DATE, rival TEXT, cancha TEXT, ronda TEXT,
      gf INTEGER DEFAULT 0, gc INTEGER DEFAULT 0,
      notas TEXT, amistoso BOOLEAN DEFAULT false, equipo_id TEXT
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS jugadores (
      id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
      dorsal INTEGER DEFAULT 0, posicion TEXT, activo BOOLEAN DEFAULT true, equipo_id TEXT
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS asistencia (
      id_partido TEXT NOT NULL, id_jugador TEXT NOT NULL,
      nombre_jugador TEXT, presente BOOLEAN DEFAULT false,
      goles INTEGER DEFAULT 0, asistencias INTEGER DEFAULT 0,
      amarillas INTEGER DEFAULT 0, rojas INTEGER DEFAULT 0,
      equipo_id TEXT,
      PRIMARY KEY (id_partido, id_jugador)
    )`;

  // Migración idempotente para DBs existentes
  await sql`ALTER TABLE campeonatos ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
  await sql`ALTER TABLE partidos    ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
  await sql`ALTER TABLE jugadores   ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
  await sql`ALTER TABLE asistencia  ADD COLUMN IF NOT EXISTS equipo_id TEXT`;

  // Crear equipo por defecto desde config legacy si no existe ninguno
  const eqCount = await sql`SELECT COUNT(*)::int AS n FROM equipos`;
  if (eqCount[0].n === 0) {
    let cfg = {};
    try {
      const rows = await sql`SELECT clave, valor FROM config`;
      rows.forEach(r => { cfg[r.clave] = r.valor || ''; });
    } catch {}
    const hash = await bcrypt.hash('1234', 10);
    const defId = generateId();
    const nombre = cfg.nombre || 'Mi Equipo';
    const slug = slugify(nombre);
    await sql`
      INSERT INTO equipos (id, slug, nombre, password_hash, color, logo, liga, planilla_v2)
      VALUES (${defId}, ${slug}, ${nombre}, ${hash},
              ${cfg.color || '#16a34a'}, ${cfg.logo || ''},
              ${cfg.liga || ''}, ${cfg.planilla_v2 || ''})
    `;
    await sql`UPDATE campeonatos SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
    await sql`UPDATE partidos    SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
    await sql`UPDATE jugadores   SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
    await sql`UPDATE asistencia  SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
    console.log(`✓ Equipo por defecto creado: "${nombre}" (slug: ${slug}, pass: 1234)`);
  }

  console.log('✓ Tables ready');
}

// ── Seed functions ─────────────────────────────────────────
async function seedConfig(ws) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const clave = norm(row['Clave'] || row['clave'] || row['Key'] || row['key'] || '');
    const valor = norm(row['Valor'] || row['valor'] || row['Value'] || row['value'] || '');
    if (!clave || clave === 'planilla_v2') continue;
    await sql`
      INSERT INTO config (clave, valor) VALUES (${clave}, ${valor})
      ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
    `;
    count++;
  }
  console.log(`✓ Config: ${count} rows`);
}

async function seedCampeonatos(ws, equipoId) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const id = norm(row['ID'] || row['Id'] || row['id'] || '');
    const nombre = norm(row['Nombre'] || row['nombre'] || '');
    if (!id || !nombre) continue;
    const ano = norm(row['Año'] || row['Ano'] || row['ano'] || row['year'] || '');
    const temporada = norm(row['Temporada'] || row['temporada'] || '');
    const tipo = norm(row['Tipo'] || row['tipo'] || 'Liga') || 'Liga';
    await sql`
      INSERT INTO campeonatos (id, nombre, ano, temporada, tipo, equipo_id)
      VALUES (${id}, ${nombre}, ${ano}, ${temporada}, ${tipo}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        nombre=EXCLUDED.nombre, ano=EXCLUDED.ano,
        temporada=EXCLUDED.temporada, tipo=EXCLUDED.tipo
    `;
    count++;
  }
  console.log(`✓ Campeonatos: ${count} rows`);
}

async function seedPartidos(ws, equipoId) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const id = norm(row['ID'] || row['Id'] || row['id'] || '');
    const rival = norm(row['Rival'] || row['rival'] || '');
    if (!id || !rival) continue;
    const idCamp = norm(row['ID_Campeonato'] || row['id_campeonato'] || '') || null;
    const rawFecha = row['Fecha'] || row['fecha'] || row['Date'] || '';
    const fecha = normDate(rawFecha);
    const cancha = norm(row['Cancha'] || row['cancha'] || '');
    const ronda = norm(row['Ronda'] || row['ronda'] || '');
    const gf = normInt(row['GF'] || row['gf'] || 0);
    const gc = normInt(row['GC'] || row['gc'] || 0);
    const notas = norm(row['Notas'] || row['notas'] || '');
    const amistoso = normBool(row['Amistoso'] || row['amistoso'] || false)
      || ronda.toLowerCase() === 'ami';
    await sql`
      INSERT INTO partidos (id, id_campeonato, fecha, rival, cancha, ronda, gf, gc, notas, amistoso, equipo_id)
      VALUES (${id}, ${idCamp}, ${fecha}, ${rival}, ${cancha}, ${ronda}, ${gf}, ${gc}, ${notas}, ${amistoso}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        id_campeonato=EXCLUDED.id_campeonato, fecha=EXCLUDED.fecha,
        rival=EXCLUDED.rival, cancha=EXCLUDED.cancha, ronda=EXCLUDED.ronda,
        gf=EXCLUDED.gf, gc=EXCLUDED.gc, notas=EXCLUDED.notas, amistoso=EXCLUDED.amistoso
    `;
    count++;
  }
  console.log(`✓ Partidos: ${count} rows`);
}

async function seedJugadores(ws, equipoId) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const id = norm(row['ID'] || row['Id'] || row['id'] || '');
    const nombre = norm(row['Nombre'] || row['nombre'] || '');
    if (!id || !nombre) continue;
    const dorsal = normInt(row['Dorsal'] || row['dorsal'] || 0);
    const posicion = norm(row['Posición'] || row['Posicion'] || row['posicion'] || row['Posición'] || '');
    const activo = normBool(row['Activo'] ?? row['activo'] ?? true);
    await sql`
      INSERT INTO jugadores (id, nombre, dorsal, posicion, activo, equipo_id)
      VALUES (${id}, ${nombre}, ${dorsal}, ${posicion}, ${activo}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        nombre=EXCLUDED.nombre, dorsal=EXCLUDED.dorsal,
        posicion=EXCLUDED.posicion, activo=EXCLUDED.activo
    `;
    count++;
  }
  console.log(`✓ Jugadores: ${count} rows`);
}

async function seedAsistencia(ws, equipoId) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const idPartido = norm(row['ID_Partido'] || row['id_partido'] || '');
    const idJugador = norm(row['ID_Jugador'] || row['id_jugador'] || '');
    if (!idPartido || !idJugador) continue;
    const nombreJugador = norm(row['Nombre_Jugador'] || row['nombre_jugador'] || row['Nombre'] || '');
    const presente = normBool(row['Presente'] || row['presente'] || false);
    const goles = normInt(row['Goles'] || row['goles'] || 0);
    const asistencias = normInt(row['Asistencias'] || row['asistencias'] || 0);
    const amarillas = normInt(row['Amarillas'] || row['amarillas'] || 0);
    const rojas = normInt(row['Rojas'] || row['rojas'] || 0);
    await sql`
      INSERT INTO asistencia (id_partido, id_jugador, nombre_jugador, presente, goles, asistencias, amarillas, rojas, equipo_id)
      VALUES (${idPartido}, ${idJugador}, ${nombreJugador}, ${presente}, ${goles}, ${asistencias}, ${amarillas}, ${rojas}, ${equipoId})
      ON CONFLICT (id_partido, id_jugador) DO UPDATE SET
        nombre_jugador=EXCLUDED.nombre_jugador, presente=EXCLUDED.presente,
        goles=EXCLUDED.goles, asistencias=EXCLUDED.asistencias,
        amarillas=EXCLUDED.amarillas, rojas=EXCLUDED.rojas
    `;
    count++;
  }
  console.log(`✓ Asistencia: ${count} rows`);
}

// ── Main ───────────────────────────────────────────────────
export async function main() {
  if (!process.env.DATABASE_URL) {
    console.error('❌  DATABASE_URL not set.');
    process.exit(1);
  }

  // Always ensure tables exist
  await createTables();

  // Skip Excel import if DB already has data (idempotent deploys)
  const rows = await sql`SELECT COUNT(*)::int AS count FROM jugadores`;
  if (rows[0].count > 0) {
    console.log(`✓ DB already has data (${rows[0].count} jugadores). Skipping Excel import.`);
    return;
  }

  // Obtener el equipo por defecto para asignar los datos importados
  const eqRows = await sql`SELECT id FROM equipos LIMIT 1`;
  if (!eqRows.length) {
    console.warn('⚠️  No hay equipo en la DB. Abortando import.');
    return;
  }
  const defaultEquipoId = eqRows[0].id;

  console.log('Reading Excel file:', XLSX_PATH);
  let wb;
  try {
    wb = xlsx.readFile(XLSX_PATH, { cellDates: true, defval: '' });
  } catch (e) {
    console.warn('⚠️  Cannot read Excel file:', e.message, '— starting with empty DB.');
    return;
  }

  const sheetNames = wb.SheetNames;
  console.log('Sheets found:', sheetNames.join(', '));

  function findSheet(candidates) {
    for (const name of candidates) {
      const found = sheetNames.find(s => s.toLowerCase() === name.toLowerCase());
      if (found) return wb.Sheets[found];
    }
    return null;
  }

  const wsConfig = findSheet(['Config', 'Configuracion', 'Configuración']);
  if (wsConfig) await seedConfig(wsConfig);

  const wsCamp = findSheet(['Campeonatos', 'campeonatos']);
  if (wsCamp) await seedCampeonatos(wsCamp, defaultEquipoId);

  const wsPartidos = findSheet(['Partidos', 'partidos']);
  if (wsPartidos) await seedPartidos(wsPartidos, defaultEquipoId);

  const wsJugadores = findSheet(['Jugadores', 'jugadores']);
  if (wsJugadores) await seedJugadores(wsJugadores, defaultEquipoId);

  const wsAsist = findSheet(['Asistencia', 'asistencia']);
  if (wsAsist) await seedAsistencia(wsAsist, defaultEquipoId);

  console.log('\n✅  Seed complete!');
}

// Run directly (node --env-file=.env.local scripts/seed.mjs)
const isMain = process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch(e => { console.error('❌  Seed failed:', e); process.exit(1); });
}

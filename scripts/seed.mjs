/**
 * seed.mjs — Imports data from "Tinder Plate.xlsx" into Neon PostgreSQL.
 * Usage: node --env-file=.env.local scripts/seed.mjs
 */
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { neon } from '@neondatabase/serverless';
import xlsx from 'xlsx';

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
  await sql`CREATE TABLE IF NOT EXISTS config (clave TEXT PRIMARY KEY, valor TEXT)`;
  await sql`
    CREATE TABLE IF NOT EXISTS campeonatos (
      id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
      ano TEXT, temporada TEXT, tipo TEXT DEFAULT 'Liga'
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS partidos (
      id TEXT PRIMARY KEY, id_campeonato TEXT,
      fecha DATE, rival TEXT, cancha TEXT, ronda TEXT,
      gf INTEGER DEFAULT 0, gc INTEGER DEFAULT 0,
      notas TEXT, amistoso BOOLEAN DEFAULT false
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS jugadores (
      id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
      dorsal INTEGER DEFAULT 0, posicion TEXT, activo BOOLEAN DEFAULT true
    )`;
  await sql`
    CREATE TABLE IF NOT EXISTS asistencia (
      id_partido TEXT NOT NULL, id_jugador TEXT NOT NULL,
      nombre_jugador TEXT, presente BOOLEAN DEFAULT false,
      goles INTEGER DEFAULT 0, asistencias INTEGER DEFAULT 0,
      amarillas INTEGER DEFAULT 0, rojas INTEGER DEFAULT 0,
      PRIMARY KEY (id_partido, id_jugador)
    )`;
  await sql`
    INSERT INTO config (clave, valor) VALUES
      ('nombre','Mi Equipo'),('liga',''),('logo',''),('color','#16a34a')
    ON CONFLICT (clave) DO NOTHING`;
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

async function seedCampeonatos(ws) {
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
      INSERT INTO campeonatos (id, nombre, ano, temporada, tipo)
      VALUES (${id}, ${nombre}, ${ano}, ${temporada}, ${tipo})
      ON CONFLICT (id) DO UPDATE SET
        nombre=EXCLUDED.nombre, ano=EXCLUDED.ano,
        temporada=EXCLUDED.temporada, tipo=EXCLUDED.tipo
    `;
    count++;
  }
  console.log(`✓ Campeonatos: ${count} rows`);
}

async function seedPartidos(ws) {
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
      INSERT INTO partidos (id, id_campeonato, fecha, rival, cancha, ronda, gf, gc, notas, amistoso)
      VALUES (${id}, ${idCamp}, ${fecha}, ${rival}, ${cancha}, ${ronda}, ${gf}, ${gc}, ${notas}, ${amistoso})
      ON CONFLICT (id) DO UPDATE SET
        id_campeonato=EXCLUDED.id_campeonato, fecha=EXCLUDED.fecha,
        rival=EXCLUDED.rival, cancha=EXCLUDED.cancha, ronda=EXCLUDED.ronda,
        gf=EXCLUDED.gf, gc=EXCLUDED.gc, notas=EXCLUDED.notas, amistoso=EXCLUDED.amistoso
    `;
    count++;
  }
  console.log(`✓ Partidos: ${count} rows`);
}

async function seedJugadores(ws) {
  const rows = sheetToObjects(ws);
  let count = 0;
  for (const row of rows) {
    const id = norm(row['ID'] || row['Id'] || row['id'] || '');
    const nombre = norm(row['Nombre'] || row['nombre'] || '');
    if (!id || !nombre) continue;
    const dorsal = normInt(row['Dorsal'] || row['dorsal'] || 0);
    const posicion = norm(row['Posición'] || row['Posicion'] || row['posicion'] || row['Posición'] || '');
    const activo = normBool(row['Activo'] || row['activo'] ?? true);
    await sql`
      INSERT INTO jugadores (id, nombre, dorsal, posicion, activo)
      VALUES (${id}, ${nombre}, ${dorsal}, ${posicion}, ${activo})
      ON CONFLICT (id) DO UPDATE SET
        nombre=EXCLUDED.nombre, dorsal=EXCLUDED.dorsal,
        posicion=EXCLUDED.posicion, activo=EXCLUDED.activo
    `;
    count++;
  }
  console.log(`✓ Jugadores: ${count} rows`);
}

async function seedAsistencia(ws) {
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
      INSERT INTO asistencia (id_partido, id_jugador, nombre_jugador, presente, goles, asistencias, amarillas, rojas)
      VALUES (${idPartido}, ${idJugador}, ${nombreJugador}, ${presente}, ${goles}, ${asistencias}, ${amarillas}, ${rojas})
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
async function main() {
  if (!process.env.DATABASE_URL) {
    console.error('❌  DATABASE_URL not set. Run: node --env-file=.env.local scripts/seed.mjs');
    process.exit(1);
  }

  console.log('Reading Excel file:', XLSX_PATH);
  let wb;
  try {
    wb = xlsx.readFile(XLSX_PATH, { cellDates: true, defval: '' });
  } catch (e) {
    console.error('❌  Cannot read Excel file:', e.message);
    process.exit(1);
  }

  const sheetNames = wb.SheetNames;
  console.log('Sheets found:', sheetNames.join(', '));

  await createTables();

  // Seed each sheet if it exists (case-insensitive match)
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
  if (wsCamp) await seedCampeonatos(wsCamp);

  const wsPartidos = findSheet(['Partidos', 'partidos']);
  if (wsPartidos) await seedPartidos(wsPartidos);

  const wsJugadores = findSheet(['Jugadores', 'jugadores']);
  if (wsJugadores) await seedJugadores(wsJugadores);

  const wsAsist = findSheet(['Asistencia', 'asistencia']);
  if (wsAsist) await seedAsistencia(wsAsist);

  console.log('\n✅  Seed complete!');
}

main().catch(e => { console.error('❌  Seed failed:', e); process.exit(1); });

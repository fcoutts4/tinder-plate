import { neon } from '@neondatabase/serverless';
import { randomUUID } from 'crypto';
import bcrypt from 'bcryptjs';

const getSql = () => neon(process.env.DATABASE_URL);

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

function esAmistosoValor(valor, ronda) {
  if (valor === true || valor === 1) return true;
  if (typeof valor === 'string') {
    const v = valor.trim().toLowerCase();
    if (v === 'true' || v === '1' || v === 'si' || v === 'sí') return true;
  }
  if (typeof ronda === 'string' && ronda.trim().toLowerCase() === 'ami') return true;
  return false;
}

function toDateStr(val) {
  if (!val) return '';
  const s = String(val);
  if (s.includes('T')) return s.split('T')[0];
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.substring(0, 10);
  return s;
}

// Helpers para extraer equipoId y valor de un payload posiblemente envuelto
function getEqId(payload) { return payload?.equipoId || null; }
function getValue(payload) {
  if (payload !== null && payload !== undefined && typeof payload === 'object' && 'value' in payload) return payload.value;
  return payload;
}

// ── Mappers ───────────────────────────────────────────────────

function mapEquipo(row) {
  return {
    id: row.id,
    slug: row.slug,
    nombre: row.nombre || '',
    color: row.color || '#16a34a',
    logo: row.logo || '',
    liga: row.liga || '',
    modalidad: row.modalidad || '11',
    jugadores_por_lado: Number(row.jugadores_por_lado) || 11,
    activo: row.activo !== false
  };
}

function mapCampeonato(row) {
  return {
    ID: row.id,
    Nombre: row.nombre || '',
    'Año': row.ano || '',
    Temporada: row.temporada || '',
    Tipo: row.tipo || 'Liga'
  };
}

function mapPartido(row) {
  return {
    ID: row.id,
    ID_Campeonato: row.id_campeonato || '',
    Fecha: toDateStr(row.fecha),
    Rival: row.rival || '',
    Cancha: row.cancha || '',
    Ronda: row.ronda || '',
    GF: Number(row.gf) || 0,
    GC: Number(row.gc) || 0,
    Notas: row.notas || '',
    Amistoso: row.amistoso === true || row.amistoso === 'true',
    Campeonato_Nombre: row.campeonato_nombre || '',
    Campeonato_Tipo: row.campeonato_tipo || '',
    'Campeonato_Año': row.campeonato_ano || '',
    Campeonato_Temporada: row.campeonato_temporada || ''
  };
}

function mapJugador(row) {
  return {
    ID: row.id,
    Nombre: row.nombre || '',
    Dorsal: Number(row.dorsal) || 0,
    'Posición': row.posicion || '',
    Activo: row.activo === true || row.activo === 'true'
  };
}

function mapAsistencia(row) {
  return {
    ID_Partido: row.id_partido,
    ID_Jugador: row.id_jugador,
    Nombre_Jugador: row.nombre_jugador || '',
    Presente: row.presente === true || row.presente === 'true',
    Goles: Number(row.goles) || 0,
    Asistencias: Number(row.asistencias) || 0,
    Amarillas: Number(row.amarillas) || 0,
    Rojas: Number(row.rojas) || 0
  };
}

// ── Init / Migración ──────────────────────────────────────────

export async function initDB() {
  const sql = getSql();
  try {
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
      )
    `;

    // Tablas de datos (con equipo_id incluido desde el inicio)
    await sql`CREATE TABLE IF NOT EXISTS config (clave TEXT PRIMARY KEY, valor TEXT)`;
    await sql`
      CREATE TABLE IF NOT EXISTS campeonatos (
        id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
        ano TEXT, temporada TEXT, tipo TEXT DEFAULT 'Liga', equipo_id TEXT
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS partidos (
        id TEXT PRIMARY KEY, id_campeonato TEXT,
        fecha DATE, rival TEXT, cancha TEXT, ronda TEXT,
        gf INTEGER DEFAULT 0, gc INTEGER DEFAULT 0,
        notas TEXT, amistoso BOOLEAN DEFAULT false, equipo_id TEXT
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS jugadores (
        id TEXT PRIMARY KEY, nombre TEXT NOT NULL,
        dorsal INTEGER DEFAULT 0, posicion TEXT,
        activo BOOLEAN DEFAULT true, equipo_id TEXT
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS asistencia (
        id_partido TEXT NOT NULL, id_jugador TEXT NOT NULL,
        nombre_jugador TEXT, presente BOOLEAN DEFAULT false,
        goles INTEGER DEFAULT 0, asistencias INTEGER DEFAULT 0,
        amarillas INTEGER DEFAULT 0, rojas INTEGER DEFAULT 0,
        equipo_id TEXT,
        PRIMARY KEY (id_partido, id_jugador)
      )
    `;

    // Migración idempotente: agregar equipo_id a tablas ya existentes
    await sql`ALTER TABLE campeonatos ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
    await sql`ALTER TABLE partidos    ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
    await sql`ALTER TABLE jugadores   ADD COLUMN IF NOT EXISTS equipo_id TEXT`;
    await sql`ALTER TABLE asistencia  ADD COLUMN IF NOT EXISTS equipo_id TEXT`;

    // Crear equipo por defecto si la tabla está vacía
    const eqCount = await sql`SELECT COUNT(*)::int AS n FROM equipos`;
    if (eqCount[0].n === 0) {
      // Leer config legacy para poblar el equipo por defecto
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

      // Asignar todos los datos históricos al equipo por defecto
      await sql`UPDATE campeonatos SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
      await sql`UPDATE partidos    SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
      await sql`UPDATE jugadores   SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
      await sql`UPDATE asistencia  SET equipo_id = ${defId} WHERE equipo_id IS NULL`;
    } else {
      // En deploys sucesivos: asignar filas sin equipo al primero
      const first = await sql`SELECT id FROM equipos ORDER BY id LIMIT 1`;
      if (first.length) {
        const fid = first[0].id;
        await sql`UPDATE campeonatos SET equipo_id = ${fid} WHERE equipo_id IS NULL`;
        await sql`UPDATE partidos    SET equipo_id = ${fid} WHERE equipo_id IS NULL`;
        await sql`UPDATE jugadores   SET equipo_id = ${fid} WHERE equipo_id IS NULL`;
        await sql`UPDATE asistencia  SET equipo_id = ${fid} WHERE equipo_id IS NULL`;
      }
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Equipos ───────────────────────────────────────────────────

export async function getEquipos() {
  const sql = getSql();
  try {
    const rows = await sql`
      SELECT id, slug, nombre, color, logo, liga, modalidad, jugadores_por_lado, activo
      FROM equipos WHERE activo = true ORDER BY nombre
    `;
    return { ok: true, equipos: rows.map(mapEquipo) };
  } catch (e) {
    return { ok: false, equipos: [], error: e.message };
  }
}

export async function getEquipo(slug) {
  const sql = getSql();
  try {
    const rows = await sql`
      SELECT id, slug, nombre, color, logo, liga, modalidad, jugadores_por_lado, activo
      FROM equipos WHERE slug = ${slug}
    `;
    if (!rows.length) return { ok: false, error: 'Equipo no encontrado' };
    return { ok: true, equipo: mapEquipo(rows[0]) };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function checkAdminKey(data) {
  const adminKey = process.env.ADMIN_KEY || '';
  if (!adminKey) return { ok: true };          // sin restricción si no hay key configurada
  const match = String(data?.adminKey || '') === adminKey;
  return { ok: match };
}

export async function crearEquipo(data) {
  const sql = getSql();
  try {
    // Verificar clave de administrador
    const adminKey = process.env.ADMIN_KEY || '';
    if (adminKey && String(data.adminKey || '') !== adminKey) {
      return { ok: false, error: 'Clave de administrador incorrecta' };
    }

    const nombre = String(data.nombre || '').trim();
    const slug = String(data.slug || slugify(nombre)).trim();
    const password = String(data.password || '').trim();
    if (!nombre) return { ok: false, error: 'Nombre requerido' };
    if (!slug) return { ok: false, error: 'Slug requerido' };
    if (!password) return { ok: false, error: 'Clave requerida' };

    const existing = await sql`SELECT id FROM equipos WHERE slug = ${slug}`;
    if (existing.length) return { ok: false, error: 'Ya existe un equipo con ese nombre de URL' };

    const hash = await bcrypt.hash(password, 10);
    const id = generateId();
    await sql`
      INSERT INTO equipos (id, slug, nombre, password_hash, color, liga)
      VALUES (${id}, ${slug}, ${nombre}, ${hash}, ${data.color || '#16a34a'}, ${data.liga || ''})
    `;
    return { ok: true, id, slug };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function checkEditKey(slug, password) {
  const sql = getSql();
  try {
    const rows = await sql`SELECT id, password_hash FROM equipos WHERE slug = ${slug}`;
    if (!rows.length) return { ok: false };
    const match = await bcrypt.compare(String(password || ''), rows[0].password_hash);
    return { ok: match };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Config (almacenada en la tabla equipos) ───────────────────

export async function getConfig(equipoId) {
  const sql = getSql();
  try {
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    const rows = await sql`
      SELECT nombre, liga, logo, color, modalidad, jugadores_por_lado
      FROM equipos WHERE id = ${equipoId}
    `;
    if (!rows.length) return { ok: false, error: 'Equipo no encontrado' };
    const eq = rows[0];
    return {
      ok: true,
      config: {
        nombre: eq.nombre || '',
        liga: eq.liga || '',
        logo: eq.logo || '',
        color: eq.color || '#16a34a',
        modalidad: eq.modalidad || '11',
        jugadores_por_lado: Number(eq.jugadores_por_lado) || 11
      }
    };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function guardarConfigCloud(data) {
  const sql = getSql();
  try {
    const equipoId = data.equipoId;
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    const jpl = data.modalidad === '7' ? 7 : 11;
    await sql`
      UPDATE equipos SET
        nombre             = ${data.nombre || 'Mi Equipo'},
        liga               = ${data.liga || ''},
        color              = ${data.color || '#16a34a'},
        modalidad          = ${data.modalidad || '11'},
        jugadores_por_lado = ${jpl}
      WHERE id = ${equipoId}
    `;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function uploadLogo(data) {
  const sql = getSql();
  try {
    const equipoId = data.equipoId;
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    const logoUrl = data.dataUrl || '';
    await sql`UPDATE equipos SET logo = ${logoUrl} WHERE id = ${equipoId}`;
    return { ok: true, logoUrl };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function cambiarClave(data) {
  const sql = getSql();
  try {
    const equipoId  = data?.equipoId;
    const nuevaClave = String(data?.nuevaClave || '').trim();
    if (!equipoId)    return { ok: false, error: 'equipoId requerido' };
    if (!nuevaClave)  return { ok: false, error: 'Nueva clave requerida' };
    const hash = await bcrypt.hash(nuevaClave, 10);
    await sql`UPDATE equipos SET password_hash = ${hash} WHERE id = ${equipoId}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function eliminarEquipo(data) {
  const sql = getSql();
  try {
    const adminKey = process.env.ADMIN_KEY || '';
    if (adminKey && String(data?.adminKey || '') !== adminKey) {
      return { ok: false, error: 'Clave de administrador incorrecta' };
    }
    const equipoId = data?.equipoId;
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    await sql`DELETE FROM asistencia  WHERE equipo_id = ${equipoId}`;
    await sql`DELETE FROM partidos    WHERE equipo_id = ${equipoId}`;
    await sql`DELETE FROM jugadores   WHERE equipo_id = ${equipoId}`;
    await sql`DELETE FROM campeonatos WHERE equipo_id = ${equipoId}`;
    await sql`DELETE FROM equipos     WHERE id        = ${equipoId}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function removeLogoCloud(data) {
  const sql = getSql();
  try {
    const equipoId = data?.equipoId;
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    await sql`UPDATE equipos SET logo = '' WHERE id = ${equipoId}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Campeonatos ──────────────────────────────────────────────

export async function getCampeonatos(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM campeonatos WHERE equipo_id = ${equipoId} ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`
      : await sql`SELECT * FROM campeonatos ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`;
    return rows.map(mapCampeonato);
  } catch (e) {
    return [];
  }
}

export async function guardarCampeonato(data) {
  const sql = getSql();
  try {
    const id = data.ID || generateId();
    const ano = data['Año'] ?? data.Ano ?? data.ano ?? '';
    const equipoId = data.equipoId || null;
    await sql`
      INSERT INTO campeonatos (id, nombre, ano, temporada, tipo, equipo_id)
      VALUES (${id}, ${data.Nombre || ''}, ${String(ano)}, ${data.Temporada || ''}, ${data.Tipo || 'Liga'}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        nombre    = EXCLUDED.nombre,
        ano       = EXCLUDED.ano,
        temporada = EXCLUDED.temporada,
        tipo      = EXCLUDED.tipo
    `;
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function eliminarCampeonato(id, equipoId) {
  const sql = getSql();
  try {
    if (equipoId) {
      await sql`DELETE FROM campeonatos WHERE id = ${id} AND equipo_id = ${equipoId}`;
    } else {
      await sql`DELETE FROM campeonatos WHERE id = ${id}`;
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Partidos ─────────────────────────────────────────────────

export async function getPartidos(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`
          SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo,
            c.ano AS campeonato_ano, c.temporada AS campeonato_temporada
          FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id
          WHERE p.equipo_id = ${equipoId} ORDER BY p.fecha DESC NULLS LAST`
      : await sql`
          SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo,
            c.ano AS campeonato_ano, c.temporada AS campeonato_temporada
          FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id
          ORDER BY p.fecha DESC NULLS LAST`;
    return rows.map(mapPartido);
  } catch (e) {
    return [];
  }
}

export async function guardarPartido(data) {
  const sql = getSql();
  try {
    const id = data.ID || generateId();
    const fecha = data.Fecha && String(data.Fecha).trim() ? String(data.Fecha).trim() : null;
    const idCamp = data.ID_Campeonato && String(data.ID_Campeonato).trim() ? String(data.ID_Campeonato).trim() : null;
    const amistoso = esAmistosoValor(data.Amistoso, data.Ronda);
    const equipoId = data.equipoId || null;
    await sql`
      INSERT INTO partidos (id, id_campeonato, fecha, rival, cancha, ronda, gf, gc, notas, amistoso, equipo_id)
      VALUES (${id}, ${idCamp}, ${fecha}, ${data.Rival || ''}, ${data.Cancha || ''}, ${data.Ronda || ''},
              ${Number(data.GF) || 0}, ${Number(data.GC) || 0}, ${data.Notas || ''}, ${amistoso}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        id_campeonato = EXCLUDED.id_campeonato, fecha    = EXCLUDED.fecha,
        rival         = EXCLUDED.rival,         cancha   = EXCLUDED.cancha,
        ronda         = EXCLUDED.ronda,         gf       = EXCLUDED.gf,
        gc            = EXCLUDED.gc,            notas    = EXCLUDED.notas,
        amistoso      = EXCLUDED.amistoso
    `;
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function eliminarPartido(id, equipoId) {
  const sql = getSql();
  try {
    await sql`DELETE FROM asistencia WHERE id_partido = ${id}`;
    if (equipoId) {
      await sql`DELETE FROM partidos WHERE id = ${id} AND equipo_id = ${equipoId}`;
    } else {
      await sql`DELETE FROM partidos WHERE id = ${id}`;
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Jugadores ────────────────────────────────────────────────

export async function getJugadores(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM jugadores WHERE activo = true AND equipo_id = ${equipoId} ORDER BY COALESCE(dorsal,99)`
      : await sql`SELECT * FROM jugadores WHERE activo = true ORDER BY COALESCE(dorsal,99)`;
    return rows.map(mapJugador);
  } catch (e) {
    return [];
  }
}

export async function getTodosJugadores(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM jugadores WHERE equipo_id = ${equipoId} ORDER BY COALESCE(dorsal,99)`
      : await sql`SELECT * FROM jugadores ORDER BY COALESCE(dorsal,99)`;
    return rows.map(mapJugador);
  } catch (e) {
    return [];
  }
}

export async function guardarJugador(data) {
  const sql = getSql();
  try {
    const id = data.ID || generateId();
    const activo = !(data.Activo === false || data.Activo === 'FALSE' || data.Activo === 'false');
    const posicion = data['Posición'] ?? data.Posicion ?? data.posicion ?? '';
    const equipoId = data.equipoId || null;
    await sql`
      INSERT INTO jugadores (id, nombre, dorsal, posicion, activo, equipo_id)
      VALUES (${id}, ${data.Nombre || ''}, ${Number(data.Dorsal) || 0}, ${posicion}, ${activo}, ${equipoId})
      ON CONFLICT (id) DO UPDATE SET
        nombre = EXCLUDED.nombre, dorsal = EXCLUDED.dorsal,
        posicion = EXCLUDED.posicion, activo = EXCLUDED.activo
    `;
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function toggleJugadorActivo(id, equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`UPDATE jugadores SET activo = NOT activo WHERE id = ${id} AND equipo_id = ${equipoId} RETURNING activo`
      : await sql`UPDATE jugadores SET activo = NOT activo WHERE id = ${id} RETURNING activo`;
    if (!rows.length) return { ok: false };
    return { ok: true, activo: rows[0].activo };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function eliminarJugador(id) {
  const sql = getSql();
  try {
    await sql`DELETE FROM jugadores WHERE id = ${id}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Asistencia ───────────────────────────────────────────────

export async function getAsistenciaPartido(idPartido, equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM asistencia WHERE id_partido = ${idPartido} AND equipo_id = ${equipoId}`
      : await sql`SELECT * FROM asistencia WHERE id_partido = ${idPartido}`;
    return rows.map(mapAsistencia);
  } catch (e) {
    return [];
  }
}

export async function getAsistenciaAll(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM asistencia WHERE equipo_id = ${equipoId}`
      : await sql`SELECT * FROM asistencia`;
    return rows.map(mapAsistencia);
  } catch (e) {
    return [];
  }
}

export async function guardarAsistencia(idPartido, lista, equipoId) {
  const sql = getSql();
  try {
    await sql`DELETE FROM asistencia WHERE id_partido = ${idPartido}`;
    for (const j of (lista || [])) {
      await sql`
        INSERT INTO asistencia
          (id_partido, id_jugador, nombre_jugador, presente, goles, asistencias, amarillas, rojas, equipo_id)
        VALUES (
          ${idPartido}, ${j.ID_Jugador}, ${j.Nombre_Jugador || ''},
          ${j.Presente === true || j.Presente === 'TRUE'},
          ${Number(j.Goles) || 0}, ${Number(j.Asistencias) || 0},
          ${Number(j.Amarillas) || 0}, ${Number(j.Rojas) || 0},
          ${equipoId || null}
        )
      `;
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Estadísticas ─────────────────────────────────────────────

export async function getEstadisticas(filtros) {
  const sql = getSql();
  const equipoId = filtros?.equipoId;
  try {
    const [campRows, partRows, jugRows, asistRows] = await Promise.all([
      equipoId
        ? sql`SELECT * FROM campeonatos WHERE equipo_id = ${equipoId} ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`
        : sql`SELECT * FROM campeonatos ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`,
      equipoId
        ? sql`SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo, c.ano AS campeonato_ano, c.temporada AS campeonato_temporada FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id WHERE p.equipo_id = ${equipoId}`
        : sql`SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo, c.ano AS campeonato_ano, c.temporada AS campeonato_temporada FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id`,
      equipoId
        ? sql`SELECT * FROM jugadores WHERE equipo_id = ${equipoId} ORDER BY COALESCE(dorsal,99)`
        : sql`SELECT * FROM jugadores ORDER BY COALESCE(dorsal,99)`,
      equipoId
        ? sql`SELECT * FROM asistencia WHERE equipo_id = ${equipoId}`
        : sql`SELECT * FROM asistencia`
    ]);

    let partidos = partRows.map(mapPartido);
    const asistencia = asistRows.map(mapAsistencia);
    const jugadores = jugRows.map(mapJugador);
    const campeonatos = campRows.map(mapCampeonato);

    const yearSet = new Set();
    partidos.forEach(p => {
      if (p.Fecha) {
        const y = new Date(p.Fecha + 'T12:00:00').getFullYear();
        if (y) yearSet.add(String(y));
      }
    });
    const yearList = Array.from(yearSet).sort((a, b) => Number(b) - Number(a));

    const idCampeonato = filtros?.idCampeonato || null;
    const año = filtros?.año ? String(filtros.año) : null;
    const tipo = filtros?.tipo || null;

    if (idCampeonato) partidos = partidos.filter(p => String(p.ID_Campeonato) === String(idCampeonato));
    if (año) partidos = partidos.filter(p => p.Fecha && String(new Date(p.Fecha + 'T12:00:00').getFullYear()) === año);
    if (tipo) partidos = partidos.filter(p => tipo === 'amistosos' ? p.Amistoso === true : p.Amistoso !== true);

    const partidoIds = new Set(partidos.map(p => String(p.ID)));
    const total = partidos.length;
    const ganados  = partidos.filter(p => Number(p.GF) > Number(p.GC)).length;
    const empatados = partidos.filter(p => Number(p.GF) === Number(p.GC)).length;
    const perdidos  = partidos.filter(p => Number(p.GF) < Number(p.GC)).length;
    const gf = partidos.reduce((s, p) => s + (Number(p.GF) || 0), 0);
    const gc = partidos.reduce((s, p) => s + (Number(p.GC) || 0), 0);

    const statsJugador = {};
    jugadores.forEach(j => {
      statsJugador[j.ID] = {
        ID: j.ID, Nombre: j.Nombre, Dorsal: j.Dorsal, 'Posición': j['Posición'],
        Activo: j.Activo, partidos: 0, presentes: 0, goles: 0, asistencias: 0, amarillas: 0, rojas: 0
      };
    });
    asistencia.forEach(a => {
      if (!statsJugador[a.ID_Jugador] || !partidoIds.has(String(a.ID_Partido))) return;
      statsJugador[a.ID_Jugador].partidos++;
      if (a.Presente === true) statsJugador[a.ID_Jugador].presentes++;
      statsJugador[a.ID_Jugador].goles       += Number(a.Goles) || 0;
      statsJugador[a.ID_Jugador].asistencias += Number(a.Asistencias) || 0;
      statsJugador[a.ID_Jugador].amarillas   += Number(a.Amarillas) || 0;
      statsJugador[a.ID_Jugador].rojas       += Number(a.Rojas) || 0;
    });

    const ranking = Object.values(statsJugador)
      .sort((a, b) => b.goles - a.goles || b.asistencias - a.asistencias);

    const campList = campeonatos
      .map(c => ({ ID: c.ID, Nombre: c.Nombre, Tipo: c.Tipo, 'Año': c['Año'], Temporada: c.Temporada }))
      .sort((a, b) => (Number(b['Año']) || 0) - (Number(a['Año']) || 0));

    return { ok: true, resumen: { total, ganados, empatados, perdidos, gf, gc }, ranking, campList, yearList };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Detalle jugador ───────────────────────────────────────────

export async function getDetalleJugador(idJugador, equipoId) {
  const sql = getSql();
  try {
    const [jugRows, asistRows, partRows] = await Promise.all([
      sql`SELECT * FROM jugadores WHERE id = ${idJugador}`,
      equipoId
        ? sql`SELECT * FROM asistencia WHERE id_jugador = ${idJugador} AND equipo_id = ${equipoId}`
        : sql`SELECT * FROM asistencia WHERE id_jugador = ${idJugador}`,
      equipoId
        ? sql`SELECT p.*, c.nombre AS campeonato_nombre FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id WHERE p.equipo_id = ${equipoId}`
        : sql`SELECT p.*, c.nombre AS campeonato_nombre FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id`
    ]);

    if (!jugRows.length) return { ok: false, error: 'Jugador no encontrado' };
    const jugador = mapJugador(jugRows[0]);
    const partidoMap = {};
    partRows.map(mapPartido).forEach(p => { partidoMap[p.ID] = p; });

    const historial = asistRows.map(mapAsistencia).map(a => {
      const p = partidoMap[a.ID_Partido] || {};
      return {
        ID_Partido: a.ID_Partido,
        Fecha: p.Fecha || '', Rival: p.Rival || '',
        GF: Number(p.GF) || 0, GC: Number(p.GC) || 0,
        Campeonato: p.Campeonato_Nombre || '',
        Presente: a.Presente === true,
        Goles: Number(a.Goles) || 0, Asistencias: Number(a.Asistencias) || 0,
        Amarillas: Number(a.Amarillas) || 0, Rojas: Number(a.Rojas) || 0
      };
    }).sort((a, b) => (b.Fecha || '').localeCompare(a.Fecha || ''));

    const totales = {
      partidos: historial.length,
      presentes: historial.filter(h => h.Presente).length,
      goles: historial.reduce((s, h) => s + h.Goles, 0),
      asistencias: historial.reduce((s, h) => s + h.Asistencias, 0),
      amarillas: historial.reduce((s, h) => s + h.Amarillas, 0),
      rojas: historial.reduce((s, h) => s + h.Rojas, 0)
    };

    return { ok: true, jugador, historial, totales };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Bootstrap ─────────────────────────────────────────────────

export async function getBootstrapData(equipoId) {
  const sql = getSql();
  try {
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };

    const [eqRows, campRows, partRows, jugRows, asistRows] = await Promise.all([
      sql`SELECT * FROM equipos WHERE id = ${equipoId}`,
      sql`SELECT * FROM campeonatos WHERE equipo_id = ${equipoId} ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`,
      sql`SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo, c.ano AS campeonato_ano, c.temporada AS campeonato_temporada FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id WHERE p.equipo_id = ${equipoId} ORDER BY p.fecha DESC NULLS LAST`,
      sql`SELECT * FROM jugadores WHERE equipo_id = ${equipoId} ORDER BY COALESCE(dorsal,99)`,
      sql`SELECT * FROM asistencia WHERE equipo_id = ${equipoId}`
    ]);

    if (!eqRows.length) return { ok: false, error: 'Equipo no encontrado' };
    const eq = eqRows[0];

    return {
      ok: true,
      config: {
        nombre: eq.nombre || '',
        liga: eq.liga || '',
        logo: eq.logo || '',
        color: eq.color || '#16a34a',
        modalidad: eq.modalidad || '11',
        jugadores_por_lado: Number(eq.jugadores_por_lado) || 11
      },
      campeonatos: campRows.map(mapCampeonato),
      partidos:    partRows.map(mapPartido),
      jugadores:   jugRows.map(mapJugador),
      asistencia:  asistRows.map(mapAsistencia)
    };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Planilla ──────────────────────────────────────────────────

export async function getPlanilla2(payload) {
  const sql = getSql();
  const equipoId = payload?.equipoId;
  try {
    if (!equipoId) return { ok: true, data: null };
    const rows = await sql`SELECT planilla_v2 FROM equipos WHERE id = ${equipoId}`;
    if (!rows.length || !rows[0].planilla_v2) return { ok: true, data: null };
    try { return { ok: true, data: JSON.parse(rows[0].planilla_v2) }; }
    catch { return { ok: true, data: null }; }
  } catch (e) {
    return { ok: false, error: e.message, data: null };
  }
}

export async function savePlanilla2(payload) {
  const sql = getSql();
  try {
    const equipoId = payload?.equipoId;
    if (!equipoId) return { ok: false, error: 'equipoId requerido' };
    // call() envuelve strings primitivos en { value, equipoId }
    const raw = payload?.value ?? (typeof payload === 'string' ? payload : JSON.stringify(payload));
    await sql`UPDATE equipos SET planilla_v2 = ${raw} WHERE id = ${equipoId}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function getJugadoresActivosPlan(equipoId) {
  const sql = getSql();
  try {
    const rows = equipoId
      ? await sql`SELECT * FROM jugadores WHERE activo = true AND equipo_id = ${equipoId} ORDER BY COALESCE(dorsal,99)`
      : await sql`SELECT * FROM jugadores WHERE activo = true ORDER BY COALESCE(dorsal,99)`;
    return {
      ok: true,
      jugadores: rows.map(r => ({
        id: r.id, dorsal: r.dorsal || '',
        nombre: r.nombre || '', posicion: r.posicion || ''
      }))
    };
  } catch (e) {
    return { ok: false, error: e.message, jugadores: [] };
  }
}

// ── Dispatch ──────────────────────────────────────────────────

export async function dispatch(action, payload) {
  switch (action) {
    case 'init':                    return initDB();
    case 'getEquipos':              return getEquipos();
    case 'checkAdminKey':           return checkAdminKey(payload);
    case 'getEquipo':               return getEquipo(getValue(payload));
    case 'crearEquipo':             return crearEquipo(payload);
    case 'checkEditKey':            return checkEditKey(payload?.slug, payload?.password);
    case 'getCampeonatos':          return getCampeonatos(getEqId(payload));
    case 'guardarCampeonato':       return guardarCampeonato(payload);
    case 'eliminarCampeonato':      return eliminarCampeonato(getValue(payload), getEqId(payload));
    case 'getPartidos':             return getPartidos(getEqId(payload));
    case 'guardarPartido':          return guardarPartido(payload);
    case 'eliminarPartido':         return eliminarPartido(getValue(payload), getEqId(payload));
    case 'getJugadores':            return getJugadores(getEqId(payload));
    case 'getTodosJugadores':       return getTodosJugadores(getEqId(payload));
    case 'guardarJugador':          return guardarJugador(payload);
    case 'toggleJugador':           return toggleJugadorActivo(getValue(payload), getEqId(payload));
    case 'eliminarJugador':         return eliminarJugador(getValue(payload));
    case 'getAsistencia':           return getAsistenciaPartido(getValue(payload), getEqId(payload));
    case 'guardarAsistencia':       return guardarAsistencia(payload?.idPartido, payload?.lista, getEqId(payload));
    case 'getEstadisticas':         return getEstadisticas(payload);
    case 'getDetalleJugador':       return getDetalleJugador(getValue(payload), getEqId(payload));
    case 'getConfig':               return getConfig(getEqId(payload));
    case 'guardarConfig':           return guardarConfigCloud(payload);
    case 'cambiarClave':            return cambiarClave(payload);
    case 'eliminarEquipo':          return eliminarEquipo(payload);
    case 'removeLogo':              return removeLogoCloud(payload);
    case 'uploadLogo':              return uploadLogo(payload);
    case 'checkAdmin':              return { isAdmin: true };
    case 'getBootstrapData':        return getBootstrapData(getEqId(payload));
    case 'getPlanilla2':            return getPlanilla2(payload);
    case 'savePlanilla2':           return savePlanilla2(payload);
    case 'getJugadoresActivosPlan': return getJugadoresActivosPlan(getEqId(payload));
    default: return { ok: false, error: `Acción desconocida: ${action}` };
  }
}

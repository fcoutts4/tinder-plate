import { neon } from '@neondatabase/serverless';
import { randomUUID } from 'crypto';

const getSql = () => neon(process.env.DATABASE_URL);

function generateId() {
  return randomUUID().substring(0, 8).toUpperCase();
}

function normalizeBool(value) {
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (['true', '1', 'si', 'sí', 'yes'].includes(v)) return true;
    if (['false', '0', 'no'].includes(v)) return false;
  }
  return false;
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

// ── Init DB ──────────────────────────────────────────────────

export async function initDB() {
  const sql = getSql();
  try {
    await sql`
      CREATE TABLE IF NOT EXISTS config (
        clave TEXT PRIMARY KEY,
        valor TEXT
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS campeonatos (
        id TEXT PRIMARY KEY,
        nombre TEXT NOT NULL,
        ano TEXT,
        temporada TEXT,
        tipo TEXT DEFAULT 'Liga'
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS partidos (
        id TEXT PRIMARY KEY,
        id_campeonato TEXT,
        fecha DATE,
        rival TEXT,
        cancha TEXT,
        ronda TEXT,
        gf INTEGER DEFAULT 0,
        gc INTEGER DEFAULT 0,
        notas TEXT,
        amistoso BOOLEAN DEFAULT false
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS jugadores (
        id TEXT PRIMARY KEY,
        nombre TEXT NOT NULL,
        dorsal INTEGER DEFAULT 0,
        posicion TEXT,
        activo BOOLEAN DEFAULT true
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS asistencia (
        id_partido TEXT NOT NULL,
        id_jugador TEXT NOT NULL,
        nombre_jugador TEXT,
        presente BOOLEAN DEFAULT false,
        goles INTEGER DEFAULT 0,
        asistencias INTEGER DEFAULT 0,
        amarillas INTEGER DEFAULT 0,
        rojas INTEGER DEFAULT 0,
        PRIMARY KEY (id_partido, id_jugador)
      )
    `;
    await sql`
      INSERT INTO config (clave, valor) VALUES
        ('nombre', 'Mi Equipo'),
        ('liga', ''),
        ('logo', ''),
        ('color', '#16a34a')
      ON CONFLICT (clave) DO NOTHING
    `;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Config ───────────────────────────────────────────────────

export async function getConfig() {
  const sql = getSql();
  try {
    const rows = await sql`SELECT * FROM config`;
    const config = {};
    rows.forEach(r => { config[r.clave] = r.valor || ''; });
    return { ok: true, config };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function guardarConfigCloud(data) {
  const sql = getSql();
  try {
    for (const key of ['nombre', 'liga', 'color']) {
      if (data[key] === undefined) continue;
      await sql`
        INSERT INTO config (clave, valor) VALUES (${key}, ${data[key]})
        ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
      `;
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function uploadLogo(data) {
  const sql = getSql();
  try {
    const logoUrl = data.dataUrl || '';
    await sql`
      INSERT INTO config (clave, valor) VALUES ('logo', ${logoUrl})
      ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
    `;
    return { ok: true, logoUrl };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function removeLogoCloud() {
  const sql = getSql();
  try {
    await sql`UPDATE config SET valor = '' WHERE clave = 'logo'`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Campeonatos ──────────────────────────────────────────────

export async function getCampeonatos() {
  const sql = getSql();
  try {
    const rows = await sql`
      SELECT * FROM campeonatos
      ORDER BY COALESCE(NULLIF(ano, '')::int, 0) DESC
    `;
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
    await sql`
      INSERT INTO campeonatos (id, nombre, ano, temporada, tipo)
      VALUES (${id}, ${data.Nombre || ''}, ${String(ano)}, ${data.Temporada || ''}, ${data.Tipo || 'Liga'})
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

export async function eliminarCampeonato(id) {
  const sql = getSql();
  try {
    await sql`DELETE FROM campeonatos WHERE id = ${id}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Partidos ─────────────────────────────────────────────────

export async function getPartidos() {
  const sql = getSql();
  try {
    const rows = await sql`
      SELECT p.*,
        c.nombre     AS campeonato_nombre,
        c.tipo       AS campeonato_tipo,
        c.ano        AS campeonato_ano,
        c.temporada  AS campeonato_temporada
      FROM partidos p
      LEFT JOIN campeonatos c ON p.id_campeonato = c.id
      ORDER BY p.fecha DESC NULLS LAST
    `;
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
    await sql`
      INSERT INTO partidos (id, id_campeonato, fecha, rival, cancha, ronda, gf, gc, notas, amistoso)
      VALUES (${id}, ${idCamp}, ${fecha}, ${data.Rival || ''}, ${data.Cancha || ''}, ${data.Ronda || ''}, ${Number(data.GF) || 0}, ${Number(data.GC) || 0}, ${data.Notas || ''}, ${amistoso})
      ON CONFLICT (id) DO UPDATE SET
        id_campeonato = EXCLUDED.id_campeonato,
        fecha         = EXCLUDED.fecha,
        rival         = EXCLUDED.rival,
        cancha        = EXCLUDED.cancha,
        ronda         = EXCLUDED.ronda,
        gf            = EXCLUDED.gf,
        gc            = EXCLUDED.gc,
        notas         = EXCLUDED.notas,
        amistoso      = EXCLUDED.amistoso
    `;
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function eliminarPartido(id) {
  const sql = getSql();
  try {
    await sql`DELETE FROM asistencia WHERE id_partido = ${id}`;
    await sql`DELETE FROM partidos WHERE id = ${id}`;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Jugadores ────────────────────────────────────────────────

export async function getJugadores() {
  const sql = getSql();
  try {
    const rows = await sql`
      SELECT * FROM jugadores WHERE activo = true
      ORDER BY COALESCE(dorsal, 99)
    `;
    return rows.map(mapJugador);
  } catch (e) {
    return [];
  }
}

export async function getTodosJugadores() {
  const sql = getSql();
  try {
    const rows = await sql`SELECT * FROM jugadores ORDER BY COALESCE(dorsal, 99)`;
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
    await sql`
      INSERT INTO jugadores (id, nombre, dorsal, posicion, activo)
      VALUES (${id}, ${data.Nombre || ''}, ${Number(data.Dorsal) || 0}, ${posicion}, ${activo})
      ON CONFLICT (id) DO UPDATE SET
        nombre   = EXCLUDED.nombre,
        dorsal   = EXCLUDED.dorsal,
        posicion = EXCLUDED.posicion,
        activo   = EXCLUDED.activo
    `;
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function toggleJugadorActivo(id) {
  const sql = getSql();
  try {
    const rows = await sql`
      UPDATE jugadores SET activo = NOT activo WHERE id = ${id} RETURNING activo
    `;
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

export async function getAsistenciaPartido(idPartido) {
  const sql = getSql();
  try {
    const rows = await sql`SELECT * FROM asistencia WHERE id_partido = ${idPartido}`;
    return rows.map(mapAsistencia);
  } catch (e) {
    return [];
  }
}

export async function getAsistenciaAll() {
  const sql = getSql();
  try {
    const rows = await sql`SELECT * FROM asistencia`;
    return rows.map(mapAsistencia);
  } catch (e) {
    return [];
  }
}

export async function guardarAsistencia(idPartido, lista) {
  const sql = getSql();
  try {
    await sql`DELETE FROM asistencia WHERE id_partido = ${idPartido}`;
    for (const j of (lista || [])) {
      await sql`
        INSERT INTO asistencia
          (id_partido, id_jugador, nombre_jugador, presente, goles, asistencias, amarillas, rojas)
        VALUES (
          ${idPartido}, ${j.ID_Jugador}, ${j.Nombre_Jugador || ''},
          ${j.Presente === true || j.Presente === 'TRUE'},
          ${Number(j.Goles) || 0}, ${Number(j.Asistencias) || 0},
          ${Number(j.Amarillas) || 0}, ${Number(j.Rojas) || 0}
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
  try {
    const [campRows, partRows, jugRows, asistRows] = await Promise.all([
      sql`SELECT * FROM campeonatos ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`,
      sql`SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo, c.ano AS campeonato_ano, c.temporada AS campeonato_temporada FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id`,
      sql`SELECT * FROM jugadores ORDER BY COALESCE(dorsal, 99)`,
      sql`SELECT * FROM asistencia`
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
    const ganados = partidos.filter(p => Number(p.GF) > Number(p.GC)).length;
    const empatados = partidos.filter(p => Number(p.GF) === Number(p.GC)).length;
    const perdidos = partidos.filter(p => Number(p.GF) < Number(p.GC)).length;
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
      statsJugador[a.ID_Jugador].goles += Number(a.Goles) || 0;
      statsJugador[a.ID_Jugador].asistencias += Number(a.Asistencias) || 0;
      statsJugador[a.ID_Jugador].amarillas += Number(a.Amarillas) || 0;
      statsJugador[a.ID_Jugador].rojas += Number(a.Rojas) || 0;
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

export async function getDetalleJugador(idJugador) {
  const sql = getSql();
  try {
    const [jugRows, asistRows, partRows] = await Promise.all([
      sql`SELECT * FROM jugadores WHERE id = ${idJugador}`,
      sql`SELECT * FROM asistencia WHERE id_jugador = ${idJugador}`,
      sql`SELECT p.*, c.nombre AS campeonato_nombre FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id`
    ]);

    if (!jugRows.length) return { ok: false, error: 'Jugador no encontrado' };
    const jugador = mapJugador(jugRows[0]);

    const partidoMap = {};
    partRows.map(mapPartido).forEach(p => { partidoMap[p.ID] = p; });

    const historial = asistRows.map(mapAsistencia).map(a => {
      const p = partidoMap[a.ID_Partido] || {};
      return {
        ID_Partido: a.ID_Partido,
        Fecha: p.Fecha || '',
        Rival: p.Rival || '',
        GF: Number(p.GF) || 0,
        GC: Number(p.GC) || 0,
        Campeonato: p.Campeonato_Nombre || '',
        Presente: a.Presente === true,
        Goles: Number(a.Goles) || 0,
        Asistencias: Number(a.Asistencias) || 0,
        Amarillas: Number(a.Amarillas) || 0,
        Rojas: Number(a.Rojas) || 0
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

export async function getBootstrapData() {
  const sql = getSql();
  try {
    const [configRows, campRows, partRows, jugRows, asistRows] = await Promise.all([
      sql`SELECT * FROM config`,
      sql`SELECT * FROM campeonatos ORDER BY COALESCE(NULLIF(ano,'')::int,0) DESC`,
      sql`SELECT p.*, c.nombre AS campeonato_nombre, c.tipo AS campeonato_tipo, c.ano AS campeonato_ano, c.temporada AS campeonato_temporada FROM partidos p LEFT JOIN campeonatos c ON p.id_campeonato = c.id ORDER BY p.fecha DESC NULLS LAST`,
      sql`SELECT * FROM jugadores ORDER BY COALESCE(dorsal, 99)`,
      sql`SELECT * FROM asistencia`
    ]);

    const config = {};
    configRows.forEach(r => { config[r.clave] = r.valor || ''; });

    return {
      ok: true,
      config,
      campeonatos: campRows.map(mapCampeonato),
      partidos: partRows.map(mapPartido),
      jugadores: jugRows.map(mapJugador),
      asistencia: asistRows.map(mapAsistencia)
    };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Planilla ──────────────────────────────────────────────────

export async function getPlanilla2() {
  const sql = getSql();
  try {
    const rows = await sql`SELECT valor FROM config WHERE clave = 'planilla_v2'`;
    if (!rows.length || !rows[0].valor) return { ok: true, data: null };
    try {
      return { ok: true, data: JSON.parse(rows[0].valor) };
    } catch {
      return { ok: true, data: null };
    }
  } catch (e) {
    return { ok: false, error: e.message, data: null };
  }
}

export async function savePlanilla2(payload) {
  const sql = getSql();
  try {
    const raw = typeof payload === 'string' ? payload : JSON.stringify(payload || {});
    await sql`
      INSERT INTO config (clave, valor) VALUES ('planilla_v2', ${raw})
      ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
    `;
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function getJugadoresActivosPlan() {
  const sql = getSql();
  try {
    const rows = await sql`SELECT * FROM jugadores WHERE activo = true ORDER BY COALESCE(dorsal, 99)`;
    return {
      ok: true,
      jugadores: rows.map(r => ({
        id: r.id,
        dorsal: r.dorsal || '',
        nombre: r.nombre || '',
        posicion: r.posicion || ''
      }))
    };
  } catch (e) {
    return { ok: false, error: e.message, jugadores: [] };
  }
}

// ── Dispatch ──────────────────────────────────────────────────

export async function dispatch(action, payload) {
  switch (action) {
    case 'init':                  return initDB();
    case 'getCampeonatos':        return getCampeonatos();
    case 'guardarCampeonato':     return guardarCampeonato(payload);
    case 'eliminarCampeonato':    return eliminarCampeonato(payload);
    case 'getPartidos':           return getPartidos();
    case 'guardarPartido':        return guardarPartido(payload);
    case 'eliminarPartido':       return eliminarPartido(payload);
    case 'getJugadores':          return getJugadores();
    case 'getTodosJugadores':     return getTodosJugadores();
    case 'guardarJugador':        return guardarJugador(payload);
    case 'toggleJugador':         return toggleJugadorActivo(payload);
    case 'eliminarJugador':       return eliminarJugador(payload);
    case 'getAsistencia':         return getAsistenciaPartido(payload);
    case 'guardarAsistencia':     return guardarAsistencia(payload.idPartido, payload.lista);
    case 'getEstadisticas':       return getEstadisticas(payload);
    case 'getDetalleJugador':     return getDetalleJugador(payload);
    case 'getConfig':             return getConfig();
    case 'guardarConfig':         return guardarConfigCloud(payload);
    case 'removeLogo':            return removeLogoCloud();
    case 'uploadLogo':            return uploadLogo(payload);
    case 'checkAdmin':            return { isAdmin: true };
    case 'getBootstrapData':      return getBootstrapData();
    case 'getPlanilla2':          return getPlanilla2();
    case 'savePlanilla2':         return savePlanilla2(payload);
    case 'getJugadoresActivosPlan': return getJugadoresActivosPlan();
    default: return { ok: false, error: `Acción desconocida: ${action}` };
  }
}

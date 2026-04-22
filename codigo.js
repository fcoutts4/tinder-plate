// ============================================================
//  FUTBOL MANAGER v2 — Code.gs
//  Google Apps Script Backend — Reestructurado
//  Sheets: Campeonatos, Partidos, Jugadores, Asistencia
// ============================================================

const SHEET_CAMPEONATOS = 'Campeonatos';
const SHEET_PARTIDOS    = 'Partidos';
const SHEET_JUGADORES   = 'Jugadores';
const SHEET_ASISTENCIA  = 'Asistencia';
const SHEET_CONFIG      = 'Config';


const CACHE_TTL_SHORT = 60;
const CACHE_TTL_MEDIUM = 300;

function getCache() {
  return CacheService.getScriptCache();
}

function cacheGetJson(key) {
  try {
    const raw = getCache().get(key);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

function cachePutJson(key, value, ttl) {
  try {
    getCache().put(key, JSON.stringify(value), ttl || CACHE_TTL_SHORT);
  } catch (e) {}
}

function clearAllAppCaches() {
  try {
    getCache().removeAll(['partidos','campeonatos','jugadores_activos','jugadores_todos','config','bootstrap']);
  } catch (e) {}
}

function cacheShardKey(prefix, input) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, String(input));
  const hex = digest.map(function(b) {
    const v = (b < 0 ? b + 256 : b).toString(16);
    return v.length === 1 ? '0' + v : v;
  }).join('');
  return prefix + '_' + hex;
}

function normalizeBool(value) {
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (['true','1','si','sí','yes'].includes(v)) return true;
    if (['false','0','no'].includes(v)) return false;
  }
  return false;
}

function normalizeDateString(value) {
  if (!value) return '';
  try {
    return Utilities.formatDate(new Date(value), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  } catch (e) {
    return '';
  }
}

function setSheetData(sheet, headers, rows) {
  sheet.clearContents();
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  if (rows.length) {
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }
  sheet.getRange(1, 1, 1, headers.length)
    .setFontWeight('bold')
    .setBackground('#1565C0')
    .setFontColor('#ffffff');
}

function readConfigMap() {
  const cached = cacheGetJson('config');
  if (cached) return cached;

  const sheet = getSheet(SHEET_CONFIG);
  const data = sheet.getDataRange().getValues();
  const config = {};
  for (let i = 1; i < data.length; i++) {
    config[data[i][0]] = data[i][1] || '';
  }
  cachePutJson('config', config, CACHE_TTL_MEDIUM);
  return config;
}

// ── Entry point ─────────────────────────────────────────────
function doGet() {
  // Read team name from Config sheet for the tab title
  let teamName = 'Fútbol Manager';
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const cfgSheet = ss.getSheetByName(SHEET_CONFIG);
    if (cfgSheet) {
      const data = cfgSheet.getDataRange().getValues();
      for (let i = 1; i < data.length; i++) {
        if (data[i][0] === 'nombre' && data[i][1]) {
          teamName = data[i][1];
          break;
        }
      }
    }
  } catch (e) {}
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('⚽ ' + teamName)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// ── Inicializar hojas si no existen ─────────────────────────
function inicializarHojas() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const headerStyle = (sheet, cols) => {
    sheet.getRange(1, 1, 1, cols).setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
  };

  if (!ss.getSheetByName(SHEET_CAMPEONATOS)) {
    const s = ss.insertSheet(SHEET_CAMPEONATOS);
    s.appendRow(['ID', 'Nombre', 'Año', 'Temporada', 'Tipo']);
    headerStyle(s, 5);
  }

  if (!ss.getSheetByName(SHEET_PARTIDOS)) {
    const s = ss.insertSheet(SHEET_PARTIDOS);
    s.appendRow(['ID', 'ID_Campeonato', 'Fecha', 'Rival', 'Cancha', 'Ronda', 'GF', 'GC', 'Notas', 'Amistoso']);
    headerStyle(s, 10);
  }

  if (!ss.getSheetByName(SHEET_JUGADORES)) {
    const s = ss.insertSheet(SHEET_JUGADORES);
    s.appendRow(['ID', 'Nombre', 'Dorsal', 'Posición', 'Activo']);
    headerStyle(s, 5);
  }

  if (!ss.getSheetByName(SHEET_ASISTENCIA)) {
    const s = ss.insertSheet(SHEET_ASISTENCIA);
    s.appendRow(['ID_Partido', 'ID_Jugador', 'Nombre_Jugador', 'Presente', 'Goles', 'Asistencias', 'Amarillas', 'Rojas']);
    headerStyle(s, 8);
  }

  if (!ss.getSheetByName(SHEET_CONFIG)) {
    const s = ss.insertSheet(SHEET_CONFIG);
    s.appendRow(['Clave', 'Valor']);
    headerStyle(s, 2);
    s.appendRow(['nombre', 'Mi Equipo']);
    s.appendRow(['liga', '']);
    s.appendRow(['logo', '']);
    s.appendRow(['color', '#16a34a']);
  }

  return { ok: true };
}

// ── Migrar hojas existentes (agrega columnas faltantes) ─────
function migrarHojas() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Migrar Asistencia: agregar Amarillas y Rojas si no existen
  const sa = ss.getSheetByName(SHEET_ASISTENCIA);
  if (sa) {
    const headers = sa.getRange(1, 1, 1, sa.getLastColumn()).getValues()[0];
    if (!headers.includes('Amarillas')) {
      const col = sa.getLastColumn() + 1;
      sa.getRange(1, col).setValue('Amarillas').setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
    }
    if (!headers.includes('Rojas')) {
      const col = sa.getLastColumn() + 1;
      sa.getRange(1, col).setValue('Rojas').setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
    }
  }

  // Migrar Partidos: agregar ID_Campeonato si no existe
  const sp = ss.getSheetByName(SHEET_PARTIDOS);
  if (sp) {
    const headers = sp.getRange(1, 1, 1, sp.getLastColumn()).getValues()[0];
    if (!headers.includes('ID_Campeonato')) {
      // Insertar columna B
      sp.insertColumnBefore(2);
      sp.getRange(1, 2).setValue('ID_Campeonato').setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
    }
    // Remover columnas viejas Tipo, Año, Temporada si aún existen
    // No las borramos para no perder datos; simplemente las ignoramos en la lectura
    // Add Amistoso column if missing
    const headersRefresh = sp.getRange(1, 1, 1, sp.getLastColumn()).getValues()[0];
    if (!headersRefresh.includes('Amistoso')) {
      const col = sp.getLastColumn() + 1;
      sp.getRange(1, col).setValue('Amistoso').setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
    }
  }

  // Crear hoja Campeonatos si no existe
  if (!ss.getSheetByName(SHEET_CAMPEONATOS)) {
    const s = ss.insertSheet(SHEET_CAMPEONATOS);
    s.appendRow(['ID', 'Nombre', 'Año', 'Temporada', 'Tipo']);
    s.getRange(1, 1, 1, 5).setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
  }

  // Crear hoja Config si no existe
  if (!ss.getSheetByName(SHEET_CONFIG)) {
    const s = ss.insertSheet(SHEET_CONFIG);
    s.appendRow(['Clave', 'Valor']);
    s.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#1565C0').setFontColor('#ffffff');
    s.appendRow(['nombre', 'Mi Equipo']);
    s.appendRow(['liga', '']);
    s.appendRow(['logo', '']);
    s.appendRow(['color', '#16a34a']);
  }

  return { ok: true };
}

// ── CONFIG (guardado en la nube) ────────────────────────────

function getConfig() {
  try {
    return { ok: true, config: readConfigMap() };
  } catch (e) { return { ok: false, error: e.message }; }
}

function guardarConfigCloud(data) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(SHEET_CONFIG);
    if (!sheet) {
      inicializarHojas();
      sheet = ss.getSheetByName(SHEET_CONFIG);
    }
    const rows = sheet.getDataRange().getValues();

    // Only save nombre, liga, and color here (logo is handled separately via uploadLogoForm)
    const keys = ['nombre', 'liga', 'color'];
    keys.forEach(key => {
      if (data[key] === undefined) return;
      let found = false;
      for (let i = 1; i < rows.length; i++) {
        if (rows[i][0] === key) {
          sheet.getRange(i + 1, 2).setValue(data[key]);
          found = true;
          break;
        }
      }
      if (!found) {
        sheet.appendRow([key, data[key]]);
      }
    });

    clearAllAppCaches();
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// This function receives a FORM element directly from the client (not via dispatch)
function uploadLogoForm(formObj) {
  if (!isAdmin()) return { ok: false, error: 'Solo el administrador puede subir logo' };
  try {
    const fileBlob = formObj.logoFile;
    if (!fileBlob || !fileBlob.getBytes || fileBlob.getBytes().length === 0) {
      return { ok: false, error: 'No file received' };
    }

    // Upload to Drive
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const ssFile = DriveApp.getFileById(ss.getId());
    const parentFolders = ssFile.getParents();
    let folder = parentFolders.hasNext() ? parentFolders.next() : DriveApp.getRootFolder();

    // Delete old logo files
    const existing = folder.getFilesByName('team_logo');
    while (existing.hasNext()) {
      existing.next().setTrashed(true);
    }

    // Create new file
    fileBlob.setName('team_logo');
    const file = folder.createFile(fileBlob);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);

    const fileId = file.getId();
    const logoUrl = 'https://lh3.googleusercontent.com/d/' + fileId;

    // Save URL in Config sheet
    let sheet = ss.getSheetByName(SHEET_CONFIG);
    if (!sheet) { inicializarHojas(); sheet = ss.getSheetByName(SHEET_CONFIG); }
    const rows = sheet.getDataRange().getValues();
    let found = false;
    for (let i = 1; i < rows.length; i++) {
      if (rows[i][0] === 'logo') {
        sheet.getRange(i + 1, 2).setValue(logoUrl);
        found = true;
        break;
      }
    }
    if (!found) { sheet.appendRow(['logo', logoUrl]); }

    clearAllAppCaches();
    return { ok: true, logoUrl: logoUrl };
  } catch (e) { return { ok: false, error: e.message }; }
}

function removeLogoCloud() {
  try {
    // Clear logo URL from Config
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(SHEET_CONFIG);
    if (!sheet) clearAllAppCaches();
    return { ok: true };
    const rows = sheet.getDataRange().getValues();
    for (let i = 1; i < rows.length; i++) {
      if (rows[i][0] === 'logo') {
        // Trash the Drive file if it exists
        const oldUrl = rows[i][1] || '';
        const match = oldUrl.match(/\/d\/([a-zA-Z0-9_-]+)/);
        if (match) {
          try { DriveApp.getFileById(match[1]).setTrashed(true); } catch(e2) {}
        }
        sheet.getRange(i + 1, 2).setValue('');
        break;
      }
    }
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Helpers ─────────────────────────────────────────────────
function getSheet(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(name);
  if (!sheet) { inicializarHojas(); sheet = ss.getSheetByName(name); }
  return sheet;
}

function generateId() {
  return Utilities.getUuid().substring(0, 8).toUpperCase();
}

function sheetToObjects(sheet) {
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  const headers = data[0];
  return data.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row[i]; });
    return obj;
  });
}

function esAmistosoValor(valor, ronda) {
  if (valor === true || valor === 1) return true;
  if (typeof valor === 'string') {
    var v = valor.trim().toLowerCase();
    if (v === 'true' || v === '1' || v === 'si' || v === 'sí') return true;
  }
  if (typeof ronda === 'string' && ronda.trim().toLowerCase() === 'ami') return true;
  return false;
}


// ── CAMPEONATOS ─────────────────────────────────────────────

function getCampeonatos() {
  try {
    const cached = cacheGetJson('campeonatos');
    if (cached) return cached;

    const data = sheetToObjects(getSheet(SHEET_CAMPEONATOS))
      .sort((a, b) => (Number(b.Año) || 0) - (Number(a.Año) || 0));

    cachePutJson('campeonatos', data, CACHE_TTL_MEDIUM);
    return data;
  } catch (e) { return []; }
}

function guardarCampeonato(data) {
  try {
    const sheet = getSheet(SHEET_CAMPEONATOS);
    const headers = ['ID', 'Nombre', 'Año', 'Temporada', 'Tipo'];
    const rows = sheetToObjects(sheet);
    const id = data.ID || generateId();
    let found = false;

    const out = rows.map(r => {
      if (String(r.ID) === String(id)) {
        found = true;
        return [id, data.Nombre, data.Año || '', data.Temporada || '', data.Tipo || 'Liga'];
      }
      return [r.ID, r.Nombre, r.Año || '', r.Temporada || '', r.Tipo || 'Liga'];
    });

    if (!found) out.push([id, data.Nombre, data.Año || '', data.Temporada || '', data.Tipo || 'Liga']);
    setSheetData(sheet, headers, out);
    clearAllAppCaches();
    return { ok: true, id };
  } catch (e) { return { ok: false, error: e.message }; }
}

function eliminarCampeonato(id) {
  try {
    const sheet = getSheet(SHEET_CAMPEONATOS);
    const headers = ['ID', 'Nombre', 'Año', 'Temporada', 'Tipo'];
    const rows = sheetToObjects(sheet)
      .filter(r => String(r.ID) !== String(id))
      .map(r => [r.ID, r.Nombre, r.Año || '', r.Temporada || '', r.Tipo || 'Liga']);
    setSheetData(sheet, headers, rows);
    clearAllAppCaches();
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── PARTIDOS ────────────────────────────────────────────────

function getPartidos() {
  try {
    const cached = cacheGetJson('partidos');
    if (cached) return cached;

    const rows = sheetToObjects(getSheet(SHEET_PARTIDOS));
    const campMap = {};
    getCampeonatos().forEach(c => { campMap[c.ID] = c; });

    const result = rows.map(r => {
      const camp = campMap[r.ID_Campeonato] || {};
      return {
        ...r,
        Fecha: normalizeDateString(r.Fecha),
        Amistoso: esAmistosoValor(r.Amistoso, r.Ronda),
        Campeonato_Nombre: camp.Nombre || '',
        Campeonato_Tipo: camp.Tipo || '',
        Campeonato_Año: camp.Año || '',
        Campeonato_Temporada: camp.Temporada || ''
      };
    }).sort((a, b) => {
      if (!a.Fecha && !b.Fecha) return 0;
      if (!a.Fecha) return 1;
      if (!b.Fecha) return -1;
      return String(b.Fecha).localeCompare(String(a.Fecha));
    });

    cachePutJson('partidos', result, CACHE_TTL_SHORT);
    return result;
  } catch (e) { return []; }
}

function guardarPartido(data) {
  try {
    const sheet = getSheet(SHEET_PARTIDOS);
    const headers = ['ID', 'ID_Campeonato', 'Fecha', 'Rival', 'Cancha', 'Ronda', 'GF', 'GC', 'Notas', 'Amistoso'];
    const rows = sheetToObjects(sheet);
    const id = data.ID || generateId();
    const fecha = data.Fecha ? new Date(data.Fecha + 'T12:00:00') : new Date();
    const amistoso = esAmistosoValor(data.Amistoso, data.Ronda);
    let found = false;

    const out = rows.map(r => {
      if (String(r.ID) === String(id)) {
        found = true;
        return [id, data.ID_Campeonato || '', fecha, data.Rival, data.Cancha || '', data.Ronda, Number(data.GF) || 0, Number(data.GC) || 0, data.Notas || '', amistoso];
      }
      return [r.ID, r.ID_Campeonato || '', r.Fecha || '', r.Rival || '', r.Cancha || '', r.Ronda || '', Number(r.GF) || 0, Number(r.GC) || 0, r.Notas || '', esAmistosoValor(r.Amistoso, r.Ronda)];
    });

    if (!found) out.push([id, data.ID_Campeonato || '', fecha, data.Rival, data.Cancha || '', data.Ronda, Number(data.GF) || 0, Number(data.GC) || 0, data.Notas || '', amistoso]);
    setSheetData(sheet, headers, out);
    clearAllAppCaches();
    return { ok: true, id };
  } catch (e) { return { ok: false, error: e.message }; }
}

function eliminarPartido(id) {
  try {
    const partidoSheet = getSheet(SHEET_PARTIDOS);
    const partidoHeaders = ['ID', 'ID_Campeonato', 'Fecha', 'Rival', 'Cancha', 'Ronda', 'GF', 'GC', 'Notas', 'Amistoso'];
    const partidosRows = sheetToObjects(partidoSheet)
      .filter(r => String(r.ID) !== String(id))
      .map(r => [r.ID, r.ID_Campeonato || '', r.Fecha || '', r.Rival || '', r.Cancha || '', r.Ronda || '', Number(r.GF) || 0, Number(r.GC) || 0, r.Notas || '', esAmistosoValor(r.Amistoso, r.Ronda)]);
    setSheetData(partidoSheet, partidoHeaders, partidosRows);

    const asistSheet = getSheet(SHEET_ASISTENCIA);
    const asistHeaders = ['ID_Partido', 'ID_Jugador', 'Nombre_Jugador', 'Presente', 'Goles', 'Asistencias', 'Amarillas', 'Rojas'];
    const asistRows = sheetToObjects(asistSheet)
      .filter(r => String(r.ID_Partido) !== String(id))
      .map(r => [r.ID_Partido, r.ID_Jugador, r.Nombre_Jugador || '', normalizeBool(r.Presente), Number(r.Goles) || 0, Number(r.Asistencias) || 0, Number(r.Amarillas) || 0, Number(r.Rojas) || 0]);
    setSheetData(asistSheet, asistHeaders, asistRows);

    clearAllAppCaches();
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── JUGADORES ───────────────────────────────────────────────

function getJugadores() {
  try {
    const cached = cacheGetJson('jugadores_activos');
    if (cached) return cached;

    const data = sheetToObjects(getSheet(SHEET_JUGADORES))
      .filter(j => j.Activo !== false && j.Activo !== 'FALSE')
      .sort((a, b) => (Number(a.Dorsal) || 99) - (Number(b.Dorsal) || 99));

    cachePutJson('jugadores_activos', data, CACHE_TTL_MEDIUM);
    return data;
  } catch (e) { return []; }
}

function getTodosJugadores() {
  try {
    const cached = cacheGetJson('jugadores_todos');
    if (cached) return cached;

    const data = sheetToObjects(getSheet(SHEET_JUGADORES))
      .sort((a, b) => (Number(a.Dorsal) || 99) - (Number(b.Dorsal) || 99));

    cachePutJson('jugadores_todos', data, CACHE_TTL_MEDIUM);
    return data;
  } catch (e) { return []; }
}

function guardarJugador(data) {
  try {
    const sheet = getSheet(SHEET_JUGADORES);
    const headers = ['ID', 'Nombre', 'Dorsal', 'Posición', 'Activo'];
    const rows = sheetToObjects(sheet);
    const id = data.ID || generateId();
    const activo = !(data.Activo === false || data.Activo === 'FALSE' || data.Activo === 'false');
    let found = false;

    const out = rows.map(r => {
      if (String(r.ID) === String(id)) {
        found = true;
        return [id, data.Nombre, Number(data.Dorsal) || 0, data.Posición || '', activo];
      }
      return [r.ID, r.Nombre, Number(r.Dorsal) || 0, r.Posición || '', !(r.Activo === false || r.Activo === 'FALSE')];
    });

    if (!found) out.push([id, data.Nombre, Number(data.Dorsal) || 0, data.Posición || '', activo]);
    setSheetData(sheet, headers, out);
    clearAllAppCaches();
    return { ok: true, id };
  } catch (e) { return { ok: false, error: e.message }; }
}

function toggleJugadorActivo(id) {
  try {
    const sheet = getSheet(SHEET_JUGADORES);
    const headers = ['ID', 'Nombre', 'Dorsal', 'Posición', 'Activo'];
    const rows = sheetToObjects(sheet);
    let updatedActivo = null;

    const out = rows.map(r => {
      let activo = !(r.Activo === false || r.Activo === 'FALSE');
      if (String(r.ID) === String(id)) {
        activo = !activo;
        updatedActivo = activo;
      }
      return [r.ID, r.Nombre, Number(r.Dorsal) || 0, r.Posición || '', activo];
    });

    if (updatedActivo === null) return { ok: false };
    setSheetData(sheet, headers, out);
    clearAllAppCaches();
    return { ok: true, activo: updatedActivo };
  } catch (e) { return { ok: false, error: e.message }; }
}

function eliminarJugador(id) {
  try {
    const sheet = getSheet(SHEET_JUGADORES);
    const headers = ['ID', 'Nombre', 'Dorsal', 'Posición', 'Activo'];
    const rows = sheetToObjects(sheet)
      .filter(r => String(r.ID) !== String(id))
      .map(r => [r.ID, r.Nombre, Number(r.Dorsal) || 0, r.Posición || '', !(r.Activo === false || r.Activo === 'FALSE')]);
    setSheetData(sheet, headers, rows);
    clearAllAppCaches();
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── ASISTENCIA ──────────────────────────────────────────────

function getAsistenciaAll() {
  try {
    return sheetToObjects(getSheet(SHEET_ASISTENCIA));
  } catch (e) { return []; }
}


function getAsistenciaPartido(idPartido) {
  try {
    const cacheKey = cacheShardKey('asistencia', idPartido);
    const cached = cacheGetJson(cacheKey);
    if (cached) return cached;

    const rows = sheetToObjects(getSheet(SHEET_ASISTENCIA))
      .filter(r => String(r.ID_Partido) === String(idPartido));

    cachePutJson(cacheKey, rows, CACHE_TTL_SHORT);
    return rows;
  } catch (e) { return []; }
}

function getAllAsistencia() {
  try {
    const cached = cacheGetJson('asistencia_all');
    if (cached) return cached;

    const rows = sheetToObjects(getSheet(SHEET_ASISTENCIA));
    cachePutJson('asistencia_all', rows, CACHE_TTL_SHORT);
    return rows;
  } catch (e) { return []; }
}

function guardarAsistencia(idPartido, listaAsistencia) {
  try {
    const sheet = getSheet(SHEET_ASISTENCIA);
    const headers = ['ID_Partido', 'ID_Jugador', 'Nombre_Jugador', 'Presente', 'Goles', 'Asistencias', 'Amarillas', 'Rojas'];

    const existentes = sheetToObjects(sheet)
      .filter(r => String(r.ID_Partido) !== String(idPartido))
      .map(r => [r.ID_Partido, r.ID_Jugador, r.Nombre_Jugador || '', normalizeBool(r.Presente), Number(r.Goles) || 0, Number(r.Asistencias) || 0, Number(r.Amarillas) || 0, Number(r.Rojas) || 0]);

    const nuevos = (listaAsistencia || []).map(j => [
      idPartido,
      j.ID_Jugador,
      j.Nombre_Jugador || '',
      j.Presente === true || j.Presente === 'TRUE',
      Number(j.Goles) || 0,
      Number(j.Asistencias) || 0,
      Number(j.Amarillas) || 0,
      Number(j.Rojas) || 0
    ]);

    setSheetData(sheet, headers, existentes.concat(nuevos));
    clearAllAppCaches();
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── ESTADÍSTICAS ────────────────────────────────────────────

function getEstadisticas(filtros) {
  try {
    const cacheKey = cacheShardKey('stats', JSON.stringify(filtros || {}));
    const cached = cacheGetJson(cacheKey);
    if (cached) return cached;

    let partidos = getPartidos();
    const asistencia = getAllAsistencia();
    const jugadores = getTodosJugadores();
    const campeonatos = getCampeonatos();

    const yearSet = new Set();
    partidos.forEach(p => {
      if (p.Fecha) {
        const y = new Date(p.Fecha + 'T12:00:00').getFullYear();
        if (y) yearSet.add(String(y));
      }
    });
    const yearList = Array.from(yearSet).sort((a, b) => Number(b) - Number(a));

    const idCampeonato = filtros && filtros.idCampeonato ? filtros.idCampeonato : null;
    const año = filtros && filtros.año ? String(filtros.año) : null;
    const tipo = filtros && filtros.tipo ? filtros.tipo : null;

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
        ID: j.ID, Nombre: j.Nombre, Dorsal: j.Dorsal, Posición: j.Posición,
        Activo: j.Activo, partidos: 0, presentes: 0, goles: 0, asistencias: 0, amarillas: 0, rojas: 0
      };
    });

    asistencia.forEach(a => {
      if (!statsJugador[a.ID_Jugador]) return;
      if (!partidoIds.has(String(a.ID_Partido))) return;
      statsJugador[a.ID_Jugador].partidos++;
      if (a.Presente === true || a.Presente === 'TRUE') statsJugador[a.ID_Jugador].presentes++;
      statsJugador[a.ID_Jugador].goles += Number(a.Goles) || 0;
      statsJugador[a.ID_Jugador].asistencias += Number(a.Asistencias) || 0;
      statsJugador[a.ID_Jugador].amarillas += Number(a.Amarillas) || 0;
      statsJugador[a.ID_Jugador].rojas += Number(a.Rojas) || 0;
    });

    const ranking = Object.values(statsJugador).sort((a, b) => b.goles - a.goles || b.asistencias - a.asistencias);
    const campList = campeonatos.map(c => ({ ID: c.ID, Nombre: c.Nombre, Tipo: c.Tipo, Año: c.Año, Temporada: c.Temporada }))
      .sort((a, b) => (Number(b.Año) || 0) - (Number(a.Año) || 0));

    const result = {
      ok: true,
      resumen: { total, ganados, empatados, perdidos, gf, gc },
      ranking,
      campList,
      yearList
    };
    cachePutJson(cacheKey, result, CACHE_TTL_SHORT);
    return result;
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Detalle de jugador (historial por partido) ──────────────

function getDetalleJugador(idJugador) {
  try {
    const cacheKey = cacheShardKey('detalle_jugador', idJugador);
    const cached = cacheGetJson(cacheKey);
    if (cached) return cached;

    const jugadores = getTodosJugadores();
    const jugador = jugadores.find(j => String(j.ID) === String(idJugador));
    if (!jugador) return { ok: false, error: 'Jugador no encontrado' };

    const asistencia = sheetToObjects(getSheet(SHEET_ASISTENCIA)).filter(a => String(a.ID_Jugador) === String(idJugador));
    const partidos = getPartidos();
    const partidoMap = {};
    partidos.forEach(p => { partidoMap[p.ID] = p; });

    const historial = asistencia.map(a => {
      const p = partidoMap[a.ID_Partido] || {};
      return {
        ID_Partido: a.ID_Partido,
        Fecha: p.Fecha || '',
        Rival: p.Rival || '',
        GF: Number(p.GF) || 0,
        GC: Number(p.GC) || 0,
        Campeonato: p.Campeonato_Nombre || '',
        Presente: a.Presente === true || a.Presente === 'TRUE',
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

    const result = { ok: true, jugador, historial, totales };
    cachePutJson(cacheKey, result, CACHE_TTL_SHORT);
    return result;
  } catch (e) { return { ok: false, error: e.message }; }
}

function getBootstrapData() {
  try {
    const cached = cacheGetJson('bootstrap');
    if (cached) return cached;

    const result = {
      ok: true,
      config: readConfigMap(),
      campeonatos: getCampeonatos(),
      partidos: getPartidos(),
      jugadores: getTodosJugadores(),
      asistencia: getAsistenciaAll()
    };
    cachePutJson('bootstrap', result, CACHE_TTL_SHORT);
    return result;
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── Dispatch desde el cliente ───────────────────────────────

// Cambiamos esto para que no valide por correo electrónico
function isAdmin() {
  // Ahora siempre devuelve true para que el servidor procese tus cambios
  return true; 
}

function checkAdmin() {
  return { isAdmin: isAdmin() };
}

// Write actions that require admin
const WRITE_ACTIONS = [
  'init', 'migrar', 'guardarCampeonato', 'eliminarCampeonato',
  'guardarPartido', 'eliminarPartido', 'guardarJugador',
  'toggleJugador', 'eliminarJugador', 'guardarAsistencia',
  'guardarConfig', 'removeLogo'
];

function dispatch(action, payload) {
  // Block write actions for non-admins
  if (WRITE_ACTIONS.includes(action) && !isAdmin()) {
    return { ok: false, error: 'Solo el administrador puede realizar esta acción' };
  }

  switch (action) {
    case 'init':               return inicializarHojas();
    case 'migrar':             return migrarHojas();
    case 'getCampeonatos':     return getCampeonatos();
    case 'guardarCampeonato':  return guardarCampeonato(payload);
    case 'eliminarCampeonato': return eliminarCampeonato(payload);
    case 'getPartidos':        return getPartidos();
    case 'guardarPartido':     return guardarPartido(payload);
    case 'eliminarPartido':    return eliminarPartido(payload);
    case 'getJugadores':       return getJugadores();
    case 'getTodosJugadores':  return getTodosJugadores();
    case 'guardarJugador':     return guardarJugador(payload);
    case 'toggleJugador':      return toggleJugadorActivo(payload);
    case 'eliminarJugador':    return eliminarJugador(payload);
    case 'getAsistencia':      return getAsistenciaPartido(payload);
    case 'guardarAsistencia':  return guardarAsistencia(payload.idPartido, payload.lista);
    case 'getEstadisticas':    return getEstadisticas(payload);
    case 'getDetalleJugador':  return getDetalleJugador(payload);
    case 'getConfig':          return getConfig();
    case 'guardarConfig':      return guardarConfigCloud(payload);
    case 'removeLogo':         return removeLogoCloud();
    case 'checkAdmin':         return checkAdmin();
    case 'getBootstrapData':   return getBootstrapData();
    default: return { ok: false, error: 'Acción desconocida: ' + action };
  }
}

// ============================================================
// PLANILLA 2 (módulo independiente, persistencia en Config)
// ============================================================
const PLANILLA2_KEY = 'planilla_v2';

function pl2_getConfigSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEET_CONFIG);
  if (!sh) {
    sh = ss.insertSheet(SHEET_CONFIG);
    sh.getRange(1,1,1,2).setValues([['clave','valor']]);
  }
  return sh;
}

function getPlanilla2() {
  try {
    const sh = pl2_getConfigSheet_();
    const data = sh.getDataRange().getValues();
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === PLANILLA2_KEY) {
        const raw = String(data[i][1] || '');
        if (!raw) return { ok: true, data: null };
        try { return { ok: true, data: JSON.parse(raw) }; }
        catch (e) { return { ok: true, data: null }; }
      }
    }
    return { ok: true, data: null };
  } catch (e) {
    return { ok: false, error: e.message, data: null };
  }
}

function savePlanilla2(payload) {
  try {
    const sh = pl2_getConfigSheet_();
    const data = sh.getDataRange().getValues();
    const raw = typeof payload === 'string' ? payload : JSON.stringify(payload || {});
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === PLANILLA2_KEY) {
        sh.getRange(i + 1, 2).setValue(raw);
        return { ok: true };
      }
    }
    sh.appendRow([PLANILLA2_KEY, raw]);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function getJugadoresActivosPlan() {
  try {
    const all = sheetToObjects(getSheet(SHEET_JUGADORES)) || [];
    const activos = all.filter(function (j) {
      return j.Activo !== false && j.Activo !== 'FALSE' && j.Activo !== 'No' && j.Activo !== 'NO';
    });
    activos.sort(function (a, b) {
      return (Number(a.Dorsal) || 99) - (Number(b.Dorsal) || 99);
    });
    return { ok: true, jugadores: activos.map(function (j) {
      return {
        id: j.ID,
        dorsal: j.Dorsal || '',
        nombre: j.Nombre || '',
        posicion: j['Posición'] || j.Posicion || ''
      };
    }) };
  } catch (e) {
    return { ok: false, error: e.message, jugadores: [] };
  }
}

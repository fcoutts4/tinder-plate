// build.js — Transforms Index.html (Google Apps Script) → public/index.html (fetch-based)
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
let html = readFileSync(join(__dirname, 'Index.html'), 'utf8').replace(/\r\n/g, '\n');

function replace(desc, oldStr, newStr) {
  if (!html.includes(oldStr)) {
    console.error(`\n❌  FAILED to find replacement target: "${desc}"`);
    console.error('   Searched for:', oldStr.substring(0, 100).replace(/\n/g, '\\n'));
    process.exit(1);
  }
  html = html.replace(oldStr, newStr);
  console.log(`✓  ${desc}`);
}

// ── 1. Replace google.script.run bridge ─────────────────────
replace(
  'GAS call() bridge → fetch',
  `function call(action, payload, onSuccess, onError) {
  google.script.run
    .withSuccessHandler(r => { if(onSuccess) onSuccess(r); })
    .withFailureHandler(e => {
      console.error(action, e);
      toast('Error: ' + e.message, true);
      if(onError) onError(e);
    })
    .dispatch(action, payload);
}`,
  `function call(action, payload, onSuccess, onError) {
  fetch('/api/dispatch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({action, payload})
  })
  .then(r => r.json())
  .then(r => { if(onSuccess) onSuccess(r); })
  .catch(e => {
    console.error(action, e);
    toast('Error: ' + e.message, true);
    if(onError) onError(e);
  });
}`
);

// ── 2. Replace Drive logo upload ─────────────────────────────
replace(
  'Logo upload (Drive → base64 fetch)',
  `  toast('Subiendo logo a Drive\u2026');

  const formEl = document.getElementById('logoUploadForm');
  google.script.run
    .withSuccessHandler(function(r) {
      if(r && r.ok && r.logoUrl) {
        STATE.config.logo = r.logoUrl;
        STATE.bootstrapLoaded = false;
        updateLogoUI();
        updateHeader();
        toast('Logo guardado \u2713');
      } else {
        toast('Error: ' + (r && r.error ? r.error : 'desconocido'), true);
      }
      document.getElementById('logoFileInput').value = '';
    })
    .withFailureHandler(function(e) {
      toast('Error al subir logo: ' + e.message, true);
      document.getElementById('logoFileInput').value = '';
    })
    .uploadLogoForm(formEl);`,
  `  toast('Subiendo logo\u2026');
  const reader = new FileReader();
  reader.onload = function(evt) {
    call('uploadLogo', {dataUrl: evt.target.result, name: file.name}, function(r) {
      if(r && r.ok && r.logoUrl) {
        STATE.config.logo = r.logoUrl;
        STATE.bootstrapLoaded = false;
        updateLogoUI();
        updateHeader();
        toast('Logo guardado \u2713');
      } else {
        toast('Error: ' + (r && r.error ? r.error : 'desconocido'), true);
      }
      document.getElementById('logoFileInput').value = '';
    }, function(e) {
      toast('Error al subir logo: ' + e.message, true);
      document.getElementById('logoFileInput').value = '';
    });
  };
  reader.readAsDataURL(file);`
);

// ── 3. Replace saveRemote in planilla ────────────────────────
replace(
  'saveRemote (planilla)',
  `  function saveRemote(){
    clearTimeout(remoteSaveTimer);
    remoteSaveTimer=setTimeout(function(){
      try{
        google.script.run
          .withSuccessHandler(function(){status('Guardado \u2713');})
          .withFailureHandler(function(){status('Guardado s\u00f3lo local');})
          .savePlanilla2(JSON.stringify(state));
      }catch(e){
        status('Guardado s\u00f3lo local');
      }
    },1200);
  }`,
  `  function saveRemote(){
    clearTimeout(remoteSaveTimer);
    remoteSaveTimer=setTimeout(function(){
      call('savePlanilla2', JSON.stringify(state),
        function(){status('Guardado \u2713');},
        function(){status('Guardado s\u00f3lo local');}
      );
    },1200);
  }`
);

// ── 4. Replace loadRemote in planilla ────────────────────────
replace(
  'loadRemote (planilla)',
  `  function loadRemote(cb){
    try{
      google.script.run
        .withSuccessHandler(function(res){
          if(res && res.ok && res.data) cb(res.data);
          else cb(null);
        })
        .withFailureHandler(function(){cb(null);})
        .getPlanilla2();
    }catch(e){cb(null);}
  }`,
  `  function loadRemote(cb){
    call('getPlanilla2', null,
      function(res){
        if(res && res.ok && res.data) cb(res.data);
        else cb(null);
      },
      function(){cb(null);}
    );
  }`
);

// ── 5. Replace loadPlayers in planilla ───────────────────────
replace(
  'loadPlayers (planilla)',
  `  function loadPlayers(cb){
    try{
      google.script.run
        .withSuccessHandler(function(res){
          if(res && res.ok && Array.isArray(res.jugadores)){
            allPlayers=res.jugadores;
            cb(null);
          } else {
            cb(new Error((res&&res.error)||'Error al cargar jugadores'));
          }
        })
        .withFailureHandler(function(err){cb(err);})
        .getJugadoresActivosPlan();
    }catch(e){cb(e);}
  }`,
  `  function loadPlayers(cb){
    call('getJugadoresActivosPlan', null,
      function(res){
        if(res && res.ok && Array.isArray(res.jugadores)){
          allPlayers=res.jugadores;
          cb(null);
        } else {
          cb(new Error((res&&res.error)||'Error al cargar jugadores'));
        }
      },
      function(err){cb(err);}
    );
  }`
);

mkdirSync(join(__dirname, 'public'), { recursive: true });
writeFileSync(join(__dirname, 'public', 'index.html'), html, 'utf8');
console.log('\n\u2705  public/index.html built successfully');

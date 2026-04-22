# Fútbol Manager

App de gestión de equipos de fútbol: partidos, jugadores, asistencia, estadísticas y planilla táctica.

## Stack

- **Frontend** — HTML/CSS/JS estático (`public/index.html`)
- **Backend** — Vercel Serverless Functions (`api/dispatch.js`)
- **Base de datos** — Neon PostgreSQL

## Estructura

```
├── public/
│   └── index.html        # App frontend (SPA)
├── api/
│   └── dispatch.js       # Único endpoint POST /api/dispatch
├── lib/
│   └── db.js             # Handlers de base de datos
├── scripts/
│   └── seed.mjs          # Importa datos iniciales (corre 1 vez en deploy)
└── build.js              # Script de build para Vercel
```

## Variables de entorno

| Variable | Descripción |
|---|---|
| `DATABASE_URL` | Connection string de Neon PostgreSQL |

## Deploy en Vercel

1. Conectar el repo en [vercel.com](https://vercel.com)
2. Agregar la variable `DATABASE_URL` en *Settings → Environment Variables*
3. Deploy — el script de build crea las tablas e importa datos automáticamente (solo si la DB está vacía)

## Desarrollo local

```bash
# Instalar dependencias
npm install

# Crear .env.local con el DATABASE_URL
cp .env.example .env.local

# Levantar servidor local
vercel dev
```

## Base de datos

Las tablas se crean automáticamente en el primer deploy:

| Tabla | Descripción |
|---|---|
| `config` | Nombre del equipo, color, logo |
| `campeonatos` | Torneos y ligas |
| `partidos` | Resultados de partidos |
| `jugadores` | Plantel |
| `asistencia` | Presencia y estadísticas por partido |

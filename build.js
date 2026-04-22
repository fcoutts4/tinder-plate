// build.js — runs during Vercel deploy: seeds the Neon database
// public/index.html is pre-built and committed to the repo

if (process.env.DATABASE_URL) {
  console.log('Synchronizing database...');
  const { main } = await import('./scripts/seed.mjs');
  await main();
} else {
  console.warn('DATABASE_URL not set — skipping seed. Add it in Vercel environment variables.');
}

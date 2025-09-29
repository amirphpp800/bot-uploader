// Cloudflare Pages Function: Telegram webhook (POST only)
// Boot the main app (sets globalThis.APP)
import '../main.js';

export async function onRequestPost(context) {
  const { request, env, waitUntil } = context;
  let app = globalThis.APP;
  if (!app || typeof app.fetch !== 'function') {
    // Fallback init in this worker
    await import('../main.js');
    app = globalThis.APP;
  }
  if (!app || typeof app.fetch !== 'function') {
    return new Response('Application not initialized', { status: 500 });
  }
  return app.fetch(request, env, { waitUntil });
}



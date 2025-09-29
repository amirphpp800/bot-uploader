/*
  Cloudflare Pages + Telegram Bot with KV
  - Features:
    1) Admin media upload -> generates unique deep-link to share
    2) Force-join channel enforcement
    3) Broadcast to all users
  - Web panel at / (and /panel) showing KV status and bot stats
*/

// Expose an app with a fetch(request, env, ctx) entrypoint
// functions/webhook.js and functions/[[path]].js import this file
// and call globalThis.APP.fetch(...)

const JSON_HEADERS = {
  'content-type': 'application/json; charset=UTF-8',
};

const HTML_HEADERS = {
  'content-type': 'text/html; charset=UTF-8',
};

function jsonResponse(obj, init = {}) {
  return new Response(JSON.stringify(obj), {
    ...init,
    headers: { ...JSON_HEADERS, ...(init.headers || {}) },
  });
}

function htmlResponse(html, init = {}) {
  return new Response(html, {
    ...init,
    headers: { ...HTML_HEADERS, ...(init.headers || {}) },
  });
}

function parseAdmins(env) {
  // Union of owners (ENV) and extra admins from KV (loaded into env.__extraAdmins by loadConfig)
  const ownersRaw = (env.ADMIN || '').trim();
  const owners = ownersRaw
    ? ownersRaw.split(',').map((s) => s.trim()).filter(Boolean)
    : [];
  const extras = Array.from(env.__extraAdmins || new Set());
  return new Set([...owners, ...extras]);
}

function ownersSet(env) {
  const ownersRaw = (env.ADMIN || '').trim();
  if (!ownersRaw) return new Set();
  return new Set(
    ownersRaw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

function isOwner(userId, env) {
  const owners = ownersSet(env);
  return owners.has(String(userId));
}

function isAdmin(userId, env) {
  const admins = parseAdmins(env);
  return admins.has(String(userId));
}

function randCode(len = 8) {
  const chars = 'abcdefghijkmnopqrstuvwxyz23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
  let out = '';
  for (let i = 0; i < len; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function tgApiUrl(env, method) {
  const token = env.BOT_TOKEN;
  if (!token) throw new Error('BOT_TOKEN missing');
  return `https://api.telegram.org/bot${token}/${method}`;
}

async function tgCall(env, method, payload) {
  const res = await fetch(tgApiUrl(env, method), {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!data.ok) {
    console.warn('Telegram API error', method, data);
  }
  return data;
}

async function tgGet(env, method, params) {
  const url = new URL(tgApiUrl(env, method));
  for (const [k, v] of Object.entries(params || {})) url.searchParams.set(k, v);
  const res = await fetch(url.toString(), { method: 'GET' });
  const data = await res.json().catch(() => ({}));
  if (!data.ok) console.warn('Telegram API error', method, data);
  return data;
}

async function ensureBotUsername(env) {
  try {
    if (env.__botUsername) return env.__botUsername;
    if (env.DATA) {
      const cached = await env.DATA.get('config:bot_username');
      if (cached) {
        env.__botUsername = cached;
        return cached;
      }
    }
    const info = await tgGet(env, 'getMe');
    const username = info?.result?.username || '';
    if (username && env.DATA) {
      try { await env.DATA.put('config:bot_username', username); } catch {}
    }
    env.__botUsername = username;
    return username;
  } catch (e) {
    console.warn('ensureBotUsername failed', e);
    return '';
  }
}

function getForceJoinChannel(env) {
  // For panel display (summary)
  const rules = getForceJoinRules(env);
  if (!rules.length) return '';
  const labels = rules.slice(0, 3).map((r) => r.type === 'username' ? `@${r.username}` : 'خصوصی');
  return rules.length > 3 ? `${labels.join('، ')} و ${rules.length - 3} مورد دیگر` : labels.join('، ');
}

async function loadConfig(env) {
  // Load KV config into env cache (per request)
  try {
    if (env.DATA) {
      const fj = await env.DATA.get('config:force_join_channel');
      if (fj) {
        try {
          const parsed = JSON.parse(fj);
          if (Array.isArray(parsed)) {
            env.__forceJoinRules = parsed;
          } else if (parsed && typeof parsed === 'object') {
            env.__forceJoinRules = [parsed];
          } else {
            env.__forceJoinRules = [];
          }
        } catch {
          // Backward compat: plain username string
          env.__forceJoinRules = [{ type: 'username', username: fj.replace(/^@/, '') }];
        }
      } else {
        env.__forceJoinRules = env.__forceJoinRules || [];
      }
      const admins = await env.DATA.get('config:admins');
      if (admins) {
        const arr = admins.split(',').map((s) => s.trim()).filter(Boolean);
        env.__extraAdmins = new Set(arr);
      } else {
        env.__extraAdmins = env.__extraAdmins || new Set();
      }
    }
  } catch (e) {
    console.warn('Config load failed', e);
  }
}

function getForceJoinRules(env) {
  return Array.isArray(env.__forceJoinRules) ? env.__forceJoinRules : [];
}

async function setForceJoinRules(env, rules) {
  env.__forceJoinRules = rules;
  if (!env.DATA) return;
  try { await env.DATA.put('config:force_join_channel', JSON.stringify(rules)); } catch (e) { console.warn('setForceJoinRules failed', e); }
}

function ruleKey(r) {
  if (!r || r.type === 'none') return 'none';
  if (r.type === 'username') return `u:${r.username}`;
  if (r.type === 'private') return `p:${r.chat_id||''}:${r.invite||''}`;
  return JSON.stringify(r);
}

// Human-friendly label for a force-join rule
function ruleLabel(r) {
  if (!r) return 'نامعتبر';
  if (r.type === 'username') return `@${(r.username || '').replace(/^@/, '')}`;
  if (r.type === 'private') return `خصوصی ${r.chat_id || ''}`;
  return 'نامعتبر';
}

// Temporary selection storage for remove-join flow
async function getRemoveSelection(env, userId) {
  if (!env.DATA) return new Set();
  try {
    const raw = await env.DATA.get(`tmp:rmjoin:${userId}`);
    if (!raw) return new Set();
    const arr = JSON.parse(raw);
    return new Set(Array.isArray(arr) ? arr : []);
  } catch {
    return new Set();
  }
}

async function setRemoveSelection(env, userId, set) {
  if (!env.DATA) return;
  try {
    await env.DATA.put(`tmp:rmjoin:${userId}`, JSON.stringify(Array.from(set || [])));
  } catch (e) { console.warn('setRemoveSelection failed', e); }
}

async function clearRemoveSelection(env, userId) {
  if (!env.DATA) return;
  try { await env.DATA.delete(`tmp:rmjoin:${userId}`); } catch (e) { console.warn('clearRemoveSelection failed', e); }
}

function buildRemoveJoinKeyboard(rules, selectedKeys) {
  const rows = [];
  for (const r of rules) {
    const key = ruleKey(r);
    const sel = selectedKeys.has(key);
    const text = `${sel ? '✅ ' : ''}${ruleLabel(r)}`;
    rows.push([{ text, callback_data: `rmjoin:toggle:${key}` }]);
  }
  // Controls row
  rows.push([
    { text: 'انتخاب همه', callback_data: 'rmjoin:all' },
    { text: 'هیچکدام', callback_data: 'rmjoin:none' },
  ]);
  rows.push([
    { text: 'حذف موارد انتخاب‌شده 🗑️', callback_data: 'rmjoin:confirm' },
  ]);
  rows.push([{ text: 'بازگشت', callback_data: 'admin:menu' }]);
  return { inline_keyboard: rows };
}

async function appendForceJoinRule(env, rule) {
  const existing = getForceJoinRules(env);
  const keys = new Set(existing.map(ruleKey));
  if (!keys.has(ruleKey(rule))) {
    existing.push(rule);
    await setForceJoinRules(env, existing);
  }
  return existing;
}

async function listAdmins(env) {
  const all = Array.from(parseAdmins(env));
  return all;
}

async function addAdmin(env, targetId) {
  if (!env.DATA) throw new Error('KV not bound');
  const id = String(targetId || '').trim();
  if (!id || !/^\d+$/.test(id)) throw new Error('invalid id');
  const current = (await env.DATA.get('config:admins')) || '';
  const set = new Set(
    current
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  );
  set.add(id);
  const out = Array.from(set).join(',');
  await env.DATA.put('config:admins', out);
  env.__extraAdmins = new Set(Array.from(set));
}

async function ensureUser(env, userId) {
  if (!env.DATA) return;
  const k = `user:${userId}`;
  try {
    const exists = await env.DATA.get(k);
    if (!exists) await env.DATA.put(k, '1');
  } catch (e) {
    console.warn('ensureUser failed', e);
  }
}

async function saveMedia(env, media) {
  // media = { code, type, file_id, caption }
  if (!env.DATA) throw new Error('KV not bound');
  const key = `media:${media.code}`;
  await env.DATA.put(key, JSON.stringify(media));
}

async function getMedia(env, code) {
  if (!env.DATA) return null;
  const key = `media:${code}`;
  const json = await env.DATA.get(key, 'json');
  return json || null;
}

// Bundle helpers
async function getBundle(env, code) {
  if (!env.DATA) return null;
  try { return (await env.DATA.get(`bundle:${code}`, 'json')) || null; } catch { return null; }
}

async function initBundle(env, code) {
  if (!env.DATA) return;
  try { await env.DATA.put(`bundle:${code}`, JSON.stringify({ code, items: [], created_at: Date.now() })); } catch {}
}

async function addBundleItem(env, code, item) {
  if (!env.DATA) return 0;
  try {
    const b = (await env.DATA.get(`bundle:${code}`, 'json')) || { code, items: [], created_at: Date.now() };
    b.items = Array.isArray(b.items) ? b.items : [];
    b.items.push(item);
    await env.DATA.put(`bundle:${code}`, JSON.stringify(b));
    return b.items.length;
  } catch (e) {
    console.warn('addBundleItem failed', e);
    return 0;
  }
}

async function getCodeType(env, code) {
  if (!env.DATA) return null;
  try {
    const m = await env.DATA.get(`media:${code}`);
    if (m) return 'media';
    const b = await env.DATA.get(`bundle:${code}`);
    if (b) return 'bundle';
  } catch (e) {
    console.warn('getCodeType failed', e);
  }
  return null;
}

async function setDisabled(env, code, disabled) {
  const t = await getCodeType(env, code);
  if (!t) return false;
  if (t === 'media') {
    const obj = await getMedia(env, code);
    if (!obj) return false;
    obj.disabled = !!disabled;
    await saveMedia(env, obj);
    return true;
  }
  if (t === 'bundle') {
    const obj = await getBundle(env, code);
    if (!obj) return false;
    obj.disabled = !!disabled;
    try { await env.DATA.put(`bundle:${code}`, JSON.stringify(obj)); } catch (e) { console.warn('save bundle failed', e); return false; }
    return true;
  }
  return false;
}

async function deleteCode(env, code) {
  if (!env.DATA) return false;
  try {
    await env.DATA.delete(`media:${code}`);
    await env.DATA.delete(`bundle:${code}`);
    return true;
  } catch (e) {
    console.warn('deleteCode failed', e);
    return false;
  }
}

async function getLinkInfo(env, code) {
  const t = await getCodeType(env, code);
  if (!t) return null;
  if (t === 'media') {
    const m = await getMedia(env, code);
    return { type: 'media', disabled: !!m?.disabled, created_at: m?.created_at || 0, caption: m?.caption || '', media_type: m?.type || '' };
  }
  const b = await getBundle(env, code);
  return { type: 'bundle', disabled: !!b?.disabled, created_at: b?.created_at || 0, count: Array.isArray(b?.items) ? b.items.length : 0 };
}

async function countByPrefix(env, prefix) {
  if (!env.DATA) return 0;
  let cursor = undefined;
  let total = 0;
  do {
    const { keys, list_complete, cursor: next } = await env.DATA.list({ prefix, cursor });
    total += (keys || []).length;
    cursor = list_complete ? undefined : next;
  } while (cursor);
  return total;
}

async function listUserIds(env) {
  if (!env.DATA) return [];
  let cursor = undefined;
  const ids = [];
  do {
    const { keys, list_complete, cursor: next } = await env.DATA.list({ prefix: 'user:', cursor });
    for (const k of keys || []) {
      const id = k.name.split(':')[1];
      if (id) ids.push(id);
    }
    cursor = list_complete ? undefined : next;
  } while (cursor);
  return ids;
}

// Simple user state in KV for admin flows
async function setState(env, userId, state) {
  if (!env.DATA) return;
  try { await env.DATA.put(`state:${userId}`, state); } catch (e) { console.warn('setState failed', e); }
}

async function getState(env, userId) {
  if (!env.DATA) return '';
  try { return (await env.DATA.get(`state:${userId}`)) || ''; } catch (e) { console.warn('getState failed', e); return ''; }
}

async function clearState(env, userId) {
  if (!env.DATA) return;
  try { await env.DATA.delete(`state:${userId}`); } catch (e) { console.warn('clearState failed', e); }
}

async function sendAdminMenu(env, chatId, userId) {
  const owner = isOwner(userId, env);
  const rows = [];
  // Upload row (all admins)
  rows.push([{ text: '📦 آپلود بسته‌ای', callback_data: 'admin:upload' }]);
  // Stats (+ Broadcast if owner)
  if (owner) {
    rows.push([
      { text: '📊 آمار', callback_data: 'admin:stats' },
      { text: '📣 پیام همگانی', callback_data: 'admin:broadcast' },
    ]);
  } else {
    rows.push([{ text: '📊 آمار', callback_data: 'admin:stats' }]);
  }
  // Join settings (owner only)
  if (owner) {
    rows.push([
      { text: '🔗 تنظیم جویـن', callback_data: 'admin:setjoin' },
      { text: '❌ حذف کانال جویـن', callback_data: 'admin:removejoin' },
    ]);
  }
  // File management (all admins)
  rows.push([{ text: '📁 مدیریت فایل‌ها', callback_data: 'admin:files' }]);
  // Admin management (owner only)
  if (owner) {
    rows.push([{ text: '🛡️ مدیریت ادمین‌ها', callback_data: 'admin:admins' }]);
  }
  // Refresh
  rows.push([{ text: '🔄 بروزرسانی منو', callback_data: 'admin:menu' }]);
  const keyboard = { inline_keyboard: rows };
  await tgCall(env, 'sendMessage', { chat_id: chatId, text: '🛠️ منوی مدیریت', reply_markup: keyboard });
}

async function buildDeepLink(env, code) {
  const username = (await ensureBotUsername(env) || '').replace(/^@/, '');
  if (!username) return `tg://resolve?domain=&start=${code}`;
  return `https://t.me/${username}?start=${code}`;
}

function siteBase(request) {
  const url = new URL(request.url);
  return `${url.protocol}//${url.host}`;
}

function num(n) {
  const x = Number(n);
  return Number.isFinite(x) ? x : 0;
}

async function handleBroadcast(env, text, ctx, chatId) {
  const start = Date.now();
  const users = await listUserIds(env);
  let sent = 0, failed = 0;
  for (const uid of users) {
    try {
      const res = await tgCall(env, 'sendMessage', {
        chat_id: uid,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      });
      if (res.ok) sent++; else failed++;
    } catch (e) {
      failed++;
    }
  }
  await tgCall(env, 'sendMessage', {
    chat_id: chatId,
    text: `Broadcast done. Sent: ${sent}, Failed: ${failed}, Time: ${Math.round((Date.now() - start)/1000)}s`,
  });
}

async function enforceJoin(env, userId) {
  const rules = getForceJoinRules(env);
  if (!rules.length) return { required: false, ok: true };
  for (const rule of rules) {
    if (rule.type === 'username') {
      const channel = rule.username.replace(/^@/, '');
      const r = await tgGet(env, 'getChatMember', { chat_id: `@${channel}`, user_id: userId });
      if (!r.ok) return { required: true, ok: false };
      const status = r.result?.status;
      const ok = status && status !== 'left' && status !== 'kicked';
      if (!ok) return { required: true, ok: false };
    } else if (rule.type === 'private') {
      const chat_id = rule.chat_id;
      if (!chat_id) return { required: true, ok: false };
      const r = await tgGet(env, 'getChatMember', { chat_id, user_id: userId });
      if (!r.ok) return { required: true, ok: false };
      const status = r.result?.status;
      const ok = status && status !== 'left' && status !== 'kicked';
      if (!ok) return { required: true, ok: false };
    }
  }
  return { required: true, ok: true };
}

async function sendMediaByType(env, chatId, media, extra = {}) {
  const caption = media.caption || undefined;
  const common = { chat_id: chatId, caption, parse_mode: 'HTML', ...extra };
  switch (media.type) {
    case 'photo':
      return tgCall(env, 'sendPhoto', { ...common, photo: media.file_id });
    case 'video':
      return tgCall(env, 'sendVideo', { ...common, video: media.file_id });
    case 'document':
      return tgCall(env, 'sendDocument', { ...common, document: media.file_id });
    case 'animation':
      return tgCall(env, 'sendAnimation', { ...common, animation: media.file_id });
    case 'audio':
      return tgCall(env, 'sendAudio', { ...common, audio: media.file_id });
    case 'voice':
      return tgCall(env, 'sendVoice', { ...common, voice: media.file_id });
    default:
      return tgCall(env, 'sendMessage', { chat_id: chatId, text: 'نوع رسانه پشتیبانی نمی‌شود.' });
  }
}

async function handleStart(env, request, update) {
  const msg = update.message;
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  await ensureUser(env, userId);

  const text = msg.text || '';
  const parts = text.trim().split(/\s+/);
  const hasCode = parts.length > 1;
  const code = hasCode ? parts.slice(1).join(' ') : '';

  if (!hasCode) {
    // Non-admin: simple welcome
    if (!isAdmin(userId, env)) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'خوش آمدید ✅' });
      return;
    }
    // Admin: show menu
    await sendAdminMenu(env, chatId, userId);
    return;
  }

  const rule = await enforceJoin(env, userId);
  if (rule.required && !rule.ok) {
    const rules = getForceJoinRules(env);
    const rows = [];
    for (const r of rules) {
      const url = r.type === 'username' ? `https://t.me/${r.username}` : (r.invite || 'https://t.me');
      rows.push([{ text: 'عضویت در کانال', url }]);
    }
    const keyboard = { inline_keyboard: [...rows, [{ text: 'بررسی عضویت ✅', callback_data: `check:${code}` }]] };
    await tgCall(env, 'sendMessage', {
      chat_id: chatId,
      text: 'لطفاً ابتدا در کانال عضو شوید و سپس روی دکمه «بررسی عضویت» بزنید.',
      reply_markup: keyboard,
    });
    return;
  }

  const media = await getMedia(env, code);
  if (media) {
    if (media.disabled) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این لینک غیرفعال شده است.' });
      return;
    }
    await sendMediaByType(env, chatId, media);
    return;
  }
  const bundle = await getBundle(env, code);
  if (!bundle || !Array.isArray(bundle.items) || bundle.items.length === 0) {
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد اشتراک نامعتبر است.' });
    return;
  }
  if (bundle.disabled) {
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این لینک غیرفعال شده است.' });
    return;
  }
  // Send bundle sequentially
  for (const item of bundle.items) {
    await sendMediaByType(env, chatId, item);
  }
}

async function handleCallback(env, update) {
  const q = update.callback_query;
  const data = q.data || '';
  const chatId = q.message?.chat?.id || q.from.id;
  const userId = q.from.id;
  const answer = (text) => tgCall(env, 'answerCallbackQuery', { callback_query_id: q.id, text, show_alert: false });

  if (data.startsWith('check:')) {
    const code = data.slice(6);
    const rule = await enforceJoin(env, userId);
    if (!rule.ok) {
      await answer('هنوز عضو کانال نشده‌اید.');
      const rules = getForceJoinRules(env);
      const rows = [];
      for (const r of rules) {
        const url = r.type === 'username' ? `https://t.me/${r.username}` : (r.invite || 'https://t.me');
        rows.push([{ text: 'عضویت در کانال', url }]);
      }
      const keyboard = { inline_keyboard: [...rows, [{ text: 'بررسی عضویت ✅', callback_data: `check:${code}` }]] };
      await tgCall(env, 'sendMessage', {
        chat_id: chatId,
        text: 'لطفاً ابتدا در کانال عضو شوید و سپس روی دکمه «بررسی عضویت» بزنید.',
        reply_markup: keyboard,
      });
      return;
    }
    await answer('عضویت تایید شد.');
    const media = await getMedia(env, code);
    if (media) {
      if (media.disabled) {
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این لینک غیرفعال شده است.' });
        return;
      }
      await sendMediaByType(env, chatId, media);
      return;
    }
    const bundle = await getBundle(env, code);
    if (!bundle || !Array.isArray(bundle.items) || bundle.items.length === 0) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد اشتراک نامعتبر است.' });
      return;
    }
    if (bundle.disabled) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این لینک غیرفعال شده است.' });
      return;
    }
    for (const item of bundle.items) {
      await sendMediaByType(env, chatId, item);
    }
    return;
  }

  if (data === 'help') {
    await answer('راهنما');
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'برای دریافت رسانه باید از لینک اشتراک استفاده کنید.' });
    return;
  }

  // Admin inline actions
  if (isAdmin(userId, env)) {
    if (data === 'admin:menu') {
      await answer('منو');
      await sendAdminMenu(env, chatId, userId);
      return;
    }
    if (data === 'admin:admins') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این بخش فقط برای صاحب ربات است.' });
        return;
      }
      await answer('ادمین‌ها');
      const kb = { inline_keyboard: [
        [{ text: 'افزودن ادمین ➕', callback_data: 'admin:addadmin' }],
        [{ text: 'لیست ادمین‌ها 📋', callback_data: 'admin:listadmins' }],
        [{ text: 'بازگشت', callback_data: 'admin:menu' }],
      ] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'مدیریت ادمین‌ها', reply_markup: kb });
      return;
    }
    if (data === 'admin:files') {
      await answer('فایل‌ها');
      const kb = { inline_keyboard: [
        [{ text: '🚫 غیرفعال‌کردن لینک', callback_data: 'admin:disable' }],
        [{ text: '✅ فعال‌کردن لینک', callback_data: 'admin:enable' }],
        [{ text: '🗑️ حذف لینک', callback_data: 'admin:delete' }],
        [{ text: 'ℹ️ اطلاعات لینک', callback_data: 'admin:info' }],
        [{ text: 'بازگشت', callback_data: 'admin:menu' }],
      ] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'مدیریت لینک‌ها', reply_markup: kb });
      return;
    }
    if (data === 'admin:disable') {
      await setState(env, userId, 'await_disable_code');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد لینک را ارسال کنید تا غیرفعال شود.', reply_markup: kb });
      await answer('غیرفعال');
      return;
    }
    if (data === 'admin:enable') {
      await setState(env, userId, 'await_enable_code');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد لینک را ارسال کنید تا فعال شود.', reply_markup: kb });
      await answer('فعال');
      return;
    }
    if (data === 'admin:delete') {
      await setState(env, userId, 'await_delete_code');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد لینک را ارسال کنید تا حذف شود.', reply_markup: kb });
      await answer('حذف');
      return;
    }
    if (data === 'admin:info') {
      await setState(env, userId, 'await_info_code');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کد لینک را ارسال کنید تا اطلاعات نمایش داده شود.', reply_markup: kb });
      await answer('اطلاعات');
      return;
    }
    if (data === 'admin:addadmin') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید.');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'فقط صاحبان (ADMIN در ENV) می‌توانند ادمین جدید اضافه کنند.' });
        return;
      }
      await setState(env, userId, 'await_add_admin');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'آی‌دی عددی کاربر را ارسال کنید.', reply_markup: kb });
      await answer('بفرستید');
      return;
    }
    if (data === 'admin:listadmins') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این بخش فقط برای صاحب ربات است.' });
        return;
      }
      const arr = await listAdmins(env);
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: arr.length ? `ادمین‌ها:\n${arr.join('\n')}` : 'ادمینی ثبت نشده.' });
      await answer('نمایش لیست');
      return;
    }
    if (data === 'admin:upload') {
      await answer('آپلود');
      const code = randCode();
      await initBundle(env, code);
      await setState(env, userId, `upload:${code}`);
      const deep = await buildDeepLink(env, code);
      const kb = { inline_keyboard: [[{ text: 'پایان آپلود ✅', callback_data: 'admin:finish' }], [{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: `حالت آپلود فعال شد. رسانه‌ها را ارسال کنید.\nکد اشتراک: ${code}\nلینک: ${deep}`, reply_markup: kb });
      return;
    }
    if (data === 'admin:finish') {
      const st = await getState(env, userId);
      if (!st || !st.startsWith('upload:')) {
        await answer('حالت آپلود فعال نیست.');
        return;
      }
      const code = st.split(':')[1];
      const bundle = await getBundle(env, code);
      await clearState(env, userId);
      const deep = await buildDeepLink(env, code);
      const count = bundle?.items?.length || 0;
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: `آپلود پایان یافت. موارد: ${count}\nلینک اشتراک: ${deep}\nمسیر وب: /s/${code}`, disable_web_page_preview: true });
      return;
    }
    if (data === 'admin:stats') {
      await answer('آمار');
      const users = await countByPrefix(env, 'user:');
      const media = await countByPrefix(env, 'media:');
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: `آمار ربات:\nکاربران: ${users}\nرسانه‌ها: ${media}` });
      return;
    }
    if (data === 'admin:broadcast') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این بخش فقط برای صاحب ربات است.' });
        return;
      }
      await answer('پیام همگانی');
      await setState(env, userId, 'await_broadcast_text');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'متن پیام همگانی را ارسال کنید.', reply_markup: kb });
      return;
    }
    if (data === 'admin:setjoin') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این بخش فقط برای صاحب ربات است.' });
        return;
      }
      await answer('تنظیم کانال');
      await setState(env, userId, 'await_join_channel');
      const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'نام کاربری کانال (بدون @) یا لینک دعوت خصوصی t.me/+... را ارسال کنید.\nبرای حذف: off\nهمچنین می‌توانید یک پیام از کانال را فوروارد کنید تا شناسه کانال ثبت شود.', reply_markup: kb });
      return;
    }
    if (data === 'admin:removejoin') {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این بخش فقط برای صاحب ربات است.' });
        return;
      }
      const rules = getForceJoinRules(env);
      if (!rules.length) {
        await answer('لیست خالی است');
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'هیچ کانالی برای حذف وجود ندارد.' });
        return;
      }
      const selected = await getRemoveSelection(env, userId);
      const kb = buildRemoveJoinKeyboard(rules, selected);
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'کانال‌های جویـن اجباری را برای حذف انتخاب کنید:', reply_markup: kb });
      await answer('لیست کانال‌ها');
      return;
    }
    if (data.startsWith('rmjoin:')) {
      if (!isOwner(userId, env)) {
        await answer('اجازه ندارید');
        return;
      }
      const rules = getForceJoinRules(env);
      const selected = await getRemoveSelection(env, userId);
      const msgId = q.message?.message_id;
      const sub = data.split(':')[1];
      if (sub === 'toggle') {
        const key = data.split(':').slice(2).join(':');
        const exists = rules.some((r) => ruleKey(r) === key);
        if (exists) {
          if (selected.has(key)) selected.delete(key); else selected.add(key);
          await setRemoveSelection(env, userId, selected);
        }
      } else if (sub === 'all') {
        for (const r of rules) selected.add(ruleKey(r));
        await setRemoveSelection(env, userId, selected);
      } else if (sub === 'none') {
        selected.clear();
        await setRemoveSelection(env, userId, selected);
      } else if (sub === 'confirm') {
        // Delete selected
        const before = rules.length;
        const remaining = rules.filter((r) => !selected.has(ruleKey(r)));
        const removed = before - remaining.length;
        await setForceJoinRules(env, remaining);
        await clearRemoveSelection(env, userId);
        await tgCall(env, 'sendMessage', { chat_id: chatId, text: removed ? `${removed} مورد حذف شد.` : 'هیچ موردی انتخاب نشده بود.' });
        await answer('انجام شد');
        return;
      }
      // Update the inline keyboard to reflect selection state
      const kb = buildRemoveJoinKeyboard(getForceJoinRules(env), await getRemoveSelection(env, userId));
      if (msgId) {
        await tgCall(env, 'editMessageReplyMarkup', { chat_id: chatId, message_id: msgId, reply_markup: kb });
      }
      await answer('به‌روز شد');
      return;
    }
    if (data === 'admin:cancel') {
      await answer('انصراف');
      await clearRemoveSelection(env, userId);
      await clearState(env, userId);
      await sendAdminMenu(env, chatId, userId);
      return;
    }
  }

  await answer('دستور نامعتبر');
}

function extractMediaFromMessage(msg) {
  // Returns { type, file_id, caption } or null
  if (msg.photo && Array.isArray(msg.photo) && msg.photo.length) {
    const p = msg.photo[msg.photo.length - 1];
    return { type: 'photo', file_id: p.file_id, caption: msg.caption || '' };
  }
  if (msg.video) return { type: 'video', file_id: msg.video.file_id, caption: msg.caption || '' };
  if (msg.document) return { type: 'document', file_id: msg.document.file_id, caption: msg.caption || '' };
  if (msg.animation) return { type: 'animation', file_id: msg.animation.file_id, caption: msg.caption || '' };
  if (msg.audio) return { type: 'audio', file_id: msg.audio.file_id, caption: msg.caption || '' };
  if (msg.voice) return { type: 'voice', file_id: msg.voice.file_id, caption: msg.caption || '' };
  return null;
}

async function handleAdminCommands(env, update, request) {
  const msg = update.message;
  const chatId = msg.chat.id;
  const text = msg.text || '';

  if (text.startsWith('/broadcast')) {
    if (!isOwner(msg.from.id, env)) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این دستور فقط برای صاحب ربات مجاز است.' });
      return;
    }
    const payload = text.replace('/broadcast', '').trim();
    await setState(env, msg.from.id, 'await_broadcast_text');
    const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
    const prompt = payload ? `متن وارد شده:\n${payload}\n\nبرای تایید، همان متن را دوباره ارسال کنید.` : 'متن پیام همگانی را ارسال کنید.';
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: prompt, reply_markup: kb });
    return;
  }

  if (text.startsWith('/setjoin')) {
    if (!isOwner(msg.from.id, env)) {
      await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'این دستور فقط برای صاحب ربات مجاز است.' });
      return;
    }
    const arg = text.replace('/setjoin', '').trim();
    await setState(env, msg.from.id, 'await_join_channel');
    const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
    const curr = getForceJoinChannel(env) || 'غیرفعال';
    const hint = arg ? `مقدار فعلی: ${curr}\nپیشنهاد شده: ${arg}\nبرای تایید، همان مقدار را دوباره ارسال کنید.` : `مقدار فعلی: ${curr}\nنام کاربری کانال (بدون @) یا لینک دعوت خصوصی t.me/+... را ارسال کنید.\nبرای حذف: off\nهمچنین می‌توانید یک پیام از کانال را فوروارد کنید تا شناسه کانال ثبت شود.`;
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: hint, reply_markup: kb });
    return;
  }

  if (text.startsWith('/stats')) {
    const users = await countByPrefix(env, 'user:');
    const media = await countByPrefix(env, 'media:');
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: `آمار ربات:\nکاربران: ${users}\nرسانه‌ها: ${media}` });
    return;
  }
}

async function handleAdminMedia(env, update, request) {
  const msg = update.message;
  const chatId = msg.chat.id;
  const m = extractMediaFromMessage(msg);
  if (!m) return false;

  if (!env.DATA) {
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'KV متصل نیست. امکان ذخیره رسانه وجود ندارد.' });
    return true;
  }

  // If admin in upload state, append to bundle instead of single link
  const userId = msg.from?.id;
  const st = userId ? await getState(env, userId) : '';
  if (st && st.startsWith('upload:')) {
    const code = st.split(':')[1];
    const count = await addBundleItem(env, code, m);
    const kb = { inline_keyboard: [[{ text: 'پایان آپلود ✅', callback_data: 'admin:finish' }], [{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: `ثبت شد. تعداد موارد: ${count}`, reply_markup: kb });
    return true;
  }

  // Save media + provide deep link
  let code = randCode();
  // best-effort uniqueness check
  for (let i = 0; i < 3; i++) {
    const exists = await getMedia(env, code);
    if (!exists) break;
    code = randCode();
  }
  const media = { code, ...m, created_at: Date.now() };
  try { await saveMedia(env, media); } catch (e) {
    console.warn('saveMedia error', e);
    await tgCall(env, 'sendMessage', { chat_id: chatId, text: 'خطا در ذخیره رسانه.' });
    return true;
  }

  const deep = await buildDeepLink(env, code);
  const webShare = `${siteBase(request)}/s/${code}`;
  await tgCall(env, 'sendMessage', {
    chat_id: chatId,
    text: `رسانه ذخیره شد.\nلینک اشتراک: ${deep}\nلینک وب: ${webShare}`,
    disable_web_page_preview: true,
  });
  return true;
}

async function handleWebhook(request, env, ctx) {
  await loadConfig(env);
  let update;
  try {
    update = await request.json();
  } catch (e) {
    return jsonResponse({ ok: false, error: 'invalid JSON' }, { status: 400 });
  }

  if (!env.BOT_TOKEN) {
    return jsonResponse({ ok: false, error: 'BOT_TOKEN missing' }, { status: 500 });
  }

  // Messages
  if (update.message) {
    const msg = update.message;
    const userId = msg.from?.id;
    if (msg.text && msg.text.startsWith('/start')) {
      await handleStart(env, request, update);
      return jsonResponse({ ok: true });
    }
    
    if (isAdmin(userId, env)) {
      // Admin state machine
      const st = await getState(env, userId);
      
      if (st === 'await_add_admin' && msg.text) {
        // Owner-only safeguard
        if (!isOwner(userId, env)) {
          await clearState(env, userId);
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'این بخش فقط برای صاحب ربات است.' });
          return jsonResponse({ ok: true });
        }
        const target = (msg.text || '').trim();
        await clearState(env, userId);
        try {
          await addAdmin(env, target);
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: `ادمین جدید اضافه شد: ${target}` });
        } catch (e) {
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'ورودی نامعتبر است. فقط آی‌دی عددی را بفرستید.' });
        }
        return jsonResponse({ ok: true });
      }
      if (st === 'await_broadcast_text' && msg.text) {
        // Owner-only safeguard
        if (!isOwner(userId, env)) {
          await clearState(env, userId);
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'این بخش فقط برای صاحب ربات است.' });
          return jsonResponse({ ok: true });
        }
        await clearState(env, userId);
        if (ctx && typeof ctx.waitUntil === 'function') {
          ctx.waitUntil(handleBroadcast(env, msg.text, ctx, msg.chat.id));
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'در حال ارسال پیام همگانی…' });
        } else {
          await handleBroadcast(env, msg.text, ctx, msg.chat.id);
        }
        return jsonResponse({ ok: true });
      }
      if (st === 'await_join_channel') {
        // Owner-only safeguard
        if (!isOwner(userId, env)) {
          await clearState(env, userId);
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'این بخش فقط برای صاحب ربات است.' });
          return jsonResponse({ ok: true });
        }
        // If forwarded from channel, capture chat_id and ask for invite link (if not already provided)
        if (msg.forward_from_chat && msg.forward_from_chat.type === 'channel') {
          const chId = msg.forward_from_chat.id;
          await setState(env, userId, `await_join_private_wait_link:${chId}`);
          const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'لینک دعوت خصوصی t.me/+... را ارسال کنید تا کامل شود.', reply_markup: kb });
          return jsonResponse({ ok: true });
        }
        if (msg.text) {
          const raw = msg.text.trim();
          if (raw.toLowerCase() === 'off') {
            await clearState(env, userId);
            await setForceJoinRules(env, []);
            await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'تمام کانال‌های جویـن حذف شدند.' });
            await sendAdminMenu(env, msg.chat.id, userId);
            return jsonResponse({ ok: true });
          }
          // Private invite link
          if (/^https?:\/\/t\.me\/\+[A-Za-z0-9_\-]+$/.test(raw)) {
            await setState(env, userId, `await_join_private:${raw}`);
            const kb = { inline_keyboard: [[{ text: 'انصراف', callback_data: 'admin:cancel' }]] };
            await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'حالا یک پیام از همان کانال خصوصی فوروارد کنید تا شناسه کانال ثبت شود.', reply_markup: kb });
            return jsonResponse({ ok: true });
          }
          // Username
          const username = raw.replace(/^@/, '');
          await clearState(env, userId);
          await appendForceJoinRule(env, { type: 'username', username });
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: `کانال اجباری اضافه شد: @${username}` });
          await sendAdminMenu(env, msg.chat.id, userId);
          return jsonResponse({ ok: true });
        }
      }
      // Complete private join when invite link was provided first
      if (st && st.startsWith('await_join_private:') && msg.forward_from_chat && msg.forward_from_chat.type === 'channel') {
        const invite = st.split(':').slice(1).join(':');
        const chId = msg.forward_from_chat.id;
        await clearState(env, userId);
        await appendForceJoinRule(env, { type: 'private', chat_id: chId, invite });
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'کانال خصوصی به جویـن اجباری اضافه شد.' });
        await sendAdminMenu(env, msg.chat.id, userId);
        return jsonResponse({ ok: true });
      }
      // Complete private join when forward came first
      if (st && st.startsWith('await_join_private_wait_link:') && msg.text) {
        const chId = st.split(':').slice(1).join(':');
        const raw = msg.text.trim();
        if (!/^https?:\/\/t\.me\/\+[A-Za-z0-9_\-]+$/.test(raw)) {
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'لینک دعوت معتبر نیست. نمونه: https://t.me/+XXXXXXXX' });
          return jsonResponse({ ok: true });
        }
        await clearState(env, userId);
        await appendForceJoinRule(env, { type: 'private', chat_id: chId, invite: raw });
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'کانال خصوصی به جویـن اجباری اضافه شد.' });
        await sendAdminMenu(env, msg.chat.id, userId);
        return jsonResponse({ ok: true });
      }
      if (st === 'await_disable_code' && msg.text) {
        const code = (msg.text || '').trim();
        await clearState(env, userId);
        const ok = await setDisabled(env, code, true);
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: ok ? `لینک ${code} غیرفعال شد.` : 'کد یافت نشد.' });
        return jsonResponse({ ok: true });
      }
      if (st === 'await_enable_code' && msg.text) {
        const code = (msg.text || '').trim();
        await clearState(env, userId);
        const ok = await setDisabled(env, code, false);
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: ok ? `لینک ${code} فعال شد.` : 'کد یافت نشد.' });
        return jsonResponse({ ok: true });
      }
      if (st === 'await_delete_code' && msg.text) {
        const code = (msg.text || '').trim();
        await clearState(env, userId);
        const ok = await deleteCode(env, code);
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: ok ? `لینک ${code} حذف شد.` : 'کد یافت نشد.' });
        return jsonResponse({ ok: true });
      }
      if (st === 'await_info_code' && msg.text) {
        const code = (msg.text || '').trim();
        await clearState(env, userId);
        const info = await getLinkInfo(env, code);
        if (!info) {
          await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'کد یافت نشد.' });
          return jsonResponse({ ok: true });
        }
        const base = siteBase(request);
        const deep = await buildDeepLink(env, code);
        const lines = [
          `کد: ${code}`,
          `نوع: ${info.type}`,
          `وضعیت: ${info.disabled ? 'غیرفعال' : 'فعال'}`,
          info.type === 'media' ? `رسانه: ${info.media_type}` : `تعداد اقلام: ${info.count}`,
          `تاریخ: ${info.created_at ? new Date(info.created_at).toLocaleString('fa-IR') : '-'}`,
          `وب: ${base}/s/${code}`,
          `تلگرام: ${deep}`,
        ].join('\n');
        await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: lines, disable_web_page_preview: true });
        return jsonResponse({ ok: true });
      }

      // Media upload path (no state)
      const used = await handleAdminMedia(env, update, request);
      if (used) return jsonResponse({ ok: true });

      // Show admin menu for other texts
      if (msg.text) {
        await sendAdminMenu(env, msg.chat.id, userId);
        return jsonResponse({ ok: true });
      }
    }

    // Non-admin: inline help
    if (msg.text) {
      const keyboard = { inline_keyboard: [[{ text: 'راهنما', callback_data: 'help' }]] };
      await tgCall(env, 'sendMessage', { chat_id: msg.chat.id, text: 'برای دریافت محتوا، لینک اشتراک را استفاده کنید.', reply_markup: keyboard });
    }
    return jsonResponse({ ok: true });
  }

  // Callback queries
  if (update.callback_query) {
    await handleCallback(env, update);
    return jsonResponse({ ok: true });
  }

  return jsonResponse({ ok: true });
}

function panelHtml({ kvConnected, users, media, forceJoin, base, rules = [], flash = '', key = '', authRequired = false }) {
  return `<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>پنل مدیریت ربات</title>
<style>
  :root{
    --bg:#080b14;
    --glass:rgba(255,255,255,.06);
    --stroke:rgba(255,255,255,.12);
    --text:#eaf0ff;
    --muted:#9fb0ff;
    --ok:#2ee59d;
    --bad:#ff6b6b;
    --accent:#2b59ff;
  }
  *{box-sizing:border-box}
  body{font-family:system-ui,Segoe UI,Tahoma,Arial,sans-serif;background:radial-gradient(1200px 800px at 10% -10%,#14204d55,transparent),radial-gradient(1200px 800px at 110% 10%,#2b59ff22,transparent),var(--bg);color:var(--text);margin:0;min-height:100vh;display:flex;flex-direction:column}
  header{padding:24px 28px;position:sticky;top:0;background:linear-gradient(180deg,rgba(8,11,20,.7),rgba(8,11,20,.3) 60%,transparent);backdrop-filter:saturate(140%) blur(12px);border-bottom:1px solid var(--stroke)}
  header h1{margin:0;font-size:20px;font-weight:700;letter-spacing:.3px}
  main{padding:28px;max-width:1100px;margin:0 auto;flex:1;width:100%}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:18px;margin-bottom:22px}
  .glass{background:var(--glass);border:1px solid var(--stroke);border-radius:16px;backdrop-filter:blur(14px) saturate(140%);-webkit-backdrop-filter:blur(14px) saturate(140%);box-shadow:0 8px 24px rgba(0,0,0,.35)}
  .tile{display:flex;flex-direction:column;gap:8px;padding:16px}
  .tile h3{margin:0 0 4px 0;font-size:14px;font-weight:600;color:var(--muted)}
  .tile p{margin:0;font-size:22px;font-weight:700}
  .muted{opacity:.85}
  .ok{color:var(--ok)}
  .bad{color:var(--bad)}
  code{background:rgba(255,255,255,.08);padding:2px 6px;border-radius:8px;border:1px solid var(--stroke)}
  footer{padding:18px 28px;border-top:1px solid var(--stroke);opacity:.75}
</style>
</head>
<body>
  <header class="glass">
    <h1>پنل مدیریت ربات</h1>
  </header>
  <main>
    <div class="grid">
      <div class="tile glass">
        <h3>اتصال KV</h3>
        <p class="${kvConnected ? 'ok' : 'bad'}">${kvConnected ? 'متصل' : 'نامتصل'}</p>
      </div>
      <div class="tile glass">
        <h3>کاربران</h3>
        <p>${users}</p>
      </div>
      <div class="tile glass">
        <h3>رسانه‌ها</h3>
        <p>${media}</p>
      </div>
      <div class="tile glass">
        <h3>جویـن اجباری</h3>
        <p>${forceJoin || 'غیرفعال'}</p>
      </div>
    </div>
    <div class="tile glass">
      <h3>وبهوک</h3>
      <p class="muted">آدرس وبهوک: <code>${base}/webhook</code></p>
    </div>
  </main>
  <footer class="glass">
    <small>Cloudflare Pages Functions + KV</small>
  </footer>
</body>
</html>`;
}

function panelAuthorized(request, env, formKey) {
  const need = (env.PANEL_KEY || '').trim();
  if (!need) return true; // no key required
  const url = new URL(request.url);
  const qKey = url.searchParams.get('key') || '';
  const k = (formKey || qKey || '').trim();
  return k && k === need;
}

async function handlePanel(request, env) {
  await loadConfig(env);
  const kvConnected = !!env.DATA;
  let users = 0, media = 0;
  if (kvConnected) {
    try {
      users = await countByPrefix(env, 'user:');
      media = await countByPrefix(env, 'media:');
    } catch (e) {
      console.warn('stats error', e);
    }
  }

  let flash = '';
  const url = new URL(request.url);
  const queryKey = url.searchParams.get('key') || '';

  if (request.method === 'POST') {
    const fd = await request.formData();
    const formKey = fd.get('key') || '';
    const action = (fd.get('action') || '').toString();
    if (!panelAuthorized(request, env, formKey)) {
      return htmlResponse('<h3 style="font-family:system-ui">403 - دسترسی غیرمجاز</h3>', { status: 403 });
    }
    if (action === 'add_username') {
      const raw = (fd.get('username') || '').toString().trim();
      if (raw) {
        const username = raw.replace(/^@/, '');
        await appendForceJoinRule(env, { type: 'username', username });
        flash = `کانال @${username} افزوده شد.`;
      }
    } else if (action === 'add_private') {
      const invite = (fd.get('invite') || '').toString().trim();
      const chat_id = (fd.get('chat_id') || '').toString().trim();
      if (invite && chat_id) {
        await appendForceJoinRule(env, { type: 'private', chat_id, invite });
        flash = 'کانال خصوصی افزوده شد.';
      }
    } else if (action === 'delete_selected') {
      const keys = fd.getAll('keys');
      const rules = getForceJoinRules(env);
      const keySet = new Set(keys.map(String));
      const remaining = rules.filter((r) => !keySet.has(ruleKey(r)));
      const removed = rules.length - remaining.length;
      await setForceJoinRules(env, remaining);
      flash = removed ? `${removed} مورد حذف شد.` : 'موردی انتخاب نشده بود.';
    }
  }

  const html = panelHtml({
    kvConnected,
    users,
    media,
    forceJoin: getForceJoinChannel(env),
    base: siteBase(request),
    rules: getForceJoinRules(env),
    flash,
    key: queryKey,
    authRequired: !!(env.PANEL_KEY || '').trim(),
  });
  return htmlResponse(html);
}

async function handleShare(request, env) {
  const url = new URL(request.url);
  const code = url.pathname.split('/').pop();
  const media = await getMedia(env, code);
  const bundle = media ? null : await getBundle(env, code);
  const base = siteBase(request);
  const deep = await buildDeepLink(env, code);
  const exists = !!media || !!bundle;
  const disabled = media ? !!media.disabled : (bundle ? !!bundle.disabled : false);
  const body = `<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>اشتراک ${code}</title>
<style>body{font-family:system-ui,Segoe UI,Tahoma,Arial,sans-serif;background:#0b1020;color:#e8eefc;display:grid;place-items:center;min-height:100vh;margin:0}
.card{background:#121938;border:1px solid #1e2a5a;border-radius:12px;padding:24px;max-width:560px}
.btn{display:inline-block;background:#2b59ff;color:#fff;text-decoration:none;padding:10px 14px;border-radius:8px}
.muted{opacity:.8}
</style>
</head>
<body>
  <div class="card">
    <h2>لینک اشتراک</h2>
    ${!exists ? `<p>کد یافت نشد یا منقضی شده است.</p>` : disabled ? `<p>این لینک غیرفعال شده است.</p>` : `<p>برای دریافت رسانه روی دکمه زیر بزنید.</p>`}
    ${exists && !disabled ? `<p><a class="btn" href="${deep}">باز کردن در تلگرام</a></p>` : ''}
    <p class="muted"><small>${base}</small></p>
  </div>
</body>
</html>`;
  return htmlResponse(body, { status: exists ? 200 : 404 });
}

const APP = {
  async fetch(request, env, ctx = {}) {
    const url = new URL(request.url);

    // Route mapping
    if (url.pathname === '/webhook' && request.method === 'POST') {
      return handleWebhook(request, env, ctx);
    }

    if (url.pathname === '/' || url.pathname === '/panel') {
      return handlePanel(request, env);
    }

    if (url.pathname.startsWith('/s/') && request.method === 'GET') {
      return handleShare(request, env);
    }

    return new Response('Not Found', { status: 404 });
  },
};

// Expose globally
globalThis.APP = APP;

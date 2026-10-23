// VoiceNexus MCP server.
// - MCP over the Streamable HTTP transport (spec 2025-11-25), one endpoint: /mcp (POST, GET, DELETE)
// - Agent Skills (skills/<name>/SKILL.md) are exposed as MCP resources (skill://<name>/SKILL.md)
// - Zero dependencies: Node 18+ only.
import http from 'node:http';
import { readFile, readdir } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8787);
const HOST = process.env.HOST ?? '127.0.0.1'; // spec: bind to localhost when running locally
const SUPPORTED_VERSIONS = ['2025-11-25', '2025-06-18'];
const LEGACY_DEFAULT_VERSION = '2025-03-26'; // spec: assume this when the header is missing
const STATE_URI = 'nexus://state';
const MAX_BODY = 1_000_000;

// Extra hosts/origins (comma separated) for non-local deployments, e.g. behind a tunnel.
const extraHosts = new Set((process.env.ALLOWED_HOSTS ?? '').split(',').map((s) => s.trim()).filter(Boolean));
const extraOrigins = new Set((process.env.ALLOWED_ORIGINS ?? '').split(',').map((s) => s.trim()).filter(Boolean));
const LOOPBACK_NAMES = new Set(['localhost', '127.0.0.1', '[::1]']);

// ---------------------------------------------------------------- Agent Skills
function parseFrontmatter(src) {
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(src);
  if (!m) return { meta: {}, body: src };
  const meta = {};
  let current = null;
  const unquote = (v) => v.trim().replace(/^(['"])(.*)\1$/, '$2');
  for (const line of m[1].split(/\r?\n/)) {
    if (!line.trim()) continue;
    const nested = /^\s+([\w.-]+):\s*(.*)$/.exec(line);
    if (nested && current) {
      meta[current][nested[1]] = unquote(nested[2]);
      continue;
    }
    const top = /^([\w.-]+):\s*(.*)$/.exec(line);
    if (top) {
      if (top[2] === '') {
        meta[top[1]] = {};
        current = top[1];
      } else {
        meta[top[1]] = unquote(top[2]);
        current = null;
      }
    }
  }
  return { meta, body: m[2] };
}

async function loadSkills() {
  const dir = path.join(__dirname, 'skills');
  const skills = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const raw = await readFile(path.join(dir, entry.name, 'SKILL.md'), 'utf8');
    const { meta } = parseFrontmatter(raw);
    // Agent Skills spec: name must match the directory name.
    if (meta.name !== entry.name) throw new Error(`skill ${entry.name}: name "${meta.name}" does not match directory`);
    if (!meta.description) throw new Error(`skill ${entry.name}: missing description`);
    const md = meta.metadata ?? {};
    skills.push({
      name: meta.name,
      description: meta.description,
      tools: (md.tools ?? '').split(',').map((s) => s.trim()).filter(Boolean),
      examples: (md.examples ?? '').split('|').map((s) => s.trim()).filter(Boolean),
      raw,
    });
  }
  return skills.sort((a, b) => a.name.localeCompare(b.name));
}

// ---------------------------------------------------------------- Simulated household state
const state = {
  devices: {
    living_room_light: { label: 'Living room light', type: 'light', on: true, brightness: 80 },
    bedroom_light: { label: 'Bedroom light', type: 'light', on: false, brightness: 0 },
    thermostat: { label: 'Thermostat', type: 'thermostat', on: true, target: 24, current: 27 },
    front_door: { label: 'Front door', type: 'lock', locked: true },
    tv: { label: 'TV', type: 'media', on: false },
  },
  lists: { shopping: ['milk'], todo: [] },
  timers: [],
  music: { index: null, playing: false, volume: 40 },
};
let timerSeq = 0;
const timerHandles = new Map();

const CATALOG = [
  { title: 'Monsoon Drive', artist: 'Neon Banyan', genre: 'synthwave', mood: 'energetic' },
  { title: 'Chai at Midnight', artist: 'The Paper Lanterns', genre: 'lofi', mood: 'chill' },
  { title: 'Blue Hour Waltz', artist: 'Ira Sethi Quartet', genre: 'jazz', mood: 'relaxing' },
  { title: 'Satellite Sunrise', artist: 'Kora Wave', genre: 'electronic', mood: 'focus' },
  { title: 'Dust and Diesel', artist: 'Highway Orchard', genre: 'rock', mood: 'energetic' },
  { title: 'Slow Rivers', artist: 'Maya and the Tide', genre: 'acoustic', mood: 'calm' },
  { title: 'Kite Season', artist: 'Saffron Static', genre: 'pop', mood: 'happy' },
  { title: 'Rooftop Raga', artist: 'Anand Rao Ensemble', genre: 'classical', mood: 'relaxing' },
  { title: 'Late Train Home', artist: 'Velvet Platform', genre: 'jazz', mood: 'chill' },
  { title: 'Deep Work', artist: 'Focus Garden', genre: 'ambient', mood: 'focus' },
];

function snapshot() {
  const now = Date.now();
  const m = state.music;
  return {
    devices: state.devices,
    lists: state.lists,
    timers: state.timers.map((t) => ({
      id: t.id,
      label: t.label,
      seconds: t.seconds,
      status: t.status,
      remainingSeconds: t.status === 'running' ? Math.max(0, Math.round((t.endsAt - now) / 1000)) : 0,
    })),
    music: { playing: m.playing, volume: m.volume, track: m.index === null ? null : CATALOG[m.index] },
    serverTime: now,
  };
}

// ---------------------------------------------------------------- Sessions & server push
const sessions = new Map(); // id -> { id, initialized, stream, subs:Set, eventSeq, lastSeen, clientInfo }

function sseWrite(res, { id, event, data, retry, comment } = {}) {
  let out = '';
  if (comment) out += `: ${comment}\n`;
  if (retry !== undefined) out += `retry: ${retry}\n`;
  if (event) out += `event: ${event}\n`;
  if (id !== undefined) out += `id: ${id}\n`;
  out += `data: ${data === undefined ? '' : data}\n\n`;
  res.write(out);
}
const nextEventId = (session) => `${session.id.slice(0, 8)}-${++session.eventSeq}`;

function pushToSession(session, message) {
  if (!session?.stream || session.stream.writableEnded) return false;
  sseWrite(session.stream, { id: nextEventId(session), data: JSON.stringify(message) });
  return true;
}

function notifyStateChanged() {
  for (const s of sessions.values()) {
    if (s.subs.has(STATE_URI)) {
      pushToSession(s, { jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: STATE_URI } });
    }
  }
}

// ---------------------------------------------------------------- Tools
const fmtDuration = (secs) => {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  const parts = [];
  if (h) parts.push(`${h} hour${h === 1 ? '' : 's'}`);
  if (m) parts.push(`${m} minute${m === 1 ? '' : 's'}`);
  if (s) parts.push(`${s} second${s === 1 ? '' : 's'}`);
  return parts.join(' ') || '0 seconds';
};

class ToolError extends Error {}

function fireTimer(timer) {
  timer.status = 'done';
  timerHandles.delete(timer.id);
  const owner = sessions.get(timer.ownerSession);
  const what = timer.label ? `Your ${timer.label} timer` : 'Your timer';
  pushToSession(owner, {
    jsonrpc: '2.0',
    method: 'notifications/message',
    params: {
      level: 'info',
      logger: 'timers',
      data: { event: 'timer_done', id: timer.id, label: timer.label, message: `${what} is done.` },
    },
  });
  notifyStateChanged();
}

const hash = (s) => {
  let h = 2166136261;
  for (const c of s.toLowerCase()) {
    h ^= c.charCodeAt(0);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
};
const CONDITIONS = ['sunny', 'partly cloudy', 'cloudy', 'light rain', 'thunderstorms', 'hazy', 'clear'];
function weatherFor(city, dayOffset) {
  const day = Math.floor(Date.now() / 86400000) + dayOffset;
  const h = hash(`${city}:${day}`);
  const base = 14 + (hash(city) % 22);
  return {
    tempC: base + ((h >>> 4) % 7) - 3,
    condition: CONDITIONS[h % CONDITIONS.length],
    humidity: 35 + ((h >>> 8) % 55),
  };
}

const deviceKeys = Object.keys(state.devices);
const titleCase = (s) => s.replace(/\b\w/g, (c) => c.toUpperCase());

function describeDevice(key) {
  const d = state.devices[key];
  switch (d.type) {
    case 'light': return `${d.label} is ${d.on ? `on at ${d.brightness}%` : 'off'}`;
    case 'thermostat': return `${d.label} is ${d.on ? `set to ${d.target}°C (room is ${d.current}°C)` : 'off'}`;
    case 'lock': return `${d.label} is ${d.locked ? 'locked' : 'unlocked'}`;
    default: return `${d.label} is ${d.on ? 'on' : 'off'}`;
  }
}

function pickTrack(query) {
  const stop = new Set(['some', 'music', 'songs', 'song', 'play', 'the', 'a', 'by', 'something', 'to', 'me', 'on', 'please', 'tracks']);
  const tokens = query.toLowerCase().split(/[^a-z0-9]+/).filter((t) => t && !stop.has(t));
  if (!tokens.length) return Math.floor(Math.random() * CATALOG.length);
  let best = -1;
  let bestScore = 0;
  CATALOG.forEach((t, i) => {
    const hay = `${t.title} ${t.artist} ${t.genre} ${t.mood}`.toLowerCase();
    const score = tokens.reduce((n, tok) => n + (hay.includes(tok) ? 1 : 0), 0);
    if (score > bestScore) { best = i; bestScore = score; }
  });
  return best;
}

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));
const nowPlayingText = () => {
  const m = state.music;
  if (m.index === null) return 'Nothing is playing right now.';
  const t = CATALOG[m.index];
  return `${m.playing ? 'Playing' : 'Paused:'} "${t.title}" by ${t.artist} at volume ${m.volume}.`;
};

const TOOLS = [
  {
    name: 'set_timer',
    title: 'Set a timer',
    description: 'Start a countdown timer. The server announces completion on the client event stream.',
    inputSchema: {
      type: 'object',
      properties: {
        seconds: { type: 'integer', minimum: 1, maximum: 86400, description: 'Duration in seconds' },
        label: { type: 'string', maxLength: 60, description: 'What the timer is for' },
      },
      required: ['seconds'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    run({ seconds, label }, ctx) {
      const timer = { id: `t${++timerSeq}`, label: label?.trim() || null, seconds, endsAt: Date.now() + seconds * 1000, status: 'running', ownerSession: ctx.session?.id };
      state.timers.push(timer);
      timerHandles.set(timer.id, setTimeout(() => fireTimer(timer), seconds * 1000));
      return { text: `OK, ${timer.label ? `${timer.label} timer` : 'timer'} set for ${fmtDuration(seconds)}.`, data: { id: timer.id, seconds, label: timer.label }, changed: true };
    },
  },
  {
    name: 'list_timers',
    title: 'List timers',
    description: 'List running timers with the time remaining.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run() {
      const running = snapshot().timers.filter((t) => t.status === 'running');
      if (!running.length) return { text: 'You have no timers running.', data: { timers: [] } };
      const parts = running.map((t) => `${t.label ? `${t.label}: ` : ''}${fmtDuration(t.remainingSeconds)} left`);
      return { text: `You have ${running.length} timer${running.length === 1 ? '' : 's'}. ${parts.join('; ')}.`, data: { timers: running } };
    },
  },
  {
    name: 'cancel_timer',
    title: 'Cancel timers',
    description: 'Cancel one timer by id, or every running timer when no id is given.',
    inputSchema: { type: 'object', properties: { id: { type: 'string', maxLength: 20 } }, additionalProperties: false },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    run({ id }) {
      const targets = state.timers.filter((t) => t.status === 'running' && (!id || t.id === id));
      if (!targets.length) return { text: id ? `I couldn't find a running timer with id ${id}.` : 'You have no timers to cancel.', data: { cancelled: 0 } };
      for (const t of targets) {
        t.status = 'cancelled';
        clearTimeout(timerHandles.get(t.id));
        timerHandles.delete(t.id);
      }
      return { text: `Cancelled ${targets.length === 1 ? 'your timer' : `${targets.length} timers`}.`, data: { cancelled: targets.length }, changed: true };
    },
  },
  {
    name: 'add_to_list',
    title: 'Add to a list',
    description: 'Add one item to the shopping list or the to-do list.',
    inputSchema: {
      type: 'object',
      properties: { list: { type: 'string', enum: ['shopping', 'todo'] }, item: { type: 'string', minLength: 1, maxLength: 80 } },
      required: ['list', 'item'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    run({ list, item }) {
      const clean = item.trim();
      const items = state.lists[list];
      if (items.some((i) => i.toLowerCase() === clean.toLowerCase())) return { text: `${clean} is already on your ${list} list.`, data: { list, items } };
      items.push(clean);
      return { text: `Added ${clean} to your ${list} list.`, data: { list, items }, changed: true };
    },
  },
  {
    name: 'remove_from_list',
    title: 'Remove from a list',
    description: 'Remove an item from the shopping list or the to-do list. Matching is case-insensitive and partial.',
    inputSchema: {
      type: 'object',
      properties: { list: { type: 'string', enum: ['shopping', 'todo'] }, item: { type: 'string', minLength: 1, maxLength: 80 } },
      required: ['list', 'item'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    run({ list, item }) {
      const needle = item.trim().toLowerCase();
      const items = state.lists[list];
      const idx = items.findIndex((i) => i.toLowerCase() === needle) >= 0 ? items.findIndex((i) => i.toLowerCase() === needle) : items.findIndex((i) => i.toLowerCase().includes(needle));
      if (idx < 0) return { text: `I couldn't find ${item.trim()} on your ${list} list.`, data: { list, items } };
      const [removed] = items.splice(idx, 1);
      return { text: `Removed ${removed} from your ${list} list.`, data: { list, items }, changed: true };
    },
  },
  {
    name: 'get_list',
    title: 'Read a list',
    description: 'Read every item on the shopping list or the to-do list.',
    inputSchema: { type: 'object', properties: { list: { type: 'string', enum: ['shopping', 'todo'] } }, required: ['list'], additionalProperties: false },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run({ list }) {
      const items = state.lists[list];
      return { text: items.length ? `Your ${list} list has ${items.length} item${items.length === 1 ? '' : 's'}: ${items.join(', ')}.` : `Your ${list} list is empty.`, data: { list, items } };
    },
  },
  {
    name: 'control_device',
    title: 'Control a smart-home device',
    description: 'Turn devices on or off, set light brightness or thermostat temperature, and lock or unlock the front door.',
    inputSchema: {
      type: 'object',
      properties: {
        device: { type: 'string', enum: deviceKeys },
        action: { type: 'string', enum: ['turn_on', 'turn_off', 'set_brightness', 'set_temperature', 'raise_temperature', 'lower_temperature', 'lock', 'unlock'] },
        value: { type: 'number', description: 'Brightness 0-100, temperature 16-30, or degrees for raise/lower' },
      },
      required: ['device', 'action'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    run({ device, action, value }) {
      const d = state.devices[device];
      const unsupported = () => { throw new ToolError(`${d.label} does not support "${action}".`); };
      const needValue = (lo, hi) => {
        if (typeof value !== 'number' || value < lo || value > hi) throw new ToolError(`"${action}" needs a value between ${lo} and ${hi}.`);
        return Math.round(value);
      };
      switch (d.type) {
        case 'light':
          if (action === 'turn_on') { d.on = true; if (!d.brightness) d.brightness = 80; }
          else if (action === 'turn_off') { d.on = false; }
          else if (action === 'set_brightness') { d.brightness = needValue(0, 100); d.on = d.brightness > 0; }
          else unsupported();
          break;
        case 'thermostat':
          if (action === 'turn_on') d.on = true;
          else if (action === 'turn_off') d.on = false;
          else if (action === 'set_temperature') { d.target = needValue(16, 30); d.on = true; }
          else if (action === 'raise_temperature') { d.target = clamp(d.target + Math.round(value ?? 1), 16, 30); d.on = true; }
          else if (action === 'lower_temperature') { d.target = clamp(d.target - Math.round(value ?? 1), 16, 30); d.on = true; }
          else unsupported();
          break;
        case 'lock':
          if (action === 'lock') d.locked = true;
          else if (action === 'unlock') d.locked = false;
          else unsupported();
          break;
        default:
          if (action === 'turn_on') d.on = true;
          else if (action === 'turn_off') d.on = false;
          else unsupported();
      }
      return { text: `${describeDevice(device)}.`, data: { device, state: d }, changed: true };
    },
  },
  {
    name: 'get_device_status',
    title: 'Smart-home status',
    description: 'Report the state of one device, or of every device when none is given.',
    inputSchema: { type: 'object', properties: { device: { type: 'string', enum: deviceKeys } }, additionalProperties: false },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run({ device }) {
      const keys = device ? [device] : deviceKeys;
      return { text: `${keys.map(describeDevice).join('. ')}.`, data: { devices: Object.fromEntries(keys.map((k) => [k, state.devices[k]])) } };
    },
  },
  {
    name: 'get_weather',
    title: 'Weather',
    description: 'Current conditions and a 3-day forecast for a city. Values are simulated.',
    inputSchema: { type: 'object', properties: { city: { type: 'string', minLength: 1, maxLength: 60 } }, required: ['city'], additionalProperties: false },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run({ city }) {
      const name = titleCase(city.trim());
      const now = weatherFor(name, 0);
      const forecast = [1, 2, 3].map((o) => ({ dayOffset: o, ...weatherFor(name, o) }));
      const tomorrow = forecast[0];
      return {
        text: `In ${name} it's ${now.tempC}°C and ${now.condition} with ${now.humidity}% humidity. Tomorrow: ${tomorrow.condition}, around ${tomorrow.tempC}°C. (Simulated weather for the demo.)`,
        data: { city: name, now, forecast, simulated: true },
      };
    },
  },
  {
    name: 'play_music',
    title: 'Play music',
    description: 'Search the demo catalog by title, artist, genre or mood and start playing the best match.',
    inputSchema: { type: 'object', properties: { query: { type: 'string', maxLength: 100 } }, required: ['query'], additionalProperties: false },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    run({ query }) {
      const idx = pickTrack(query);
      if (idx < 0) return { text: `I couldn't find anything for "${query.trim()}" in the demo catalog. Try jazz, lofi, rock, focus or chill.`, data: { found: false } };
      state.music.index = idx;
      state.music.playing = true;
      const t = CATALOG[idx];
      return { text: `Playing "${t.title}" by ${t.artist}.`, data: { track: t }, changed: true };
    },
  },
  {
    name: 'music_control',
    title: 'Music controls',
    description: 'Pause, resume, stop, skip tracks, and change the volume.',
    inputSchema: {
      type: 'object',
      properties: {
        action: { type: 'string', enum: ['pause', 'resume', 'stop', 'next', 'previous', 'volume_up', 'volume_down', 'set_volume'] },
        value: { type: 'integer', minimum: 0, maximum: 100 },
      },
      required: ['action'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    run({ action, value }) {
      const m = state.music;
      const step = (dir) => {
        m.index = m.index === null ? 0 : (m.index + dir + CATALOG.length) % CATALOG.length;
        m.playing = true;
      };
      switch (action) {
        case 'pause': if (m.index === null) return { text: 'Nothing is playing.', data: {} }; m.playing = false; break;
        case 'resume':
          if (m.index === null) step(1); // nothing loaded: start the catalog from the top
          else m.playing = true;
          break;
        case 'stop': m.playing = false; m.index = null; break;
        case 'next': step(1); break;
        case 'previous': step(-1); break;
        case 'volume_up': m.volume = clamp(m.volume + 10, 0, 100); break;
        case 'volume_down': m.volume = clamp(m.volume - 10, 0, 100); break;
        case 'set_volume':
          if (typeof value !== 'number') throw new ToolError('"set_volume" needs a value between 0 and 100.');
          m.volume = value;
          break;
      }
      const text = action === 'stop' ? 'Music stopped.' : action.startsWith('volume') || action === 'set_volume' ? `Volume ${m.volume}.` : nowPlayingText();
      return { text, data: { music: snapshot().music }, changed: true };
    },
  },
  {
    name: 'get_now_playing',
    title: 'Now playing',
    description: 'Report the current track, playback state and volume.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run() {
      return { text: nowPlayingText(), data: { music: snapshot().music } };
    },
  },
];
const toolByName = new Map(TOOLS.map((t) => [t.name, t]));

// Minimal JSON Schema validation for the subset used above.
function validateArgs(args, schema) {
  if (typeof args !== 'object' || args === null || Array.isArray(args)) return 'arguments must be an object';
  for (const key of schema.required ?? []) if (!(key in args)) return `missing required argument "${key}"`;
  for (const [key, val] of Object.entries(args)) {
    const prop = schema.properties?.[key];
    if (!prop) { if (schema.additionalProperties === false) return `unknown argument "${key}"`; continue; }
    if (prop.type === 'string' && typeof val !== 'string') return `"${key}" must be a string`;
    if (prop.type === 'integer' && !Number.isInteger(val)) return `"${key}" must be an integer`;
    if (prop.type === 'number' && typeof val !== 'number') return `"${key}" must be a number`;
    if (prop.enum && !prop.enum.includes(val)) return `"${key}" must be one of: ${prop.enum.join(', ')}`;
    if (typeof val === 'string') {
      if (prop.minLength !== undefined && val.trim().length < prop.minLength) return `"${key}" is too short`;
      if (prop.maxLength !== undefined && val.length > prop.maxLength) return `"${key}" is too long`;
    }
    if (typeof val === 'number') {
      if (prop.minimum !== undefined && val < prop.minimum) return `"${key}" must be at least ${prop.minimum}`;
      if (prop.maximum !== undefined && val > prop.maximum) return `"${key}" must be at most ${prop.maximum}`;
    }
  }
  return null;
}

// ---------------------------------------------------------------- JSON-RPC dispatch
class RpcError extends Error {
  constructor(code, message, data) { super(message); this.code = code; this.data = data; }
}
let SKILLS = [];

function handleRequest(method, params, session) {
  switch (method) {
    case 'initialize': {
      const requested = params?.protocolVersion;
      const version = SUPPORTED_VERSIONS.includes(requested) ? requested : SUPPORTED_VERSIONS[0];
      session.clientInfo = params?.clientInfo ?? null;
      return {
        protocolVersion: version,
        capabilities: { tools: { listChanged: false }, resources: { subscribe: true, listChanged: false }, logging: {} },
        serverInfo: { name: 'voicenexus-mcp', title: 'VoiceNexus household assistant (demo)', version: '1.0.0' },
        instructions: 'Household assistant demo. Read the skill:// resources for how to use each group of tools. Keep a GET event stream open to receive timer alerts. All devices, weather and music are simulated.',
      };
    }
    case 'ping': return {};
    case 'logging/setLevel': return {};
    case 'tools/list': return { tools: TOOLS.map(({ run, ...def }) => def) };
    case 'tools/call': {
      const tool = toolByName.get(params?.name);
      if (!tool) throw new RpcError(-32602, `Unknown tool: ${params?.name}`);
      const args = params.arguments ?? {};
      const problem = validateArgs(args, tool.inputSchema);
      if (problem) throw new RpcError(-32602, `Invalid arguments for ${tool.name}: ${problem}`);
      try {
        const out = tool.run(args, { session });
        if (out.changed) notifyStateChanged();
        return { content: [{ type: 'text', text: out.text }], structuredContent: out.data, isError: false };
      } catch (e) {
        if (e instanceof ToolError) return { content: [{ type: 'text', text: e.message }], isError: true };
        throw e;
      }
    }
    case 'resources/list': {
      return {
        resources: [
          { uri: STATE_URI, name: 'household-state', title: 'Household state', description: 'Devices, lists, timers and music as JSON. Subscribe for updates.', mimeType: 'application/json' },
          ...SKILLS.map((s) => ({
            uri: `skill://${s.name}/SKILL.md`,
            name: s.name,
            title: s.name,
            description: s.description,
            mimeType: 'text/markdown',
            _meta: { skill: { name: s.name, tools: s.tools, examples: s.examples } },
          })),
        ],
      };
    }
    case 'resources/read': {
      const uri = params?.uri;
      if (uri === STATE_URI) return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(snapshot()) }] };
      const m = /^skill:\/\/([a-z0-9-]+)\/SKILL\.md$/.exec(uri ?? '');
      const skill = m && SKILLS.find((s) => s.name === m[1]);
      if (!skill) throw new RpcError(-32002, 'Resource not found', { uri });
      return { contents: [{ uri, mimeType: 'text/markdown', text: skill.raw }] };
    }
    case 'resources/subscribe':
      if (params?.uri !== STATE_URI) throw new RpcError(-32602, 'Only nexus://state supports subscriptions');
      session.subs.add(params.uri);
      return {};
    case 'resources/unsubscribe':
      session.subs.delete(params?.uri);
      return {};
    default:
      throw new RpcError(-32601, `Method not found: ${method}`);
  }
}

// ---------------------------------------------------------------- HTTP helpers
function sendJson(res, status, body, headers = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload), ...headers });
  res.end(payload);
}
const rpcErrorBody = (code, message, id = null) => ({ jsonrpc: '2.0', id, error: { code, message } });

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) { reject(new RpcError(-32600, 'Request body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

// DNS-rebinding protection: validate the Host header AND the Origin header.
function hostAllowed(req) {
  const host = (req.headers.host ?? '').toLowerCase();
  if (extraHosts.has(host)) return true;
  const name = host.startsWith('[') ? host.slice(0, host.indexOf(']') + 1) : host.split(':')[0];
  return LOOPBACK_NAMES.has(name);
}
function originAllowed(req) {
  const origin = req.headers.origin;
  if (!origin) return true; // non-browser clients send no Origin
  if (extraOrigins.has(origin)) return true;
  try { return LOOPBACK_NAMES.has(new URL(origin).hostname.replace(/^(\[::1\])$/, '[::1]')); } catch { return false; }
}
function corsHeaders(req) {
  const origin = req.headers.origin;
  if (!origin || !extraOrigins.has(origin)) return {};
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Headers': 'Content-Type, Accept, Mcp-Session-Id, MCP-Protocol-Version, Last-Event-ID',
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Access-Control-Expose-Headers': 'Mcp-Session-Id',
    Vary: 'Origin',
  };
}

function getSession(req, res) {
  const id = req.headers['mcp-session-id'];
  if (!id) { sendJson(res, 400, rpcErrorBody(-32000, 'Bad Request: Mcp-Session-Id header is required')); return null; }
  const s = sessions.get(id);
  if (!s) { sendJson(res, 404, rpcErrorBody(-32001, 'Session not found')); return null; }
  s.lastSeen = Date.now();
  return s;
}

// ---------------------------------------------------------------- /mcp endpoint
async function handleMcp(req, res) {
  const cors = corsHeaders(req);
  if (!originAllowed(req)) return sendJson(res, 403, rpcErrorBody(-32000, 'Forbidden: invalid Origin'));
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }
  const send = (status, body, extra = {}) => sendJson(res, status, body, { ...cors, ...extra });

  if (req.method === 'POST') {
    const accept = req.headers.accept ?? '';
    if (!accept.includes('application/json') || !accept.includes('text/event-stream')) {
      return send(406, rpcErrorBody(-32000, 'Not Acceptable: Accept must list application/json and text/event-stream'));
    }
    let msg;
    try {
      msg = JSON.parse(await readBody(req));
    } catch (e) {
      return send(e instanceof RpcError ? 413 : 400, rpcErrorBody(e instanceof RpcError ? e.code : -32700, e instanceof RpcError ? e.message : 'Parse error'));
    }
    if (Array.isArray(msg)) return send(400, rpcErrorBody(-32600, 'Batching is not supported: send one JSON-RPC message per POST'));
    if (!msg || msg.jsonrpc !== '2.0') return send(400, rpcErrorBody(-32600, 'Invalid Request'));

    const isRequest = typeof msg.method === 'string' && msg.id !== undefined && msg.id !== null;
    const isNotification = typeof msg.method === 'string' && (msg.id === undefined || msg.id === null);
    const isResponse = msg.method === undefined && msg.id !== undefined && ('result' in msg || 'error' in msg);
    if (!isRequest && !isNotification && !isResponse) return send(400, rpcErrorBody(-32600, 'Invalid Request'));

    const isInit = isRequest && msg.method === 'initialize';
    let session;
    if (isInit) {
      session = { id: randomUUID(), initialized: false, stream: null, subs: new Set(), eventSeq: 0, lastSeen: Date.now(), clientInfo: null };
      sessions.set(session.id, session);
    } else {
      const version = req.headers['mcp-protocol-version'] ?? LEGACY_DEFAULT_VERSION;
      if (!SUPPORTED_VERSIONS.includes(version) && version !== LEGACY_DEFAULT_VERSION) {
        return send(400, rpcErrorBody(-32000, `Bad Request: unsupported MCP-Protocol-Version ${version}`));
      }
      session = getSession(req, res);
      if (!session) return;
    }

    if (isNotification || isResponse) {
      if (isNotification && msg.method === 'notifications/initialized') session.initialized = true;
      res.writeHead(202, cors);
      return res.end();
    }

    // JSON-RPC request
    if (msg.method === 'tools/call') {
      // Demonstrates the SSE response mode: prime event, a progress log, then the response.
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', ...cors });
      sseWrite(res, { id: nextEventId(session), data: '' });
      sseWrite(res, { id: nextEventId(session), data: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'debug', logger: 'tools', data: `calling ${msg.params?.name}` } }) });
      let reply;
      try {
        reply = { jsonrpc: '2.0', id: msg.id, result: handleRequest(msg.method, msg.params, session) };
      } catch (e) {
        reply = e instanceof RpcError ? { jsonrpc: '2.0', id: msg.id, error: { code: e.code, message: e.message, data: e.data } } : { jsonrpc: '2.0', id: msg.id, error: { code: -32603, message: 'Internal error' } };
        if (!(e instanceof RpcError)) console.error(e);
      }
      sseWrite(res, { id: nextEventId(session), data: JSON.stringify(reply) });
      return res.end();
    }

    try {
      const result = handleRequest(msg.method, msg.params, session);
      return send(200, { jsonrpc: '2.0', id: msg.id, result }, isInit ? { 'Mcp-Session-Id': session.id } : {});
    } catch (e) {
      if (isInit) sessions.delete(session.id);
      if (e instanceof RpcError) return send(200, { jsonrpc: '2.0', id: msg.id, error: { code: e.code, message: e.message, data: e.data } });
      console.error(e);
      return send(200, { jsonrpc: '2.0', id: msg.id, error: { code: -32603, message: 'Internal error' } });
    }
  }

  if (req.method === 'GET') {
    if (!(req.headers.accept ?? '').includes('text/event-stream')) return send(406, rpcErrorBody(-32000, 'Not Acceptable: Accept must include text/event-stream'));
    const session = getSession(req, res);
    if (!session) return;
    // Only one stream per session: a new GET replaces the old one (a message is never sent on two streams).
    if (session.stream && !session.stream.writableEnded) session.stream.end();
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', ...cors });
    sseWrite(res, { id: nextEventId(session), retry: 3000, data: '' });
    session.stream = res;
    const beat = setInterval(() => res.write(': keep-alive\n\n'), 15000);
    req.on('close', () => {
      clearInterval(beat);
      if (session.stream === res) session.stream = null;
    });
    return;
  }

  if (req.method === 'DELETE') {
    const session = getSession(req, res);
    if (!session) return;
    if (session.stream && !session.stream.writableEnded) session.stream.end();
    sessions.delete(session.id);
    res.writeHead(200, cors);
    return res.end();
  }

  res.writeHead(405, { Allow: 'GET, POST, DELETE, OPTIONS', ...cors });
  res.end();
}

// ---------------------------------------------------------------- Static web app
const STATIC = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/style.css': ['style.css', 'text/css; charset=utf-8'],
};
async function serveStatic(req, res, pathname) {
  const entry = STATIC[pathname];
  if (!entry || (req.method !== 'GET' && req.method !== 'HEAD')) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
  const body = await readFile(path.join(__dirname, 'public', entry[0]));
  res.writeHead(200, { 'Content-Type': entry[1], 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
  res.end(req.method === 'HEAD' ? undefined : body);
}

SKILLS = await loadSkills();

const server = http.createServer(async (req, res) => {
  try {
    if (!hostAllowed(req)) { res.writeHead(403, { 'Content-Type': 'text/plain' }); return res.end('Forbidden host'); }
    const { pathname } = new URL(req.url, 'http://localhost');
    if (pathname === '/mcp') return await handleMcp(req, res);
    if (pathname === '/healthz') return sendJson(res, 200, { ok: true, skills: SKILLS.length, tools: TOOLS.length, sessions: sessions.size });
    return await serveStatic(req, res, pathname);
  } catch (e) {
    console.error(e);
    if (!res.headersSent) res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Internal server error');
  }
});

setInterval(() => {
  const cutoff = Date.now() - 2 * 60 * 60 * 1000;
  for (const [id, s] of sessions) if (s.lastSeen < cutoff && !s.stream) sessions.delete(id);
}, 10 * 60 * 1000).unref();

server.listen(PORT, HOST, () => {
  console.log(`VoiceNexus  ->  http://${HOST}:${PORT}/   (MCP endpoint: http://${HOST}:${PORT}/mcp)`);
  console.log(`Skills: ${SKILLS.map((s) => s.name).join(', ')}`);
});

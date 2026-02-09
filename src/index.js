import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason
} from '@whiskeysockets/baileys';
import P from 'pino';
import qrcode from 'qrcode-terminal';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.resolve(__dirname, '..', 'data');
const PREFIX_FILE = path.join(DATA_DIR, 'prefixes.json');
const AUTH_DIR = path.resolve(__dirname, '..', 'auth');

function loadPrefixes() {
  try {
    const raw = fs.readFileSync(PREFIX_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (data && typeof data === 'object') return data;
    return {};
  } catch {
    return {};
  }
}

function savePrefixes(prefixes) {
  fs.writeFileSync(PREFIX_FILE, JSON.stringify(prefixes, null, 2));
}

function getText(msg) {
  if (!msg) return '';
  return (
    msg.conversation ||
    msg.extendedTextMessage?.text ||
    msg.imageMessage?.caption ||
    msg.videoMessage?.caption ||
    ''
  );
}

function parseCommand(text, prefix) {
  if (!text || !text.startsWith(prefix)) return null;
  const withoutPrefix = text.slice(prefix.length).trim();
  if (!withoutPrefix) return null;
  const [cmd, ...args] = withoutPrefix.split(/\s+/);
  return { cmd: cmd.toLowerCase(), args };
}

async function start() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(PREFIX_FILE)) savePrefixes({});

  const logger = P({ level: 'silent' });
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    logger,
    auth: state
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) qrcode.generate(qr, { small: true });
    if (connection === 'close') {
      const shouldReconnect =
        lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
      if (shouldReconnect) start();
    }
  });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    const m = messages?.[0];
    if (!m || m.key.fromMe) return;

    const chatId = m.key.remoteJid;
    const body = getText(m.message);

    const prefixes = loadPrefixes();
    const prefix = prefixes[chatId] || '-';

    const parsed = parseCommand(body, prefix);
    if (!parsed) return;

    const { cmd, args } = parsed;

    switch (cmd) {
      case 'ping':
        await sock.sendMessage(chatId, { text: 'pong' }, { quoted: m });
        break;

      case 'help':
        await sock.sendMessage(
          chatId,
          {
            text:
              `CIPHERPHANTOM Befehle (Prefix: ${prefix})\n` +
              `${prefix}help\n` +
              `${prefix}ping\n` +
              `${prefix}prefix <neues_prefix>`
          },
          { quoted: m }
        );
        break;

      case 'prefix':
      case 'setprefix': {
        const next = args[0];
        if (!next) {
          await sock.sendMessage(
            chatId,
            { text: `Aktueller Prefix: ${prefix}\nUsage: ${prefix}prefix <neues_prefix>` },
            { quoted: m }
          );
          break;
        }
        if (next.length > 3) {
          await sock.sendMessage(
            chatId,
            { text: 'Prefix zu lang. Maximal 3 Zeichen.' },
            { quoted: m }
          );
          break;
        }
        prefixes[chatId] = next;
        savePrefixes(prefixes);
        await sock.sendMessage(
          chatId,
          { text: `Prefix gesetzt auf: ${next}` },
          { quoted: m }
        );
        break;
      }

      default:
        await sock.sendMessage(
          chatId,
          { text: `Unbekannter Befehl. ${prefix}help` },
          { quoted: m }
        );
        break;
    }
  });
}

start();

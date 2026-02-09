import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import P from "pino";
import qrcode from "qrcode-terminal";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import os from "os";

// Pfad-Utilities fuer ES Modules (kein __dirname von Haus aus)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Speicherorte fuer Daten und WhatsApp-Session
const DATA_DIR = path.resolve(__dirname, "..", "data");
const PREFIX_FILE = path.join(DATA_DIR, "prefixes.json");
const AUTH_DIR = path.resolve(__dirname, "..", "auth");

// Einfache ANSI-Farben fuer Terminal-Ausgabe
const COLORS = {
  reset: "\x1b[0m",
  gray: "\x1b[90m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  cyan: "\x1b[36m",
  magenta: "\x1b[35m",
  bold: "\x1b[1m",
};

// Logger: farbig + Emojis + kurzer deutscher Zeitstempel
function log(type, msg) {
  const ts = new Date().toLocaleString("de-DE", {
    year: "2-digit",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  let color = COLORS.gray;
  let icon = "•";
  if (type === "qr") {
    color = COLORS.cyan;
    icon = "📱";
  }
  if (type === "open") {
    color = COLORS.green;
    icon = "✅";
  }
  if (type === "close") {
    color = COLORS.yellow;
    icon = "⚠️";
  }
  if (type === "cmd") {
    color = COLORS.cyan;
    icon = "🧩";
  }
  if (type === "error") {
    color = COLORS.red;
    icon = "❌";
  }
  const line = `${COLORS.cyan}[${ts}]${COLORS.reset} ${color}${icon} ${msg}${COLORS.reset}`;
  console.log(line);
}

// Bytes menschenlesbar formatieren (KB/MB/GB)
function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = bytes;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

// Sekunden in lesbare Uptime umrechnen
function formatUptime(seconds) {
  const s = Math.floor(seconds % 60);
  const m = Math.floor((seconds / 60) % 60);
  const h = Math.floor((seconds / 3600) % 24);
  const d = Math.floor(seconds / 86400);
  const parts = [];
  if (d) parts.push(`${d}d`);
  if (h || d) parts.push(`${h}h`);
  parts.push(`${m}m`, `${s}s`);
  return parts.join(" ");
}

// Start-Banner mit Systeminfos ausgeben
function printBanner() {
  const title = "CIPHERPHANTOM";
  const pad = 2;
  const width = title.length + pad * 2;
  const top = `┌${"─".repeat(width)}┐`;
  const mid = `│${" ".repeat(pad)}${title}${" ".repeat(pad)}│`;
  const bot = `└${"─".repeat(width)}┘`;

  // System-RAM
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;
  const memLine = `${formatBytes(usedMem)} / ${formatBytes(totalMem)} RAM`;

  // RAM-Nutzung des Bots selbst
  const procMem = process.memoryUsage();
  const procLine = `Bot RAM: ${formatBytes(procMem.rss)}`;

  // CPU-Load (1 Minute)
  const load = os.loadavg()[0]?.toFixed(2) ?? "0.00";
  const cpuLine = `CPU Load (1m): ${load}`;

  // Prozess-Uptime (wie lange der Bot laeuft)
  const upLine = `Uptime: ${formatUptime(process.uptime())}`;

  // System-Infos
  const hostLine = `Host: ${os.hostname()} (${os.platform()} ${os.arch()})`;
  const coresLine = `CPU Cores: ${os.cpus()?.length || 0}`;
  const nodeLine = `Node.js: ${process.version}`;

  console.log(
    `${COLORS.magenta}${COLORS.bold}${top}\n${mid}\n${bot}${COLORS.reset}`,
  );
  console.log(`${COLORS.bold}System Overview${COLORS.reset}`);
  console.log(`- ${hostLine}`);
  console.log(`- ${coresLine}`);
  console.log(`- ${cpuLine}`);
  console.log(`- ${memLine}`);
  console.log(`- ${procLine}`);
  console.log(`- ${upLine}`);
  console.log(`- ${nodeLine}`);
  console.log("");
}

// Prefixe aus JSON laden (pro Chat gespeichert)
function loadPrefixes() {
  try {
    const raw = fs.readFileSync(PREFIX_FILE, "utf8");
    const data = JSON.parse(raw);
    if (data && typeof data === "object") return data;
    return {};
  } catch {
    return {};
  }
}

// Prefixe in JSON speichern
function savePrefixes(prefixes) {
  fs.writeFileSync(PREFIX_FILE, JSON.stringify(prefixes, null, 2));
}

// Text aus unterschiedlichen Message-Typen holen
function getText(msg) {
  if (!msg) return "";
  return (
    msg.conversation ||
    msg.extendedTextMessage?.text ||
    msg.imageMessage?.caption ||
    msg.videoMessage?.caption ||
    ""
  );
}

// Command aus Text parsen, wenn Prefix stimmt
function parseCommand(text, prefix) {
  if (!text || !text.startsWith(prefix)) return null;
  const withoutPrefix = text.slice(prefix.length).trim();
  if (!withoutPrefix) return null;
  const [cmd, ...args] = withoutPrefix.split(/\s+/);
  return { cmd: cmd.toLowerCase(), args };
}

// Hauptstart: WhatsApp verbinden, Events registrieren
async function start() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(PREFIX_FILE)) savePrefixes({});

  printBanner();

  // Baileys internes Logging deaktivieren
  const logger = P({ level: "silent" });
  // Session laden/speichern (Multi-File Auth)
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  // Neueste kompatible Baileys-Version holen
  const { version } = await fetchLatestBaileysVersion();

  // Socket/Client erstellen
  const sock = makeWASocket({
    version,
    logger,
    auth: state,
  });

  // Neue Credentials automatisch speichern
  sock.ev.on("creds.update", saveCreds);

  // Verbindungsstatus behandeln (QR, open, close)
  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      log("qr", "QR erhalten - bitte mit WhatsApp scannen");
      qrcode.generate(qr, { small: true });
    }
    if (connection === "close") {
      const shouldReconnect =
        lastDisconnect?.error?.output?.statusCode !==
        DisconnectReason.loggedOut;
      log("close", `Verbindung geschlossen. Reconnect: ${shouldReconnect}`);
      if (shouldReconnect) start();
    }
    if (connection === "open") log("open", "Verbunden");
  });

  // Eingehende Nachrichten behandeln
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const m = messages?.[0];
    if (!m || m.key.fromMe) return;

    // Chat-ID und Nachrichtentext
    const chatId = m.key.remoteJid;
    const body = getText(m.message);

    // Prefix pro Chat laden (Default: "-")
    const prefixes = loadPrefixes();
    const prefix = prefixes[chatId] || "-";

    // Command parsen
    const parsed = parseCommand(body, prefix);
    if (!parsed) return;

    const { cmd, args } = parsed;
    log("cmd", `Befehl: ${cmd} | Chat: ${chatId}`);

    // Befehle per switch/case
    switch (cmd) {
      case "ping":
        // Ping-Pong Test: prueft ob Bot reagiert
        await sock.sendMessage(chatId, { text: "pong" }, { quoted: m });
        break;

      case "help":
        // Hilfe-Menue mit allen Befehlen anzeigen
        await sock.sendMessage(
          chatId,
          {
            text:
              `CIPHERPHANTOM Befehle (Prefix: ${prefix})\n` +
              `${prefix}help\n` +
              `${prefix}ping\n` +
              `${prefix}prefix <neues_prefix>`,
          },
          { quoted: m },
        );
        break;

      case "prefix":
      case "setprefix": {
        // Prefix fuer den aktuellen Chat setzen
        const next = args[0];
        if (!next) {
          // Kein neuer Prefix angegeben -> aktuellen anzeigen + Usage
          await sock.sendMessage(
            chatId,
            {
              text: `Aktueller Prefix: ${prefix}\nUsage: ${prefix}prefix <neues_prefix>`,
            },
            { quoted: m },
          );
          break;
        }
        if (next.length > 3) {
          // Sicherheitslimit: Prefix nicht zu lang
          await sock.sendMessage(
            chatId,
            { text: "Prefix zu lang. Maximal 3 Zeichen." },
            { quoted: m },
          );
          break;
        }
        // Prefix speichern und bestaetigen
        prefixes[chatId] = next;
        savePrefixes(prefixes);
        await sock.sendMessage(
          chatId,
          { text: `Prefix gesetzt auf: ${next}` },
          { quoted: m },
        );
        break;
      }

      default:
        // Unbekannter Befehl -> Hinweis auf Hilfe
        await sock.sendMessage(
          chatId,
          { text: `Unbekannter Befehl. ${prefix}help` },
          { quoted: m },
        );
        break;
    }
  });
}

start();

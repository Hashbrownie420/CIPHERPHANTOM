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
import PDFDocument from "pdfkit";
import {
  initDb,
  getUser,
  createUser,
  setProfileName,
  addBalance,
  addXp,
  setDaily,
  setWeekly,
  setDsgvoAccepted,
  setUserRole,
  setLevelRole,
  setNameChange,
  setPreDsgvoAccepted,
  getPreDsgvoAccepted,
  clearPreDsgvoAccepted,
  getFriendByCode,
  addFriend,
  listFriends,
  listQuests,
  ensureUserQuest,
  getUserQuest,
  updateQuestProgress,
  completeQuest,
  claimQuest,
  deleteUser,
  dumpAll,
} from "./db.js";

// Pfad-Utilities fuer ES Modules (kein __dirname von Haus aus)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Speicherorte fuer Daten und WhatsApp-Session
const DATA_DIR = path.resolve(__dirname, "..", "data");
const PREFIX_FILE = path.join(DATA_DIR, "prefixes.json");
const AUTH_DIR = path.resolve(__dirname, "..", "auth");
const CURRENCY = "PHN";
const CURRENCY_NAME = "Phantoms";
const DSGVO_VERSION = "2026-02-09";
const pendingDeletes = new Map();
const OWNER_IDS = new Set([
  "72271934840903@lid",
  "77112346173682@lid",
]);
const pendingNameChanges = new Map();

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

// Zufalls-Freundescode generieren
function generateFriendCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let out = "";
  for (let i = 0; i < 8; i += 1) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

// Datum als YYYY-MM-DD
function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function yesterdayStr() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

// ISO-Woche als YYYY-WW
function isoWeekStr(date = new Date()) {
  const d = new Date(
    Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()),
  );
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
  const weekStr = String(week).padStart(2, "0");
  return `${d.getUTCFullYear()}-${weekStr}`;
}

// XP -> Level (einfache Progression)
function xpToLevel(xp) {
  return Math.floor(xp / 1000) + 1;
}

function xpToNextLevel(xp) {
  const level = xpToLevel(xp);
  const nextAt = level * 1000;
  return Math.max(0, nextAt - xp);
}

// Nutzerrolle: Owner oder User (Owner ist fest im Code verdrahtet)
function getUserRole(senderId) {
  return OWNER_IDS.has(senderId) ? "owner" : "user";
}

// Levelrolle abhaengig vom Level
function getLevelRole(level) {
  if (level >= 30) return "Legend";
  if (level >= 20) return "Elite";
  if (level >= 10) return "Pro";
  if (level >= 5) return "Adept";
  return "Rookie";
}

// Owner bekommen unsichtbar x2 auf PHN/XP
function rewardMultiplier(senderId) {
  return OWNER_IDS.has(senderId) ? 2 : 1;
}

function isOwner(senderId) {
  return OWNER_IDS.has(senderId);
}

// XP aendern + Levelrolle automatisch aktualisieren
async function applyXp(db, chatId, currentXp, deltaXp) {
  const newXp = currentXp + deltaXp;
  const newLevel = xpToLevel(newXp);
  const levelRole = getLevelRole(newLevel);
  await addXp(db, chatId, deltaXp, newLevel);
  await setLevelRole(db, chatId, levelRole);
  return { newXp, newLevel, levelRole };
}

// Rollen aus DB mit Code-Logik abgleichen
async function syncRoles(db, user, senderId) {
  const userRole = getUserRole(senderId);
  const levelRole = getLevelRole(user.level);
  if (user.user_role !== userRole) {
    await setUserRole(db, user.chat_id, userRole);
  }
  if (user.level_role !== levelRole) {
    await setLevelRole(db, user.chat_id, levelRole);
  }
  return { userRole, levelRole };
}

function formatDumpSection(title, rows) {
  const header = `=== ${title} (${rows.length}) ===`;
  if (rows.length === 0) return `${header}\n<leer>\n`;
  const lines = rows.map((r) => JSON.stringify(r));
  return `${header}\n${lines.join("\n")}\n`;
}

async function sendDbDump(sock, chatId, db) {
  const dump = await dumpAll(db);
  const content =
    formatDumpSection("users", dump.users) +
    formatDumpSection("friends", dump.friends) +
    formatDumpSection("quests", dump.quests) +
    formatDumpSection("user_quests", dump.userQuests) +
    formatDumpSection("dsgvo_accepts", dump.dsgvoAccepts);

  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const filePath = path.resolve(DATA_DIR, `dbdump-${ts}.txt`);
  fs.writeFileSync(filePath, content, "utf8");

  await sock.sendMessage(
    chatId,
    {
      document: fs.readFileSync(filePath),
      fileName: path.basename(filePath),
      mimetype: "text/plain",
      caption: "DB Dump (Text)",
    },
  );
}

function drawTable(doc, title, rows, columns, addPage) {
  const pageWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const minCol = 70;
  const maxCol = 140;
  const rowPadding = 4;

  const ensureSpace = (needed) => {
    const bottom = doc.page.height - doc.page.margins.bottom;
    if (doc.y + needed > bottom) addPage();
  };

  // Ueberschrift + Abstand
  ensureSpace(28);
  doc.fontSize(14).fillColor("#0B0F1A").text(title);
  doc.moveDown(0.2);

  if (!rows || rows.length === 0) {
    ensureSpace(16);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
    doc.moveDown(0.8);
    return;
  }

  const cols =
    columns && columns.length
      ? columns
      : Object.keys(rows[0]).map((k) => ({ key: k, label: k }));

  const formatValue = (key, val) => {
    if (val === null || val === undefined) return "";
    if (/_at$/.test(key) || key.includes("created_at") || key.includes("accepted_at")) {
      return new Date(val).toLocaleString("de-DE");
    }
    return String(val);
  };

  // Spaltenbreiten nach Textlaenge
  const colWeights = cols.map((c) => {
    const headerW = doc.widthOfString(c.label);
    const sample = rows.slice(0, 40).map((r) =>
      formatValue(c.key, r[c.key]),
    );
    const maxW = Math.max(
      headerW,
      ...sample.map((v) => doc.widthOfString(v)),
    );
    return Math.min(maxCol, Math.max(minCol, maxW + 18));
  });
  const total = colWeights.reduce((a, b) => a + b, 0);
  const scale = total > pageWidth ? pageWidth / total : 1;
  const colWidths = colWeights.map((w) => Math.max(minCol, w * scale));

  const drawHeader = () => {
    const y = doc.y + 6;
    const headerHeight = 18;
    doc.save();
    doc
      .rect(doc.page.margins.left, y - 4, pageWidth, headerHeight + 6)
      .fill("#E9EEF5");
    doc.restore();
    doc.fontSize(9).fillColor("#0F172A");
    let x = doc.page.margins.left;
    cols.forEach((c, i) => {
      doc.text(c.label, x + 2, y, { width: colWidths[i] - 4 });
      x += colWidths[i];
    });
    // Vertikale Trennlinien (Header)
    doc.save().strokeColor("#D1D5DB").lineWidth(0.8);
    let vx = doc.page.margins.left;
    for (let i = 0; i < colWidths.length - 1; i += 1) {
      vx += colWidths[i];
      doc
        .moveTo(vx, y - 4)
        .lineTo(vx, y - 4 + headerHeight + 6)
        .stroke();
    }
    doc.restore();
    doc.y = y + headerHeight;
  };

  ensureSpace(22);
  drawHeader();

  let rowIndex = 0;
  for (const r of rows) {
    const cells = cols.map((c) => formatValue(c.key, r[c.key]));
    const heights = cells.map((val, i) =>
      doc.heightOfString(val, { width: colWidths[i] - 4, lineGap: 1 }),
    );
    const rowHeight = Math.max(...heights) + rowPadding;

    if (doc.y + rowHeight > doc.page.height - doc.page.margins.bottom) {
      addPage();
      drawHeader();
    }

    if (rowIndex % 2 === 0) {
      doc.save();
      doc
        .rect(
          doc.page.margins.left,
          doc.y - 1,
          pageWidth,
          rowHeight + 2,
        )
        .fill("#F7F9FC");
      doc.restore();
    }

    doc.fontSize(9).fillColor("#111827");
    let x = doc.page.margins.left;
    cells.forEach((val, i) => {
      const textHeight = heights[i];
      const offsetY = Math.max(0, (rowHeight - rowPadding - textHeight) / 2);
      doc.text(val, x + 2, doc.y + offsetY, { width: colWidths[i] - 4, lineGap: 1 });
      x += colWidths[i];
    });
    // Zeilentrenner + vertikale Trennlinien
    doc.save().strokeColor("#E5E7EB").lineWidth(0.8);
    doc
      .moveTo(doc.page.margins.left, doc.y + rowHeight - 1)
      .lineTo(doc.page.margins.left + pageWidth, doc.y + rowHeight - 1)
      .stroke();
    let vx = doc.page.margins.left;
    for (let i = 0; i < colWidths.length - 1; i += 1) {
      vx += colWidths[i];
      doc
        .moveTo(vx, doc.y - 1)
        .lineTo(vx, doc.y + rowHeight - 1)
        .stroke();
    }
    doc.restore();
    doc.y += rowHeight;
    rowIndex += 1;
  }

  // Natuerlicher Abstand zur naechsten Tabelle
  doc.moveDown(0.6);
}

function estimateUserSectionHeight(doc, user) {
  const pageWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const keyWidth = Math.min(170, pageWidth * 0.35);
  const valWidth = pageWidth - keyWidth;
  const rowPad = 4;
  const rows = [
    ["Freundescode", user.friend_code],
    ["Level", user.level],
    ["XP", user.xp],
    ["PHN", user.phn],
    ["Rolle", user.user_role],
    ["Levelrolle", user.level_role],
    ["Daily Streak", user.daily_streak],
    ["Erstellt", formatDateTime(user.created_at)],
    ["DSGVO akzeptiert", user.dsgvo_accepted_at ? formatDateTime(user.dsgvo_accepted_at) : "-"],
    ["DSGVO Version", user.dsgvo_version || "-"],
  ];
  let estimated = 24; // Titel + Abstand
  for (const [k, v] of rows) {
    const key = String(k);
    const val = v === null || v === undefined ? "" : String(v);
    const h = Math.max(
      doc.heightOfString(key, { width: keyWidth }),
      doc.heightOfString(val, { width: valWidth }),
    ) + rowPad;
    estimated += h;
  }
  estimated += 8;
  return estimated;
}

function drawUserSection(doc, user, addPage) {
  const ensureSpace = (needed) => {
    const bottom = doc.page.height - doc.page.margins.bottom;
    if (doc.y + needed > bottom) addPage();
  };

  const title = `User: ${user.profile_name} (${user.chat_id})`;
  ensureSpace(28);
  doc.fontSize(13).fillColor("#0B0F1A").text(title);
  doc.moveDown(0.2);

  const rows = [
    ["Freundescode", user.friend_code],
    ["Level", user.level],
    ["XP", user.xp],
    ["PHN", user.phn],
    ["Rolle", user.user_role],
    ["Levelrolle", user.level_role],
    ["Daily Streak", user.daily_streak],
    ["Erstellt", formatDateTime(user.created_at)],
    ["DSGVO akzeptiert", user.dsgvo_accepted_at ? formatDateTime(user.dsgvo_accepted_at) : "-"],
    ["DSGVO Version", user.dsgvo_version || "-"],
  ];

  const pageWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const keyWidth = Math.min(170, pageWidth * 0.35);
  const valWidth = pageWidth - keyWidth;
  const rowPad = 4;

  // Abschnitts-Hoehe abschaetzen, um Split zu vermeiden
  ensureSpace(estimateUserSectionHeight(doc, user));

  for (const [k, v] of rows) {
    const key = String(k);
    const val = v === null || v === undefined ? "" : String(v);
    const h = Math.max(
      doc.heightOfString(key, { width: keyWidth }),
      doc.heightOfString(val, { width: valWidth }),
    ) + rowPad;

    doc.fontSize(9).fillColor("#6B7280").text(key, doc.page.margins.left, doc.y, {
      width: keyWidth,
    });
    doc.fontSize(10).fillColor("#111827").text(
      val,
      doc.page.margins.left + keyWidth,
      doc.y,
      { width: valWidth },
    );
    doc.y += h;
  }

  doc.moveDown(0.6);
}

function drawRecordSection(doc, title, row, columns, addPage) {
  addPage();
  doc.fontSize(13).fillColor("#0B0F1A").text(title);
  doc.moveDown(0.3);

  const pageWidth =
    doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const keyWidth = Math.min(180, pageWidth * 0.35);
  const valWidth = pageWidth - keyWidth;
  const rowPad = 4;

  for (const c of columns) {
    const key = c.label;
    const raw = row[c.key];
    const val =
      raw === null || raw === undefined
        ? ""
        : /_at$/.test(c.key) || c.key.includes("created_at") || c.key.includes("accepted_at")
        ? new Date(raw).toLocaleString("de-DE")
        : String(raw);
    const h = Math.max(
      doc.heightOfString(key, { width: keyWidth }),
      doc.heightOfString(val, { width: valWidth }),
    ) + rowPad;

    doc.fontSize(9).fillColor("#6B7280").text(key, doc.page.margins.left, doc.y, {
      width: keyWidth,
    });
    doc.fontSize(10).fillColor("#111827").text(
      val,
      doc.page.margins.left + keyWidth,
      doc.y,
      { width: valWidth },
    );
    doc.y += h;
  }
}

function drawPdfHeader(doc, pageNumber, meta) {
  const left = doc.page.margins.left;
  const right = doc.page.width - doc.page.margins.right;

  // Header: Brand + Document info
  doc.fontSize(22).fillColor("#0B0F1A").text("CIPHERPHANTOM", left, doc.y);
  doc
    .fontSize(9)
    .fillColor("#6B7280")
    .text(`Dokument: ${meta.title}`, right - 220, doc.y - 16, {
      width: 220,
      align: "right",
    })
    .text(`Datum: ${meta.date}`, right - 220, doc.y, {
      width: 220,
      align: "right",
    })
    .text(`Dokument-ID: ${meta.docId}`, right - 220, doc.y + 12, {
      width: 220,
      align: "right",
    });

  doc.moveDown(0.4);
  doc
    .strokeColor("#111827")
    .lineWidth(1)
    .moveTo(left, doc.y)
    .lineTo(right, doc.y)
    .stroke();
  doc.moveDown(0.6);

  // Footer mit Seitenzahl
  const footerY = doc.page.height - doc.page.margins.bottom + 10;
  doc
    .fontSize(9)
    .fillColor("#6B7280")
    .text(`Seite ${pageNumber}`, left, footerY, {
      width: right - left,
      align: "right",
    });
}

async function sendDbDumpPdf(sock, chatId, db) {
  const dump = await dumpAll(db);
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const filePath = path.resolve(DATA_DIR, `dbdump-${ts}.pdf`);

  const doc = new PDFDocument({ margin: 40, autoFirstPage: false });
  const stream = fs.createWriteStream(filePath);
  doc.pipe(stream);

  const meta = {
    title: "Datenbank Export",
    date: new Date().toLocaleString("de-DE"),
    docId: `DB-${ts}`,
  };

  const page = { n: 0 };
  const addPage = () => {
    doc.addPage();
    page.n += 1;
    drawPdfHeader(doc, page.n, meta);
  };

  // Kein Deckblatt: direkt mit Eintraegen starten

  const schemas = {
    users: [
      { key: "chat_id", label: "Chat-ID" },
      { key: "profile_name", label: "Profil" },
      { key: "friend_code", label: "Code" },
      { key: "level", label: "Level" },
      { key: "xp", label: "XP" },
      { key: "phn", label: "PHN" },
      { key: "user_role", label: "Rolle" },
      { key: "level_role", label: "Levelrolle" },
      { key: "created_at", label: "Erstellt" },
    ],
    friends: [
      { key: "user_id", label: "User" },
      { key: "friend_id", label: "Freund" },
      { key: "created_at", label: "Seit" },
    ],
    quests: [
      { key: "id", label: "ID" },
      { key: "key", label: "Key" },
      { key: "title", label: "Titel" },
      { key: "period", label: "Typ" },
      { key: "target", label: "Ziel" },
      { key: "reward_phn", label: "PHN" },
      { key: "reward_xp", label: "XP" },
      { key: "active", label: "Aktiv" },
    ],
    user_quests: [
      { key: "user_id", label: "User" },
      { key: "quest_id", label: "Quest" },
      { key: "progress", label: "Progress" },
      { key: "completed_at", label: "Abgeschlossen" },
      { key: "claimed_at", label: "Geclaimt" },
    ],
    dsgvo_accepts: [
      { key: "chat_id", label: "Chat-ID" },
      { key: "accepted_at", label: "Akzeptiert" },
      { key: "version", label: "Version" },
    ],
  };

  // Users als einzelne, saubere Sektionen
  if (dump.users.length === 0) {
    addPage();
    doc.fontSize(13).fillColor("#0B0F1A").text("Users (0)");
    doc.moveDown(0.2);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
  } else {
    for (const u of dump.users) {
      addPage();
      drawUserSection(doc, u, () => {});
    }
  }
  if (dump.friends.length === 0) {
    addPage();
    doc.fontSize(13).fillColor("#0B0F1A").text("Friends (0)");
    doc.moveDown(0.2);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
  } else {
    dump.friends.forEach((r, i) =>
      drawRecordSection(
        doc,
        `Friend ${i + 1}/${dump.friends.length}`,
        r,
        schemas.friends,
        addPage,
      ),
    );
  }

  if (dump.quests.length === 0) {
    addPage();
    doc.fontSize(13).fillColor("#0B0F1A").text("Quests (0)");
    doc.moveDown(0.2);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
  } else {
    dump.quests.forEach((r, i) =>
      drawRecordSection(
        doc,
        `Quest ${i + 1}/${dump.quests.length}`,
        r,
        schemas.quests,
        addPage,
      ),
    );
  }

  if (dump.userQuests.length === 0) {
    addPage();
    doc.fontSize(13).fillColor("#0B0F1A").text("User Quests (0)");
    doc.moveDown(0.2);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
  } else {
    dump.userQuests.forEach((r, i) =>
      drawRecordSection(
        doc,
        `User Quest ${i + 1}/${dump.userQuests.length}`,
        r,
        schemas.user_quests,
        addPage,
      ),
    );
  }

  if (dump.dsgvoAccepts.length === 0) {
    addPage();
    doc.fontSize(13).fillColor("#0B0F1A").text("DSGVO Accepts (0)");
    doc.moveDown(0.2);
    doc.fontSize(10).fillColor("#6B7280").text("Keine Daten");
  } else {
    dump.dsgvoAccepts.forEach((r, i) =>
      drawRecordSection(
        doc,
        `DSGVO Accept ${i + 1}/${dump.dsgvoAccepts.length}`,
        r,
        schemas.dsgvo_accepts,
        addPage,
      ),
    );
  }

  // Signaturblock auf der letzten Seite
  const left = doc.page.margins.left;
  const right = doc.page.width - doc.page.margins.right;
  if (doc.y + 120 > doc.page.height - doc.page.margins.bottom) addPage();
  doc.moveDown(0.8);
  doc.fontSize(11).fillColor("#111111").text("Systemsignatur");
  doc.moveDown(0.4);
  doc
    .strokeColor("#9CA3AF")
    .lineWidth(1)
    .moveTo(left, doc.y)
    .lineTo(left + 220, doc.y)
    .stroke();
  doc.fontSize(10).fillColor("#374151").text("CIPHERPHANTOM System", left, doc.y + 4);
  doc
    .fontSize(9)
    .fillColor("#6B7280")
    .text("Automatisch generierte Unterschrift", left, doc.y + 18);

  // Nur eine Unterschrift

  doc.end();

  await new Promise((resolve) => stream.on("finish", resolve));

  await sock.sendMessage(
    chatId,
    {
      document: fs.readFileSync(filePath),
      fileName: path.basename(filePath),
      mimetype: "application/pdf",
      caption: "DB Dump (PDF)",
    },
  );
}

// Datum/Zeit lesbar im deutschen Format
function formatDateTime(iso) {
  if (!iso) return "-";
  return new Date(iso).toLocaleString("de-DE");
}

// Einfache DSGVO-Info (Kurzfassung) fuer den Bot
function dsgvoText(prefix) {
  return (
    "DSGVO Kurzinfo (CIPHERPHANTOM)\n" +
    "1) Gespeicherte Daten: chat_id, Profilname, Freundescode, Wallet-Stand, XP/Level, Quest-Progress, Daily/Weekly Status.\n" +
    "2) Zweck: Spiel-Features, Fortschritt, Freunde, Belohnungen.\n" +
    "3) Speicherung: lokal in einer SQLite-Datenbank auf dem Bot-System.\n" +
    "4) Keine Weitergabe an Dritte, kein Tracking von IPs.\n" +
    "5) Du kannst Loeschung verlangen (Befehl folgt spaeter).\n\n" +
    `Bestaetigen mit: ${prefix}accept`
  );
}

// Verifiziert nur weitermachen: registriert + DSGVO bestaetigt
async function requireVerified(sock, m, chatId, prefix, user) {
  if (!user) {
    await sock.sendMessage(
      chatId,
      { text: `Bitte zuerst registrieren: ${prefix}register <name>` },
      { quoted: m },
    );
    return null;
  }
  if (!user.dsgvo_accepted_at) {
    await sock.sendMessage(
      chatId,
      { text: `Bitte zuerst DSGVO lesen und bestaetigen: ${prefix}dsgvo` },
      { quoted: m },
    );
    return null;
  }
  return user;
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

  const db = await initDb();

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
    const senderId = m.key.participant || m.key.remoteJid;
    const body = getText(m.message);

    // Prefix pro Chat laden (Default: "-")
    const prefixes = loadPrefixes();
    const prefix = prefixes[chatId] || "-";

    // Command parsen
    const parsed = parseCommand(body, prefix);
    if (!parsed) return;

    const { cmd, args } = parsed;
    log("cmd", `Befehl: ${cmd} | Chat: ${chatId}`);

    const publicCmds = new Set(["help", "register", "dsgvo", "accept"]);

    const user = await getUser(db, chatId);
    const preAccept = await getPreDsgvoAccepted(db, chatId);

    // Nutzung nur wenn registriert + DSGVO bestaetigt
    if (!publicCmds.has(cmd)) {
      if (!user) {
        if (preAccept) {
          await sock.sendMessage(
            chatId,
            { text: `Bitte jetzt registrieren: ${prefix}register <name>` },
            { quoted: m },
          );
        } else {
          await sock.sendMessage(
            chatId,
            { text: `Bitte zuerst DSGVO lesen und bestaetigen: ${prefix}dsgvo` },
            { quoted: m },
          );
        }
        return;
      }
      if (!user.dsgvo_accepted_at) {
        await sock.sendMessage(
          chatId,
          { text: `Bitte zuerst DSGVO lesen und bestaetigen: ${prefix}dsgvo` },
          { quoted: m },
        );
        return;
      }
    }

    // Befehle per switch/case
    switch (cmd) {
      case "ping":
        // Ping-Pong Test: prueft ob Bot reagiert
        await sock.sendMessage(chatId, { text: "pong" }, { quoted: m });
        break;

      case "help":
        // Hilfe-Menue mit allen Befehlen anzeigen
        if (!user && !preAccept) {
          await sock.sendMessage(
            chatId,
            {
              text:
                `CIPHERPHANTOM (vor Registrierung)\n` +
                `${prefix}dsgvo`,
            },
            { quoted: m },
          );
          break;
        }
        if (!user && preAccept) {
          await sock.sendMessage(
            chatId,
            {
              text:
                `CIPHERPHANTOM (naechster Schritt)\n` +
                `${prefix}register <name>`,
            },
            { quoted: m },
          );
          break;
        }
        if (user && !user.dsgvo_accepted_at) {
          await sock.sendMessage(
            chatId,
            {
              text:
                `CIPHERPHANTOM (DSGVO fehlt)\n` +
                `${prefix}dsgvo`,
            },
            { quoted: m },
          );
          break;
        }
        await sock.sendMessage(
          chatId,
          {
            text:
              `CIPHERPHANTOM Befehle (Prefix: ${prefix})\n` +
              `${prefix}help\n` +
              `${prefix}profile\n` +
              `${prefix}name <neuer_name>\n` +
              `${prefix}wallet\n` +
              `${prefix}daily\n` +
              `${prefix}weekly\n` +
              `${prefix}quests <daily|weekly|progress>\n` +
              `${prefix}claim <quest_id>\n` +
              `${prefix}friendcode\n` +
              `${prefix}addfriend <code>\n` +
              `${prefix}friends\n` +
              `${prefix}delete\n` +
              `${prefix}prefix <neues_prefix>\n` +
              `${prefix}ping` +
              (isOwner(senderId)
                ? `\n${prefix}chatid\n${prefix}syncroles\n${prefix}dbdump`
                : ""),
          },
          { quoted: m },
        );
        break;

      case "dsgvo": {
        // DSGVO Kurzinfo anzeigen
        await sock.sendMessage(
          chatId,
          { text: dsgvoText(prefix) },
          { quoted: m },
        );
        break;
      }

      case "accept": {
        // DSGVO bestaetigen
        const acceptedAt = new Date().toISOString();
        if (user?.dsgvo_accepted_at) {
          await sock.sendMessage(
            chatId,
            { text: "DSGVO bereits bestaetigt." },
            { quoted: m },
          );
          break;
        }
        if (user) {
          await setDsgvoAccepted(db, chatId, acceptedAt, DSGVO_VERSION);
        } else {
          if (preAccept) {
            await sock.sendMessage(
              chatId,
              { text: `DSGVO bereits bestaetigt. Jetzt registrieren: ${prefix}register <name>` },
              { quoted: m },
            );
            break;
          }
          await setPreDsgvoAccepted(db, chatId, acceptedAt, DSGVO_VERSION);
        }
        await sock.sendMessage(
          chatId,
          {
            text: user
              ? "Danke! DSGVO bestaetigt. Du kannst den Bot nun nutzen."
              : `Danke! DSGVO bestaetigt. Jetzt registrieren: ${prefix}register <name>`,
          },
          { quoted: m },
        );
        break;
      }

      case "register": {
        // Registrierung: Profil anlegen + Wallet + Freundescode
        const nameFromArgs = args.join(" ").trim();
        const profileName = nameFromArgs || m.pushName || "Spieler";

        let user = await getUser(db, chatId);
        if (user) {
          await sock.sendMessage(
            chatId,
            { text: "Du bist bereits registriert." },
            { quoted: m },
          );
          break;
        }

        const pre = preAccept;
        if (!pre) {
          await sock.sendMessage(
            chatId,
            { text: `Bitte zuerst DSGVO bestaetigen: ${prefix}dsgvo` },
            { quoted: m },
          );
          break;
        }

        // Freundescode sicher einzigartig generieren
        let code = generateFriendCode();
        while (await getFriendByCode(db, code)) code = generateFriendCode();

        const userRole = getUserRole(senderId);
        const levelRole = getLevelRole(1);
        await createUser(db, chatId, profileName, code, userRole, levelRole);
        user = await getUser(db, chatId);
        await setDsgvoAccepted(db, chatId, pre.accepted_at, pre.version);
        await clearPreDsgvoAccepted(db, chatId);

        await sock.sendMessage(
          chatId,
          {
            text:
              `Registrierung erfolgreich!\n` +
              `Profil: ${user.profile_name}\n` +
              `Freundescode: ${user.friend_code}\n` +
              `Wallet: ${user.phn} ${CURRENCY}`,
          },
          { quoted: m },
        );
        break;
      }

      case "profile": {
        // Profil anzeigen (Level, XP, Wallet, Streak)
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const roles = await syncRoles(db, user, senderId);
        await sock.sendMessage(
          chatId,
          {
            text:
              `Profil: ${user.profile_name}\n` +
              `Rolle: ${roles.userRole} | Levelrolle: ${roles.levelRole}\n` +
              `Level: ${user.level} (XP: ${user.xp}, bis Level-Up: ${xpToNextLevel(user.xp)})\n` +
              `Wallet: ${user.phn} ${CURRENCY}\n` +
              `Daily Streak: ${user.daily_streak}\n` +
              `Freundescode: ${user.friend_code}\n` +
              `Erstellt: ${formatDateTime(user.created_at)}`,
          },
          { quoted: m },
        );
        break;
      }

      case "name": {
        // Profilname aendern
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const newName = args.join(" ").trim();
        if (!newName) {
          await sock.sendMessage(
            chatId,
            { text: `Usage: ${prefix}name <neuer_name>` },
            { quoted: m },
          );
          break;
        }
        if (newName.length > 30) {
          await sock.sendMessage(
            chatId,
            { text: "Name zu lang. Maximal 30 Zeichen." },
            { quoted: m },
          );
          break;
        }
        // 7 Tage Cooldown (Owner ausgenommen)
        if (!isOwner(senderId) && user.last_name_change) {
          const last = new Date(user.last_name_change).getTime();
          const now = Date.now();
          const diff = now - last;
          const cooldownMs = 7 * 24 * 60 * 60 * 1000;
          if (diff < cooldownMs) {
            const remaining = cooldownMs - diff;
            const days = Math.ceil(remaining / (24 * 60 * 60 * 1000));
            await sock.sendMessage(
              chatId,
              { text: `Name kann erst in ${days} Tag(en) wieder geaendert werden.` },
              { quoted: m },
            );
            break;
          }
        }

        // Bestaetigungscode aehnlich wie delete
        const token = Math.random().toString(36).slice(2, 8).toUpperCase();
        const expiresAt = Date.now() + 2 * 60 * 1000;
        pendingNameChanges.set(chatId, { token, expiresAt, newName });

        await sock.sendMessage(
          chatId,
          {
            text:
              `Achtung: Nach der Bestaetigung kannst du deinen Namen 7 Tage lang nicht mehr aendern.\n` +
              `Bestaetige mit: ${prefix}confirmname ${token}`,
          },
          { quoted: m },
        );
        break;
      }

      case "confirmname": {
        // Bestaetigung fuer Namensaenderung
        const entry = pendingNameChanges.get(chatId);
        if (!entry || entry.expiresAt < Date.now()) {
          pendingNameChanges.delete(chatId);
          await sock.sendMessage(
            chatId,
            { text: `Kein gueltiger Vorgang. Starte mit ${prefix}name <neuer_name>.` },
            { quoted: m },
          );
          break;
        }
        const token = (args[0] || "").toUpperCase();
        if (!token || token !== entry.token) {
          await sock.sendMessage(
            chatId,
            { text: "Bestaetigungscode ist falsch." },
            { quoted: m },
          );
          break;
        }
        pendingNameChanges.delete(chatId);
        await setProfileName(db, chatId, entry.newName);
        await setNameChange(db, chatId, new Date().toISOString());
        await sock.sendMessage(
          chatId,
          { text: `Name aktualisiert: ${entry.newName}` },
          { quoted: m },
        );
        break;
      }

      case "wallet": {
        // Wallet anzeigen
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await sock.sendMessage(
          chatId,
          { text: `Wallet: ${user.phn} ${CURRENCY} (${CURRENCY_NAME})` },
          { quoted: m },
        );
        break;
      }

      case "daily": {
        // Tagesbonus mit Streak
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await syncRoles(db, user, senderId);
        const today = todayStr();
        if (user.last_daily === today) {
          await sock.sendMessage(
            chatId,
            { text: "Daily Bonus bereits abgeholt. Komm morgen wieder." },
            { quoted: m },
          );
          break;
        }
        const streak =
          user.last_daily === yesterdayStr() ? user.daily_streak + 1 : 1;
        const mult = rewardMultiplier(senderId);
        const reward = (100 + streak * 20) * mult;
        const xpReward = (25 + streak * 5) * mult;

        const newXp = user.xp + xpReward;
        await addBalance(db, chatId, reward);
        await applyXp(db, chatId, user.xp, xpReward);
        await setDaily(db, chatId, today, streak);

        await sock.sendMessage(
          chatId,
          {
            text:
              `Daily Bonus: +${reward} ${CURRENCY}\n` +
              `XP: +${xpReward}\n` +
              `Streak: ${streak}`,
          },
          { quoted: m },
        );
        break;
      }

      case "weekly": {
        // Wochenbonus
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await syncRoles(db, user, senderId);
        const week = isoWeekStr();
        if (user.last_weekly === week) {
          await sock.sendMessage(
            chatId,
            { text: "Weekly Bonus bereits abgeholt. Komm naechste Woche wieder." },
            { quoted: m },
          );
          break;
        }
        const mult = rewardMultiplier(senderId);
        const reward = 500 * mult;
        const xpReward = 200 * mult;
        const newXp = user.xp + xpReward;
        await addBalance(db, chatId, reward);
        await applyXp(db, chatId, user.xp, xpReward);
        await setWeekly(db, chatId, week);

        await sock.sendMessage(
          chatId,
          {
            text:
              `Weekly Bonus: +${reward} ${CURRENCY}\n` +
              `XP: +${xpReward}`,
          },
          { quoted: m },
        );
        break;
      }

      case "quests": {
        // Quests anzeigen (daily/weekly/progress)
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const period = (args[0] || "daily").toLowerCase();
        if (!["daily", "weekly", "progress"].includes(period)) {
          await sock.sendMessage(
            chatId,
            { text: `Unbekannter Typ. Nutze: ${prefix}quests daily|weekly|progress` },
            { quoted: m },
          );
          break;
        }

        const quests = await listQuests(db, period);
        const lines = [];
        for (const q of quests) {
          await ensureUserQuest(db, chatId, q.id);
          let uq = await getUserQuest(db, chatId, q.id);

          // Progress-Quest: Level dynamisch uebernehmen
          if (q.period === "progress" && q.key === "progress_level_5") {
            const progress = user.level;
            if (progress !== uq.progress) {
              await updateQuestProgress(db, chatId, q.id, progress);
              uq = await getUserQuest(db, chatId, q.id);
            }
          }

          // Automatisch abschliessen wenn Ziel erreicht
          if (!uq.completed_at && uq.progress >= q.target) {
            await completeQuest(db, chatId, q.id, new Date().toISOString());
            uq = await getUserQuest(db, chatId, q.id);
          }

          const status = uq.claimed_at
            ? "✅ abgeschlossen"
            : uq.completed_at
            ? "🎁 bereit zum Claim"
            : `⏳ ${uq.progress}/${q.target}`;
          lines.push(
            `#${q.id} ${q.title} | ${status} | +${q.reward_phn} ${CURRENCY}, +${q.reward_xp} XP`,
          );
        }

        await sock.sendMessage(
          chatId,
          {
            text:
              `Quests (${period})\n` +
              lines.join("\n") +
              `\n\nClaim: ${prefix}claim <quest_id>`,
          },
          { quoted: m },
        );
        break;
      }

      case "claim": {
        // Quest-Belohnung abholen
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await syncRoles(db, user, senderId);
        const id = Number(args[0]);
        if (!id) {
          await sock.sendMessage(
            chatId,
            { text: `Usage: ${prefix}claim <quest_id>` },
            { quoted: m },
          );
          break;
        }
        const quests = await listQuests(db, "daily")
          .concat(await listQuests(db, "weekly"))
          .concat(await listQuests(db, "progress"));
        const q = quests.find((x) => x.id === id);
        if (!q) {
          await sock.sendMessage(
            chatId,
            { text: "Quest nicht gefunden." },
            { quoted: m },
          );
          break;
        }
        await ensureUserQuest(db, chatId, q.id);
        const uq = await getUserQuest(db, chatId, q.id);
        if (!uq.completed_at) {
          await sock.sendMessage(
            chatId,
            { text: "Quest noch nicht abgeschlossen." },
            { quoted: m },
          );
          break;
        }
        if (uq.claimed_at) {
          await sock.sendMessage(
            chatId,
            { text: "Quest bereits geclaimt." },
            { quoted: m },
          );
          break;
        }

        const mult = rewardMultiplier(senderId);
        const phnReward = q.reward_phn * mult;
        const xpReward = q.reward_xp * mult;
        await addBalance(db, chatId, phnReward);
        await applyXp(db, chatId, user.xp, xpReward);
        await claimQuest(db, chatId, q.id, new Date().toISOString());

        await sock.sendMessage(
          chatId,
          {
            text:
              `Belohnung erhalten: +${phnReward} ${CURRENCY}, +${xpReward} XP`,
          },
          { quoted: m },
        );
        break;
      }

      case "friendcode": {
        // Eigenen Freundescode anzeigen
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await sock.sendMessage(
          chatId,
          { text: `Dein Freundescode: ${user.friend_code}` },
          { quoted: m },
        );
        break;
      }

      case "addfriend": {
        // Freund per Code hinzufuegen
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const code = (args[0] || "").toUpperCase();
        if (!code) {
          await sock.sendMessage(
            chatId,
            { text: `Usage: ${prefix}addfriend <code>` },
            { quoted: m },
          );
          break;
        }
        const friend = await getFriendByCode(db, code);
        if (!friend) {
          await sock.sendMessage(
            chatId,
            { text: "Freund nicht gefunden." },
            { quoted: m },
          );
          break;
        }
        if (friend.chat_id === chatId) {
          await sock.sendMessage(
            chatId,
            { text: "Du kannst dich nicht selbst adden." },
            { quoted: m },
          );
          break;
        }
        await addFriend(db, chatId, friend.chat_id);
        await addFriend(db, friend.chat_id, chatId);
        await sock.sendMessage(
          chatId,
          { text: `Freund hinzugefuegt: ${friend.profile_name}` },
          { quoted: m },
        );
        break;
      }

      case "friends": {
        // Freundesliste anzeigen
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const friends = await listFriends(db, chatId);
        if (friends.length === 0) {
          await sock.sendMessage(
            chatId,
            { text: "Du hast noch keine Freunde hinzugefuegt." },
            { quoted: m },
          );
          break;
        }
        const lines = friends.map(
          (f) => `- ${f.profile_name} (${f.friend_code})`,
        );
        await sock.sendMessage(
          chatId,
          { text: `Freunde:\n${lines.join("\n")}` },
          { quoted: m },
        );
        break;
      }

      case "syncroles": {
        // Owner: Rollen mit Code-Logik abgleichen
        if (!isOwner(senderId)) {
          await sock.sendMessage(
            chatId,
            { text: "Kein Zugriff." },
            { quoted: m },
          );
          break;
        }
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        const roles = await syncRoles(db, user, senderId);
        await sock.sendMessage(
          chatId,
          { text: `Rollen synchronisiert: ${roles.userRole} / ${roles.levelRole}` },
          { quoted: m },
        );
        break;
      }

      case "chatid": {
        // Chat-ID anzeigen
        if (!isOwner(senderId)) {
          await sock.sendMessage(
            chatId,
            { text: "Kein Zugriff." },
            { quoted: m },
          );
          break;
        }
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await sock.sendMessage(
          chatId,
          { text: `Chat-ID: ${chatId}` },
          { quoted: m },
        );
        break;
      }

      case "dbdump": {
        // Owner: Datenbank-Dump als Textdatei senden
        if (!isOwner(senderId)) {
          await sock.sendMessage(
            chatId,
            { text: "Kein Zugriff." },
            { quoted: m },
          );
          break;
        }
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;
        await sendDbDumpPdf(sock, chatId, db);
        break;
      }

      case "delete": {
        // Account-Loeschung mit Bestaetigungscode
        const user = await requireVerified(
          sock,
          m,
          chatId,
          prefix,
          await getUser(db, chatId),
        );
        if (!user) break;

        const token =
          Math.random().toString(36).slice(2, 8).toUpperCase();
        const expiresAt = Date.now() + 2 * 60 * 1000;
        pendingDeletes.set(chatId, { token, expiresAt });

        await sock.sendMessage(
          chatId,
          {
            text:
              `Achtung: Dieser Vorgang loescht dein Profil dauerhaft.\n` +
              `Bestaetige mit: ${prefix}confirmdelete ${token}\n` +
              `Gueltig fuer 2 Minuten.`,
          },
          { quoted: m },
        );
        break;
      }

      case "confirmdelete": {
        // Account-Loeschung final bestaetigen
        const entry = pendingDeletes.get(chatId);
        if (!entry || entry.expiresAt < Date.now()) {
          pendingDeletes.delete(chatId);
          await sock.sendMessage(
            chatId,
            { text: "Kein gueltiger Loesch-Request. Starte mit -delete." },
            { quoted: m },
          );
          break;
        }
        const token = (args[0] || "").toUpperCase();
        if (!token || token !== entry.token) {
          await sock.sendMessage(
            chatId,
            { text: "Bestaetigungscode ist falsch." },
            { quoted: m },
          );
          break;
        }
        pendingDeletes.delete(chatId);
        await deleteUser(db, chatId);
        await sock.sendMessage(
          chatId,
          {
            text:
              "Dein Account wurde geloescht.\n" +
              "Alle gespeicherten Daten (Profil, Wallet, Quests, Freunde, DSGVO-Zustimmung) wurden entfernt.",
          },
          { quoted: m },
        );
        break;
      }

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

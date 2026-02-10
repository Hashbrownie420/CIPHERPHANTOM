import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.resolve(__dirname, "..", "data");
const DB_FILE = path.join(DATA_DIR, "cipherphantom.db");

export async function initDb() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  const db = await open({
    filename: DB_FILE,
    driver: sqlite3.Database,
  });

  await db.exec("PRAGMA journal_mode = WAL");

  await db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      chat_id TEXT PRIMARY KEY,
      profile_name TEXT NOT NULL,
      friend_code TEXT UNIQUE NOT NULL,
      created_at TEXT NOT NULL,
      xp INTEGER NOT NULL DEFAULT 0,
      level INTEGER NOT NULL DEFAULT 1,
      phn INTEGER NOT NULL DEFAULT 0,
      last_daily TEXT,
      daily_streak INTEGER NOT NULL DEFAULT 0,
      last_weekly TEXT,
      dsgvo_accepted_at TEXT,
      dsgvo_version TEXT,
      user_role TEXT NOT NULL DEFAULT 'user',
      level_role TEXT NOT NULL DEFAULT 'Rookie',
      last_name_change TEXT
    );

    CREATE TABLE IF NOT EXISTS friends (
      user_id TEXT NOT NULL,
      friend_id TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(user_id, friend_id)
    );

    CREATE TABLE IF NOT EXISTS quests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL,
      title TEXT NOT NULL,
      period TEXT NOT NULL,
      target INTEGER NOT NULL,
      reward_phn INTEGER NOT NULL,
      reward_xp INTEGER NOT NULL,
      active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS user_quests (
      user_id TEXT NOT NULL,
      quest_id INTEGER NOT NULL,
      progress INTEGER NOT NULL DEFAULT 0,
      completed_at TEXT,
      claimed_at TEXT,
      UNIQUE(user_id, quest_id)
    );

    CREATE TABLE IF NOT EXISTS dsgvo_accepts (
      chat_id TEXT PRIMARY KEY,
      accepted_at TEXT NOT NULL,
      version TEXT NOT NULL
    );
  `);

  await ensureUserColumns(db);
  await seedQuests(db);
  return db;
}

async function ensureUserColumns(db) {
  const cols = await db.all("PRAGMA table_info(users)");
  const names = new Set(cols.map((c) => c.name));
  if (!names.has("dsgvo_accepted_at")) {
    await db.exec("ALTER TABLE users ADD COLUMN dsgvo_accepted_at TEXT");
  }
  if (!names.has("dsgvo_version")) {
    await db.exec("ALTER TABLE users ADD COLUMN dsgvo_version TEXT");
  }
  if (!names.has("user_role")) {
    await db.exec("ALTER TABLE users ADD COLUMN user_role TEXT NOT NULL DEFAULT 'user'");
  }
  if (!names.has("level_role")) {
    await db.exec("ALTER TABLE users ADD COLUMN level_role TEXT NOT NULL DEFAULT 'Rookie'");
  }
  if (!names.has("last_name_change")) {
    await db.exec("ALTER TABLE users ADD COLUMN last_name_change TEXT");
  }
}

async function seedQuests(db) {
  const row = await db.get("SELECT COUNT(*) as c FROM quests");
  if (row?.c > 0) return;

  const seed = [
    {
      key: "daily_play_3",
      title: "Spiele 3 Runden",
      period: "daily",
      target: 3,
      reward_phn: 120,
      reward_xp: 60,
    },
    {
      key: "daily_win_1",
      title: "Gewinne 1 Runde",
      period: "daily",
      target: 1,
      reward_phn: 90,
      reward_xp: 50,
    },
    {
      key: "daily_duel_1",
      title: "Bestreite 1 Duell",
      period: "daily",
      target: 1,
      reward_phn: 110,
      reward_xp: 60,
    },
    {
      key: "weekly_play_20",
      title: "Spiele 20 Runden",
      period: "weekly",
      target: 20,
      reward_phn: 600,
      reward_xp: 350,
    },
    {
      key: "weekly_win_5",
      title: "Gewinne 5 Runden",
      period: "weekly",
      target: 5,
      reward_phn: 500,
      reward_xp: 300,
    },
    {
      key: "progress_level_5",
      title: "Erreiche Level 5",
      period: "progress",
      target: 5,
      reward_phn: 800,
      reward_xp: 0,
    },
  ];

  await db.exec("BEGIN");
  try {
    for (const q of seed) {
      await db.run(
        "INSERT INTO quests (key, title, period, target, reward_phn, reward_xp) VALUES (?, ?, ?, ?, ?, ?)",
        q.key,
        q.title,
        q.period,
        q.target,
        q.reward_phn,
        q.reward_xp,
      );
    }
    await db.exec("COMMIT");
  } catch (e) {
    await db.exec("ROLLBACK");
    throw e;
  }
}

export async function getUser(db, chatId) {
  return db.get("SELECT * FROM users WHERE chat_id = ?", chatId);
}

export async function createUser(db, chatId, profileName, friendCode, userRole, levelRole) {
  const now = new Date().toISOString();
  await db.run(
    `INSERT INTO users (chat_id, profile_name, friend_code, created_at)
     VALUES (?, ?, ?, ?)`
  , chatId, profileName, friendCode, now);
  await db.run(
    "UPDATE users SET user_role = ?, level_role = ? WHERE chat_id = ?",
    userRole,
    levelRole,
    chatId
  );
}

export async function setProfileName(db, chatId, profileName) {
  await db.run("UPDATE users SET profile_name = ? WHERE chat_id = ?", profileName, chatId);
}

export async function setNameChange(db, chatId, ts) {
  await db.run("UPDATE users SET last_name_change = ? WHERE chat_id = ?", ts, chatId);
}

export async function setBalance(db, chatId, phn) {
  await db.run("UPDATE users SET phn = ? WHERE chat_id = ?", phn, chatId);
}

export async function setUserRole(db, chatId, role) {
  await db.run("UPDATE users SET user_role = ? WHERE chat_id = ?", role, chatId);
}

export async function setLevelRole(db, chatId, role) {
  await db.run("UPDATE users SET level_role = ? WHERE chat_id = ?", role, chatId);
}

export async function addBalance(db, chatId, delta) {
  await db.run("UPDATE users SET phn = phn + ? WHERE chat_id = ?", delta, chatId);
}

export async function addXp(db, chatId, deltaXp, newLevel) {
  await db.run("UPDATE users SET xp = xp + ?, level = ? WHERE chat_id = ?", deltaXp, newLevel, chatId);
}

export async function setDaily(db, chatId, dateStr, streak) {
  await db.run(
    "UPDATE users SET last_daily = ?, daily_streak = ? WHERE chat_id = ?",
    dateStr,
    streak,
    chatId
  );
}

export async function setWeekly(db, chatId, weekStr) {
  await db.run("UPDATE users SET last_weekly = ? WHERE chat_id = ?", weekStr, chatId);
}

export async function setDsgvoAccepted(db, chatId, acceptedAt, version) {
  await db.run(
    "UPDATE users SET dsgvo_accepted_at = ?, dsgvo_version = ? WHERE chat_id = ?",
    acceptedAt,
    version,
    chatId
  );
}

export async function setPreDsgvoAccepted(db, chatId, acceptedAt, version) {
  await db.run(
    "INSERT OR REPLACE INTO dsgvo_accepts (chat_id, accepted_at, version) VALUES (?, ?, ?)",
    chatId,
    acceptedAt,
    version
  );
}

export async function getPreDsgvoAccepted(db, chatId) {
  return db.get("SELECT * FROM dsgvo_accepts WHERE chat_id = ?", chatId);
}

export async function clearPreDsgvoAccepted(db, chatId) {
  await db.run("DELETE FROM dsgvo_accepts WHERE chat_id = ?", chatId);
}

export async function getFriendByCode(db, code) {
  return db.get("SELECT * FROM users WHERE friend_code = ?", code);
}

export async function addFriend(db, userId, friendId) {
  const now = new Date().toISOString();
  await db.run(
    "INSERT OR IGNORE INTO friends (user_id, friend_id, created_at) VALUES (?, ?, ?)",
    userId,
    friendId,
    now
  );
}

export async function listFriends(db, userId) {
  return db.all(
    `SELECT u.chat_id, u.profile_name, u.friend_code
     FROM friends f
     JOIN users u ON u.chat_id = f.friend_id
     WHERE f.user_id = ?`,
    userId
  );
}

export async function listQuests(db, period) {
  return db.all("SELECT * FROM quests WHERE active = 1 AND period = ?", period);
}

export async function ensureUserQuest(db, userId, questId) {
  await db.run(
    "INSERT OR IGNORE INTO user_quests (user_id, quest_id) VALUES (?, ?)",
    userId,
    questId
  );
}

export async function getUserQuest(db, userId, questId) {
  return db.get(
    "SELECT * FROM user_quests WHERE user_id = ? AND quest_id = ?",
    userId,
    questId
  );
}

export async function updateQuestProgress(db, userId, questId, progress) {
  await db.run(
    "UPDATE user_quests SET progress = ? WHERE user_id = ? AND quest_id = ?",
    progress,
    userId,
    questId
  );
}

export async function completeQuest(db, userId, questId, ts) {
  await db.run(
    "UPDATE user_quests SET completed_at = ? WHERE user_id = ? AND quest_id = ?",
    ts,
    userId,
    questId
  );
}

export async function claimQuest(db, userId, questId, ts) {
  await db.run(
    "UPDATE user_quests SET claimed_at = ? WHERE user_id = ? AND quest_id = ?",
    ts,
    userId,
    questId
  );
}

export async function deleteUser(db, chatId) {
  await db.exec("BEGIN");
  try {
    await db.run("DELETE FROM user_quests WHERE user_id = ?", chatId);
    await db.run(
      "DELETE FROM friends WHERE user_id = ? OR friend_id = ?",
      chatId,
      chatId
    );
    await db.run("DELETE FROM users WHERE chat_id = ?", chatId);
    await db.run("DELETE FROM dsgvo_accepts WHERE chat_id = ?", chatId);
    await db.exec("COMMIT");
  } catch (e) {
    await db.exec("ROLLBACK");
    throw e;
  }
}

export async function dumpAll(db) {
  const users = await db.all("SELECT * FROM users");
  const friends = await db.all("SELECT * FROM friends");
  const quests = await db.all("SELECT * FROM quests");
  const userQuests = await db.all("SELECT * FROM user_quests");
  const dsgvoAccepts = await db.all("SELECT * FROM dsgvo_accepts");
  return { users, friends, quests, userQuests, dsgvoAccepts };
}

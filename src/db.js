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
      wallet_address TEXT UNIQUE,
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
      last_name_change TEXT,
      game_daily_date TEXT,
      game_daily_profit INTEGER NOT NULL DEFAULT 0
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

    CREATE TABLE IF NOT EXISTS characters (
      user_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      health INTEGER NOT NULL DEFAULT 100,
      hunger INTEGER NOT NULL DEFAULT 0,
      last_work TEXT,
      last_feed TEXT,
      last_tick TEXT,
      created_at TEXT NOT NULL,
      rename_count INTEGER NOT NULL DEFAULT 0,
      last_maintenance TEXT
    );

    CREATE TABLE IF NOT EXISTS bans (
      user_id TEXT PRIMARY KEY,
      reason TEXT,
      expires_at TEXT,
      created_at TEXT NOT NULL,
      created_by TEXT NOT NULL
    );
  `);

  await ensureUserColumns(db);

  // Ensure unique constraint for upserts
  await db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_quests_key ON quests(key)");
  await db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_wallet ON users(wallet_address)");
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
  if (!names.has("game_daily_date")) {
    await db.exec("ALTER TABLE users ADD COLUMN game_daily_date TEXT");
  }
  if (!names.has("game_daily_profit")) {
    await db.exec("ALTER TABLE users ADD COLUMN game_daily_profit INTEGER NOT NULL DEFAULT 0");
  }
  if (!names.has("wallet_address")) {
    await db.exec("ALTER TABLE users ADD COLUMN wallet_address TEXT");
  }
}

async function seedQuests(db) {
  const quests = [
    {
      key: "daily_work_1",
      title: "Arbeite 1x",
      period: "daily",
      target: 1,
      reward_phn: 120,
      reward_xp: 80,
    },
    {
      key: "daily_feed_1",
      title: "Fuehre 1x Essen/Medizin aus",
      period: "daily",
      target: 1,
      reward_phn: 90,
      reward_xp: 70,
    },
    {
      key: "daily_keep_health",
      title: "Gesundheit >= 80 halten",
      period: "daily",
      target: 1,
      reward_phn: 110,
      reward_xp: 90,
    },
    {
      key: "daily_hunger_low",
      title: "Sattheit unter 30 halten",
      period: "daily",
      target: 1,
      reward_phn: 100,
      reward_xp: 80,
    },
    {
      key: "weekly_work_5",
      title: "Arbeite 5x",
      period: "weekly",
      target: 5,
      reward_phn: 500,
      reward_xp: 300,
    },
    {
      key: "weekly_feed_5",
      title: "Fuehre 5x Essen/Medizin aus",
      period: "weekly",
      target: 5,
      reward_phn: 450,
      reward_xp: 280,
    },
    {
      key: "monthly_work_20",
      title: "Arbeite 20x",
      period: "monthly",
      target: 20,
      reward_phn: 1800,
      reward_xp: 900,
    },
    {
      key: "monthly_feed_20",
      title: "Fuehre 20x Essen/Medizin aus",
      period: "monthly",
      target: 20,
      reward_phn: 1600,
      reward_xp: 850,
    },
    {
      key: "progress_age_30",
      title: "Erreiche Alter 30",
      period: "progress",
      target: 30,
      reward_phn: 900,
      reward_xp: 0,
    },
  ];

  await db.exec("BEGIN");
  try {
    for (const q of quests) {
      await db.run(
        `INSERT INTO quests (key, title, period, target, reward_phn, reward_xp, active)
         VALUES (?, ?, ?, ?, ?, ?, 1)
         ON CONFLICT(key) DO UPDATE SET
           title=excluded.title,
           period=excluded.period,
           target=excluded.target,
           reward_phn=excluded.reward_phn,
           reward_xp=excluded.reward_xp,
           active=1`,
        q.key,
        q.title,
        q.period,
        q.target,
        q.reward_phn,
        q.reward_xp,
      );
    }
    // Deaktiviert alte Quests (Spiel-Quests)
    const keys = quests.map((q) => q.key);
    await db.run(
      `UPDATE quests SET active = 0 WHERE key NOT IN (${keys.map(() => "?").join(",")})`,
      keys,
    );
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

export async function setWalletAddress(db, chatId, walletAddress) {
  await db.run("UPDATE users SET wallet_address = ? WHERE chat_id = ?", walletAddress, chatId);
}

export async function getUserByWalletAddress(db, walletAddress) {
  return db.get("SELECT * FROM users WHERE wallet_address = ?", walletAddress);
}

export async function setNameChange(db, chatId, ts) {
  await db.run("UPDATE users SET last_name_change = ? WHERE chat_id = ?", ts, chatId);
}

export async function setGameDailyProfit(db, chatId, dateStr, profit) {
  await db.run(
    "UPDATE users SET game_daily_date = ?, game_daily_profit = ? WHERE chat_id = ?",
    dateStr,
    profit,
    chatId
  );
}

export async function getCharacter(db, userId) {
  return db.get("SELECT * FROM characters WHERE user_id = ?", userId);
}

export async function createCharacter(db, userId, name) {
  const now = new Date().toISOString();
  await db.run(
    "INSERT INTO characters (user_id, name, created_at, last_tick) VALUES (?, ?, ?, ?)",
    userId,
    name,
    now,
    now
  );
}

export async function updateCharacter(db, userId, fields) {
  const keys = Object.keys(fields);
  if (keys.length === 0) return;
  const sets = keys.map((k) => `${k} = ?`).join(", ");
  const vals = keys.map((k) => fields[k]);
  vals.push(userId);
  await db.run(`UPDATE characters SET ${sets} WHERE user_id = ?`, vals);
}

export async function setBan(db, userId, reason, expiresAt, createdBy) {
  const now = new Date().toISOString();
  await db.run(
    `INSERT INTO bans (user_id, reason, expires_at, created_at, created_by)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(user_id) DO UPDATE SET
       reason = excluded.reason,
       expires_at = excluded.expires_at,
       created_at = excluded.created_at,
       created_by = excluded.created_by`,
    userId,
    reason || null,
    expiresAt || null,
    now,
    createdBy
  );
}

export async function getBan(db, userId) {
  return db.get("SELECT * FROM bans WHERE user_id = ?", userId);
}

export async function clearBan(db, userId) {
  await db.run("DELETE FROM bans WHERE user_id = ?", userId);
}

export async function listBans(db) {
  return db.all("SELECT * FROM bans ORDER BY created_at DESC");
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

export async function getQuestByKey(db, key) {
  return db.get("SELECT * FROM quests WHERE key = ? AND active = 1", key);
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
    await db.run("DELETE FROM characters WHERE user_id = ?", chatId);
    await db.run("DELETE FROM bans WHERE user_id = ?", chatId);
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

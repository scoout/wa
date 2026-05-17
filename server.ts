import "dotenv/config";
import express from "express";
import { createServer as createHttpServer } from "http";
import { Server as SocketServer } from "socket.io";
import { createServer as createViteServer } from "vite";
import path from "path";
import makeWASocket, { 
  DisconnectReason, 
  useMultiFileAuthState, 
  fetchLatestBaileysVersion,
  delay,
  downloadMediaMessage
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import axios from "axios";
import { parsePhoneNumberFromString } from "libphonenumber-js";
import { fileTypeFromBuffer } from "file-type";
import QRCode from "qrcode";
import fs from "fs";
import NodeCache from "node-cache";
import { google } from "googleapis";
import { GoogleGenAI } from "@google/genai";
import cron from "node-cron";
import stringSimilarity from "string-similarity";
import multer from "multer";
import { initializeApp, getApps, getApp, cert } from "firebase-admin/app";
import { getFirestore, Firestore } from "firebase-admin/firestore";
import { MongoClient, Db } from "mongodb";

// SET TIMEZONE TO JAKARTA
process.env.TZ = "Asia/Jakarta";
console.log(`>>> [SERVER] SYSTEM TIMEZONE SET TO: ${process.env.TZ}`);
console.log(`>>> [SERVER] CURRENT TIME: ${new Date().toLocaleString("id-ID")}`);

console.log(">>> [SERVER] BOOTING UP...");

// Load Firebase Config safely
const getFirebaseConfig = () => {
    try {
        if (fs.existsSync("./firebase-applet-config.json")) {
            return JSON.parse(fs.readFileSync("./firebase-applet-config.json", "utf-8"));
        }
    } catch (err) {
        console.warn("Could not load firebase-applet-config.json");
    }
    return {};
};

const firebaseConfig = getFirebaseConfig();

// Initialize Firebase Admin Lazily
let db_cloud: Firestore | null = null;
let isCloudHealthy = true;
let lastCloudErrorTime = 0;
let rootSock: any = null;
let currentConfig: any = {
    adminNumber: "6285771373003",
    targetGroupId: "120363344994495614@g.us"
};
const AUTH_COLLECTION = "bot_sessions"; 
const sheetCache = new NodeCache({ stdTTL: 300, checkperiod: 60 }); 

function initFirebase() {
    // If it was unhealthy, retry after 1 minute to avoid spamming
    if (!isCloudHealthy && Date.now() - lastCloudErrorTime > 1 * 60 * 1000) {
        console.log(">>> [RELIABILITY] Retrying Firebase initialization...");
        isCloudHealthy = true;
    }

    if (db_cloud && isCloudHealthy) return db_cloud;
    if (!isCloudHealthy) return null;
    
    let app;
    const apps = getApps();
    const serviceAccountPath = "./service_account.json";
    
    if (apps.length === 0) {
        let projId = firebaseConfig.projectId;
        if (!projId || projId === "REPLACE_WITH_YOUR_PROJECT_ID") {
            projId = "laporan-pembangkit"; // Explicit fallback from user
        }
        
        if (!projId) {
            console.warn(">>> [FIREBASE] CRITICAL: No projectId in firebase-applet-config.json!");
            isCloudHealthy = false;
            lastCloudErrorTime = Date.now();
            return null;
        }

        try {
            let options: any = { projectId: projId };
            
            if (fs.existsSync(serviceAccountPath)) {
                try {
                    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf-8"));
                    options.credential = cert(serviceAccount);
                    options.projectId = serviceAccount.project_id || projId;
                    console.log(`>>> [FIREBASE] AUTH: Using service_account.json (Project: ${options.projectId}, Client Email: ${serviceAccount.client_email})`);
                } catch (e) {
                    console.error(">>> [FIREBASE] AUTH ERROR: Failed to parse service_account.json:", e);
                }
            } else {
                console.log(">>> [FIREBASE] AUTH: No service_account.json found. Using Default Cloud Identity (ADC).");
                console.log(">>> [FIREBASE] AUTH: If you are connecting to an EXTERNAL project (like 'laporan-pembangkit'),");
                console.log(">>> [FIREBASE] AUTH: you MUST upload 'service_account.json' to the root of this bot.");
            }

            app = initializeApp(options);
            console.log(`>>> [FIREBASE] App Initialized for Project: ${options.projectId}`);
        } catch (err: any) {
            console.error(">>> [FIREBASE] initializeApp FAILED:", err.message);
            isCloudHealthy = false;
            lastCloudErrorTime = Date.now();
            return null;
        }
    } else {
        app = apps[0];
    }

    try {
        let dbId = firebaseConfig.firestoreDatabaseId;
        if (!dbId || dbId === "(default)") {
            dbId = "ai-studio-cdd026c2-f5f4-4060-982b-e4a082579f98"; // Explicit fallback from user
        }
        
        console.log(`>>> [FIREBASE] Connecting to Firestore Database ID: "${dbId || "(default)"}"`);
        
        if (dbId && dbId !== "(default)") {
            // In firebase-admin, getFirestore(app, databaseId) is the way to specify a database
            db_cloud = getFirestore(app, dbId);
        } else {
            db_cloud = getFirestore(app);
        }
        
        // Test connection with a dummy read on the correct collection
        console.log(`>>> [FIREBASE] Testing connection to ${dbId || "(default)"} [Collection: ${AUTH_COLLECTION}]...`);
        db_cloud.collection(AUTH_COLLECTION).doc("connection_test").get()
            .then(() => {
                console.log(">>> [FIREBASE] Connection TEST SUCCESSFUL! Cloud Sync is ACTIVE.");
                isCloudHealthy = true;
            })
            .catch((err: any) => {
                console.error(">>> [FIREBASE] Connection TEST FAILED:", err.message);
                if (err.message.includes("RESOURCE_EXHAUSTED")) {
                    isCloudHealthy = false;
                    console.warn(">>> [FIREBASE] Quota Exceeded during test. Falling back to local-only mode.");
                } else if (err.message.includes("PERMISSION_DENIED")) {
                    isCloudHealthy = false;
                    console.error(">>> [FIREBASE] CRITICAL: Permission Denied on database read test.");
                    console.error(`>>> [FIREBASE] PROJECT: ${app.options.projectId}, DATABASE: ${dbId}`);
                    console.error(">>> [FIREBASE] HINT: Check if Service Account has 'Cloud Datastore User' role on THIS specific database.");
                }
            });
            
    } catch (err: any) {
        console.error(">>> [FIREBASE] Firestore Setup Error:", err.message);
        isCloudHealthy = false;
        lastCloudErrorTime = Date.now();
        return null;
    }
    
    return db_cloud;
}

// --- MONGO DB SETUP ---
let mongo_db: Db | null = null;
let isMongoHealthy = false;
let lastMongoFailTime = 0;
const MONGO_AUTH_COLLECTION = "wa_bot_sessions";

async function initMongo() {
    const uri = process.env.MONGODB_URI;
    if (!uri) return null;
    if (mongo_db && isMongoHealthy) return mongo_db;
    
    // Skip if failed recently (1 minute cooldown)
    if (Date.now() - lastMongoFailTime < 1 * 60 * 1000) {
        return null;
    }

    try {
        console.log(">>> [MONGO] Connecting to MongoDB Atlas...");
        const client = new MongoClient(uri, {
            serverSelectionTimeoutMS: 15000, 
            socketTimeoutMS: 20000, 
            maxPoolSize: 5,
            family: 4, 
            tls: true,
            retryWrites: true,
            connectTimeoutMS: 15000 
        });
        await client.connect();
        
        const dbName = uri.split("/").pop()?.split("?")[0] || "whatsapp_bot";
        mongo_db = client.db(dbName);
        
        await mongo_db.command({ ping: 1 });
        console.log(`>>> [MONGO] Connection SUCCESSFUL! Database: "${dbName}"`);
        isMongoHealthy = true;
        return mongo_db;
    } catch (err: any) {
        console.error(">>> [MONGO] Connection FAILED:", err.message);
        isMongoHealthy = false;
        lastMongoFailTime = Date.now();
        return null;
    }
}

async function useMongoAuthState() {
    const db = await initMongo();
    if (!db || !isMongoHealthy) return null;

    const collection = db.collection(MONGO_AUTH_COLLECTION);
    const { initAuthCreds, BufferJSON } = await import("@whiskeysockets/baileys");

    const readData = async (id: string) => {
        try {
            const data = await collection.findOne({ _id: id as any });
            if (data && data.data) {
                return JSON.parse(data.data as string, BufferJSON.reviver);
            }
        } catch (e) {
            console.error(`[MONGO] Read Error [${id}]:`, e);
        }
        return null;
    };

    const writeData = async (data: any, id: string) => {
        try {
            const json = JSON.stringify(data, BufferJSON.replacer);
            await collection.updateOne(
                { _id: id as any },
                { $set: { data: json, updatedAt: new Date() } },
                { upsert: true }
            );
        } catch (e) {
            console.error(`[MONGO] Write Error [${id}]:`, e);
        }
    };

    const removeData = async (id: string) => {
        try {
            await collection.deleteOne({ _id: id as any });
        } catch (e) {
            console.error(`[MONGO] Delete Error [${id}]:`, e);
        }
    };

    const creds = await readData("creds") || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data: any = {};
                    const mongoIds = ids.map(id => `${type}-${id}`);
                    try {
                        const results = await collection.find({ _id: { $in: mongoIds as any } }).toArray();
                        results.forEach(res => {
                            const id = res._id.toString().split("-").slice(1).join("-");
                            data[id] = JSON.parse(res.data as string, BufferJSON.reviver);
                        });
                    } catch (e) {
                        console.error(`[MONGO] Keys Get Error [${type}]:`, e);
                    }
                    return data;
                },
                set: async (data: any) => {
                    const ops = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const mongoId = `${category}-${id}`;
                            if (value) {
                                const json = JSON.stringify(value, BufferJSON.replacer);
                                ops.push({
                                    updateOne: {
                                        filter: { _id: mongoId as any },
                                        update: { $set: { data: json, updatedAt: new Date() } },
                                        upsert: true
                                    }
                                });
                            } else {
                                ops.push({
                                    deleteOne: {
                                        filter: { _id: mongoId as any }
                                    }
                                });
                            }
                        }
                    }
                    if (ops.length > 0) {
                        try {
                            await collection.bulkWrite(ops);
                        } catch (e) {
                            console.error(`[MONGO] Bulk Write Error:`, e);
                        }
                    }
                }
            }
        },
        saveCreds: async () => {
            await writeData(creds, "creds");
        }
    };
}

let lastActivityTime = Date.now();
let isConnecting = false;

// Global logging helper
let dashboardLog: (msg: string) => void = (msg: string) => console.log(msg);
function addLog(msg: string) {
    lastActivityTime = Date.now();
    dashboardLog(msg);
}

// Helper for Firestore State - STRICT CLOUD PERSISTENCE
async function useFirestoreAuthState() {
    const db = initFirebase();
    
    if (!db || !isCloudHealthy) {
        addLog("⚠️ [AUTH] Cloud Database failure. Fallback to local (Non-Persistent on dynamic server).");
        // We still return something to avoid crash, but it won't persist across restarts if DB is down
        return useMultiFileAuthState("auth_info_baileys");
    }

    const credsDoc = db.collection(AUTH_COLLECTION).doc("creds");
    const keysCollection = db.collection(AUTH_COLLECTION).doc("keys").collection("data");

    const { initAuthCreds, BufferJSON } = await import("@whiskeysockets/baileys");

    const readData = async (key: string, retryCount = 0): Promise<any> => {
        try {
            if (!isCloudHealthy) return null;
            const fetchPromise = key === 'creds' ? credsDoc.get() : keysCollection.doc(key).get();
            
            const doc = await Promise.race([
                fetchPromise,
                new Promise((_, reject) => setTimeout(() => reject(new Error("TIMEOUT")), 10000))
            ]) as any;

            if (!doc.exists) return null;
            const data = doc.data()?.data;
            if (!data) return null;

            return JSON.parse(data, BufferJSON.reviver);
        } catch (e: any) {
            if (e.message && e.message.includes("RESOURCE_EXHAUSTED")) {
                isCloudHealthy = false;
                addLog("🛑 [FIREBASE] Quota Exceeded. Switching to LOCAL-ONLY mode.");
                return null;
            }
            if ((e.message === "TIMEOUT" || e.message === "BATCH_TIMEOUT") && retryCount < 1) {
                addLog(`🔄 [FIREBASE] Retrying read for [${key}] after timeout...`);
                return readData(key, retryCount + 1);
            }
            console.error(`Firebase Read Error [${key}]:`, e.message);
            if (e.message === "TIMEOUT" || e.message === "BATCH_TIMEOUT") {
                addLog(`⚠️ [FIREBASE] Read Timeout for [${key}] after retry. Check network or Cloud Region.`);
            }
            if (e.message && e.message.includes("PERMISSION_DENIED")) {
                isCloudHealthy = false;
                addLog("❌ [FIREBASE] PERMISSION_DENIED.");
            }
            return null;
        }
    };

    const writeData = async (data: any, key: string) => {
        try {
            if (!isCloudHealthy) return;
            const json = JSON.stringify(data, BufferJSON.replacer);
            
            if (key === 'creds') {
                await credsDoc.set({ data: json });
            } else {
                await keysCollection.doc(key).set({ data: json });
            }
        } catch (e: any) {
            if (e.message && e.message.includes("RESOURCE_EXHAUSTED")) {
                isCloudHealthy = false;
                addLog("🛑 [FIREBASE] Quota Exceeded during write. Disabling Cloud storage.");
                return;
            }
            console.error(`Firebase Write Error [${key}]:`, e.message);
        }
    };

    const removeData = async (key: string) => {
        try {
            if (!isCloudHealthy) return;
            if (key === 'creds') {
                await credsDoc.delete();
            } else {
                await keysCollection.doc(key).delete();
            }
        } catch (e) {
            console.error(`Firebase Delete Error [${key}]:`, e);
        }
    };
    
    // Initial Load
    const creds = await readData("creds") || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data: any = {};
                    if (!isCloudHealthy || !ids || ids.length === 0) return data;

                    const performGet = async (attempt = 0): Promise<any> => {
                        try {
                            const docRefs = ids.map(id => keysCollection.doc(`${type}-${id}`));
                            const snaps = await Promise.race([
                                db.getAll(...docRefs),
                                new Promise<any>((_, reject) => setTimeout(() => reject(new Error("BATCH_TIMEOUT")), 15000))
                            ]);
                            return snaps;
                        } catch (e: any) {
                            if (e.message === "BATCH_TIMEOUT" && attempt < 1) {
                                addLog(`🔄 [FIREBASE] Retrying batch read for [${type}]...`);
                                return performGet(attempt + 1);
                            }
                            throw e;
                        }
                    };

                    try {
                        const snaps = await performGet();
                        
                        snaps.forEach((doc: any, index: number) => {
                            if (doc.exists) {
                                const id = ids[index];
                                try {
                                    const rawData = doc.data()?.data;
                                    if (rawData) {
                                        data[id] = JSON.parse(rawData, BufferJSON.reviver);
                                    }
                                } catch (e: any) {
                                    console.error(`Parse Error for ${type}-${id}:`, e.message);
                                }
                            }
                        });
                    } catch (e: any) {
                        console.error(`Firebase Batch Read Error [${type}]:`, e.message);
                    }
                    return data;
                },
                set: async (data: any) => {
                    if (!isCloudHealthy) return;
                    let batch = db.batch();
                    let count = 0;
                    
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            const docRef = (category === 'creds' && id === 'creds') ? credsDoc : keysCollection.doc(key);
                            
                            if (value) {
                                const json = JSON.stringify(value, BufferJSON.replacer);
                                batch.set(docRef, { data: json });
                            } else {
                                batch.delete(docRef);
                            }
                            count++;
                            
                            if (count >= 400) { // Safety margin
                                await batch.commit();
                                batch = db.batch();
                                count = 0;
                            }
                        }
                    }
                    if (count > 0) await batch.commit();
                }
            }
        },
        saveCreds: () => writeData(creds, "creds")
    };
}

// Initialize Pino with pretty logging for dev
const logger = pino({
    level: "info",
    transport: {
        target: "pino-pretty",
        options: {
            colorize: true,
            translateTime: "HH:MM:ss Z",
            ignore: "pid,hostname"
        }
    }
});

// Helper for phone formatting
function formatJid(number: string) {
    if (number.includes("@")) return number;
    const parsed = parsePhoneNumberFromString(number, "ID");
    if (parsed) return `${parsed.countryCallingCode}${parsed.nationalNumber}@s.whatsapp.net`;
    return `${number.replace(/\D/g, "")}@s.whatsapp.net`;
}
const getAi = () => {
    let key = process.env.GEMINI_API_KEY;
    if (!key) {
        console.warn("GEMINI_API_KEY is missing. AI features will be disabled.");
        return null;
    }
    // Trim to avoid issues with copy-pasted keys containing whitespace
    key = key.trim();
    if (key.startsWith('"') && key.endsWith('"')) {
        key = key.slice(1, -1);
    }
    console.log(`>>> [AI] Gemini API Key found (Length: ${key.length})`);
    return new GoogleGenAI({ apiKey: key });
};
const ai = getAi();
const PORT = 3000;

// Load Mapping dari mapping.json
let ROW_MAP: { [key: string]: number } = {};
function refreshMapping() {
  try {
    const mappingData = fs.readFileSync("mapping.json", "utf-8");
    ROW_MAP = JSON.parse(mappingData);
    console.log("Mapping successfully loaded from mapping.json");
    return true;
  } catch (err) {
    console.error("Failed to load mapping.json:", err);
    return false;
  }
}
refreshMapping();

async function getAIResponse(prompt: string) {
  if (!ai) return "Maaf, fitur AI sedang tidak aktif (API Key belum diset).";
  try {
    const response = await ai.models.generateContent({
      model: "gemini-3-flash-preview",
      contents: prompt,
      config: {
          systemInstruction: "Anda adalah asisten bot WhatsApp untuk monitoring pembangkit listrik. Bantu menjawab pertanyaan operator dengan ramah dan sopan. Gunakan Bahasa Indonesia. Anda bisa membantu menjelaskan data teknis, cara kerja bot, atau hanya sekedar mengobrol santai."
      }
    });
    return response.text;
  } catch (err: any) {
    const errMsg = err.message || String(err);
    console.error("AI Error:", errMsg);
    if (errMsg.includes("API key not valid") || errMsg.includes("400") || errMsg.includes("invalid")) {
        addLog("❌ [AI] Error: API Key Gemini TIDAK VALID atau Gagal Otentikasi.");
        return "🤖 *Maaf Pak, fitur AI sedang tidak stabil (Masalah API Key).* \n\nSilakan gunakan perintah manual seperti *menu* atau *ceklist* untuk saat ini. Bapak juga bisa periksa setelan Secret di AI Studio.";
    }
    return "Maaf, terjadi kesalahan saat menghubungi AI. Silakan coba lagi nanti.";
  }
}

// Google Sheets Setup
const auth = new google.auth.GoogleAuth({
  keyFile: "service_account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

async function updateSheetCell(spreadsheetId: string, day: number, column: string, row: number, value: string) {
  const range = `${day}!${column}${row}`;
  const cacheKey = `cell-${spreadsheetId}-${day}-${column}${row}`;
  
  try {
    if (!spreadsheetId) throw new Error("Spreadsheet ID is missing");
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: "USER_ENTERED",
      requestBody: {
        values: [[value]],
      },
    });
    
    // Update cache immediately on write
    sheetCache.set(cacheKey, value);

    console.log(`[SHEETS] Success: Updated ${range} in ${spreadsheetId} with value ${value}`);
    addLog(`✅ [SHEETS] Data disimpan ke Sheet ${day}, Kolom ${column}, Baris ${row}: ${value}`);
    return true;
  } catch (err: any) {
    const errMsg = err.message || String(err);
    console.error(`[SHEETS] Error updating sheet at ${range}:`, errMsg);
    addLog(`❌ [SHEETS] Gagal simpan ke Sheet: ${errMsg}`);
    
    if (errMsg.toLowerCase().includes("permission") || errMsg.includes("403")) {
        addLog("⚠️ [SHEETS] Error: Akses Ditolak (403). Pastikan email bot sudah di-share ke Sheet.");
        // Kirim notifikasi ke admin atau grup jika gagal karena izin
        notifyAdminSheetsFailure(spreadsheetId);
    }
    return false;
  }
}

let lastFailureNotify = 0;
async function notifyAdminSheetsFailure(sheetId: string) {
    const now = Date.now();
    if (now - lastFailureNotify < 60 * 60 * 1000) return; // Maks 1x per jam
    lastFailureNotify = now;
    
    try {
        const sa = JSON.parse(fs.readFileSync("service_account.json", "utf-8"));
        const email = sa.client_email;
        const msg = `⚠️ *[PERINGATAN BOT]*\n\nBot gagal menulis ke Google Sheet karena *Akses Ditolak (403)*.\n\n*Penyebab:* Email bot belum di-share ke Google Sheet Anda.\n*Solusi:* Silakan Share/Bagikan Sheet Anda ke email ini sebagai *Editor*:\n\n\`${email}\`\n\nSheet ID: \`${sheetId}\``;
        
        // Kirim ke admin number dari config
        const adminJid = `${currentConfig.adminNumber}@s.whatsapp.net`;
        if (rootSock) {
            await rootSock.sendMessage(adminJid, { text: msg });
        }
    } catch (e) {}
}

async function getCellValue(sheetId: string, sheetTitle: string, cellRange: string): Promise<string | null> {
  const cacheKey = `cell-${sheetId}-${sheetTitle}-${cellRange}`;
  const cached = sheetCache.get(cacheKey);
  if (cached !== undefined) return cached as string;

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${sheetTitle}!${cellRange}`,
    });
    const values = response.data.values;
    const result = (values && values[0][0]) || null;
    if (result !== null) sheetCache.set(cacheKey, result);
    return result;
  } catch (err) {
    return null;
  }
}

function getPreviousColumnName(currentCol: string, day: number): string | null {
  const cols = day === 1 ? ["D", "E", "F", "G", "H"] : ["D", "E", "F", "G"];
  const idx = cols.indexOf(currentCol);
  if (idx > 0) return cols[idx - 1];
  return null;
}

// --- SMART PARSER HELPERS ---

function getJakartaTime() {
  // Since process.env.TZ is set to Asia/Jakarta, new Date() is already Jakarta time
  return new Date();
}

function getEffectiveTarget(timeStr: string): { day: number, column: string, finalTime: string } {
  const nowIdx = getJakartaTime();
  let targetDay = nowIdx.getDate();
  const hours = nowIdx.getHours();
  const minutes = nowIdx.getMinutes();
  const totalMin = hours * 60 + minutes;

  let selectedTime = timeStr;
  
  // LOGIKA AUTO: Berdasarkan Jendela Waktu Operasional
  if (timeStr.toLowerCase() === "auto") {
    // 21:30 ke atas (1290 min) sampai 03:59 pagi (239 min) dianggap Shift 24:00 (H-1)
    if (totalMin >= 1290 || totalMin < 240) {
      if (totalMin < 240) {
        const yesterday = new Date(nowIdx.getTime() - 86400000);
        targetDay = yesterday.getDate();
      }
      selectedTime = "24.00";
    } else if (totalMin >= 240 && totalMin < 600) {
      // 04:00 - 09:59
      // Fitur: Khusus tgl 1, mulai jam 09:35 bisa isi kolom 10:00
      if (targetDay === 1 && totalMin >= 575) {
        selectedTime = "10.00";
      } else {
        selectedTime = "06.00";
      }
    } else if (totalMin >= 600 && totalMin < 1050) {
       // 10:00 - 17:29
       // Jika tanggal 1 dan jam sebelum jam 12:00, prioritaskan 10:00
       selectedTime = (targetDay === 1 && totalMin < 720) ? "10.00" : "14.00";
    } else {
       // 17:30 - 21:29 -> Shift 21:00
       selectedTime = "21.00";
    }
  } else {
    // LOGIKA MANUAL: Jika user ketik jam 24:00 di jam 01:00 pagi (besoknya)
    if ((timeStr.includes("24") || timeStr.includes("00")) && hours < 6) {
       const yesterday = new Date(nowIdx.getTime() - 86400000);
       targetDay = yesterday.getDate();
    }
  }

  // Cari Kolom
  const column = getColumnForTime(targetDay, selectedTime) || "D";
  return { day: targetDay, column, finalTime: selectedTime };
}

function getColumnForTime(day: number, time: string): string | null {
  const t = time.replace(".", ":");
  if (day === 1) {
    if (t.includes("06")) return "D";
    if (t.includes("10")) return "E";
    if (t.includes("14")) return "F";
    if (t.includes("21")) return "G";
    if (t.includes("24") || t.includes("00")) return "H";
  } else {
    if (t.includes("06")) return "D";
    if (t.includes("14")) return "E";
    if (t.includes("21")) return "F";
    if (t.includes("24") || t.includes("00")) return "G";
  }
  return null;
}

async function startServer() {
  const app = express();
  const httpServer = createHttpServer(app);

  app.use(express.json());

  // Health check instant
  app.get("/api/health", (req, res) => res.json({ status: "alive" }));

  // Serve uploads folder
  const uploadsPath = path.join(process.cwd(), "uploads");
  if (!fs.existsSync(uploadsPath)) {
    fs.mkdirSync(uploadsPath, { recursive: true });
  }
  app.use("/uploads", express.static(uploadsPath));

  // --- VAULT MANAGEMENT ---
  const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, 'uploads/');
    },
    filename: (req, file, cb) => {
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
      const ext = path.extname(file.originalname);
      cb(null, `${file.fieldname}-${uniqueSuffix}${ext}`);
    }
  });
  const upload = multer({ storage });

  app.get("/api/vault", async (req, res) => {
    if (!db_cloud) initFirebase();
    if (!db_cloud) return res.status(500).json({ error: "Firebase not initialized" });
    try {
      const snapshot = await db_cloud.collection("vault").orderBy("timestamp", "desc").limit(100).get();
      const items = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
      res.json(items);
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.post("/api/vault/upload", upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: "No file uploaded" });
    if (!db_cloud) initFirebase();
    if (!db_cloud) return res.status(500).json({ error: "Firebase not initialized" });

    try {
      const newItem = {
        filename: req.file.originalname,
        path: req.file.filename,
        type: req.file.mimetype,
        timestamp: new Date(),
        sender: "Dashboard Upload",
        size: req.file.size
      };

      const docRef = await db_cloud.collection("vault").add(newItem);
      res.json({ id: docRef.id, ...newItem });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.patch("/api/vault/:id", async (req, res) => {
    const { id } = req.params;
    const { filename } = req.body;
    if (!filename) return res.status(400).json({ error: "Filename is required" });
    if (!db_cloud) initFirebase();
    if (!db_cloud) return res.status(500).json({ error: "Firebase not initialized" });

    try {
      await db_cloud.collection("vault").doc(id).update({ filename });
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.delete("/api/vault/:id", async (req, res) => {
    const { id } = req.params;
    if (!db_cloud) initFirebase();
    if (!db_cloud) return res.status(500).json({ error: "Firebase not initialized" });

    try {
      const doc = await db_cloud.collection("vault").doc(id).get();
      if (!doc.exists) return res.status(404).json({ error: "Item not found" });
      
      const data = doc.data();
      if (data?.path) {
        const filePath = path.join(process.cwd(), "uploads", data.path);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        }
      }

      await db_cloud.collection("vault").doc(id).delete();
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.get("/api/mapping", (req, res) => {
    try {
      const mappingData = fs.readFileSync("mapping.json", "utf-8");
      res.json(JSON.parse(mappingData));
    } catch (err) {
      res.status(500).json({ error: "Failed to read mapping.json" });
    }
  });

  app.get("/api/session-status", async (req, res) => {
    const status: any = {
      mongo: { healthy: isMongoHealthy, hasSession: false },
      firestore: { healthy: isCloudHealthy, hasSession: false }
    };

    try {
      if (isMongoHealthy && mongo_db) {
        const collection = mongo_db.collection(MONGO_AUTH_COLLECTION);
        const creds = await collection.findOne({ _id: "creds" as any });
        status.mongo.hasSession = !!creds;
      }
      
      if (isCloudHealthy && db_cloud) {
        const doc = await db_cloud.collection(AUTH_COLLECTION).doc("creds").get();
        status.firestore.hasSession = doc.exists;
      }
    } catch (e: any) {
      status.error = e.message;
    }

    res.json(status);
  });

  app.post("/api/mapping", (req, res) => {
    try {
      const newMapping = req.body;
      if (typeof newMapping !== 'object') {
        return res.status(400).json({ error: "Invalid mapping data" });
      }
      fs.writeFileSync("mapping.json", JSON.stringify(newMapping, null, 2));
      refreshMapping();
      io.emit("bot:mapping", newMapping);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Failed to save mapping.json" });
    }
  });
  
  const io = new SocketServer(httpServer, {
    cors: { origin: "*" }
  });

  let sock: any = null;
  let qrCode: string | null = null;
  let connectionStatus: "connecting" | "open" | "close" | "qr" | "initializing" | "error" = "initializing";
  let consecutive428Count = 0;
  let statusMessage = "Server booting up...";

  const updateBotStatus = (status: typeof connectionStatus, msg?: string) => {
    connectionStatus = status;
    if (msg) statusMessage = msg;
    io.emit("bot:status", connectionStatus);
    io.emit("bot:statusMessage", statusMessage);
    if (msg) addLog(`[STATUS] ${status.toUpperCase()}: ${msg}`);
  };
  const CONFIG_FILE = path.join(process.cwd(), "config.json");
  let targetSheetId: string = "1UUczN7BKH9Vecq8QjTx25fs1mdkl11PtHTdPE8es_n8";
  let targetGroupId: string = "120363344994495614@g.us";
  let adminNumber: string = "6285771373003";
  let botNumber: string = "6282337726122";
  let adminLid: string = "105931123757067";
  const KEGIATAN_GROUP_ID = "120363351907221345@g.us";
  const ACTIVITIES_FILE = "activities.json";
  const STATUS_FILE = "status.json";

function getStatus(): Record<string, string> {
    try {
      if (fs.existsSync(STATUS_FILE)) {
        return JSON.parse(fs.readFileSync(STATUS_FILE, "utf-8"));
      }
    } catch (e) {
      console.error("Error reading status:", e);
    }
    return {};
  }

  function updateStatus(updates: Record<string, string>) {
    try {
      const status = getStatus();
      const newStatus = { ...status, ...updates };
      fs.writeFileSync(STATUS_FILE, JSON.stringify(newStatus, null, 2));
    } catch (e) {
      console.error("Error updating status:", e);
    }
  }

  interface Activity {
    time: string;
    text: string;
  }

  function getActivities(): Activity[] {
    try {
      if (fs.existsSync(ACTIVITIES_FILE)) {
        return JSON.parse(fs.readFileSync(ACTIVITIES_FILE, "utf-8"));
      }
    } catch (e) {
      console.error("Error reading activities:", e);
    }
    return [];
  }

  function saveActivity(text: string, time?: string) {
    try {
      const activities = getActivities();
      const activityTime = time || getJakartaTime().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', hour12: false }).replace(':', '.');
      activities.push({ time: activityTime, text });
      fs.writeFileSync(ACTIVITIES_FILE, JSON.stringify(activities, null, 2));
    } catch (e) {
      console.error("Error saving activity:", e);
    }
  }

  function clearActivities() {
    try {
      fs.writeFileSync(ACTIVITIES_FILE, JSON.stringify([], null, 2));
    } catch (e) {
      console.error("Error clearing activities:", e);
    }
  }

  async function generateReportText(timeStr: string) {
    try {
      const now = getJakartaTime();
      const target = getEffectiveTarget(timeStr);
      const day = target.day;
      const col = target.column;
      const sheetTitle = day.toString();

      addLog(`⚙️ Menghasilkan draf laporan untuk Shift ${timeStr}...`);

      const status = getStatus();

      // Helper untuk ambil status dengan toleransi nama
      const getStatusVal = (key: string) => {
        if (status[key]) return status[key];
        const lowerKey = key.toLowerCase();
        const foundKey = Object.keys(status).find(k => {
           const lk = k.toLowerCase();
           return lk.includes(lowerKey) || lowerKey.includes(lk);
        });
        return foundKey ? status[foundKey] : "-";
      };

      // Fetch data from Spreadsheet
      const getVal = async (row: number) => {
        const val = await getCellValue(targetSheetId, sheetTitle, `${col}${row}`);
        return (val || "-").trim();
      };

      // Helper UNTUK MENGHITUNG DELTA (SELISIH)
      const getDelta = async (row: number) => {
        const currValStr = await getVal(row);
        if (currValStr === "-") return "-";
        
        let prevValStr = "-";
        const colsArr = (day === 1 ? ["D", "E", "F", "G", "H"] : ["D", "E", "F", "G"]);
        const idx = colsArr.indexOf(col);
        
        if (idx > 0) {
          const prevCol = colsArr[idx - 1];
          prevValStr = await getCellValue(targetSheetId, sheetTitle, `${prevCol}${row}`);
        } else {
          const reportDate = new Date(now.getFullYear(), now.getMonth(), day);
          const yesterdayDate = new Date(reportDate.getTime() - 86400000);
          const yesterdayTitle = yesterdayDate.getDate().toString();
          const isYesterdayFirstOfMonth = yesterdayDate.getDate() === 1;
          const prevCol = isYesterdayFirstOfMonth ? "H" : "G";
          prevValStr = await getCellValue(targetSheetId, yesterdayTitle, `${prevCol}${row}`);
        }

        if (!prevValStr || prevValStr === "-" || prevValStr.trim() === "") {
            return "-"; 
        }

        const currClean = currValStr.replace(/,/g, "").split("/")[0];
        const prevClean = prevValStr.replace(/,/g, "").split("/")[0];
        const curr = parseFloat(currClean);
        const prev = parseFloat(prevClean);

        if (isNaN(curr) || isNaN(prev)) return currValStr;
        const delta = curr - prev;
        if (delta < 0) return currValStr;
        return delta.toFixed(1);
      };

      // Data Mapping
      const gas11Mw = await getDelta(7);    
      const gas11Val = await getVal(17);  
      const gas12Mw = await getDelta(8);    
      const gas12Val = await getVal(18);  
      const gas13Mw = await getDelta(9);    
      const gas13Val = await getVal(19);  
      const stgMw = await getDelta(10);     

      const pheVol = await getVal(21); 
      const pheEner = await getVal(22);
      const nrVol = await getVal(23); 
      const nrEner = await getVal(24); 
      const pgnVol = await getVal(25); 
      const pgnEner = await getVal(26); 

      const hw = await getDelta(37);

      const hsdt1 = await getVal(45);     
      const hsdt2 = await getVal(46);     
      const swt = await getVal(47);
      const rwt = await getVal(48);
      const mut = await getVal(49);

      const rawActivities = getActivities();
      const uniqueActivities = rawActivities.filter((activity, index, self) =>
        index === self.findIndex((t) => {
          const normA = activity.time.replace(":", ".");
          const normB = t.time.replace(":", ".");
          return normA === normB && t.text.trim() === activity.text.trim();
        })
      );
      const sortedActivities = uniqueActivities.sort((a, b) => {
        const normA = a.time.replace(":", ".");
        const normB = b.time.replace(":", ".");
        return normA.localeCompare(normB);
      });

      const activitiesStr = sortedActivities.length > 0 
        ? sortedActivities.map((a) => `${a.time.replace(":", ".")}   ${a.text}`).join("\n")
        : "Nihil";

      const monthNames = ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"];
      const dayNames = ["Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"];
      const reportDateObj = new Date(now.getFullYear(), now.getMonth(), day);
      const dateText = `${dayNames[reportDateObj.getDay()]}, ${day} ${monthNames[reportDateObj.getMonth()]} ${reportDateObj.getFullYear()}`;

      const totalLoad = (parseFloat(gas11Mw) || 0) + (parseFloat(gas12Mw) || 0) + (parseFloat(stgMw) || 0);

      const report = `*LAPORAN OPERASI*
*PLTGU Blok 1 UP Muara Karang*
*${dateText} – Pukul ${timeStr} WIB*
============================= 
BLOK-1 Operasi : *2 – 2 – 1*
LFC : OFF
Total Beban : *${totalLoad.toFixed(1)} MW, - MVAR*
NPHR Blok 1 : *- Kcal/kWh*
IBT1 : - MW; IBT2 : - MW
Total IBT : *- MW*
Navitas : 100 % Recording : 100 %
============================= 
GTG#1.1 : ${gas11Mw} MW, - MVAR, Gas: ${gas11Val} MMBTU, DP: - mmH2O
GTG#1.2 : ${gas12Mw} MW, - MVAR, Gas: ${gas12Val} MMBTU, DP: - mmH2O
GTG#1.3 : ${gas13Mw === "-" ? "STOP / F01 / RS /" : gas13Mw + " MW, - MVAR"}
STG#1.0 : ${stgMw} MW, - MVAR
Condensor Vacuum : *- mmHg*
Temperatur Exhaust : -°C 
Air Penambah Hotwell : *${hw} m³*
============================= 
*Auxiliary :*
Desal Plant A : Stop S/B 
Desal Plant B : //
>>Destilate : *- µs/cm(dump) ; - ton/hr*
>>Condensate: - µs/cm ; - ton/hr*
Demin MBX-1 :// *- µs/cm*
Demin MBX-2 : Stop S/B
Hypo Plant A : Stop S/B
Hypo Plant B : //*${getStatusVal('hypo')} Amp*
H2 Plant 1A : // *${getStatusVal('h2')} bar*
H2 Plant 1B : Stop TS/B 
D/G Emergency I : Stop S/B
D/G Emergency II : Stop S/B
Diesel Fire Pump : Stop S/B
============================= 
*Peralatan Abnormal / Tidak Standby :*
1. -
============================= 
*Data Pemakaian Gas :*
PHE Volume : ${pheVol} mmSCF 
PHE Energi : ${pheEner} mmBTU 
Press / Flow : - Psi / - mmBTU 

NR Volume : ${nrVol} mmSCF 
NR Energi : ${nrEner} mmBTU 
Press / Flow : - Psi / - mmBTU 

PGN Volume : ${pgnVol} mmSCF 
PGN Energi : ${pgnEner} mmBTU 
Press / Flow : - Psi / - mmBTU 
============================= 
*Level HSD :*
HSDT-1 : ${hsdt1} mm 
HSDT-2 : ${hsdt2} mm
*Level Air :*
SWT : ${swt} mm 
RWT : ${rwt} mm 
MUT : ${mut} mm
=============================
*Kegiatan :*
${activitiesStr}
============================= 
*Botol H2 (isi / Kosong)*
GTG 1.1 : - / -
GTG 1.2 : - / - 
STG 1.0 : - / - 
*Botol CO2 (isi/Kosong/Terpasang)*
GTG 1.1 : - / - / - 
GTG 1.2 : - / - / - 
GTG 1.3 : - / - / - 
STG 1.0 : - / - / - 
============================= 
*CWP : 1A*
Pressure : ${getStatusVal('cwp_press')} Bar
Arus : - Ampere
Vibrasi : ${getStatusVal('cwp_vib_v')} / ${getStatusVal('cwp_vib_h')} mm/s
Bypass (U/S) : -% / -% 
Inlet Condensor (T/B) : ${getStatusVal('inlet_t')} bar / ${getStatusVal('inlet_b')} bar
DP Debris Filter (T/B) : ${getStatusVal('debris_t')} / ${getStatusVal('debris_b')} mbar
Level Siphon : - cm
=============================
*Regu C*`;

      return report;
    } catch (e) {
      console.error("Error generating report text:", e);
      return null;
    }
  }

  async function generateAndSendReport(timeStr: string, attempt = 1) {
    try {
      addLog(`⏰ Menjalankan Laporan Otomatis untuk Shift ${timeStr} (Attempt ${attempt})...`);
      const report = await generateReportText(timeStr);
      
      if (!report) {
         addLog(`⚠️ [REPORT] No data to report for shift ${timeStr}.`);
         return;
      }

      if (sock && connectionStatus === "open") {
        await sock.sendMessage(KEGIATAN_GROUP_ID, { text: report });
        addLog(`✅ Laporan Otomatis jam ${timeStr} dikirim ke grup.`);
      } else {
        addLog(`❌ [REPORT] WebSocket DOWN. Cannot send report ${timeStr}. Status: ${connectionStatus}`);
        if (attempt < 3) {
          addLog("🔄 [SELF-HEALING] Reconnecting first before retry...");
          connectToWhatsApp();
          setTimeout(() => generateAndSendReport(timeStr, attempt + 1), 15000);
        }
      }
    } catch (e) {
      console.error("Error in generateAndSendReport:", e);
      if (attempt < 3) {
        addLog(`🔄 [REPORT] Error occurred, retrying in 10s... (${e instanceof Error ? e.message : String(e)})`);
        setTimeout(() => generateAndSendReport(timeStr, attempt + 1), 10000);
      }
    }
  }

  // Load Persisted Config
  function loadConfig() {
    try {
      if (fs.existsSync(CONFIG_FILE)) {
        const data = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf-8"));
        targetSheetId = data.targetSheetId || targetSheetId;
        targetGroupId = data.targetGroupId || targetGroupId;
        adminNumber = data.adminNumber || adminNumber;
        botNumber = data.botNumber || botNumber;
        adminLid = data.adminLid || adminLid;
        currentConfig = { targetSheetId, targetGroupId, adminNumber, botNumber, adminLid };
        console.log("Config loaded from file");
      }
    } catch (e) {
      console.log("Error loading config:", e);
    }
  }

  function saveConfig() {
    try {
      fs.writeFileSync(CONFIG_FILE, JSON.stringify({ targetSheetId, targetGroupId, adminNumber, adminLid }, null, 2));
    } catch (e) {
      console.log("Error saving config:", e);
    }
  }

  loadConfig();

  // HEARBEAT / KEEP-ALIVE (Pings itself every 5 minutes to prevent sleep)
  const keepAlive = () => {
    setInterval(async () => {
      try {
        // Use 127.0.0.1 to avoid potential IPv6 or networking issues with "localhost"
        const response = await fetch(`http://127.0.0.1:${PORT}/api/ping`);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(`HTTP ${response.status}: ${text.substring(0, 100)}`);
        }
        const data = await response.json();
        const now = new Date().toLocaleString("id-ID", { timeZone: "Asia/Jakarta" });
        console.log(`[STABILITY] Heartbeat success at ${now}:`, data.status);
      } catch (err: any) {
        console.warn(`[STABILITY] Heartbeat failed:`, err.message || String(err));
      }
    }, 5 * 60 * 1000); // 5 minutes
  };
  keepAlive();

  app.get("/api/api-status", (req, res) => {
    try {
        const sa = JSON.parse(fs.readFileSync("service_account.json", "utf-8"));
        res.json({
            gemini: !!process.env.GEMINI_API_KEY,
            spreadsheetId: targetSheetId,
            serviceAccountEmail: sa.client_email,
            cloudHealthy: isCloudHealthy,
            whatsappStatus: connectionStatus
        });
    } catch (e) {
        res.status(500).json({ error: "Failed to read service account" });
    }
  });

  app.get("/api/firebase-status", (req, res) => {
    res.json({
        healthy: isCloudHealthy,
        projectId: firebaseConfig.projectId,
        databaseId: firebaseConfig.firestoreDatabaseId || "(default)",
        lastErrorTime: lastCloudErrorTime,
        timestamp: new Date().toISOString()
    });
  });

  app.get("/api/health", (req, res) => {
    const memory = process.memoryUsage();
    res.json({ 
      status: connectionStatus,
      uptime: process.uptime(),
      memory: {
        heapUsed: Math.round(memory.heapUsed / 1024 / 1024) + "MB",
        rss: Math.round(memory.rss / 1024 / 1024) + "MB"
      },
      lastActivity: new Date(lastActivityTime).toISOString(),
      selfHealing: "active"
    });
  });

  app.get("/api/ping", (req, res) => {
    res.json({ status: "alive", time: new Date().toISOString() });
  });

  dashboardLog = (msg: string) => {
    console.log(msg);
    io.emit("bot:log", msg);
  };

  /**
   * Fuzzy Matching: Mencari alias terdekat dari ROW_MAP jika input tidak pas.
   * Sekarang juga mempertimbangkan nilai (historical value) untuk disambiguasi.
   */
  function findBestAlias(input: string, providedValue?: string): { alias: string, row: number, score: number } | null {
      const aliases = Object.keys(ROW_MAP);
      if (aliases.length === 0) return null;
      
      const matches = stringSimilarity.findBestMatch(input.toLowerCase(), aliases);
      
      // Ambil top 5 candidate yang ratingnya lumayan (> 0.45)
      const topMatches = matches.ratings
          .filter(r => r.rating > 0.45)
          .sort((a, b) => b.rating - a.rating)
          .slice(0, 5);

      if (topMatches.length === 0) return null;

      // Jika ada value, coba validasi dengan history (Memory Match)
      if (providedValue) {
          const valNum = parseFloat(providedValue.replace(/,/g, ""));
          if (!isNaN(valNum)) {
              const status = getStatus();
              let bestByValue = null;
              let minDiff = Infinity;

              for (const match of topMatches) {
                  const lastVal = status[match.target];
                  if (lastVal) {
                      const lastNum = parseFloat(String(lastVal).replace(/,/g, ""));
                      // Jika lastNum adalah 0 atau string "0", kita skip atau perlakukan beda
                      if (lastNum > 0) {
                          const diff = Math.abs(valNum - lastNum) / lastNum;
                          if (diff < minDiff) {
                              minDiff = diff;
                              bestByValue = match;
                          }
                      }
                  }
              }

              // Jika ada yang selisihnya sangat kecil (< 10%) DAN alias match score-nya masih masuk akal
              // Kita ambil itu sebagai pemenang walaupun bukan top alias score.
              if (bestByValue && minDiff < 0.10 && bestByValue.rating > 0.45) {
                   addLog(`✨ Disambiguation: Historical value match found for [${input}] -> [${bestByValue.target}] (Diff: ${(minDiff*100).toFixed(1)}%)`);
                   return {
                       alias: bestByValue.target,
                       row: ROW_MAP[bestByValue.target],
                       score: bestByValue.rating
                   };
              }
          }
      }

      // Default: Ambil best match dari string similarity jika rating cukup tinggi
      const bestMatch = topMatches[0];
      if (bestMatch.rating > 0.6) {
          return {
              alias: bestMatch.target,
              row: ROW_MAP[bestMatch.target],
              score: bestMatch.rating
          };
      }
      return null;
  }

  /**
   * Contextual Inference: Menebak kategori berdasarkan pola angka DAN data terakhir (Memory/Sheet).
   */
  function inferCategoryFromValue(valueStr: string): { category: string, possibleAliases: string[] } | null {
      const cleanValue = valueStr.replace(/,/g, "").split("/")[0];
      const val = parseFloat(cleanValue);
      if (isNaN(val)) return null;

      // --- STEP 1: Memory-Based Check (Melihat data terakhir di status.json) ---
      // status.json biasanya paling update karena disync dari WA dan Sheet
      const status = getStatus();
      let memoryMatches: { alias: string, diff: number }[] = [];
      
      Object.entries(status).forEach(([alias, lastVal]) => {
          const lastNum = parseFloat(String(lastVal).replace(/,/g, ""));
          if (!isNaN(lastNum) && lastNum > 0) {
              const diffPercent = Math.abs(val - lastNum) / lastNum;
              // Jika selisihnya di bawah 15%, ini kandidat kuat
              if (diffPercent < 0.15) {
                  memoryMatches.push({ alias, diff: diffPercent });
              }
          }
      });

      if (memoryMatches.length > 0) {
          memoryMatches.sort((a, b) => a.diff - b.diff);
          const topMatches = memoryMatches.map(m => m.alias.toUpperCase());
          return {
              category: "Berdasarkan Data Terakhir (History)",
              possibleAliases: topMatches
          };
      }

      // --- STEP 2: General Range Check (Jika tidak ada di memory) ---
      if (val > 100000) {
          return { 
              category: "MWh (Energy Production)", 
              possibleAliases: ["G1", "G2", "G3", "STG", "MWH1", "MWH2"] 
          };
      }
      
      if (val >= 20 && val <= 1000) {
          return { 
              category: "Gas / MW Load / Technical Data", 
              possibleAliases: ["GAS1", "GAS2", "GAS3", "LOAD"] 
          };
      }

      if (val >= 0 && val < 20) {
          return { 
              category: "Level (HSD / Hotwell / Tank)", 
              possibleAliases: ["HSDT1", "HSDT2", "HW", "DA", "CB"] 
          };
      }

      return null;
  }

  /**
   * Sync data dari Google Sheet ke status.json lokal.
   * Berguna saat bot restart atau user libur lama agar bot tahu angka terakhir di spreadsheet.
   */
  async function syncStatusFromSheet() {
      addLog("🔄 [SYNC] Memulai sinkronisasi data dari Google Sheet...");
      try {
          const now = getJakartaTime();
          const daysToTry = [now.getDate()]; 
          const yesterday = new Date(now);
          yesterday.setDate(now.getDate() - 1);
          daysToTry.push(yesterday.getDate());

          const sheets = google.sheets({ version: "v4", auth });
          const allUpdates: Record<string, string> = {};

          for (const day of daysToTry) {
              const sheetTitle = day.toString();
              try {
                  const response = await sheets.spreadsheets.values.get({
                      spreadsheetId: targetSheetId,
                      range: `${sheetTitle}!D1:H500`,
                  });

                  const rows = response.data.values;
                  if (!rows || rows.length === 0) continue;

                  Object.entries(ROW_MAP).forEach(([alias, rowNum]) => {
                      const rowIndex = rowNum - 1;
                      if (rows[rowIndex]) {
                          const colData = rows[rowIndex].slice(3, 8); 
                          for (let i = colData.length - 1; i >= 0; i--) {
                              if (colData[i] && colData[i].trim() !== "") {
                                  if (!allUpdates[alias]) {
                                      allUpdates[alias] = colData[i].trim();
                                  }
                                  break; 
                              }
                          }
                      }
                  });
              } catch (sheetErr) {
              }
          }

          if (Object.keys(allUpdates).length > 0) {
              updateStatus(allUpdates);
              addLog(`✅ [SYNC] Berhasil sinkronisasi ${Object.keys(allUpdates).length} data dari Sheet.`);
          } else {
              addLog("ℹ️ [SYNC] Tidak ada data baru yang ditemukan di Sheet.");
          }
      } catch (err: any) {
          addLog(`❌ [SYNC] Gagal: ${err.message}`);
      }
  }

  // --- SELF-HEALING & PERSISTENCE CONTROLLER ---
  // Memastikan bot tetap jalan dan responsif 24/7
  let selfHealingInterval: NodeJS.Timeout | null = null;

  const startSelfHealing = () => {
    if (selfHealingInterval) clearInterval(selfHealingInterval);
    
    selfHealingInterval = setInterval(async () => {
      const now = Date.now();
      
      // 1. Cek Aktivitas: Jika tidak ada log baru dalam 30 menit dan status 'open'
      const inactiveLimit = 45 * 60 * 1000; // Increased to 45m for Cloud Run stability
      const isStale = (now - lastActivityTime > inactiveLimit);

      if (isStale && connectionStatus === "open") {
        addLog("⚙️ [SELF-HEALING] Connection seems stale (no activity). Refreshing...");
        if (sock) {
          try {
            sock.end(new Error("Stale connection restart"));
            sock = null;
          } catch (e) {}
        }
        isConnecting = false;
        setTimeout(() => connectToWhatsApp(), 5000);
        return;
      }

      // 2. Anti-Stuck Initialization: Jika status initializing/connecting lebih dari 5 menit
      const initLimit = 5 * 60 * 1000;
      const isInitStuck = (now - lastActivityTime > initLimit) && 
                          (connectionStatus === "initializing" || connectionStatus === "connecting");
      
      if (isInitStuck) {
        addLog("⚙️ [SELF-HEALING] Bot seems stuck in initialization. Resetting lock...");
        isConnecting = false;
        setTimeout(() => connectToWhatsApp(), 2000);
      }

      // 4. Persistent Offline Recovery: Jika status 'close' tapi sistem tidak sedang mencoba koneksi
      if (connectionStatus === "close" && !isConnecting) {
          addLog("⚙️ [SELF-HEALING] Bot detected as OFFLINE & IDLE. Forcing reconnection protocol...");
          // Reset status to allow reconnect
          isConnecting = false; 
          setTimeout(() => connectToWhatsApp(), 5000);
      }

      // 5. Hourly Active Log: Memberi tanda bot masih hidup setiap jam
      const oneHour = 60 * 60 * 1000;
      if (! (global as any).lastHourlyHeartbeat || (now - (global as any).lastHourlyHeartbeat > oneHour)) {
          (global as any).lastHourlyHeartbeat = now;
          addLog("💓 [SYSTEM] 24/7 Engine Heartbeat: ACTIVE - Protocol running normally.");
      }
      
      // 5. Daily Sync: Jalankan sinkron otomatis setiap ganti hari (sekitar tengah malam)
      const currentDay = new Date().getDate();
      if (! (global as any).lastSyncDay) (global as any).lastSyncDay = currentDay;
      
      if (currentDay !== (global as any).lastSyncDay) {
          addLog(`📅 [SYSTEM] Day changed (${(global as any).lastSyncDay} -> ${currentDay}). Running Auto-Sync...`);
          (global as any).lastSyncDay = currentDay;
          syncStatusFromSheet().catch(e => addLog(`⚠️ Auto-sync failed: ${e.message}`));
      }
      
      // Heartbeat: Emit status ke frontend secara paksa
      io.emit("bot:status", connectionStatus);
    }, 2 * 60 * 1000); // Cek setiap 2 menit (lebih agresif)
  };

  // Override addLog untuk track aktivitas
  const originalAddLog = addLog;
  const enhancedAddLog = (msg: string) => {
    lastActivityTime = Date.now();
    originalAddLog(msg);
  };
  (global as any).addLog = enhancedAddLog;

// --- FUNGSI PEMBERSIH SESI KORUP ---
async function clearCloudSession() {
    try {
        // Clear Mongo if active
        if (process.env.MONGODB_URI) {
            try {
                const db = await initMongo();
                if (db) {
                    addLog("🧹 [AUTH] Cleaning session data in MongoDB...");
                    await db.collection(MONGO_AUTH_COLLECTION).deleteMany({});
                    addLog("✅ MongoDB session cleared.");
                }
            } catch (e: any) {
                addLog(`⚠️ Mongo cleanup error: ${e.message}`);
            }
        }

        if (!db_cloud || !isCloudHealthy) {
            addLog("⚠️ [AUTH] Cloud Firestore not healthy or not configured, skipping Firestore cleanup...");
            return;
        }
        
        addLog("🧹 [AUTH] Membersihkan data sesi di Firestore (Chunked)...");
        
        // Hapus Creds
        const credsDoc = db_cloud.collection(AUTH_COLLECTION).doc("creds");
        await credsDoc.delete();
        
        // Hapus semua Keys (Subcollection data) dengan chunking
        const keysRef = db_cloud.collection(AUTH_COLLECTION).doc("keys").collection("data");
        const snaps = await keysRef.get();
        const docs = snaps.docs;
        
        const chunkSize = 400;
        for (let i = 0; i < docs.length; i += chunkSize) {
            const batch = db_cloud.batch();
            const chunk = docs.slice(i, i + chunkSize);
            chunk.forEach(doc => batch.delete(doc.ref));
            await batch.commit();
            addLog(`🗑️ Deleted chunk ${Math.floor(i/chunkSize) + 1}/${Math.ceil(docs.length/chunkSize)}`);
        }
        
        addLog("✅ Cloud session cleared successfully.");
    } catch (e: any) {
        addLog(`⚠️ Cloud cleanup error: ${e.message}`);
    }
}

  async function useCloudAuthState() {
    // Try Mongo first if configured
    if (process.env.MONGODB_URI) {
        try {
            const mongoResult = await useMongoAuthState();
            if (mongoResult) {
                addLog("✅ [AUTH] Using MongoDB Atlas for persistent storage.");
                return mongoResult;
            }
        } catch (e: any) {
            addLog(`⚠️ [AUTH] MongoDB failure: ${e.message}. Falling back to Firestore...`);
        }
    }

    // Fallback to Firestore
    return useFirestoreAuthState();
}

async function connectToWhatsApp() {
    if (isConnecting) {
      addLog("⚠️ Connection already in progress. Skipping redundant request.");
      return;
    }
    isConnecting = true;
    
    try {
      updateBotStatus("initializing", "Initializing Auth Engine...");
      addLog(">>> [ENGINE] Step 1: Loading Auth State...");
      const { state, saveCreds } = await useCloudAuthState();
      const msgRetryCounterCache = new NodeCache();
      
      updateBotStatus("initializing", "Checking Baileys Version...");
      addLog(">>> [ENGINE] Step 2: Fetching Baileys Version...");
      let version: any = [2, 3100, 0]; // Default version
      try {
        const latest = await Promise.race([
            fetchLatestBaileysVersion(),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout version fetch")), 10000))
        ]).catch(() => null);

        if (latest && (latest as any).version) {
          version = (latest as any).version;
        }
      } catch (e) {
         console.warn("Failed to fetch Baileys version, using fallback.");
      }

      addLog(`🚀 [ENGINE] Step 3: Starting WhatsApp Core v${version.join(".")}...`);

      // Pastikan sock dibersihkan jika ada sisa koneksi
      if (sock) {
        try {
          sock.ev.removeAllListeners();
          sock.end();
        } catch (e) {}
      }

      sock = makeWASocket({
        version,
        auth: state,
        logger,
        printQRInTerminal: false, 
        browser: ["macOS", "Chrome", "118.0.0.0"],
        mobile: false,
        syncFullHistory: false,
        connectTimeoutMs: 30000, // Reduced from 120s
        defaultQueryTimeoutMs: 30000, // Reduced from 60s
        keepAliveIntervalMs: 15000, 
        retryRequestDelayMs: 2000,
        maxRetries: 5,
        linkPreview: false, // Faster sending
        shouldSyncHistoryMessage: () => false, 
        msgRetryCounterCache, 
        getMessage: async (key) => {
            // Decryption recovery helper
            return undefined;
        }
      });

      // RESOURCE MONITORING & KEEP-ALIVE
      setInterval(() => {
        const memory = process.memoryUsage();
        const used = (memory.heapUsed / 1024 / 1024).toFixed(2);
        const total = (memory.heapTotal / 1024 / 1024).toFixed(2);
        const rss = (memory.rss / 1024 / 1024).toFixed(2);
        
        console.log(`[MONITOR] Status: ${connectionStatus} | RAM: ${used}MB used / ${total}MB total (RSS: ${rss}MB)`);
        
        // PING: Prevent Cloud Run instance sleep if there is an active session
        if (connectionStatus === "open" && sock) {
            sock.sendPresenceUpdate("available");
        }
      }, 5 * 60 * 1000); // Every 5 minutes

      sock.ev.on("creds.update", saveCreds);

      sock.ev.on("connection.update", async (update: any) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
          qrCode = await QRCode.toDataURL(qr);
          connectionStatus = "qr";
          io.emit("bot:status", "qr");
          io.emit("bot:qr", qrCode);
          addLog("New QR Code generated");
        }

        if (connection === "close") {
          isConnecting = false;
          const error = lastDisconnect?.error as Boom;
          const reason = error?.output?.statusCode;
          qrCode = null;
          updateBotStatus("close", `Disconnected (${reason})`);

          addLog(`Connection closed. Reason: ${reason}. Message: ${error?.message}`);

          const isQRTimeout = error?.message?.includes("QR refs attempts ended");
          const isPairingFailed = error?.message?.includes("Pairing with number failed") || error?.message?.includes("Gagal menautkan");
          const isSyncOrPatchError = error?.message?.includes("failed to sync state") || error?.message?.includes("decode patch") || error?.message?.includes("failed to find key");
          
          if (isQRTimeout || isSyncOrPatchError) {
              consecutive428Count++; // Reuse counter
              const errType = isQRTimeout ? "QR Timeout" : "Sync/Patch Corruption";
              addLog(`⚠️ [RELIABILITY] ${errType} detected [Attempt ${consecutive428Count}/2]`);
              if (consecutive428Count >= 2 || isSyncOrPatchError) {
                  addLog("🚨 [CRITICAL] Persistent Error or Session Corruption. Clearing session to force fresh state...");
                  await clearCloudSession();
                  consecutive428Count = 0;
              }
          }
          
          // SPECIFIC CLOUD RUN RECOVERY LOGIC
          const isTimeout = reason === 408 || reason === DisconnectReason.timedOut;
          const isPrecondition = reason === 428;
          const isServiceUnavailable = reason === 503;
          const isTerminated = error?.message?.includes("Connection Terminated") || error?.message?.includes("Connection Closed");
          
          const shouldReconnect = reason !== DisconnectReason.loggedOut && !isPairingFailed;
          
            if (isPairingFailed || reason === DisconnectReason.loggedOut) {
              addLog(`❌ [AUTH] Terminal Error (Reason: ${reason}). Auto-Cleaning session...`);
              await clearCloudSession();
              // Restart process on terminal failure to ensure fresh state
              setTimeout(() => {
                  addLog("♻️ [SYSTEM] Restarting process for fresh auth state...");
                  process.exit(1); 
              }, 3000);
          }

          if (shouldReconnect) {
             const isStreamError = reason === 515 || reason === DisconnectReason.connectionLost || reason === DisconnectReason.connectionClosed || reason === 440 || isTimeout || isPrecondition || isServiceUnavailable || isTerminated;
             let delayMs = (reason === DisconnectReason.restartRequired || isStreamError || isQRTimeout) ? 5000 : 10000;
             
             if (isQRTimeout) {
               addLog("⚠️ [RELIABILITY] QR Timeout. Forced engine restart in 5s...");
               delayMs = 5000;
             } else if (isTimeout) {
               addLog("📡 [RELIABILITY] Network Timeout (408). Reconnecting in 5s...");
               delayMs = 5000;
             } else if (isPrecondition) {
                consecutive428Count++;
                addLog(`🔒 [RELIABILITY] Precondition Failed (428) [Attempt ${consecutive428Count}/3]. Retrying...`);
                
                if (consecutive428Count >= 3) {
                    addLog("🚨 [CRITICAL] 428 Error loop detected. Session may be corrupted. Cleaning Cloud Session...");
                    await clearCloudSession();
                    consecutive428Count = 0;
                    delayMs = 2000; 
                }
             } else if (isServiceUnavailable) {
               addLog("🏢 [RELIABILITY] WhatsApp Service Unavailable (503). Retrying in 15s...");
               delayMs = 15000;
             }

             addLog(`🔄 [PERSISTENCE] Standalone Reconnecting in ${delayMs/1000}s... (Code: ${reason})`);
             
             if (sock) {
                 try {
                     sock.ev.removeAllListeners();
                     sock.end();
                     sock = null;
                 } catch (e) {}
             }
             
             setTimeout(() => connectToWhatsApp(), delayMs);
          } else {
             addLog("❌ [AUTH] Bot Logged Out. Cleaning Cloud Session...");
             if (db_cloud && isCloudHealthy) {
                 try {
                     const batch = db_cloud.batch();
                     const snaps = await db_cloud.collection(AUTH_COLLECTION).doc("keys").collection("data").get();
                     snaps.forEach(doc => batch.delete(doc.ref));
                     batch.delete(db_cloud.collection(AUTH_COLLECTION).doc("creds"));
                     await batch.commit();
                     addLog("🧹 Session cleared. Bot will require fresh QR scan.");
                 } catch (e: any) {
                     addLog(`⚠️ Cloud cleanup error: ${e.message}`);
                 }
             }
             if (fs.existsSync("auth_info_baileys")) {
               fs.rmSync("auth_info_baileys", { recursive: true, force: true });
             }
             setTimeout(() => connectToWhatsApp(), 5000);
          }
        } else if (connection === "connecting") {
           updateBotStatus("connecting", "Establishing secure link...");
        } else if (connection === "open") {
          isConnecting = false;
          consecutive428Count = 0; // Reset on success
          updateBotStatus("open", "Connected to WhatsApp");
          rootSock = sock; // Update global reference
          addLog("✅ Gateway Protocol: OPTIMAL - Secure link established.");
          qrCode = null;
          syncStatusFromSheet().catch(err => addLog(`⚠️ [SYNC] Background sync failed: ${err.message}`));
        }
      });
    } catch (err: any) {
      isConnecting = false;
      addLog(`❌ [CRITICAL] Connection Setup Failed: ${err.message}`);
      updateBotStatus("error", `Engine failure: ${err.message}`);
      // Retry after some time if it crashed during setup
      setTimeout(() => connectToWhatsApp(), 30000);
    }

    startSelfHealing();

    sock.ev.on("messages.upsert", async (m: any) => {
      try {
        if (m.type === "notify") {
          for (const msg of m.messages) {
            try {
              if (!msg.message) continue;

              const rawJid = msg.key.remoteJid;
              const message = msg.message;
              let text = message?.conversation || 
                           message?.extendedTextMessage?.text || 
                           message?.imageMessage?.caption || 
                           message?.videoMessage?.caption || 
                           message?.documentMessage?.caption || 
                           message?.buttonsResponseMessage?.selectedButtonId || 
                           message?.listResponseMessage?.singleSelectReply?.selectedRowId || "";
              
              const isQuoted = !!message?.extendedTextMessage?.contextInfo?.quotedMessage;
              const quotedMsg = message?.extendedTextMessage?.contextInfo?.quotedMessage;
              const messageType = Object.keys(message || {})[0];

              if (!rawJid) continue;
              
              // Skip self messages or status messages
              if (rawJid === "status@broadcast") continue;

              // Immediate reaction to show the bot is working (Responsiveness)
              if (!isQuoted && !text.startsWith("!") && text.length > 2) {
                  await sock.sendMessage(rawJid, { react: { text: "⏳", key: msg.key } }).catch(() => {});
              }

              addLog(`📢 [MSG] Incoming from ${rawJid} (${messageType}): ${text.slice(0, 100)}`);

            const isFromMe = !!msg.key.fromMe;
            const senderJid = msg.key.remoteJid;
            if (!senderJid) continue;
            
            const userNumber = senderJid.split("@")[0].split(":")[0] || "";
            const cleanUser = userNumber.replace(/[^0-9]/g, "");
            const cleanAdmin = (adminNumber || "").replace(/[^0-9]/g, "");
            
            const isFromAdmin = (cleanUser && cleanAdmin && cleanUser.length >= 10 && cleanAdmin.length >= 10 && 
                               (cleanUser.endsWith(cleanAdmin.slice(-10)) || cleanAdmin.endsWith(cleanUser.slice(-10)))) ||
                               (cleanUser === adminLid) || isFromMe; // Admin if it's from me too

            const isPrivateChat = !senderJid.includes("@g.us");
            const isTargetGroup = (senderJid === targetGroupId);
            const isActivityGroup = (senderJid === KEGIATAN_GROUP_ID);
            const isAllowedAdminPrivate = (isPrivateChat && isFromAdmin);
            const lowerText = text.toLowerCase().trim();

            // --- FITUR LAPOR OPERASIONAL (MW, MVAR, FREK) ---
            if (lowerText.startsWith("!lapor")) {
                const parts = lowerText.split(/\s+/);
                if (parts.length >= 5) {
                    const unit = parts[1].toUpperCase();
                    const mw = parseFloat(parts[2]);
                    const mvar = parseFloat(parts[3]);
                    const freq = parseFloat(parts[4]);
                    
                    if (isNaN(mw) || isNaN(mvar) || isNaN(freq)) {
                        await sock.sendMessage(senderJid, { text: "⚠️ Format angka tidak valid.\nContoh: `!lapor GT1 50 10 50.01`" });
                    } else {
                        try {
                            if (db_cloud && isCloudHealthy) {
                                await db_cloud.collection("operational_reports").add({
                                    unit,
                                    mw,
                                    mvar,
                                    frequency: freq,
                                    operator: cleanUser,
                                    timestamp: new Date().toISOString()
                                });
                                await sock.sendMessage(senderJid, { text: `✅ *LAPORAN DITERIMA*\n\n📍 Unit: *${unit}*\n⚡ Beban: ${mw} MW\n📈 Reaktif: ${mvar} MVAR\n🌀 Freq: ${freq} Hz\n👤 Op: ${cleanUser}\n\n_Data aman di Firestore (Project: laporan-pembangkit)_` });
                                addLog(`📝 Report saved for unit ${unit} by ${cleanUser}`);
                            } else {
                                await sock.sendMessage(senderJid, { text: "⚠️ *CLOUD OFFLINE*\nLaporan hanya tersimpan di log lokal. Cek izin service account." });
                            }
                        } catch (err: any) {
                            addLog(`❌ Error saving report: ${err.message}`);
                            await sock.sendMessage(senderJid, { text: `❌ Gagal menyimpan ke Cloud: ${err.message}` });
                        }
                    }
                } else {
                    await sock.sendMessage(senderJid, { text: "*[ 📋 FORMAT LAPORAN ]*\n\nKetik: `!lapor [UNIT] [MW] [MVAR] [FREQ]`\nContoh: `!lapor GT1 50 10 50.01`" });
                }
                continue;
            }

            // --- PERINTAH BANTUAN DASAR (MENU, CEKLIST) ---
            if (lowerText === "menu" || lowerText === "!menu" || lowerText === "help" || lowerText === "!help") {
                let menu = `🤖 *WA BOT MENU*\n\n`;
                menu += `📁 *Note/Vault:* (Semua User)\n`;
                menu += `• \`!simpan <nama> <isi>\`: Simpan catatan\n`;
                menu += `• \`!buka <nama>\`: Lihat catatan\n`;
                menu += `• \`!listnote\`: Lihat semua nama catatan\n\n`;
                menu += `✨ *AI Features:* (Semua User)\n`;
                menu += `• Chat Langsung: Ketik apa saja di Japri untuk ngobrol dengan AI.\n\n`;
                menu += `📝 *Laporan:* (User & Grup Target)\n`;
                menu += `• Cukup ketik: *Alias* spasi *Angka*\n`;
                menu += `• Contoh: *G1 562021*\n`;
                menu += `• Ketik *ceklist* untuk daftar alias.\n\n`;
                
                if (isFromAdmin) {
                    menu += `⚙️ *Admin Commands:*\n`;
                    menu += `• \`!sync\`: Sinkron data dari Sheets\n`;
                    menu += `• \`!map <alias> <row>\`: Edit mapping baris\n`;
                    menu += `• \`!restart\`: Mulai ulang bot\n`;
                    menu += `• \`!stats\`: Statistik bot\n`;
                }
                
                await sock.sendMessage(senderJid, { text: menu });
                continue;
            }

            if (lowerText === "ceklist" || lowerText === "!ceklist") {
                let responseMsg = "📋 *DAFTAR LENGKAP KATA KUNCI*\n\n";
                const rowToAliases: Record<number, string[]> = {};
                Object.entries(ROW_MAP).forEach(([alias, row]) => {
                    if (!rowToAliases[row]) rowToAliases[row] = [];
                    rowToAliases[row].push(alias.toUpperCase());
                });
                const sortedRows = Object.keys(rowToAliases).map(Number).sort((a, b) => a - b);
                sortedRows.forEach(row => {
                    const aliasesArr = rowToAliases[row].map(a => `*${a}*`).join(", ");
                    responseMsg += `• Baris ${row}: ${aliasesArr}\n`;
                });
                await sock.sendMessage(senderJid, { text: responseMsg });
                continue;
            }

            // --- PERINTAH STATUS ---
            if (lowerText === "!status" || lowerText === "!ping") {
                const uptime = process.uptime();
                const days = Math.floor(uptime / (3600 * 24));
                const hours = Math.floor((uptime % (3600 * 24)) / 3600);
                const minutes = Math.floor((uptime % 3600) / 60);
                
                const mongoStatus = (isMongoHealthy && mongo_db) ? "✅" : "❌";
                const fireStatus = (isCloudHealthy && db_cloud) ? "✅" : "❌";
                const cloudStatus = `${mongoStatus} (Mongo) / ${fireStatus} (Firestore)`;
                
                const waStatus = (connectionStatus === "open") ? "✅ CONNECTED (STABLE)" : "⚠️ " + (connectionStatus || "DISCONNECTED").toUpperCase();
                
                const msg = `*[ 🛡️ BOT ENGINE STATUS ]*\n\n` +
                            `🟢 Status: ${waStatus}\n` +
                            `⏳ Uptime: ${days > 0 ? days + "d " : ""}${hours}h ${minutes}m\n` +
                            `☁️ Cloud Sync: ${cloudStatus}\n` +
                            `💓 Heartbeat: Active (Every 2m)\n` +
                            `🔧 Self-Healing: Enabled\n\n` +
                            `_Bot ini dirancang untuk tetap online 24 jam dengan sistem pemulihan otomatis._`;
                await sock.sendMessage(senderJid, { text: msg });
                continue;
            }

            // --- PERINTAH SYNC MANUAL ---
            if (lowerText === "!sync" || lowerText === "sync") {
                if (isFromAdmin || isTargetGroup) {
                    await sock.sendMessage(senderJid, { text: "🔄 Sedang mensinkronisasi data terakhir dari Google Sheet... Mohon tunggu." });
                    await syncStatusFromSheet();
                    await sock.sendMessage(senderJid, { text: "✅ Sinkronisasi Selesai! Sekarang bot sudah tahu angka terakhir di spreadsheet." });
                }
                continue;
            }

            // --- PERINTAH EDIT MAPPING ---
            // Format: !map [alias] [baris]
            // --- PERINTAH EDIT MAPPING ---
            // Format: !map [alias] [baris] atau !map list/del
            if (lowerText.startsWith("!map")) {
                if (isFromAdmin) {
                    const parts = lowerText.split(/\s+/).filter(p => p.length > 0);
                    const sub = parts[1]?.toLowerCase();

                    // 1. LIST MAPPING
                    if (sub === "list" || parts.length === 1) {
                        let listMsg = "*[ ⚙️ BOT MAPPING LIST ]*\n\n";
                        const sortedKeys = Object.keys(ROW_MAP).sort();
                        if (sortedKeys.length === 0) {
                            listMsg += "Mapping masih kosong.";
                        } else {
                            sortedKeys.forEach(k => {
                                listMsg += `• _${k}_ -> Row *${ROW_MAP[k]}*\n`;
                            });
                        }
                        listMsg += "\n_Gunakan !map <alias> <row> untuk menambah/edit_";
                        listMsg += "\n_Gunakan !map del <alias> untuk menghapus_";
                        await sock.sendMessage(senderJid, { text: listMsg });
                    }
                    
                    // 2. DELETE MAPPING
                    else if (sub === "del" || sub === "delete") {
                        const alias = parts[2]?.toLowerCase();
                        if (!alias || !ROW_MAP[alias]) {
                            return await sock.sendMessage(senderJid, { text: `❌ Alias *${alias || ""}* tidak ditemukan.` });
                        }
                        delete ROW_MAP[alias];
                        try {
                            fs.writeFileSync("mapping.json", JSON.stringify(ROW_MAP, null, 2));
                            refreshMapping();
                            io.emit("bot:mapping", ROW_MAP);
                            await sock.sendMessage(senderJid, { text: `✅ Berhasil menghapus alias *${alias}*.` });
                        } catch (err: any) {
                            await sock.sendMessage(senderJid, { text: `❌ Gagal menyimpan mapping: ${err.message}` });
                        }
                        continue;
                    }

                    // 3. SET/UPDATE MAPPING
                    else {
                        const alias = sub;
                        const row = parseInt(parts[2]);
                        
                        if (alias && !isNaN(row)) {
                            ROW_MAP[alias] = row;
                            try {
                                fs.writeFileSync("mapping.json", JSON.stringify(ROW_MAP, null, 2));
                                refreshMapping();
                                io.emit("bot:mapping", ROW_MAP);
                                await sock.sendMessage(senderJid, { text: `✅ Berhasil memperbarui mapping:\n*${alias}* -> Baris *${row}*` });
                                addLog(`Mapping updated via WA: ${alias} -> ${row}`);
                            } catch (err: any) {
                                await sock.sendMessage(senderJid, { text: `❌ Gagal menyimpan mapping: ${err.message}` });
                            }
                        } else {
                            await sock.sendMessage(senderJid, { text: "⚠️ Format salah.\nContoh: `!map stg 10` atau `!map list`" });
                        }
                    }
                } else {
                    await sock.sendMessage(senderJid, { text: "🚫 Maaf, perintah ini hanya untuk Admin." });
                }
                continue;
            }

            // --- FITUR VAULT (SIMPAN, AMBIL, LIST, HAPUS) ---
            const VAULT_LOCAL_DIR = path.join(process.cwd(), "vault_data");
            if (!fs.existsSync(VAULT_LOCAL_DIR)) fs.mkdirSync(VAULT_LOCAL_DIR, { recursive: true });

            const getLocalNotes = (user: string) => {
                const userFile = path.join(VAULT_LOCAL_DIR, `${user}.json`);
                if (fs.existsSync(userFile)) return JSON.parse(fs.readFileSync(userFile, "utf-8"));
                return {};
            };

            const saveLocalNote = (user: string, keyword: string, data: any) => {
                const notes = getLocalNotes(user);
                notes[keyword] = data;
                fs.writeFileSync(path.join(VAULT_LOCAL_DIR, `${user}.json`), JSON.stringify(notes, null, 2));
            };

            const removeLocalNote = (user: string, keyword: string) => {
                const notes = getLocalNotes(user);
                if (notes[keyword]) {
                    delete notes[keyword];
                    fs.writeFileSync(path.join(VAULT_LOCAL_DIR, `${user}.json`), JSON.stringify(notes, null, 2));
                    return true;
                }
                return false;
            };
            
            // Perintah !simpan, !buka, !listnote, !hapusnote
            if (lowerText.startsWith("!simpan") || (isPrivateChat && isFromAdmin && (msg.message.documentMessage || msg.message.imageMessage || msg.message.videoMessage))) {
                try {
                    let type: 'note' | 'image' | 'video' | 'pdf' | 'other' = 'note';
                    let rawContent = text.replace(/!simpan/i, "").trim();
                    let keyword = "";
                    let content = "";
                    
                    const firstSpaceIndex = rawContent.search(/\s/);
                    if (firstSpaceIndex !== -1) {
                        keyword = rawContent.substring(0, firstSpaceIndex).toLowerCase();
                        content = rawContent.substring(firstSpaceIndex).trim();
                    } else if (rawContent !== "") {
                        keyword = rawContent.toLowerCase();
                        content = "(Tanpa isi pesan)";
                    }

                    let fileName = `note_${Date.now()}.txt`;
                    let mimeType = 'text/plain';

                    const hasDirectMedia = !!(msg.message?.imageMessage || msg.message?.videoMessage || msg.message?.documentMessage);
                    const hasQuotedMedia = !!(quotedMsg?.imageMessage || quotedMsg?.videoMessage || quotedMsg?.documentMessage);

                    if (hasDirectMedia || hasQuotedMedia) {
                        addLog("📥 Media detected for storage, downloading...");
                        const messageToDownload = hasDirectMedia ? msg : { message: quotedMsg };
                        const mediaMsg = hasDirectMedia 
                            ? (msg.message.imageMessage || msg.message.videoMessage || msg.message.documentMessage)
                            : (quotedMsg.imageMessage || quotedMsg.videoMessage || quotedMsg.documentMessage);

                        const { downloadMediaMessage } = await import("@whiskeysockets/baileys");
                        const buffer = await downloadMediaMessage(messageToDownload as any, 'buffer', {});
                        
                        if (msg.message?.imageMessage || quotedMsg?.imageMessage) type = 'image';
                        else if (msg.message?.videoMessage || quotedMsg?.videoMessage) type = 'video';
                        else if (msg.message?.documentMessage || quotedMsg?.documentMessage) type = 'pdf';
                        
                        mimeType = mediaMsg.mimetype || 'application/octet-stream';
                        const ext = mimeType.split('/')[1]?.split(';')[0] || 'bin';
                        fileName = `${type}_${Date.now()}.${ext}`;
                        
                        if (!fs.existsSync(path.join(process.cwd(), "uploads"))) {
                            fs.mkdirSync(path.join(process.cwd(), "uploads"), { recursive: true });
                        }
                        const filePath = path.join(process.cwd(), "uploads", fileName);
                        fs.writeFileSync(filePath, buffer);
                        
                        const baseUrl = process.env.APP_URL || "";
                        if (baseUrl) {
                            content = `${baseUrl}/uploads/${fileName}`;
                        } else {
                            content = `(File saved locally: ${fileName})`;
                        }
                        addLog(`✅ Media saved to vault: ${fileName}`);
                    }

                    if (keyword) {
                        const noteData = {
                            type,
                            keyword,
                            content,
                            fileName,
                            sender: cleanUser,
                            timestamp: new Date().toISOString(),
                            mimeType
                        };

                        // 1. Simpan Lokal (Selalu)
                        saveLocalNote(cleanUser, keyword, noteData);
                        
                        // 2. Simpan Cloud (Jika Aktif)
                        const db = initFirebase();
                        if (db && isCloudHealthy) {
                            try {
                                const existing = await db.collection("vault")
                                    .where("keyword", "==", keyword)
                                    .where("sender", "==", cleanUser)
                                    .get();
                                
                                if (!existing.empty) {
                                    await existing.docs[0].ref.update(noteData);
                                } else {
                                    await db.collection("vault").add(noteData);
                                }
                            } catch (e) {
                                addLog(`⚠️ Firebase Vault sync failed: ${e}`);
                            }
                        }
                        
                        await sock.sendMessage(senderJid, { text: `✅ Note *${keyword}* berhasil disimpan!` });
                    } else if (text.toLowerCase().trim() === "!simpan") {
                        await sock.sendMessage(senderJid, { text: "ℹ️ Format: `!simpan <nama> <isi>` atau kirim media dengan caption `!simpan <nama>`" });
                    }
                } catch (err) {
                    addLog(`❌ Gagal menyimpan ke Vault: ${err}`);
                    await sock.sendMessage(senderJid, { text: `❌ Gagal menyimpan: ${err instanceof Error ? err.message : String(err)}` });
                }
                continue; 
            }

            if (lowerText.startsWith("!buka ") || lowerText.startsWith("!ambil ")) {
                const keywordPart = lowerText.replace(/!(buka|ambil)\s+/, "").trim().split(/\s+/)[0];
                if (keywordPart) {
                    // Cek Lokal Dulu
                    const localNotes = getLocalNotes(cleanUser);
                    let note = localNotes[keywordPart];

                    if (!note) {
                        // Cek Cloud
                        const db = initFirebase();
                        if (db && isCloudHealthy) {
                            try {
                                const query = await db.collection("vault")
                                    .where("keyword", "==", keywordPart)
                                    .where("sender", "==", cleanUser)
                                    .get();
                                if (!query.empty) note = query.docs[0].data();
                            } catch (e) { addLog(`Firebase read failed: ${e}`); }
                        }
                    }
                    
                    if (note) {
                        let responseText = `📌 *Note: ${note.keyword}*\n\n${note.content}`;
                        if (note.type !== 'note') {
                            responseText += `\n\nType: ${note.type}\nUpdate: ${new Date(note.timestamp).toLocaleString()}`;
                        }
                        await sock.sendMessage(senderJid, { text: responseText });
                    } else {
                        await sock.sendMessage(senderJid, { text: `⚠️ Note *${keywordPart}* tidak ditemukan.` });
                    }
                }
                continue;
            }

            if (lowerText.startsWith("!hapusnote ")) {
                const keywordPart = lowerText.replace("!hapusnote ", "").trim().split(/\s+/)[0];
                if (keywordPart) {
                    const removed = removeLocalNote(cleanUser, keywordPart);
                    
                    const db = initFirebase();
                    if (db && isCloudHealthy) {
                        try {
                            const query = await db.collection("vault")
                                .where("keyword", "==", keywordPart)
                                .where("sender", "==", cleanUser)
                                .get();
                            if (!query.empty) await query.docs[0].ref.delete();
                        } catch (e) { addLog(`Firebase delete failed: ${e}`); }
                    }
                    
                    if (removed) {
                        await sock.sendMessage(senderJid, { text: `🗑️ Note *${keywordPart}* telah dihapus.` });
                    } else {
                        await sock.sendMessage(senderJid, { text: `⚠️ Note *${keywordPart}* tidak ditemukan untuk dihapus.` });
                    }
                }
                continue;
            }

            if (lowerText === "!listnote" || lowerText === "!vault") {
                const localNotes = getLocalNotes(cleanUser);
                const keywords = Object.keys(localNotes);
                
                if (keywords.length > 0) {
                    let responseText = "📂 *DAFTAR NOTE ANDA*\n\n";
                    keywords.forEach(kw => {
                        const icon = localNotes[kw].type === 'note' ? '📝' : '📁';
                        responseText += `${icon} *${kw}*\n`;
                    });
                    responseText += "\nKetik `!buka <nama>` untuk melihat isi.";
                    await sock.sendMessage(senderJid, { text: responseText });
                } else {
                    await sock.sendMessage(senderJid, { text: "📭 Anda belum memiliki catatan." });
                }
                continue;
            }

            if (lowerText === "!stats" && isFromAdmin) {
                const db = initFirebase();
                const stats = `📊 *BOT STATISTICS*
• *Status:* Online ✅
• *Admin:* ${cleanAdmin}
• *Target Group:* ${targetGroupId ? 'Set' : 'Not Set'}
• *Firestore:* ${db && isCloudHealthy ? 'Connected 🟢' : 'Disconnected/Local 🔴'}
• *Uptime:* ${Math.floor(process.uptime() / 60)} minutes
• *Platform:* AI Studio`;
                await sock.sendMessage(senderJid, { text: stats });
                continue;
            }

            if (lowerText === "!help" || lowerText === "!menu") {
                let menu = `🤖 *WA BOT MENU*\n\n`;
                menu += `📁 *Note/Vault:* (Semua User)\n`;
                menu += `• \`!simpan <nama> <isi>\`: Simpan catatan\n`;
                menu += `• \`!buka <nama>\`: Lihat catatan\n`;
                menu += `• \`!listnote\`: Lihat semua nama catatan\n\n`;
                menu += `✨ *AI Features:* (Semua User)\n`;
                menu += `• Chat Langsung: Ketik apa saja di Japri untuk ngobrol dengan AI.\n\n`;
                
                if (isFromAdmin) {
                    menu += `⚙️ *Admin Commands:*\n`;
                    menu += `• \`!sync\`: Sinkron data dari Sheets\n`;
                    menu += `• \`!map <alias> <row>\`: Edit mapping baris\n`;
                    menu += `• \`!restart\`: Mulai ulang bot\n`;
                    menu += `• \`!stats\`: Statistik bot\n`;
                }
                
                await sock.sendMessage(senderJid, { text: menu });
                continue;
            }

            if (lowerText === "!restart" && isFromAdmin) {
                await sock.sendMessage(senderJid, { text: "🔄 Memulai ulang bot (Sesi tetap tersimpan)..." });
                addLog("♻️ Restart command received from admin.");
                process.exit(0); // Applet platform will auto-restart the container
            }

            // Filter Keamanan Utuh
            if (!isTargetGroup && !isAllowedAdminPrivate && !isActivityGroup) {
                const reason = isPrivateChat ? `Bukan Admin (User: ${cleanUser} vs Admin: ${cleanAdmin})` : `Bukan Grup Target (Incoming: ${senderJid} vs Config: ${targetGroupId})`;
                addLog(`🚫 PESAN DIABAIKAN: ${reason}`);
                continue;
            }

            addLog(`✅ Incoming [${senderJid}]: ${text} (Admin: ${isFromAdmin}, Private: ${isPrivateChat}, GroupTarget: ${isTargetGroup}, GroupActivity: ${isActivityGroup})`);

            // --- SISANYA ADALAH LOGIKA BOT LAMA (REPORT, TIKET, DLL) ---
            // ------------------------------------------

            if (!text) {
                addLog(`DEBUG: Pesan dari ${senderJid} diabaikan karena teks kosong`);
                continue;
            }

            // Jika dari Grup Kegiatan, catat sebagai aktivitas via AI Log-Cleaner
            if (isActivityGroup && !isFromAdmin) {
                // Cek apakah ini laporan teknis (G1, G2, etc)? Jika ya, jangan skip, biarkan lanjut ke bawah
                const firstLine = text.split("\n")[0].trim().toLowerCase();
                const isTechnical = Object.keys(ROW_MAP).some(alias => firstLine.startsWith(alias));
                
                if (!isTechnical && !text.startsWith(".") && !text.startsWith("!")) { 
                    processMessageWithAI(text).then(async (results) => {
                        if (Array.isArray(results) && results.length > 0) {
                            for (const res of results) {
                                if (res.text) {
                                    saveActivity(res.text, res.time);
                                    addLog(`📝 AI Log-Cleaner [${res.time}]: ${res.text}`);
                                }
                            }
                            await sock.sendMessage(senderJid, { react: { text: "✅", key: msg.key } });
                        }
                    }).catch(err => {
                        addLog(`❌ AI Log-Cleaner Error: ${err.message}`);
                    });
                    continue;
                }
            }

            if (text.toLowerCase().trim().startsWith("lapor ")) {
                const jam = text.toLowerCase().replace("lapor", "").trim();
                await sock.sendMessage(senderJid, { text: `⏳ Sedang memproses laporan jam ${jam}. Mohon tunggu sebentar...` });
                generateAndSendReport(jam); // Process in background
                continue;
            }

            if (text.toLowerCase().trim() === "hapus kegiatan" && isFromAdmin) {
                clearActivities();
                await sock.sendMessage(senderJid, { text: "🗑️ Semua catatan kegiatan telah dihapus." });
                continue;
            }

            // FITUR: Deteksi Copy-Paste Raw WhatsApp (Misal: [22/4, 08.50] ...)
            if (text.includes("[") && text.includes("]") && text.includes(":") && (text.includes("/") || text.includes("."))) {
                const waLines = text.split("\n");
                let currentItem: { time: string, text: string } | null = null;
                let extractedCount = 0;

                for (const line of waLines) {
                    // Regex untuk format: [tgl/bln, jam.mnt] Nama: Pesan
                    const match = line.match(/^\[\d{1,2}\/\d{1,2},\s+(\d{1,2}\.\d{2})\]\s+[^:]+:\s+(.*)/i);
                    
                    if (match) {
                        // Simpan item sebelumnya jika ada
                        if (currentItem) {
                            saveActivity(currentItem.text, currentItem.time);
                            extractedCount++;
                            
                            // Ekstraksi data teknis dari sub-line (misal: "sf6 5.2")
                            const linesForDirect = currentItem.text.split(" - ").map(s => s.trim()).filter(s => s.length > 0);
                            for (const subLine of linesForDirect) {
                                // Regex diperluas untuk menangkap nilai seperti 3,6/1,9 atau 5,3/880
                                const subMatch = subLine.match(/^([a-zA-Z0-9.]+)\s+[:=]?\s*([0-9.,/]+)/i);
                                if (subMatch) {
                                    const alias = subMatch[1].toLowerCase();
                                    const value = subMatch[2];
                                    
                                    // Selalu update status
                                    updateStatus({ [alias]: value });

                                    let row = ROW_MAP[alias];
                                    if (!row) {
                                        const smart = findBestAlias(alias, value);
                                        if (smart) {
                                            row = smart.row;
                                            addLog(`🧠 Smart Match (Raw): [${alias}] -> [${smart.alias}] (Score: ${smart.score.toFixed(2)})`);
                                        }
                                    }

                                    if (row) {
                                        const target = getEffectiveTarget(currentItem.time);
                                        updateSheetCell(targetSheetId, target.day, target.column, row, value);
                                    }
                                }
                            }
                        }
                        currentItem = { 
                            time: match[1], 
                            text: match[2].trim() 
                        };
                    } else if (currentItem && line.trim().length > 0) {
                        // Ini adalah baris lanjutan dari pesan sebelumnya
                        currentItem.text += " " + line.trim();
                    }
                }
                
                // Simpan item terakhir dan coba proses datanya
                if (currentItem) {
                    saveActivity(currentItem.text, currentItem.time);
                    extractedCount++;
                    
                    // Juga coba masukkan datanya ke sheet jika formatnya cocok (Data teknis dalam satu baris)
                    const linesForDirect = currentItem.text.split(" - ").map(s => s.trim()).filter(s => s.length > 0);
                    for (const subLine of linesForDirect) {
                         const subMatch = subLine.match(/^([a-zA-Z0-9.]+)\s+[:=]?\s*([0-9.,/]+)/i);
                         if (subMatch) {
                             const alias = subMatch[1].toLowerCase();
                             const value = subMatch[2];
                             
                             // Selalu update status untuk data teknis apapun yang ditemukan
                             updateStatus({ [alias]: value });
                             
                             // Jika ada di mapping, update ke sheet
                             let row = ROW_MAP[alias];
                             if (!row) {
                                 const smart = findBestAlias(alias, value);
                                 if (smart) {
                                     row = smart.row;
                                     addLog(`🧠 Smart Match (Raw-End): [${alias}] -> [${smart.alias}] (Score: ${smart.score.toFixed(2)})`);
                                 }
                             }

                             if (row) {
                                 const target = getEffectiveTarget(currentItem.time);
                                 updateSheetCell(targetSheetId, target.day, target.column, row, value);
                             }
                         }
                    }
                }
                
                if (extractedCount > 0) {
                    await sock.sendMessage(senderJid, { text: `✅ Berhasil mengekstrak *${extractedCount}* kegiatan & data teknis dari raw data WhatsApp.` });
                    if (!isPrivateChat) {
                        await sock.sendMessage(senderJid, { react: { text: "✅", key: msg.key } });
                    }
                    continue;
                }
            }

            const logMsg = `WA Message [${senderJid}]: "${text}"`;
            addLog(logMsg);
            console.log(`[DEBUG] ROW_MAP size: ${Object.keys(ROW_MAP).length}`);
            console.log(`[DEBUG] targetSheetId: ${targetSheetId}`);
            
            // 1. Jalur Borongan / Multi-line Parser
            const lines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
            let savedResults: string[] = [];
            let failedLines: string[] = [];
            const updatePromises: Promise<any>[] = [];

            for (const line of lines) {
                const match = line.match(/([a-z0-9.#]{1,15})(?:\s*[:=\s-]+\s*)([0-9.,/]+)/i);
                
                if (match) {
                    const aliasRaw = match[1].trim().toLowerCase();
                    const value = match[2].trim();
                    
                    let row = ROW_MAP[aliasRaw];
                    let aliasFinal = aliasRaw;
                    
                    if (!row) {
                        const smart = findBestAlias(aliasRaw, value);
                        if (smart) {
                            row = smart.row;
                            aliasFinal = smart.alias;
                            addLog(`🧠 Smart Match (Fuzzy): [${aliasRaw}] -> [${aliasFinal}] (Score: ${smart.score.toFixed(2)})`);
                        }
                    }

                    if (row) {
                        const target = getEffectiveTarget("auto");
                        
                        // We check counter down asynchronously to keep things moving
                        const checkAndSave = async () => {
                            let warningText = "";
                            const isLevelUnit = (row >= 45 && row <= 49);
                            const isResetManual = text.toLowerCase().includes("reset") || line.toLowerCase().includes("reset");
                            
                            if (!isLevelUnit) {
                                const prevCol = getPreviousColumnName(target.column, target.day);
                                if (prevCol) {
                                    const prevValueStr = await getCellValue(targetSheetId, target.day.toString(), `${prevCol}${row}`);
                                    if (prevValueStr) {
                                        const prevVal = parseFloat(prevValueStr.replace(/,/g, ""));
                                        const currVal = parseFloat(value.replace(/,/g, ""));
                                        if (!isNaN(prevVal) && !isNaN(currVal) && currVal < prevVal) {
                                            if (isResetManual) {
                                                warningText = `\n🔄 *INFO:* Counter reset.`;
                                            } else {
                                                warningText = `\n⚠️ *PERINGATAN:* Angka turun!`;
                                            }
                                        }
                                    }
                                }
                            }

                            const ok = await updateSheetCell(targetSheetId, target.day, target.column, row, value);
                            if (ok) {
                                savedResults.push(`${aliasFinal.toUpperCase()}: ${value}${warningText}`);
                                updateStatus({ [aliasFinal]: value });
                            } else {
                                failedLines.push(line);
                            }
                        };

                        updatePromises.push(checkAndSave());
                    } else {
                        failedLines.push(line);
                    }
                } else {
                    failedLines.push(line);
                }
            }

            // Wait for all updates in this block to complete before summarizing
            if (updatePromises.length > 0) {
                await Promise.all(updatePromises);
            }

            // Jika ada yang berhasil disimpan secara langsung
            if (savedResults.length > 0) {
                let summary = `✅ *HASIL LAPORAN*\n\n`;
                summary += `Berhasil menyimpan *${savedResults.length}* data.\n`;
                summary += `\n📅 Tgl ${getJakartaTime().getDate()}, Kolom ${getEffectiveTarget("auto").column}`;
                
                if (isPrivateChat) {
                    await sock.sendMessage(senderJid, { text: summary });
                } else {
                    await sock.sendMessage(senderJid, { react: { text: "✅", key: msg.key } });
                }

                // Jika semua baris berhasil, selesai
                if (failedLines.length === 0) {
                    continue;
                }
                
                // Jika ada sisa baris yang gagal, biarkan diproses AI (tapi text-nya hanya yang gagal)
                text = failedLines.join("\n");
            }

            // 2. Jika ada yang gagal atau format tidak baku (narasi), gunakan AI
            addLog(`Executing AI Analysis for text: "${text.slice(0, 50)}..."`);
            processMessageWithAI(text).then(async (results) => {
                addLog(`AI Result: ${JSON.stringify(results)}`);
                if (Array.isArray(results) && results.length > 0) {
                    const statusUpdates: Record<string, string> = {};
                    for (const res of results) {
                        // LOG-CLEANER MODE: Jika ada field 'text' (kegiatan), simpan sebagai aktivitas
                        if (res.text) {
                            saveActivity(res.text, res.time);
                            addLog(`📝 AI Activity Extracted: ${res.text}`);
                        }
                        
                        // DATA-EXTRACTOR MODE: Jika ada field 'field' dan 'value', masukkan ke sheet
                        if (res.field && res.value) {
                            const alias = res.field.toLowerCase();
                            statusUpdates[alias] = res.value; 
                            
                            // Coba mapping row dengan fallback
                            let row = ROW_MAP[alias];
                            
                            // --- SMART LOGIC: Fuzzy Matching (AI result) ---
                            if (!row) {
                                const smart = findBestAlias(alias, res.value || "");
                                if (smart) {
                                    row = smart.row;
                                    addLog(`🧠 Smart Match (AI): [${alias}] -> [${smart.alias}] (Score: ${smart.score.toFixed(2)})`);
                                }
                            }

                            if (!row) row = ROW_MAP[alias.replace(/\s+/g, "")];
                            if (!row && alias.includes(" ")) row = ROW_MAP[alias.split(" ")[0]];

                            if (row) {
                                const target = getEffectiveTarget(res.time || "auto");
                                const ok = await updateSheetCell(targetSheetId, target.day, target.column, row, res.value);
                                if (ok) {
                                    if (isPrivateChat) {
                                        await sock.sendMessage(senderJid, { text: `✅ *TERSIMPAN (AI)*: ${res.field}=${res.value} (Tgl ${target.day})` });
                                    }
                                    addLog(`Updated ${res.field} to ${res.value} via AI`);
                                }
                            }
                        }
                    }
                    if (Object.keys(statusUpdates).length > 0) {
                        updateStatus(statusUpdates);
                        if (!isPrivateChat) {
                             await sock.sendMessage(senderJid, { react: { text: "✅", key: msg.key } });
                        }
                    }
                } else {
                    // Jika dari Japri, beri tahu kalau format tidak dikenal, atau CHAT dengan AI
                    if (isPrivateChat) {
                        // Jika bukan perintah dan bukan data teknis, ajak AI ngobrol
                        addLog(`🤖 Falling back to General AI Chat for: "${text}"`);
                        const aiResponse = await getAIResponse(text);
                        await sock.sendMessage(senderJid, { text: aiResponse || "🤖 *Maaf Pak, Bot tidak mengenali format laporan tersebut.*\n\nCoba ketik *menu* untuk melihat contoh cara laporan atau *ceklist* untuk daftar alias." });
                    }
                }
            }).catch(async (err) => {
                addLog(`AI Processing Error: ${err.message}`);
                if (isPrivateChat) {
                    await sock.sendMessage(senderJid, { text: "⚠️ Terjadi kesalahan pada sistem AI. Mohon coba lagi nanti." });
                }
            });
          } catch (innerErr: any) {
            addLog(`❌ [MSG LOOP ERROR] ${innerErr.message}`);
          }
        }
      }
    } catch (outerErr: any) {
      addLog(`❌ [MSG EVENT ERROR] ${outerErr.message}`);
    }
  });
}

    async function processMessageWithAI(text: string) {
        addLog(`AI Processing: ${text}`);
        
        const prompt = `Kamu adalah sistem otomatisasi laporan operasional (Log-Cleaner) PLTGU Muara Karang Blok 1.
Tugas: Mengekstrak kegiatan operasional dan nilai teknis dari chat WhatsApp.

Pesan: "${text}"

ATURAN OUTPUT (JSON ARRAY):
1. KEGIATAN: {"time": "HH:mm", "text": "KALIMAT_FORMAL"}
2. DATA TEKNIS: {"field": "ALIAS", "value": "ANGKA"}

Panduan:
- Jika ada "STG 123", ekstrak sebagai {"field": "STG", "value": "123"}.
- Jika ada pesan jam, gunakan jam tersebut.

Balas HANYA JSON array. Jika tidak ada yang relevan, balas [].`;

    try {
      const response = await ai.models.generateContent({ 
        model: "gemini-3-flash-preview",
        contents: prompt,
        config: { 
            responseMimeType: "application/json",
            temperature: 0.1
        }
      });
      let textResponse = response.text;
      addLog(`AI Raw Response: ${textResponse}`);
      
      textResponse = textResponse.replace(/```json/g, "").replace(/```/g, "").trim();
      const parsed = JSON.parse(textResponse || "[]");
      return Array.isArray(parsed) ? parsed : [parsed];
    } catch (e) {
      addLog(`AI Parse Error: ${e}`);
      return [];
    }
    }

  const cellUpdateTimestamps: Record<string, number> = {};

  async function isCellEmpty(sheetId: string, sheetTitle: string, cellRange: string): Promise<boolean> {
    const sheets = google.sheets({ version: "v4", auth });
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetTitle}!${cellRange}`,
      });
      const values = response.data.values;
      return !values || values.length === 0 || !values[0][0];
    } catch (err) {
      return true;
    }
  }

  function getNextColumn(currentCol: string, day: number): string | null {
    const cols = day === 1 ? ["D", "E", "F", "G", "H"] : ["D", "E", "F", "G"];
    const idx = cols.indexOf(currentCol);
    if (idx !== -1 && idx < cols.length - 1) {
      return cols[idx + 1];
    }
    return null;
  }

  io.on("connection", (socket) => {
    socket.emit("bot:status", connectionStatus);
    socket.emit("bot:statusMessage", statusMessage);
    socket.emit("bot:currentSheet", targetSheetId);
    socket.emit("bot:currentGroup", targetGroupId);
    socket.emit("bot:currentAdmin", adminNumber);
    socket.emit("bot:currentBot", botNumber);
    socket.emit("bot:mapping", ROW_MAP);
    socket.emit("bot:cloudStatus", { 
        healthy: isCloudHealthy, 
        projectId: firebaseConfig.projectId || "unknown",
        databaseId: firebaseConfig.firestoreDatabaseId || "(default)"
    });
    if (qrCode) socket.emit("bot:qr", qrCode);
    
    socket.on("bot:retryCloud", () => {
        console.log(">>> [RELIABILITY] User requested cloud retry.");
        isCloudHealthy = true;
        lastCloudErrorTime = 0; // Reset cooldown
        db_cloud = null; // Force re-init
        initFirebase();
        socket.emit("bot:cloudStatus", { 
            healthy: isCloudHealthy, 
            projectId: firebaseConfig.projectId || "unknown",
            databaseId: firebaseConfig.firestoreDatabaseId || "(default)"
        });
        addLog("🔄 [CLOUD] Mencoba menyambungkan kembali ke Firestore...");
    });
    
    // Safety check: Notify user if Cloud Persistence is disabled
    if (!isCloudHealthy) {
        socket.emit("bot:log", "⚠️ [PERSISTENCE WARNING] Cloud Firestore is DISABLED. Your session will be lost if the server restarts. Please enable Firestore API in Google Cloud Console.");
    }

    socket.on("bot:requestPairingCode", async () => {
      try {
        if (!botNumber) throw new Error("Nomor HP Bot belum dikonfigurasi");
        let cleanNumber = botNumber.replace(/[^0-9]/g, "");
        
        // Auto convert 08... to 628...
        if (cleanNumber.startsWith("0")) {
          cleanNumber = "62" + cleanNumber.slice(1);
        }

        addLog(`Preparing FRESH connection for pairing code for Bot: ${cleanNumber}...`);
        
        // 1. Matikan socket yang sedang berjalan
        addLog("Forcing reconnection lock reset...");
        isConnecting = false; 
        
        if (sock) {
          addLog("Stopping current socket...");
          try {
            sock.ev.removeAllListeners();
            sock.end();
          } catch (e) {}
          sock = null;
          await delay(2000);
        }

        // 2. HAPUS sesi cloud lama
        addLog("Removing old session data...");
        await clearCloudSession();
        
        if (fs.existsSync("auth_info_baileys")) {
            fs.rmSync("auth_info_baileys", { recursive: true, force: true });
        }
        await delay(1000);

        // 3. Inisialisasi ulang
        addLog("Initializing fresh WhatsApp engine...");
        await connectToWhatsApp();
        
      // 4. Tunggu sampai socket benar-benar ada
        let attempts = 0;
        addLog("Engine starting... Polling for socket readiness.");
        while (!sock && attempts < 60) { // Increased to 60s for slow environments
          await delay(1000);
          attempts++;
          if (attempts % 5 === 0) addLog(`Waiting for engine... (${attempts}s)`);
        }

        if (!sock) {
            isConnecting = false;
            throw new Error("Socket failed to initialize after 60s. This usually means a dependency hang or critical auth error. Check console logs.");
        }

        // 5. Minta Pairing Code
        addLog(`Requesting FRESH Pairing Code from WhatsApp for ${cleanNumber}...`);
        await delay(6000); // Reduced to 6s
        
        let code;
        let pairingRetry = 0;
        while (pairingRetry < 3) {
            try {
                code = await Promise.race([
                    sock.requestPairingCode(cleanNumber),
                    new Promise((_, reject) => setTimeout(() => reject(new Error("Pairing code request TIMEOUT")), 45000))
                ]) as string;
                break; // Success
            } catch (e: any) {
                pairingRetry++;
                addLog(`❌ PAIRING ATTEMPT ${pairingRetry} FAILED: ${e.message}`);
                
                if (e.message?.includes("428") || e.message?.includes("Precondition")) {
                    addLog("🔒 Reliability: Precondition Failed. Sanitizing and retrying in 5s...");
                    await clearCloudSession();
                    await delay(5000);
                    // Re-initialize socket may be needed, but let's try just retrying the request first
                } else {
                    await delay(3000);
                }
                
                if (pairingRetry >= 3) {
                    isConnecting = false;
                    throw e;
                }
            }
        }
        
        addLog(`🔥 FIRE PAIRING CODE (UA: macOS): ${code}`);
        addLog(`👉 Masukkan kode ini di HP Bapak SEKARANG.`);
        
        io.emit("bot:pairingCode", code);
        socket.emit("bot:pairingCode", code); 
        qrCode = null; 
        io.emit("bot:qr", null);
        isConnecting = false;
        setIsGeneratingCode(false); // Helper state synchronization
      } catch (err: any) {
        isConnecting = false;
        setIsGeneratingCode(false);
        const errMsg = err.message || err;
        addLog(`❌ PAIRING FAILED: ${errMsg}`);
        socket.emit("bot:error", `Gagal membuat kode: ${errMsg}`);
      }
    });

    // Helper state
    function setIsGeneratingCode(val: boolean) {
        io.emit("bot:log", val ? "Pairing code generated..." : "Pairing operation complete.");
    }

    socket.on("bot:requestPreview", async (timeStr: string) => {
      try {
        addLog(`📝 Menyiapkan draf laporan untuk jam ${timeStr}...`);
        const report = await generateReportText(timeStr);
        if (report) {
          socket.emit("bot:reportPreview", report);
          addLog(`✅ Draf laporan jam ${timeStr} siap ditinjau.`);
        }
      } catch (err) {
        addLog(`❌ Gagal menyiapkan draf: ${err}`);
      }
    });

    socket.on("bot:sendManualReport", async (text: string) => {
       if (sock && connectionStatus === "open") {
         try {
           await sock.sendMessage(KEGIATAN_GROUP_ID, { text });
           addLog(`🚀 Laporan manual berhasil dikirim.`);
           socket.emit("bot:reportSent", true);
         } catch (err) {
           addLog(`❌ Gagal mengirim laporan manual: ${err}`);
         }
       } else {
         addLog("⚠️ WhatsApp tidak terhubung. Laporan gagal dikirim.");
       }
    });

    socket.on("bot:clearActivities", () => {
      clearActivities();
      addLog("🧹 Log kegiatan telah dikosongkan.");
    });

    socket.on("bot:resetSession", async () => {
      try {
        addLog("🛑 Resetting WhatsApp Session (Local & Cloud)...");
        if (sock) {
          try { sock.ev.removeAllListeners(); } catch(e) {}
          try { await sock.logout(); } catch(e) {}
          try { sock.end(); } catch(e) {}
          sock = null;
        }
        
        // 1. Hapus cloud data jika ada
        await clearCloudSession();

        // 2. Hapus folder auth_info secara rekursif
        if (fs.existsSync("auth_info_baileys")) {
          fs.rmSync("auth_info_baileys", { recursive: true, force: true });
        }
        
        addLog("✅ Full Reset Success. Restarting engine...");
        setTimeout(() => {
          connectToWhatsApp();
        }, 3000);
      } catch (err) {
        addLog(`❌ Error resetting session: ${err}`);
      }
    });

    socket.on("bot:retryCloud", async () => {
        addLog("Manual retry for Cloud Auth initiated...");
        isCloudHealthy = true;
        lastCloudErrorTime = 0;
        db_cloud = null; // Clear cached instance
        initFirebase();
        socket.emit("bot:cloudStatus", { 
            healthy: isCloudHealthy, 
            projectId: firebaseConfig.projectId, 
            databaseId: firebaseConfig.firestoreDatabaseId || "(default)" 
        });
        if (isCloudHealthy) {
            addLog("✅ Cloud status reset to Healthy. Testing connection...");
            const testDb = initFirebase();
            if (testDb) {
                try {
                    await testDb.collection("test_connection").doc("ping").get();
                    addLog("🚀 [FIREBASE] Test Sync SUCCESSFUL!");
                } catch (e: any) {
                    addLog(`❌ Sync still fails: ${e.message}`);
                }
            }
        }
    });

    socket.on("bot:restart", async () => {
        addLog("♻️ [SYSTEM] Manual restart initiated by user.");
        updateBotStatus("connecting", "Restarting system components...");
        
        // Clean disconnect
        if (sock) {
            try {
                addLog("📡 Closing WhatsApp link securely...");
                sock.ev.removeAllListeners();
                sock.end(undefined);
                sock = null;
            } catch (e) {}
        }
        
        isConnecting = false;
        qrCode = null;
        
        // Short delay before reconnecting
        addLog("🔄 System standby... Re-initializing core in 3s");
        setTimeout(() => {
            addLog("🚀 Re-launching WhatsApp Engine...");
            connectToWhatsApp();
            io.emit("bot:status", connectionStatus);
        }, 3000);
    });

    socket.on("bot:setSheet", (id: string) => { targetSheetId = id; saveConfig(); socket.emit("bot:currentSheet", id); });
    socket.on("bot:setGroup", (id: string) => { targetGroupId = id; saveConfig(); socket.emit("bot:currentGroup", id); });
    socket.on("bot:setAdmin", (num: string) => { adminNumber = num; saveConfig(); socket.emit("bot:currentAdmin", num); });
    socket.on("bot:setBot", (num: string) => { botNumber = num; saveConfig(); socket.emit("bot:currentBot", num); });

    socket.on("bot:ai_result", async (data: { field: string, value: string, time: string, sender: string }) => {
      let { day, column, finalTime } = getEffectiveTarget(data.time);
      const fieldKey = data.field.toLowerCase().trim();
      const row = ROW_MAP[fieldKey];

      if (row && column) {
        const cleanValue = parseFloat(data.value.replace(",", "."));
        if (isNaN(cleanValue)) return;

        const cellKey = `${day}-${column}-${row}`;
        const now = Date.now();
        
        // 1. Ambil DATA SEBELUMNYA untuk analisa
        const prevCol = getPreviousColumnName(column, day);
        let prevValue: number | null = null;
        if (prevCol) {
          const rawPrev = await getCellValue(targetSheetId, day.toString(), `${prevCol}${row}`);
          if (rawPrev) prevValue = parseFloat(rawPrev.replace(",", "."));
        }

        // 2. LOGIKA ANALISA & NOTIFIKASI
        let analysisMsg = "";
        if (prevValue !== null) {
          const delta = cleanValue - prevValue;
          if (delta < 0) {
             analysisMsg = `⚠️ *PERINGATAN ANOMALI!*\nAngka baru (${cleanValue}) lebih KECIL dari jam sebelumnya (${prevValue}). Mohon cek ulang pengambilan data!`;
          } else if (delta === 0) {
             analysisMsg = `ℹ️ *Status: Standby/Stop*\nTidak ada penambahan counter dari jam sebelumnya.`;
          } else {
             analysisMsg = `📈 *Laporan Valid*\nPenambahan (Delta): *+${delta.toFixed(3)}*`;
          }
        } else {
          analysisMsg = `📝 *Laporan Awal Shift*\nData pertama hari ini telah dicatat.`;
        }

        const isEmpty = await isCellEmpty(targetSheetId, day.toString(), `${column}${row}`);
        
        if (!isEmpty) {
          const lastUpdate = cellUpdateTimestamps[cellKey] || 0;
          if (now - lastUpdate > 10 * 60 * 1000) {
            const nextCol = getNextColumn(column, day);
            if (nextCol) {
              addLog(`Cell ${column}${row} occupied, jumping to next column ${nextCol}${row}`);
              column = nextCol;
            }
          }
        }

        const success = await updateSheetCell(targetSheetId, day, column, row, cleanValue.toString());
        if (success) {
          cellUpdateTimestamps[`${day}-${column}-${row}`] = now;
          addLog(`Updated ${data.field} at ${column}${row} (Day ${day})`);
          if (sock) {
             const contextMsg = isEmpty ? "" : "\n_(Lompat ke kolom berikutnya karena data sebelumnya sudah ada)_";
             await sock.sendMessage(data.sender, { 
                text: `✅ *BERHASIL DICATAT*\n\n📊 Field: *${data.field}*\n⏰ Kolom: *${column}*\n🔢 Nilai: *${cleanValue}*${contextMsg}\n\n${analysisMsg}` 
             });
          }
        }
      } else {
        addLog(`Mapping failed: ${data.field} | ${data.time}`);
      }
    });
  });

  // Ensure uploads directory exists
  if (!fs.existsSync(path.join(process.cwd(), "uploads"))) {
    fs.mkdirSync(path.join(process.cwd(), "uploads"), { recursive: true });
  }
  app.use("/uploads", express.static(path.join(process.cwd(), "uploads")));

  // 1. Setup API / Static Middleware (Global parts)
  app.use("/uploads", express.static(path.join(process.cwd(), "uploads")));

  const isProd = process.env.NODE_ENV === "production";
  if (isProd) {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res, next) => {
      if (req.path.startsWith("/api") || req.path.startsWith("/socket.io") || req.path.startsWith("/uploads")) return next();
      res.sendFile(path.join(distPath, "index.html"));
    });
    console.log(">>> [SERVER] Production Assets Configured.");
  } else {
    // In dev, we still need Vite, but we should start it before listening if possible
    // or at least not block the listener
  }

  // 2. Start listening IMMEDIATELY to satisfy platform health checks
  httpServer.listen(PORT, "0.0.0.0", async () => {
    console.log(`>>> [SERVER] Listening on port ${PORT} [${isProd ? "PRODUCTION" : "DEVELOPMENT"}]`);
    
    // 3. Background Initialization
    try {
      if (!isProd) {
        console.log(">>> [SERVER] Preparing Vite Display Engine...");
        const vite = await createViteServer({
          server: { middlewareMode: true },
          appType: "spa",
        });
        app.use(vite.middlewares);
        console.log(">>> [SERVER] Vite Display Engine Ready.");
      }
      
      // Heavy initialization after server is responsive
      connectToWhatsApp();
      console.log(">>> [SERVER] WhatsApp Engine background start initiated.");
    } catch (err) {
      console.error(">>> [CRITICAL] Background Initialization Failed:", err);
    }
  });
}

// RELIABILITY & SHUTDOWN HANDLERS
process.on("unhandledRejection", (reason, promise) => {
  console.error(">>> [RELIABILITY] Unhandled Rejection at:", promise, "reason:", reason);
});

process.on("uncaughtException", (err) => {
  console.error(">>> [RELIABILITY] Unhandled Exception thrown:", err);
});

process.on("SIGINT", async () => {
  console.log(">>> [SERVER] Shutting down gracefully...");
  // sock is global, so we can access it here if it's exported or in scope
  // If not in scope, we might need a different shutdown mechanism, but for now let's keep it simple
  process.exit(0);
});

startServer().catch(err => {
    console.error("CRITICAL SERVER CRASH ON STARTUP:", err);
});

/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useEffect, useState } from "react";
import { io, Socket } from "socket.io-client";
import { motion, AnimatePresence } from "motion/react";
import { QrCode, MessageSquare, Signal, SignalLow, Power, Terminal, Settings, ShieldCheck, Brain, Key, X, FolderHeart, FileText, Download, Trash2, Image, Video, FileDigit, Copy, Check, Database, Save, RotateCcw, Plus, Search, AlertCircle, Cloud } from "lucide-react";
import { GoogleGenAI } from "@google/genai";

// Use lazy initialization or check for process.env safely
const getAi = () => {
  const apiKey = (import.meta as any).env?.VITE_GEMINI_API_KEY || (typeof process !== 'undefined' ? process.env.GEMINI_API_KEY : '');
  if (!apiKey) return null;
  return new GoogleGenAI({ apiKey });
};

export default function App() {
  const [socket, setSocket] = useState<Socket | null>(null);
  const [status, setStatus] = useState<string>("connecting");
  const [qr, setQr] = useState<string | null>(null);
  const [logs, setLogs] = useState<{ time: string, msg: string }[]>([]);
  const [sheetId, setSheetId] = useState<string>("");
  const [groupId, setGroupId] = useState<string>("");
  const [adminNum, setAdminNum] = useState<string>("");
  const [botNumber, setBotNumber] = useState<string>("6282337726122");
  const [pairingCode, setPairingCode] = useState<string | null>(null);
  const [mapping, setMapping] = useState<Record<string, number>>({});
  const [hasSession, setHasSession] = useState<boolean>(false);
  const [reportDraft, setReportDraft] = useState<string>("");
  const [selectedShift, setSelectedShift] = useState<string>("14.00");
  const [isGenerating, setIsGenerating] = useState<boolean>(false);
  const [isGeneratingCode, setIsGeneratingCode] = useState<boolean>(false);
  const [vaultItems, setVaultItems] = useState<any[]>([]);
  const [activeTab, setActiveTab] = useState<"dashboard" | "controls" | "vault" | "mapping">("dashboard");

  const [resetCount, setResetCount] = useState<number>(0);
  const [copied, setCopied] = useState<boolean>(false);
  const [mappingSearch, setMappingSearch] = useState<string>("");
  const [isSavingMapping, setIsSavingMapping] = useState<boolean>(false);
  const [isUploading, setIsUploading] = useState<boolean>(false);

  const fetchVault = () => {
    fetch("/api/vault")
      .then(res => {
        if (!res.ok) throw new Error("Server error");
        return res.json();
      })
      .then(data => {
        if (Array.isArray(data)) {
          setVaultItems(data);
        } else {
          setVaultItems([]);
        }
      })
      .catch(err => {
        console.error("Failed to fetch vault:", err);
        setVaultItems([]);
      });
  };

  const handleCopyCode = (code: string) => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  useEffect(() => {
    fetch("/api/mapping")
      .then(res => {
        if (!res.ok) throw new Error("Server error");
        return res.json();
      })
      .then(data => {
        if (data && typeof data === 'object' && !data.error) {
          setMapping(data);
        }
      })
      .catch(err => console.error("Failed to fetch mapping:", err));
  }, []);

  const handleSaveMapping = () => {
    setIsSavingMapping(true);
    fetch("/api/mapping", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mapping)
    })
      .then(res => res.json())
      .then(() => {
        addLog("✅ Mapping updated and saved to server");
        setIsSavingMapping(false);
      })
      .catch(err => {
        addLog(`❌ Failed to save mapping: ${err}`);
        setIsSavingMapping(false);
      });
  };

  const updateMappingEntry = (key: string, value: number) => {
    setMapping(prev => ({ ...prev, [key]: value }));
  };

  const deleteMappingEntry = (key: string) => {
    const newMapping = { ...mapping };
    delete newMapping[key];
    setMapping(newMapping);
  };

  const addMappingEntry = () => {
    const key = prompt("Enter new alias (lowercase):");
    if (key) {
      const row = parseInt(prompt("Enter target row number:") || "0");
      if (!isNaN(row)) {
        setMapping(prev => ({ ...prev, [key.toLowerCase()]: row }));
      }
    }
  };


  useEffect(() => {
    if (resetCount > 0) {
      const timer = setTimeout(() => setResetCount(0), 3000);
      return () => clearTimeout(timer);
    }
  }, [resetCount]);

  const [statusMessage, setStatusMessage] = useState<string>("Initializing...");
  const [cloudStatus, setCloudStatus] = useState<{ healthy: boolean; projectId: string; databaseId: string }>({ healthy: true, projectId: "", databaseId: "" });
  const [apiStatus, setApiStatus] = useState<{ gemini: boolean, spreadsheetId: string, serviceAccountEmail: string, whatsappStatus: string } | null>(null);

  useEffect(() => {
    const checkApi = () => {
      fetch("/api/api-status")
        .then(res => {
          if (!res.ok) throw new Error("Server error");
          return res.json();
        })
        .then(data => {
          if (data && !data.error) {
            setApiStatus(data);
          }
        })
        .catch(() => {});
    };
    checkApi();
    const interval = setInterval(checkApi, 30000); // Every 30s
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const s = io();
    setSocket(s);

    s.on("bot:statusMessage", (msg: string) => {
      setStatusMessage(msg);
    });

    s.on("bot:cloudStatus", (status: { healthy: boolean; projectId: string; databaseId: string }) => {
      setCloudStatus(status);
    });

    s.on("bot:log", (msg: string) => {
      addLog(msg);
      if (msg.includes("Saved session found")) {
        setHasSession(true);
      }
      if (msg.includes("Failed to request pairing code") || msg.includes("Pairing code generated") || msg.includes("✅ SUCCESS!")) {
        setIsGeneratingCode(false);
      }
      if (msg.includes("Initializing fresh WhatsApp engine") || msg.includes("Removing old session")) {
        setStatus("connecting");
        setPairingCode(null);
      }
      if (msg.includes("Ready for QR scan or Pairing Code")) {
        setStatus("ready_for_pairing");
      }
    });

    s.on("bot:reportPreview", (report: string) => {
      setReportDraft(report);
      setIsGenerating(false);
      addLog("Laporan draf siap ditinjau");
    });

    s.on("bot:reportSent", () => {
      addLog("Laporan berhasil dikirim ke WhatsApp");
    });

    s.on("bot:status", (status: string) => {
      setStatus(status);
      addLog(`Status updated to: ${status}`);
    });

    s.on("bot:currentSheet", (id: string) => {
      setSheetId(id);
    });

    s.on("bot:currentGroup", (id: string) => {
      setGroupId(id);
    });

    s.on("bot:currentAdmin", (num: string) => {
      setAdminNum(num);
    });
    
    s.on("bot:currentBot", (num: string) => {
      setBotNumber(num);
    });

    s.on("bot:mapping", (m: Record<string, number>) => {
      setMapping(m);
      addLog("Mapping configuration synced");
    });

    s.on("bot:pairingCode", (code: string) => {
      setPairingCode(code);
      setIsGeneratingCode(false);
      addLog(`Pairing code received: ${code}`);
    });

    s.on("bot:error", (msg: string) => {
      setIsGeneratingCode(false);
      addLog(`ERR: ${msg}`);
      alert(msg);
    });

    s.on("bot:qr", (qrData: string) => {
      setQr(qrData);
      setStatus("qr");
      addLog("New QR Code received");
    });

    return () => {
      s.disconnect();
    };
  }, []);

  const handleUpdateSheet = () => {
    if (socket && sheetId) {
      socket.emit("bot:setSheet", sheetId);
      addLog(`Target Sheet ID updated: ${sheetId}`);
    }
  };

  const handleVaultUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.[0]) return;
    setIsUploading(true);
    const formData = new FormData();
    formData.append("file", e.target.files[0]);

    fetch("/api/vault/upload", {
      method: "POST",
      body: formData
    })
      .then(res => res.json())
      .then(() => {
        addLog("✅ File uploaded to vault");
        fetchVault();
        setIsUploading(false);
      })
      .catch(err => {
        addLog(`❌ Upload failed: ${err}`);
        setIsUploading(false);
      });
  };

  const handleVaultRename = (id: string, currentName: string) => {
    const newName = prompt("Enter new filename:", currentName);
    if (newName && newName !== currentName) {
      fetch(`/api/vault/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filename: newName })
      })
        .then(() => {
          addLog("✅ File renamed");
          fetchVault();
        })
        .catch(err => addLog(`❌ Rename failed: ${err}`));
    }
  };

  const handleVaultDelete = (id: string) => {
    if (confirm("Are you sure you want to delete this item?")) {
      fetch(`/api/vault/${id}`, {
        method: "DELETE"
      })
        .then(() => {
          addLog("✅ Item deleted from vault");
          fetchVault();
        })
        .catch(err => addLog(`❌ Delete failed: ${err}`));
    }
  };

  useEffect(() => {
    if (activeTab === "vault") {
      fetchVault();
    }
  }, [activeTab]);

  const addLog = (msg: string) => {
    setLogs(prev => [{ time: new Date().toLocaleTimeString(), msg }, ...prev].slice(0, 50));
  };

  const getStatusColor = () => {
    switch (status) {
      case "open": return "bg-green-500 shadow-green-500/50 text-green-500";
      case "qr": return "bg-yellow-500 shadow-yellow-500/50 text-yellow-500";
      case "close":
      case "error": return "bg-red-500 shadow-red-500/50 text-red-500";
      case "connecting":
      case "initializing": return "bg-blue-500 shadow-blue-500/50 text-blue-500 animate-pulse";
      default: return "bg-zinc-500 shadow-zinc-500/50 text-zinc-500";
    }
  };

  const getStatusText = () => {
    switch (status) {
      case "open": return "Online";
      case "close": return "Offline";
      case "qr": return "Waiting Scan";
      case "connecting": return "Connecting";
      case "initializing": return "Initializing";
      case "error": return "Error";
      default: return status;
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 font-sans selection:bg-zinc-800">
      {/* HUD Header */}
      {/* Modal Pairing Code */}
      <AnimatePresence>
        {pairingCode && (
          <div className="fixed inset-0 z-[100] flex items-center justify-center px-4">
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setPairingCode("")}
              className="absolute inset-0 bg-zinc-950/80 backdrop-blur-sm"
            />
            <motion.div 
              initial={{ scale: 0.9, opacity: 0, y: 20 }}
              animate={{ scale: 1, opacity: 1, y: 0 }}
              exit={{ scale: 0.9, opacity: 0, y: 20 }}
              className="relative w-full max-w-sm bg-zinc-900 border border-zinc-800 rounded-2xl p-6 shadow-2xl shadow-indigo-500/10"
            >
              <div className="flex justify-between items-center mb-6">
                <div className="bg-indigo-500/10 p-2 rounded-lg">
                  <Key className="w-5 h-5 text-indigo-400" />
                </div>
                <button 
                  onClick={() => setPairingCode("")}
                  className="text-zinc-500 hover:text-zinc-300 transition-colors"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              <div className="text-center mb-8">
                <h2 className="text-xs uppercase tracking-widest text-zinc-500 font-bold mb-2">WhatsApp Pairing Code</h2>
                <div className="flex justify-center gap-1 mb-4">
                  {(pairingCode || "").split('').map((char, index) => (
                    <div key={index} className="w-8 h-12 bg-zinc-950 border border-zinc-800 rounded-lg flex items-center justify-center text-2xl font-mono font-bold text-indigo-400 shadow-inner">
                      {char}
                    </div>
                  ))}
                </div>
                <button
                  onClick={() => handleCopyCode(pairingCode || "")}
                  className="mx-auto flex items-center gap-2 px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-zinc-300 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all border border-zinc-700"
                >
                  {copied ? (
                    <>
                      <Check className="w-3 h-3 text-emerald-400" />
                      <span className="text-emerald-400">Tersalin!</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-3 h-3" />
                      <span>Salin Kode</span>
                    </>
                  )}
                </button>
              </div>

              <div className="space-y-3 bg-zinc-950/50 rounded-xl p-4 border border-zinc-800/50">
                <h3 className="text-[10px] uppercase tracking-wider text-indigo-400 font-bold">Cara Menautkan:</h3>
                <ul className="text-[10px] text-zinc-400 space-y-2 list-decimal pl-4 leading-relaxed">
                  <li>Buka WhatsApp di HP Anda.</li>
                  <li>Tunggu notifikasi **"Tautkan Perangkat"** (Pop-up Resmi WA) muncul di bar notifikasi HP Anda.</li>
                  <li>**Klik notifikasi tersebut** dari HP Anda.</li>
                  <li>Masukkan 8 karakter kode biru di atas.</li>
                  <li>Jika notifikasi tetap tidak muncul, baru coba via menu Perangkat Tertaut manual.</li>
                </ul>
              </div>

              <button 
                onClick={() => setPairingCode("")}
                className="w-full mt-6 py-3 bg-indigo-500 hover:bg-indigo-600 text-white rounded-xl text-xs font-bold uppercase tracking-widest transition-all shadow-lg shadow-indigo-500/20"
              >
                Paham, Saya Mengerti
              </button>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Tab Header Navigation */}
      <nav className="border-b border-zinc-800 bg-zinc-900/50 backdrop-blur-md sticky top-0 z-50 pt-[safe-area-inset-top]">
        <div className="max-w-7xl mx-auto px-4 md:px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2 md:gap-3">
              <div className="w-8 h-8 md:w-9 md:h-9 bg-zinc-800 rounded-lg flex items-center justify-center border border-zinc-700 shadow-inner">
                <MessageSquare className="w-4 h-4 md:w-5 md:h-5 text-indigo-400" />
              </div>
              <h1 className="text-[10px] md:text-sm font-bold tracking-wider uppercase leading-tight">
                PLTGU <span className="text-zinc-500 font-normal block md:inline">MUARA KARANG BLOK-1</span>
              </h1>
            </div>

            <div className="hidden md:flex items-center gap-1 p-1 bg-zinc-950 rounded-xl border border-zinc-800">
              <button 
                onClick={() => setActiveTab("dashboard")}
                className={`px-4 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all ${activeTab === 'dashboard' ? 'bg-indigo-600 text-white shadow-lg' : 'text-zinc-500 hover:text-zinc-300'}`}
              >
                Dashboard
              </button>
              <button 
                onClick={() => setActiveTab("mapping")}
                className={`px-4 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all ${activeTab === 'mapping' ? 'bg-indigo-600 text-white shadow-lg' : 'text-zinc-500 hover:text-zinc-300'}`}
              >
                Mapping
              </button>
              <button 
                onClick={() => setActiveTab("vault")}
                className={`px-4 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all ${activeTab === 'vault' ? 'bg-indigo-600 text-white shadow-lg' : 'text-zinc-500 hover:text-zinc-300'}`}
              >
                Digital Vault
              </button>
            </div>
          </div>

            <div className="flex flex-col items-end">
              <div className="flex items-center gap-2">
                <div className={`w-1.5 h-1.5 rounded-full ${getStatusColor()} animate-pulse shadow-[0_0_8px]`} />
                <span className="text-[9px] md:text-[10px] font-mono uppercase tracking-widest text-zinc-100 leading-none">
                  {getStatusText()}
                </span>
              </div>
              <span className="text-[7px] md:text-[8px] font-mono text-zinc-500 uppercase tracking-tighter truncate max-w-[100px] md:max-w-none">
                {statusMessage}
              </span>
            </div>
          </div>
      </nav>

      {/* Mobile Bottom Nav */}
      <div className="md:hidden fixed bottom-6 left-1/2 -translate-x-1/2 z-[80] w-[90%] max-w-sm">
        <div className="bg-zinc-900/90 backdrop-blur-xl border border-zinc-800/50 rounded-2xl p-1.5 flex items-center justify-around shadow-2xl shadow-black/50 overflow-hidden ring-1 ring-white/5">
          {[
            { id: 'dashboard', label: 'Dashboard', icon: Terminal },
            { id: 'mapping', label: 'Mapping', icon: Database },
            { id: 'vault', label: 'Vault', icon: FolderHeart }
          ].map((item) => {
            const Icon = item.icon;
            const isActive = activeTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id as any)}
                className={`flex flex-col items-center gap-1.5 py-2.5 px-4 rounded-xl transition-all duration-300 relative ${isActive ? 'text-indigo-400' : 'text-zinc-500 active:bg-zinc-800/50'}`}
              >
                {isActive && (
                  <motion.div 
                    layoutId="activeTabBg"
                    className="absolute inset-0 bg-indigo-500/10 rounded-xl"
                  />
                )}
                <Icon size={18} className={`relative z-10 ${isActive ? 'scale-110' : ''} transition-transform`} />
                <span className={`text-[8px] font-black uppercase tracking-[0.15em] relative z-10 transition-opacity ${isActive ? 'opacity-100' : 'opacity-40'}`}>
                  {item.label}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      <main className="max-w-7xl mx-auto p-4 md:p-10 pb-32 md:pb-10">
        <AnimatePresence mode="wait">
          {activeTab === "dashboard" ? (
            <motion.div 
              key="dashboard"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="grid grid-cols-1 lg:grid-cols-12 gap-8"
            >
              {/* Main Control Panel */}
              <div className="lg:col-span-8 space-y-8">
                <section className="bg-zinc-900/50 backdrop-blur-xl border border-zinc-800/50 rounded-2xl md:rounded-3xl p-4 md:p-8 overflow-hidden relative shadow-2xl">
                  <div className="absolute top-0 right-0 p-10 opacity-5 pointer-events-none hidden md:block">
                    <Terminal className="w-64 h-64" />
                  </div>
                  
                  <div className="relative z-10 h-full flex flex-col">
                    <header className="mb-6 md:mb-10 flex flex-col md:flex-row justify-between items-start gap-4">
                      <div className="flex flex-col">
                        <div className="flex items-center gap-2 mb-2">
                          <div className="w-1.5 h-1.5 rounded-full bg-indigo-500 animate-pulse shadow-[0_0_10px_rgba(99,102,241,0.5)]" />
                          <span className="text-[8px] md:text-[10px] font-black text-indigo-400 uppercase tracking-[0.3em]">Cortex Engine v2.0</span>
                        </div>
                        <h2 className="text-xl md:text-3xl font-light tracking-tight text-white leading-none">
                          WA <span className="text-indigo-400 font-medium">Control Unit</span>
                        </h2>
                        <div className="flex items-center gap-2 mt-2">
                          <div className="flex items-center gap-1 px-1.5 py-0.5 bg-emerald-500/10 border border-emerald-500/20 rounded-md">
                            <ShieldCheck className="w-2 h-2 md:w-2.5 md:h-2.5 text-emerald-500" />
                            <span className="text-[7px] md:text-[8px] font-black text-emerald-500 uppercase tracking-widest">Active</span>
                          </div>
                        </div>
                      </div>
                      <div className="flex gap-2 w-full md:w-auto">
                        <button 
                          onClick={() => {
                            if (confirm("Restart WhatsApp Engine? (Session stays active)")) {
                              socket?.emit("bot:restart");
                            }
                          }}
                          className="flex-1 md:flex-none flex items-center justify-center gap-2 p-3 md:p-2.5 md:px-4 bg-indigo-500/10 hover:bg-indigo-500/20 rounded-xl text-indigo-400 hover:text-indigo-300 transition-all border border-indigo-500/20 shadow-inner group active:scale-95"
                          title="Restart Bot Engine"
                        >
                          <RotateCcw size={16} className="group-active:rotate-180 transition-transform duration-500" />
                          <span className="text-[9px] md:text-[10px] font-black uppercase tracking-widest whitespace-nowrap">Restart</span>
                        </button>
                        <button 
                          onClick={() => {
                            setLogs([]);
                            addLog("Console cleared.");
                          }}
                          className="flex-1 md:flex-none flex items-center justify-center p-3 md:p-2.5 bg-zinc-800/50 hover:bg-zinc-700/50 rounded-xl text-zinc-400 hover:text-white transition-all border border-zinc-700/50 shadow-inner active:scale-95"
                          title="Clear Logs"
                        >
                          <Trash2 size={16} />
                          <span className="md:hidden ml-2 text-[9px] font-black uppercase tracking-widest">Clear</span>
                        </button>
                      </div>
                    </header>

                    <div className="flex-1 flex flex-col items-center justify-center py-10 min-h-[400px]">
                      <AnimatePresence mode="wait">
                        {status === "qr" && qr ? (
                          <motion.div 
                            key="qr"
                            initial={{ opacity: 0, scale: 0.9, rotate: -2 }}
                            animate={{ opacity: 1, scale: 1, rotate: 0 }}
                            className="relative group"
                          >
                            <div className="absolute -inset-4 bg-white/10 rounded-[40px] blur-2xl group-hover:bg-white/20 transition-all duration-500" />
                            <div className="relative p-10 bg-white rounded-[32px] shadow-2xl">
                              <img src={qr} alt="QR Code" className="w-64 h-64 mix-blend-multiply" referrerPolicy="no-referrer" />
                              <div className="absolute -bottom-6 left-1/2 -translate-x-1/2 bg-indigo-600 text-[10px] px-6 py-2 rounded-full shadow-xl shadow-indigo-500/40 whitespace-nowrap uppercase tracking-[0.2em] font-black text-white border-2 border-white/20">
                                Authentication Required
                              </div>
                            </div>
                          </motion.div>
                        ) : status === "ready_for_pairing" && !hasSession ? (
                          <motion.div 
                            key="ready"
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            className="flex flex-col items-center gap-8 text-center"
                          >
                            <div className="relative">
                              <div className="absolute inset-0 bg-indigo-500/20 rounded-full blur-2xl animate-pulse" />
                              <div className="relative w-24 h-24 bg-zinc-800/50 rounded-[2rem] flex items-center justify-center border border-indigo-500/30 group transition-all hover:scale-110">
                                <QrCode className="w-12 h-12 text-indigo-400 group-hover:text-indigo-300 transition-colors" />
                              </div>
                            </div>
                            <div className="space-y-3">
                              <h3 className="text-xl font-medium text-white tracking-tight">Deployment Ready</h3>
                              <p className="text-[11px] text-zinc-500 max-w-xs mx-auto leading-relaxed">
                                Gateway is primed for connection. Request a <span className="text-indigo-400 font-bold underline underline-offset-4 decoration-indigo-500/30">Pairing Code</span> from the peripheral panel to initiate link.
                              </p>
                            </div>
                          </motion.div>
                        ) : (status === "open" || status === "connecting" || status === "initializing" || status === "close" || status === "error" || status === "ready_for_pairing") ? (
                          <motion.div 
                            key="report-area"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="w-full h-full flex flex-col space-y-8"
                          >
                            <div className="flex flex-col sm:flex-row items-stretch md:items-center justify-between gap-4 p-2 bg-zinc-950/50 rounded-2xl border border-zinc-800/50 shadow-inner">
                              <div className="flex gap-1 overflow-x-auto no-scrollbar">
                                {["06.00", "14.00", "21.00"].map(t => (
                                  <button
                                    key={t}
                                    onClick={() => setSelectedShift(t)}
                                    className={`flex-1 md:flex-none px-4 md:px-6 py-2.5 rounded-xl text-[9px] md:text-[10px] font-black uppercase tracking-widest transition-all duration-300 ${selectedShift === t ? 'bg-white text-zinc-950 shadow-xl' : 'text-zinc-600 hover:text-zinc-300 hover:bg-zinc-800/50'}`}
                                  >
                                    {t}
                                  </button>
                                ))}
                              </div>
                              <button
                                disabled={isGenerating}
                                onClick={() => {
                                  setIsGenerating(true);
                                  socket?.emit("bot:requestPreview", selectedShift);
                                }}
                                className="group relative overflow-hidden px-4 md:px-8 py-3.5 md:py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white rounded-xl text-[9px] md:text-[10px] font-bold uppercase tracking-widest flex items-center justify-center gap-3 transition-all shadow-xl shadow-indigo-600/20 active:scale-95"
                              >
                                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent -translate-x-full group-hover:animate-[shimmer_2s_infinite]" />
                                {isGenerating ? <div className="w-3 h-3 border-2 border-white/20 border-t-white rounded-full animate-spin" /> : <Brain className="w-3 h-3" />}
                                Generate Report
                              </button>
                            </div>

                            <div className="relative group flex-1 min-h-[350px] md:min-h-[400px]">
                              <textarea
                                value={reportDraft}
                                onChange={(e) => setReportDraft(e.target.value)}
                                placeholder="Draf laporan akan diproses secara neural..."
                                className="relative w-full h-full bg-zinc-950/80 border border-zinc-800/50 rounded-2xl p-4 md:p-8 font-mono text-[10px] md:text-[11px] leading-relaxed text-zinc-300 focus:outline-none focus:border-indigo-500/50 transition-all resize-none shadow-2xl backdrop-blur-md custom-scrollbar overflow-y-auto"
                              />
                            </div>

                            <div className="flex flex-col md:flex-row justify-between items-center gap-6 bg-zinc-950/30 p-6 rounded-2xl border border-zinc-800/30">
                               <div className="flex items-center gap-3">
                                 <div className="w-10 h-10 bg-zinc-800 rounded-lg flex items-center justify-center text-zinc-500">
                                    <Terminal size={18} />
                                 </div>
                                 <div>
                                   <p className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest">Automation Protocol</p>
                                   <p className="text-[9px] text-zinc-600">Manual injection via <span className="text-zinc-500 font-mono">!simpan</span> active</p>
                                 </div>
                               </div>
                               <div className="flex items-center gap-3 w-full md:w-auto">
                                {status !== "open" && (
                                  <div className="hidden lg:flex items-center gap-2 px-5 py-2.5 bg-amber-500/5 rounded-xl border border-amber-500/20">
                                    <div className="w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse shadow-[0_0_8px_rgba(245,158,11,0.5)]" />
                                    <span className="text-[9px] text-amber-500 font-black uppercase tracking-widest">Relinking Handler</span>
                                  </div>
                                )}
                                <button
                                  disabled={!reportDraft || status !== "open"}
                                  onClick={() => socket?.emit("bot:sendManualReport", reportDraft)}
                                  className="flex-1 md:flex-none px-10 py-4 bg-emerald-500 hover:bg-emerald-400 disabled:opacity-20 text-zinc-950 rounded-xl text-[10px] font-black uppercase tracking-[0.2em] flex items-center justify-center gap-3 transition-all active:scale-95 shadow-2xl shadow-emerald-500/20 border-t border-white/20"
                                >
                                  <MessageSquare className="w-4 h-4" />
                                  Execute Transmission
                                </button>
                               </div>
                            </div>
                          </motion.div>
                        ) : (
                          <motion.div 
                            key="loading"
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="flex flex-col items-center gap-6"
                          >
                            <div className="relative">
                              <div className="w-16 h-16 border-4 border-zinc-800 border-t-indigo-500 rounded-full animate-spin" />
                              <div className="absolute inset-0 flex items-center justify-center text-[10px] font-mono text-indigo-400">SOC</div>
                            </div>
                            <span className="text-[10px] font-black uppercase tracking-[0.3em] text-zinc-500 animate-pulse">{statusMessage}</span>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                  </div>
                </section>
              </div>

              {/* Sidebar / Logs */}
              <div className="lg:col-span-4 space-y-8">
                {/* Status HUD Widget */}
                <div className="bg-zinc-900/50 backdrop-blur-xl border border-zinc-800/50 rounded-3xl p-6 shadow-xl relative overflow-hidden">
                  <div className="absolute -top-4 -right-4 w-24 h-24 bg-indigo-500/10 rounded-full blur-2xl" />
                  <div className="flex items-center gap-4 mb-6">
                    <div className="w-12 h-12 bg-zinc-800/50 rounded-2xl flex items-center justify-center border border-zinc-700/50 relative">
                      <Signal className="w-6 h-6 text-zinc-400" />
                      <div className="absolute -top-1 -right-1 w-3 h-3 bg-indigo-500 rounded-full border-2 border-zinc-900 animate-ping" />
                    </div>
                    <div>
                      <h4 className="text-[10px] font-black uppercase tracking-widest text-zinc-500">Persistent Engine</h4>
                      <p className="text-lg font-medium text-white">{getStatusText()}</p>
                    </div>
                  </div>
                  {!cloudStatus.healthy ? (
                    <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 mb-6">
                      <div className="flex items-start gap-3">
                        <AlertCircle className="w-5 h-5 text-red-500 shrink-0 mt-0.5" />
                        <div>
                          <p className="text-[10px] text-red-500 font-black uppercase tracking-wider mb-1">
                            Cloud Auth Mati
                          </p>
                          <p className="text-[9px] text-red-200/60 leading-tight mb-2">
                             Firestore API belum aktif atau Database <b>{cloudStatus.databaseId}</b> belum dibuat di project <b>{cloudStatus.projectId}</b>.
                          </p>
                          <div className="flex flex-col gap-2">
                            <button 
                              onClick={() => socket?.emit("bot:retryCloud")}
                              className="inline-flex items-center justify-center gap-1.5 px-3 py-1.5 bg-indigo-600 text-white font-black uppercase text-[8px] rounded-lg hover:bg-indigo-500 transition-all shadow-lg"
                            >
                              <RotateCcw className="w-2.5 h-2.5" /> Retry Connection
                            </button>
                            <a 
                              href={`https://console.developers.google.com/apis/api/firestore.googleapis.com/overview?project=${cloudStatus.projectId}`} 
                              target="_blank" 
                              rel="noreferrer"
                              className="inline-flex items-center justify-center gap-1.5 px-3 py-1.5 bg-red-500 text-white font-black uppercase text-[8px] rounded-lg hover:bg-red-400 transition-all shadow-lg shadow-red-500/20"
                            >
                              1. Aktifkan API (Project: {cloudStatus.projectId})
                            </a>
                            <a 
                              href={`https://console.firebase.google.com/project/${cloudStatus.projectId}/firestore`} 
                              target="_blank" 
                              rel="noreferrer"
                              className="inline-flex items-center justify-center gap-1.5 px-3 py-1.5 bg-zinc-800 text-white font-black uppercase text-[8px] rounded-lg hover:bg-zinc-700 transition-all border border-zinc-700"
                            >
                              2. Cek Database di Firebase Console
                            </a>
                          </div>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-xl p-3 mb-6">
                      <p className="text-[9px] text-indigo-400 font-bold leading-tight">
                        ℹ️ Bot berjalan 24 jam di cloud. Browser boleh ditutup dan bot tetap aktif meskipun HP offline sementara.
                      </p>
                    </div>
                  )}

                  {/* API HEALTH SECTION */}
                  <div className="space-y-4 border-t border-zinc-800 pt-6 mt-6">
                    <h4 className="text-[10px] font-black uppercase tracking-widest text-zinc-500 flex items-center gap-2">
                        <ShieldCheck size={12} className="text-zinc-500" />
                        Infrastructure Health
                    </h4>
                    
                    <div className="grid grid-cols-2 gap-3">
                        {/* Gemini Status */}
                        <div className={`p-3 rounded-2xl border ${apiStatus?.gemini ? 'bg-emerald-500/5 border-emerald-500/20 shadow-[0_0_15px_-5px_rgba(16,185,129,0.1)]' : 'bg-red-500/5 border-red-500/20 shadow-[0_0_15px_-5px_rgba(239,68,68,0.1)]'} transition-all`}>
                            <div className="flex items-center gap-2 mb-1.5">
                                <Brain size={14} className={apiStatus?.gemini ? 'text-emerald-500' : 'text-red-500'} />
                                <span className="text-[9px] font-black uppercase tracking-wider text-zinc-100">AI Engine</span>
                            </div>
                            <p className={`text-[8px] font-medium ${apiStatus?.gemini ? 'text-emerald-400' : 'text-red-400'}`}>
                                {apiStatus?.gemini ? 'API Key Valid' : 'Invalid API Key'}
                            </p>
                        </div>

                        {/* Sheets Status */}
                        <div className="p-3 rounded-2xl border bg-indigo-500/5 border-indigo-500/20 shadow-[0_0_15px_-5px_rgba(99,102,241,0.1)] transition-all">
                            <div className="flex items-center gap-2 mb-1.5">
                                <Plus size={14} className="text-indigo-500" />
                                <span className="text-[9px] font-black uppercase tracking-wider text-zinc-100">Sheets DB</span>
                            </div>
                            <p className="text-[8px] font-medium text-indigo-400 truncate" title={apiStatus?.spreadsheetId}>
                                Connected
                            </p>
                        </div>
                    </div>

                    {apiStatus && !apiStatus.gemini && (
                        <div className="p-4 bg-red-500/10 border border-red-500/20 rounded-xl space-y-2">
                            <p className="text-[9px] font-bold text-red-400 uppercase tracking-wider">⚠️ Tindakan Diperlukan</p>
                            <p className="text-[9px] text-red-200/60 leading-normal">
                                API Key Gemini bermasalah. Silakan update di <b>Settings (kiri bawah)</b> dan pastikan key sudah benar.
                            </p>
                        </div>
                    )}

                    <div className="p-4 bg-zinc-950/40 border border-zinc-800/50 rounded-2xl space-y-3">
                         <div className="flex items-center gap-2 mb-1 border-b border-zinc-800/50 pb-2">
                            <Settings size={12} className="text-zinc-500" />
                            <span className="text-[9px] font-black uppercase tracking-widest text-zinc-400">Sheet Integration</span>
                         </div>
                         <div className="space-y-2.5">
                            <div>
                                <p className="text-[8px] font-black text-zinc-500 uppercase tracking-widest mb-1">Service Account Bot:</p>
                                <div className="flex items-center gap-2">
                                    <code className="text-[9px] text-indigo-300 bg-indigo-500/10 px-2 py-1 rounded-md border border-indigo-500/20 flex-1 truncate">{apiStatus?.serviceAccountEmail}</code>
                                    <button 
                                        onClick={() => handleCopyCode(apiStatus?.serviceAccountEmail || "")}
                                        className="p-1.5 bg-zinc-800 hover:bg-zinc-700 rounded-md text-zinc-400 transition-colors"
                                    >
                                        <Copy size={12} />
                                    </button>
                                </div>
                            </div>
                            <p className="text-[9px] text-zinc-500 italic leading-snug">
                                💡 <b>PENTING:</b> Pastikan email di atas sudah di-SHARE ke Google Sheet Anda dengan akses <b>"EDITOR"</b> agar data bisa masuk.
                            </p>
                         </div>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="p-4 bg-zinc-950/50 rounded-2xl border border-zinc-800/50">
                      <p className="text-[8px] font-black uppercase tracking-widest text-zinc-600 mb-1">Process ID</p>
                      <p className="text-xs font-mono text-zinc-400">#{Math.floor(Math.random() * 9000) + 1000}</p>
                    </div>
                    <div className="p-4 bg-zinc-950/50 rounded-2xl border border-zinc-800/50">
                      <p className="text-[8px] font-black uppercase tracking-widest text-zinc-600 mb-1">Latency</p>
                      <p className="text-xs font-mono text-emerald-400">12ms</p>
                    </div>
                  </div>
                </div>

                {/* Console Logs */}
                <div className="bg-zinc-900/80 backdrop-blur-md border border-zinc-800/50 rounded-3xl p-6 h-[400px] flex flex-col shadow-2xl">
                  <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 bg-zinc-800 rounded-lg flex items-center justify-center">
                        <Terminal size={14} className="text-zinc-500" />
                      </div>
                      <span className="text-[10px] font-black uppercase tracking-widest text-zinc-400">System Logs</span>
                    </div>
                    <div className="w-2 h-2 rounded-full bg-emerald-500 animate-ping" />
                  </div>
                  <div className="flex-1 overflow-y-auto custom-scrollbar font-mono text-[10px] space-y-2 pr-2">
                    {logs.map((log, i) => (
                      <div key={i} className="flex gap-3 group">
                        <span className="text-zinc-700 shrink-0 font-bold">{log.time}</span>
                        <span className={`break-words transition-colors ${log.msg.includes("❌") ? "text-red-400" : log.msg.includes("✅") ? "text-emerald-400" : "text-zinc-500 group-hover:text-zinc-300"}`}>
                          <span className="opacity-50 mr-1">{">"}</span>{log.msg}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Configuration Panel - Compact */}
                <div className="bg-zinc-900 border border-zinc-800/50 rounded-3xl p-6 shadow-2xl">
                  <div className="flex items-center gap-3 mb-6">
                    <div className="w-8 h-8 bg-zinc-800 rounded-lg flex items-center justify-center">
                      <Settings size={14} className="text-zinc-500" />
                    </div>
                    <span className="text-[10px] font-black uppercase tracking-widest text-zinc-400">Core Engine Config</span>
                  </div>
                  <div className="space-y-6">
                    <div className="space-y-3">
                      <label className="text-[9px] uppercase font-black text-zinc-600 tracking-tighter">Spreadsheet Reference</label>
                      <div className="relative group">
                        <input 
                          type="text" 
                          value={sheetId}
                          onChange={(e) => setSheetId(e.target.value)}
                          className="w-full bg-zinc-950 border border-zinc-800 rounded-xl px-4 py-3 text-[11px] font-mono text-white focus:border-indigo-500 transition-all outline-none"
                        />
                        <button 
                          onClick={handleUpdateSheet}
                          className="absolute right-2 top-2 p-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-500 hover:text-white rounded-lg transition-all"
                        >
                          <RotateCcw size={12} />
                        </button>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4 pt-4 border-t border-zinc-800/50">
                      <div className="space-y-2">
                        <label className="text-[8px] uppercase font-black text-zinc-500">Nomor Admin (Owner)</label>
                        <input 
                          type="text" 
                          value={adminNum}
                          onChange={(e) => setAdminNum(e.target.value)}
                          placeholder="Admin Phone"
                          className="w-full bg-zinc-950 border border-zinc-800 rounded-lg px-3 py-2 text-[9px] font-mono text-zinc-400 focus:border-indigo-500 outline-none"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-[8px] uppercase font-black text-zinc-500">Nomor Bot WA (Login)</label>
                        <input 
                          type="text" 
                          value={botNumber}
                          onChange={(e) => setBotNumber(e.target.value)}
                          placeholder="Bot Phone"
                          className="w-full bg-zinc-950 border border-zinc-800 rounded-lg px-3 py-2 text-[9px] font-mono text-zinc-400 focus:border-indigo-500 outline-none"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-[8px] uppercase font-black text-zinc-500">Group ID</label>
                        <input 
                          type="text" 
                          value={groupId}
                          onChange={(e) => setGroupId(e.target.value)}
                          placeholder="Group JID"
                          className="w-full bg-zinc-950 border border-zinc-800 rounded-lg px-3 py-2 text-[9px] font-mono text-zinc-400 focus:border-indigo-500 outline-none"
                        />
                      </div>
                      <button 
                        onClick={() => socket?.emit("bot:setAdmin", adminNum)}
                        className="bg-zinc-800 hover:bg-zinc-700 py-2 rounded-lg text-[8px] font-black uppercase text-zinc-400 transition-all"
                      >
                        Set Admin
                      </button>
                      <button 
                        onClick={() => socket?.emit("bot:setBot", botNumber)}
                        className="bg-zinc-800 hover:bg-zinc-700 py-2 rounded-lg text-[8px] font-black uppercase text-zinc-400 transition-all"
                      >
                        Set Bot
                      </button>
                      <button 
                        onClick={() => socket?.emit("bot:setGroup", groupId)}
                        className="col-span-2 bg-zinc-800 hover:bg-zinc-700 py-2 rounded-lg text-[8px] font-black uppercase text-zinc-400 transition-all"
                      >
                        Set Group
                      </button>
                    </div>

                    <div className="grid grid-cols-2 gap-4 pt-4 border-t border-zinc-800/50">
                       <button 
                        disabled={isGeneratingCode}
                        onClick={() => {
                          setIsGeneratingCode(true);
                          socket?.emit("bot:requestPairingCode");
                        }}
                        className="col-span-2 bg-indigo-600 hover:bg-indigo-500 py-3 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all text-white flex items-center justify-center gap-2 shadow-xl shadow-indigo-600/20 active:scale-[0.98]"
                      >
                        {isGeneratingCode ? (
                          <>
                            <div className="w-3 h-3 border-2 border-white/20 border-t-white rounded-full animate-spin" />
                            Generating Bot Code...
                          </>
                        ) : (
                          <>
                            <Key className="w-3 h-3" />
                            Request Bot Pairing Code
                          </>
                        )}
                      </button>
                    </div>

                    <div className="pt-4 border-t border-zinc-800/50">
                      <button 
                        onClick={() => {
                          if (resetCount === 0) setResetCount(1);
                          else {
                            socket?.emit("bot:resetSession");
                            setResetCount(0);
                          }
                        }}
                        className={`w-full py-3 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all border ${resetCount === 0 ? 'bg-red-500/5 border-red-500/10 text-red-500/60 hover:bg-red-500/10 hover:text-red-500' : 'bg-red-600 border-red-500 text-white animate-pulse shadow-lg shadow-red-600/30'}`}
                      >
                        {resetCount === 0 ? "Purge Handshake Data" : "Confirm Emergency Purge"}
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </motion.div>
          ) : activeTab === "mapping" ? (
            <motion.div 
              key="mapping"
              initial={{ opacity: 0, scale: 0.98 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 1.02 }}
              className="space-y-8"
            >
              <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <Database className="w-3.5 h-3.5 text-indigo-400" />
                    <span className="text-[8px] md:text-[10px] font-black text-zinc-500 uppercase tracking-[0.3em]">Mapping System</span>
                  </div>
                  <h2 className="text-xl md:text-3xl font-light tracking-tight text-white mb-1">
                    Cell <span className="text-indigo-400 font-medium">Mapping</span>
                  </h2>
                </div>
                <div className="flex items-center gap-2 w-full md:w-auto">
                    <button 
                      onClick={addMappingEntry}
                      className="flex-1 md:flex-none px-4 py-3 bg-zinc-800/50 hover:bg-zinc-700/50 text-white rounded-xl text-[9px] md:text-[10px] font-black uppercase tracking-widest border border-zinc-700 transition-all flex items-center justify-center gap-2"
                    >
                      <Plus size={14} /> Add
                    </button>
                    <button 
                      disabled={isSavingMapping}
                      onClick={handleSaveMapping}
                      className="flex-1 md:flex-none px-6 py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white rounded-xl text-[9px] md:text-[10px] font-black uppercase tracking-widest shadow-xl shadow-indigo-600/20 transition-all flex items-center justify-center gap-2"
                    >
                      {isSavingMapping ? <div className="w-3 h-3 border-2 border-white/20 border-t-white rounded-full animate-spin" /> : <Save size={14} />}
                      Commit
                    </button>
                </div>
              </div>

              <div className="relative mb-6">
                <div className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-600 px-1 pointer-events-none">
                  <Search size={16} />
                </div>
                <input 
                  type="text" 
                  placeholder="FILTER BY ALIAS OR ROW..."
                  value={mappingSearch}
                  onChange={(e) => setMappingSearch(e.target.value)}
                  className="w-full bg-zinc-900 border border-zinc-800 rounded-2xl pl-12 pr-6 py-4 text-xs font-mono uppercase tracking-widest text-zinc-300 focus:outline-none focus:border-indigo-500/50 transition-all shadow-inner"
                />
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                {Object.entries(mapping)
                  .filter(([key, val]) => 
                    key.toLowerCase().includes(mappingSearch.toLowerCase()) || 
                    val.toString().includes(mappingSearch)
                  )
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, val]) => (
                    <motion.div 
                      key={key}
                      layout
                      className="bg-zinc-900/40 backdrop-blur-md border border-zinc-800/50 rounded-[24px] p-6 group hover:border-indigo-500/50 hover:bg-zinc-900/60 transition-all duration-300 relative overflow-hidden"
                    >
                      <div className="absolute top-0 right-0 w-24 h-24 bg-indigo-500/5 rounded-full -mr-12 -mt-12 blur-2xl group-hover:bg-indigo-500/10 transition-all" />
                      
                      <div className="flex justify-between items-start mb-6">
                        <div className="px-3 py-1 bg-indigo-500/10 rounded-lg border border-indigo-500/20 text-[9px] font-black text-indigo-400 font-mono uppercase tracking-widest">
                          ENTRY_ID
                        </div>
                        <button 
                          onClick={() => deleteMappingEntry(key)}
                          className="text-zinc-700 hover:text-red-400 transition-all p-2 hover:bg-red-500/10 rounded-xl"
                        >
                          <Trash2 size={16} />
                        </button>
                      </div>

                      <div className="mb-6">
                         <h4 className="text-xs font-black text-zinc-500 mb-1 uppercase tracking-[0.2em]">Alias</h4>
                         <p className="text-lg font-light text-white tracking-tight">{key}</p>
                      </div>

                      <div className="space-y-3">
                        <label className="text-[9px] uppercase font-black text-zinc-600 tracking-widest pl-1">Target Mapping Row</label>
                        <div className="relative">
                          <input 
                            type="number" 
                            value={val}
                            onChange={(e) => updateMappingEntry(key, parseInt(e.target.value) || 0)}
                            className="w-full bg-zinc-950/50 border border-zinc-800/80 rounded-2xl px-5 py-3 text-sm font-mono text-indigo-400 focus:border-indigo-500 focus:bg-zinc-950 transition-all outline-none shadow-inner"
                          />
                          <div className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-700 pointer-events-none">
                            <Plus size={14} className="rotate-45" />
                          </div>
                        </div>
                      </div>
                    </motion.div>
                  ))}
              </div>

              {Object.keys(mapping).length === 0 && (
                <div className="py-32 flex flex-col items-center justify-center bg-zinc-900/30 rounded-[32px] border-2 border-dashed border-zinc-800">
                  <Database size={48} className="text-zinc-800 mb-6" />
                  <h3 className="text-lg font-medium text-zinc-500">No Mappings Configured</h3>
                  <button onClick={addMappingEntry} className="mt-4 text-indigo-400 text-xs font-black uppercase hover:underline">Download Default Templates</button>
                </div>
              )}
            </motion.div>
          ) : (
            <motion.div 
              key="vault"
              initial={{ opacity: 0, scale: 0.98 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 1.02 }}
              className="space-y-6 md:space-y-8"
            >
              <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <FolderHeart className="w-3.5 h-3.5 text-indigo-400" />
                    <span className="text-[8px] md:text-[10px] font-black text-zinc-500 uppercase tracking-[0.3em]">Asset Repository</span>
                  </div>
                  <h2 className="text-xl md:text-3xl font-light tracking-tight text-white mb-1">
                    Digital <span className="text-indigo-400 font-medium">Vault</span>
                  </h2>
                </div>
                <div className="flex items-center gap-2 w-full md:w-auto">
                  <div className="relative">
                    <input 
                      type="file" 
                      onChange={handleVaultUpload} 
                      className="absolute inset-0 opacity-0 cursor-pointer z-10" 
                      disabled={isUploading}
                    />
                    <button className="px-4 py-3 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-[9px] md:text-[10px] font-black uppercase tracking-widest border border-indigo-500 transition-all flex items-center justify-center gap-2 active:scale-95">
                      {isUploading ? <div className="w-3 h-3 border-2 border-white/20 border-t-white rounded-full animate-spin" /> : <Plus size={14} />}
                      Upload
                    </button>
                  </div>
                  <button 
                    onClick={fetchVault}
                    className="flex-1 md:flex-none px-4 py-3 bg-zinc-800/50 hover:bg-zinc-700/50 text-white rounded-xl text-[9px] md:text-[10px] font-black uppercase tracking-widest border border-zinc-700 transition-all flex items-center justify-center gap-2 active:scale-95"
                  >
                    <RotateCcw size={14} /> Refresh
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 md:gap-6">
                {!Array.isArray(vaultItems) || vaultItems.length === 0 ? (
                  <div className="col-span-full py-32 flex flex-col items-center justify-center bg-zinc-900/50 rounded-3xl border-2 border-dashed border-zinc-800">
                    <FolderHeart size={64} className="text-zinc-800 mb-6" />
                    <h3 className="text-lg font-medium text-zinc-400">Vault Masih Kosong</h3>
                    <p className="text-xs text-zinc-600 mt-2 max-w-sm text-center">
                      Kirim PDF, Gambar, atau Video ke bot WhatsApp anda, atau ketik <span className="text-indigo-400">!simpan ...</span> untuk menyimpan catatan.
                    </p>
                  </div>
                ) : (
                  vaultItems.map((item, i) => (
                    <motion.div 
                      key={i}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: i * 0.05 }}
                      className="bg-zinc-900 border border-zinc-800 rounded-2xl overflow-hidden hover:border-indigo-500/50 transition-all group"
                    >
                      <div className="p-6">
                        <div className="flex justify-between items-start mb-4">
                          <div className={`p-3 rounded-xl ${
                             item.type?.includes('image') ? 'bg-blue-500/10 text-blue-400' :
                             item.type?.includes('video') ? 'bg-purple-500/10 text-purple-400' :
                             item.type?.includes('pdf') || item.type?.includes('document') ? 'bg-red-500/10 text-red-400' :
                             'bg-emerald-500/10 text-emerald-400'
                          }`}>
                            {item.type?.includes('image') && <Image size={24} />}
                            {item.type?.includes('video') && <Video size={24} />}
                            {(item.type?.includes('pdf') || item.type?.includes('document')) && <FileText size={24} />}
                            {item.type === 'note' && <FileDigit size={24} />}
                            {!item.type?.includes('image') && !item.type?.includes('video') && !item.type?.includes('pdf') && !item.type?.includes('document') && item.type !== 'note' && <FileText size={24} />}
                          </div>
                          <div className="flex flex-col items-end gap-2">
                            <span className="text-[10px] font-mono text-zinc-600">{new Date(item.timestamp).toLocaleDateString('id-ID')}</span>
                            <div className="flex gap-2">
                              <button 
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleVaultRename(item.id, item.filename || item.fileName);
                                }}
                                className="p-3 bg-zinc-800/50 hover:bg-zinc-700/50 rounded-xl text-zinc-400 hover:text-indigo-400 transition-all active:scale-90 relative z-20 border border-zinc-700/50"
                                title="Rename"
                              >
                                <Save size={18} />
                              </button>
                              <button 
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleVaultDelete(item.id);
                                }}
                                className="p-3 bg-zinc-800/50 hover:bg-zinc-700/50 rounded-xl text-zinc-400 hover:text-red-400 transition-all active:scale-90 relative z-20 border border-zinc-700/50"
                                title="Delete"
                              >
                                <Trash2 size={18} />
                              </button>
                            </div>
                          </div>
                        </div>
                        
                        <div className="group/title relative mb-2">
                           <h4 className="text-sm font-bold truncate text-zinc-100 pr-8">{item.filename || item.fileName || "Note/Catatan"}</h4>
                        </div>
                        <p className="text-xs text-zinc-500 italic line-clamp-3 mb-6 bg-zinc-950/50 p-3 rounded-lg border border-zinc-800">
                          {item.type === 'note' ? item.content : `Pengirim: ${item.sender}`}
                        </p>
                        <div className="flex flex-col gap-2">
                             {item.driveLink && (
                               <a 
                                 href={item.driveLink} 
                                 target="_blank" 
                                 rel="noreferrer"
                                 className="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white text-[10px] font-bold py-2.5 rounded-lg flex items-center justify-center gap-2 transition-all shadow-lg shadow-emerald-600/10"
                               >
                                 <Cloud size={14} /> BUKA DI GOOGLE DRIVE
                               </a>
                             )}
                             <div className="flex gap-2">
                            {item.path ? (
                              <a 
                                href={`/api/vault/download/${item.id}`} 
                                target="_blank" 
                                rel="noreferrer"
                                className="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-[10px] font-bold py-2.5 rounded-lg flex items-center justify-center gap-2 transition-all shadow-lg shadow-indigo-600/10"
                              >
                                <Download size={14} /> UNDUH FILE
                              </a>
                            ) : item.type === 'note' ? (
                              <button 
                                onClick={() => handleCopyCode(item.content)}
                                className="flex-1 bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-[10px] font-bold py-2.5 rounded-lg flex items-center justify-center gap-2 transition-all border border-zinc-700"
                              >
                                SALIN CATATAN
                              </button>
                            ) : (
                              <a 
                                href={item.content} 
                                target="_blank" 
                                rel="noreferrer"
                                className="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-[10px] font-bold py-2.5 rounded-lg flex items-center justify-center gap-2 transition-all shadow-lg shadow-indigo-600/10"
                              >
                                <Download size={14} /> BUKA LINK
                              </a>
                            )}
                            </div>
                        </div>
                         </div>
                    </motion.div>
                  ))
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
}

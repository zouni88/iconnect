import { convertFileSrc, invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import "./styles.css";

interface FileRecord {
  name: string;
  size: number;
  saved_to: string;
  time: number;
}

interface OutboxItem {
  id: string;
  kind: string;
  name: string;
  size: number | null;
  text: string | null;
  time: number;
}

type ChatMsg = {
  kind: string;
  name: string;
  size: number | null;
  text: string | null;
  time: number;
  /** 仅电脑端收到的文件：本地保存路径 */
  saved_to?: string;
};

interface TextRecord {
  text: string;
  time: number;
}

interface ServerInfo {
  running: boolean;
  url: string;
  ip: string;
  port: number;
  save_dir: string;
}

const $ = <T extends HTMLElement>(id: string): T => document.getElementById(id) as T;

const statusPill = $("statusPill");
const statusText = $("statusText");
const qrPlaceholder = $("qrPlaceholder");
const qrBox = $("qrBox");
const urlRow = $("urlRow");
const urlText = $("urlText");
const copyBtn = $<HTMLButtonElement>("copyBtn");
const dirInput = $<HTMLInputElement>("dirInput");
const startBtn = $<HTMLButtonElement>("startBtn");
const stopBtn = $<HTMLButtonElement>("stopBtn");
const openDirBtn = $<HTMLButtonElement>("openDirBtn");
const chatList = $("chatList");
const chatStatus = $("chatStatus");
const plusBtn = $<HTMLButtonElement>("plusBtn");
const menuMask = $("menuMask");
const moreMenu = $("moreMenu");
const sendTextInput = $<HTMLTextAreaElement>("sendTextInput");
const sendTextBtn = $<HTMLButtonElement>("sendTextBtn");

let currentUrl = "";

async function init(): Promise<void> {
  // 默认保存目录
  try {
    dirInput.value = await invoke<string>("default_save_dir");
  } catch {
    /* 忽略 */
  }

  startBtn.addEventListener("click", start);
  stopBtn.addEventListener("click", stop);
  copyBtn.addEventListener("click", copyUrl);
  $("chooseDirBtn").addEventListener("click", chooseDir);
  openDirBtn.addEventListener("click", openSaveDir);
  sendTextBtn.addEventListener("click", sendTextToPhone);
  sendTextInput.addEventListener("keydown", (e) => {
    // Enter 发送，Shift+Enter 换行
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void sendTextToPhone();
    }
  });
  // 多行输入时自动增高（上限 140px）
  sendTextInput.addEventListener("input", () => {
    sendTextInput.style.height = "auto";
    sendTextInput.style.height = `${Math.min(sendTextInput.scrollHeight, 140)}px`;
  });

  // 加号菜单：展开/收起
  plusBtn.addEventListener("click", () => {
    const open = moreMenu.classList.toggle("open");
    plusBtn.classList.toggle("active", open);
    menuMask.classList.toggle("show", open);
  });
  menuMask.addEventListener("click", closeMenu);
  $("pickFileBtn").addEventListener("click", () => {
    closeMenu();
    void sendFileToPhone(false);
  });
  $("pickImgBtn").addEventListener("click", () => {
    closeMenu();
    void sendFileToPhone(true);
  });

  // 实时接收手机发来的事件
  await listen<FileRecord>("file-received", (e) => {
    addChatMsg("in", {
      kind: "file",
      name: e.payload.name,
      size: e.payload.size,
      text: null,
      time: e.payload.time,
      saved_to: e.payload.saved_to,
    });
  });
  await listen<TextRecord>("text-received", (e) => addChatMsg("in", { kind: "text", name: "", size: null, text: e.payload.text, time: e.payload.time }));

  // 恢复状态与历史
  const info = await invoke<ServerInfo | null>("server_status").catch(() => null);
  if (info) applyRunning(info);

  // 聊天记录按时间先后合并加载（get_outbox / get_texts / get_history 均为最新在前）
  const history = await invoke<FileRecord[]>("get_history").catch(() => []);
  const msgs: { dir: "in" | "out"; item: ChatMsg }[] = [];
  const outbox = await invoke<OutboxItem[]>("get_outbox").catch(() => []);
  outbox.forEach((o) => msgs.push({ dir: "out", item: o }));
  const texts = await invoke<TextRecord[]>("get_texts").catch(() => []);
  texts.forEach((t) => msgs.push({ dir: "in", item: { kind: "text", name: "", size: null, text: t.text, time: t.time } }));
  history.forEach((h) =>
    msgs.push({ dir: "in", item: { kind: "file", name: h.name, size: h.size, text: null, time: h.time, saved_to: h.saved_to } })
  );
  msgs.sort((a, b) => a.item.time - b.item.time);
  msgs.forEach((m) => addChatMsg(m.dir, m.item));
}

async function sendTextToPhone(): Promise<void> {
  const text = sendTextInput.value.trim();
  if (!text) return;
  sendTextBtn.disabled = true;
  try {
    const item = await invoke<OutboxItem>("send_text", { text });
    sendTextInput.value = "";
    addChatMsg("out", item);
  } catch (e) {
    alert(`发送失败：${e}`);
  } finally {
    sendTextBtn.disabled = false;
  }
}

async function sendFileToPhone(image: boolean): Promise<void> {
  const path = await invoke<string | null>("pick_file", { image }).catch(() => null);
  if (!path) return;
  sendTextBtn.disabled = true;
  try {
    const item = await invoke<OutboxItem>("send_file", { path });
    addChatMsg("out", item);
  } catch (e) {
    alert(`发送失败：${e}`);
  } finally {
    sendTextBtn.disabled = false;
  }
}

function closeMenu(): void {
  moreMenu.classList.remove("open");
  plusBtn.classList.remove("active");
  menuMask.classList.remove("show");
}

function addChatMsg(dir: "in" | "out", item: ChatMsg): void {
  removeEmpty(chatList);
  const li = document.createElement("li");
  li.className = `msg ${dir} ${item.kind === "file" ? "file" : ""}`;
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  if (item.kind === "file") {
    // 图片 / 视频直接在气泡内预览
    if (item.saved_to && isImage(item.name)) {
      const img = document.createElement("img");
      img.className = "msg-preview";
      img.src = convertFileSrc(item.saved_to);
      img.alt = item.name;
      img.loading = "lazy";
      img.addEventListener("click", () => openPreview(img.src, item.name!));
      bubble.appendChild(img);
    } else if (item.saved_to && isVideo(item.name)) {
      const video = document.createElement("video");
      video.className = "msg-preview video";
      video.src = convertFileSrc(item.saved_to);
      video.controls = true;
      video.preload = "metadata";
      bubble.appendChild(video);
    }
    const name = document.createElement("div");
    name.className = "msg-name";
    name.textContent = item.name;
    const size = document.createElement("div");
    size.className = "msg-size";
    size.textContent = formatSize(item.size ?? 0);
    bubble.append(name, size);
    if (dir === "in" && item.saved_to) {
      const open = document.createElement("button");
      open.className = "open-btn";
      open.textContent = "打开";
      open.addEventListener("click", () => {
        void invoke("open_in_explorer", { path: item.saved_to });
      });
      bubble.appendChild(open);
    }
  } else {
    bubble.textContent = item.text ?? "";
  }
  const meta = document.createElement("div");
  meta.className = "msg-meta";
  meta.textContent = formatClock(item.time);
  bubble.appendChild(meta);
  li.appendChild(bubble);
  chatList.appendChild(li);
  chatList.scrollTop = chatList.scrollHeight;
  while (chatList.children.length > 200) chatList.firstChild?.remove();
}

function removeEmpty(list: HTMLElement): void {
  const empty = list.querySelector(".empty");
  if (empty) empty.remove();
}

function isImage(name: string): boolean {
  return /\.(png|jpe?g|gif|webp|bmp|svg|ico)$/i.test(name);
}

function isVideo(name: string): boolean {
  return /\.(mp4|webm|mov|m4v|mkv|avi)$/i.test(name);
}

// 图片大图预览（点击放大）
const previewModal = $("previewModal");
const previewImg = $<HTMLImageElement>("previewImg");

function openPreview(src: string, title: string): void {
  previewImg.src = src;
  previewImg.alt = title;
  previewModal.classList.add("show");
}

previewModal.addEventListener("click", () => previewModal.classList.remove("show"));
window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") previewModal.classList.remove("show");
});

function formatClock(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

async function start(): Promise<void> {
  startBtn.disabled = true;
  startBtn.textContent = "启动中...";
  try {
    const info = await invoke<ServerInfo>("start_server", { saveDir: dirInput.value });
    applyRunning(info);
  } catch (e) {
    alert(`启动失败：${e}`);
  } finally {
    startBtn.disabled = false;
    startBtn.textContent = "启动服务";
  }
}

async function stop(): Promise<void> {
  stopBtn.disabled = true;
  stopBtn.textContent = "停止中...";
  try {
    await invoke("stop_server");
    applyStopped();
  } catch (e) {
    alert(`停止失败：${e}`);
    stopBtn.disabled = false;
    stopBtn.textContent = "停止服务";
  }
}

function applyRunning(info: ServerInfo): void {
  currentUrl = info.url;
  urlText.textContent = info.url;
  dirInput.value = info.save_dir;

  statusPill.className = "pill on";
  statusText.textContent = `服务已启动 · ${info.ip}`;
  chatStatus.textContent = `已连接 · ${info.ip}`;
  chatStatus.classList.add("on");
  startBtn.disabled = true;
  stopBtn.disabled = false;
  openDirBtn.disabled = false;

  urlRow.style.display = "flex";
  qrBox.style.display = "block";
  qrPlaceholder.style.display = "none";
  void invoke<string>("qr_code", { url: info.url })
    .then((svg) => {
      qrBox.innerHTML = svg;
    })
    .catch(() => {
      qrBox.innerHTML = "<p>二维码生成失败</p>";
    });
}

function applyStopped(): void {
  currentUrl = "";
  statusPill.className = "pill off";
  statusText.textContent = "未启动";
  chatStatus.textContent = "服务未启动";
  chatStatus.classList.remove("on");
  startBtn.disabled = false;
  stopBtn.disabled = true;
  openDirBtn.disabled = true;
  urlRow.style.display = "none";
  qrBox.style.display = "none";
  qrBox.innerHTML = "";
  qrPlaceholder.style.display = "flex";
}

async function chooseDir(): Promise<void> {
  const dir = await invoke<string | null>("choose_dir").catch(() => null);
  if (dir) dirInput.value = dir;
}

function openSaveDir(): void {
  void invoke("open_in_explorer", { path: dirInput.value });
}

async function copyUrl(): Promise<void> {
  try {
    await navigator.clipboard.writeText(currentUrl);
    copyBtn.textContent = "已复制";
    setTimeout(() => (copyBtn.textContent = "复制"), 1500);
  } catch {
    alert(`复制失败，请手动复制：${currentUrl}`);
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

init();

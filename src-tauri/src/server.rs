use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Multipart, Path as AxumPath, State};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{Stream, StreamExt};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tauri::{AppHandle, Emitter, Runtime};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, Mutex};
use tokio_util::io::ReaderStream;

use crate::AppState;

/// 服务运行信息
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub running: bool,
    pub url: String,
    pub ip: String,
    pub port: u16,
    pub save_dir: String,
}

/// 一条已接收的文件记录（手机 -> 电脑）
#[derive(Debug, Clone, Serialize)]
pub struct FileRecord {
    pub name: String,
    pub size: u64,
    pub saved_to: String,
    /// 接收时间（Unix 毫秒时间戳）
    pub time: u64,
}

/// 一条从手机收到的文本（手机 -> 电脑）
#[derive(Debug, Clone, Serialize)]
pub struct TextRecord {
    pub text: String,
    pub time: u64,
}

/// 一条发送到手机的内容（电脑 -> 手机），kind: "file" | "text"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxItem {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub size: Option<u64>,
    pub text: Option<String>,
    pub time: u64,
}

/// 在 Tauri 状态与 HTTP 服务之间共享的数据
#[derive(Clone)]
pub struct Shared {
    pub save_dir: Arc<Mutex<PathBuf>>,
    pub history: Arc<Mutex<Vec<FileRecord>>>,
    pub texts: Arc<Mutex<Vec<TextRecord>>>,
    pub outbox: Arc<Mutex<Vec<OutboxItem>>>,
    /// 实时推送 outbox 新条目（SSE 订阅）
    pub outbox_tx: Arc<broadcast::Sender<OutboxItem>>,
}

impl Default for Shared {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            save_dir: Arc::new(Mutex::new(PathBuf::new())),
            history: Arc::new(Mutex::new(Vec::new())),
            texts: Arc::new(Mutex::new(Vec::new())),
            outbox: Arc::new(Mutex::new(Vec::new())),
            outbox_tx: Arc::new(tx),
        }
    }
}

/// 运行中的 HTTP 服务
pub struct RunningServer {
    pub info: ServerInfo,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tauri::async_runtime::JoinHandle<()>,
}

/// axum 路由共享上下文
#[derive(Clone)]
struct Ctx<R: Runtime> {
    app: AppHandle<R>,
    shared: Shared,
    tmp_dir: PathBuf,
    outbox_dir: PathBuf,
}

const MOBILE_PAGE: &str = include_str!("../assets/mobile/index.html");

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}-{}", now_ms(), COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// 获取局域网 IPv4 地址（过滤回环地址）
fn lan_ip() -> String {
    local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

/// 默认保存目录：下载目录 -> 桌面 -> 临时目录
pub fn default_save_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(dirs::desktop_dir)
        .or_else(|| Some(std::env::temp_dir()))
        .expect("无法获取默认目录")
}

/// 生成二维码 SVG
pub fn qr_svg(url: &str) -> Result<String, String> {
    use qrcode::render::svg;
    use qrcode::QrCode;
    let code = QrCode::new(url.as_bytes()).map_err(|e| e.to_string())?;
    Ok(code
        .render::<svg::Color>()
        .min_dimensions(240, 240)
        .build())
}

/// 发送文本到手机（入队待推送）
pub async fn send_text(shared: &Shared, text: String) -> Result<OutboxItem, String> {
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err("文本不能为空".to_string());
    }
    if text.chars().count() > 10000 {
        return Err("文本过长".to_string());
    }
    let item = OutboxItem {
        id: new_id("t"),
        kind: "text".to_string(),
        name: text_preview(&text, 20),
        size: None,
        text: Some(text),
        time: now_ms(),
    };
    shared.outbox.lock().await.push(item.clone());
    let _ = shared.outbox_tx.send(item.clone());
    Ok(item)
}

/// 发送文件到手机（复制进 outbox 目录并广播）
pub async fn send_file(shared: &Shared, outbox_dir: &Path, path: PathBuf) -> Result<OutboxItem, String> {
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|e| format!("无法读取文件: {e}"))?;
    if !meta.is_file() {
        return Err("请选择一个文件".to_string());
    }
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "文件".to_string());
    let id = new_id("f");
    tokio::fs::create_dir_all(outbox_dir)
        .await
        .map_err(|e| format!("创建发送目录失败: {e}"))?;
    tokio::fs::copy(&path, outbox_dir.join(&id))
        .await
        .map_err(|e| format!("复制文件失败: {e}"))?;
    let item = OutboxItem {
        id,
        kind: "file".to_string(),
        name,
        size: Some(meta.len()),
        text: None,
        time: now_ms(),
    };
    shared.outbox.lock().await.push(item.clone());
    let _ = shared.outbox_tx.send(item.clone());
    Ok(item)
}

/// 启动 HTTP 服务，监听 0.0.0.0 随机空闲端口
pub async fn start<R: Runtime>(app: AppHandle<R>, state: &AppState) -> Result<ServerInfo, String> {
    if let Some(running) = state.server.lock().await.as_ref() {
        return Ok(running.info.clone());
    }

    let save_dir = state.shared.save_dir.lock().await.clone();

    // 清理历史遗留的分片临时文件与发送目录
    let _ = std::fs::remove_dir_all(&state.tmp_dir);
    let _ = std::fs::remove_dir_all(&state.outbox_dir);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("端口绑定失败: {e}"))?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let ip = lan_ip();
    let url = format!("http://{ip}:{port}");

    let ctx = Arc::new(Ctx {
        app,
        shared: state.shared.clone(),
        tmp_dir: state.tmp_dir.clone(),
        outbox_dir: state.outbox_dir.clone(),
    });

    let router = Router::new()
        .route("/", get(index_page))
        .route("/favicon.ico", get(favicon))
        .route("/api/health", get(health))
        .route("/upload", post(upload::<R>))
        .route("/api/text", post(receive_text::<R>))
        .route("/api/outbox", get(outbox_list::<R>))
        .route("/api/events", get(events_sse::<R>))
        .route("/api/download/{id}", get(download::<R>))
        .layer(DefaultBodyLimit::disable())
        .with_state(ctx);

    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tauri::async_runtime::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });

    let info = ServerInfo {
        running: true,
        url,
        ip,
        port,
        save_dir: save_dir.display().to_string(),
    };
    *state.server.lock().await = Some(RunningServer {
        info: info.clone(),
        shutdown: tx,
        task,
    });
    Ok(info)
}

/// 停止 HTTP 服务
pub async fn stop(state: &AppState) -> Result<(), String> {
    let mut guard = state.server.lock().await;
    if let Some(server) = guard.take() {
        let _ = server.shutdown.send(());
        let _ = server.task.await;
    }
    Ok(())
}

async fn index_page() -> impl IntoResponse {
    Html(MOBILE_PAGE)
}

async fn favicon() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// 手机 -> 电脑：接收文本
async fn receive_text<R: Runtime>(
    State(ctx): State<Arc<Ctx<R>>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let text = payload
        .get("text")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if text.is_empty() {
        return err_response(StatusCode::BAD_REQUEST, "文本不能为空".to_string());
    }
    if text.chars().count() > 10000 {
        return err_response(StatusCode::BAD_REQUEST, "文本过长".to_string());
    }
    let record = TextRecord { text: text.clone(), time: now_ms() };
    ctx.shared.texts.lock().await.push(record.clone());
    let _ = ctx.app.emit("text-received", &record);
    (StatusCode::OK, Json(json!({ "ok": true })))
}

/// 电脑 -> 手机：当前待发送列表
async fn outbox_list<R: Runtime>(State(ctx): State<Arc<Ctx<R>>>) -> impl IntoResponse {
    Json(ctx.shared.outbox.lock().await.clone())
}

/// 电脑 -> 手机：SSE 实时推送 outbox
async fn events_sse<R: Runtime>(
    State(ctx): State<Arc<Ctx<R>>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = ctx.shared.outbox_tx.subscribe();
    let snapshot = ctx.shared.outbox.lock().await.clone();
    let iter = snapshot.into_iter();
    let stream = futures_util::stream::unfold((iter, rx), |(mut iter, mut rx)| async move {
        if let Some(item) = iter.next() {
            return Some((item, (iter, rx)));
        }
        loop {
            match rx.recv().await {
                Ok(item) => return Some((item, (iter, rx))),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => return None,
            }
        }
    })
    .map(|item| {
        let data = serde_json::to_string(&item).unwrap_or_default();
        Ok::<_, Infallible>(Event::default().event("outbox").data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// 电脑 -> 手机：下载 outbox 中的文件
async fn download<R: Runtime>(
    State(ctx): State<Arc<Ctx<R>>>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let item = ctx
        .shared
        .outbox
        .lock()
        .await
        .iter()
        .find(|i| i.id == id && i.kind == "file")
        .cloned();
    let Some(item) = item else {
        return err_response(StatusCode::NOT_FOUND, "文件不存在".to_string()).into_response();
    };
    let path = ctx.outbox_dir.join(&item.id);
    match tokio::fs::File::open(&path).await {
        Ok(file) => {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
            let encoded = utf8_percent_encode(&item.name, NON_ALPHANUMERIC).to_string();
            let value = format!(
                "attachment; filename=\"{}\"; filename*=UTF-8''{encoded}",
                ascii_filename(&item.name)
            );
            headers.insert(CONTENT_DISPOSITION, value.parse().unwrap());
            (StatusCode::OK, headers, axum::body::Body::from_stream(ReaderStream::new(file)))
                .into_response()
        }
        Err(_) => err_response(StatusCode::NOT_FOUND, "文件不存在".to_string()).into_response(),
    }
}

/// 分片上传接口
///
/// multipart 字段: filename / fileId / index / total / file
/// 最后一个分片到达时自动合并为完整文件，并通过 Tauri 事件通知桌面端。
async fn upload<R: Runtime>(State(ctx): State<Arc<Ctx<R>>>, mut multipart: Multipart) -> impl IntoResponse {
    let mut filename: Option<String> = None;
    let mut file_id: Option<String> = None;
    let mut index: Option<usize> = None;
    let mut total: Option<usize> = None;
    let mut data: Option<Bytes> = None;

    while let Some(field) = match multipart.next_field().await {
        Ok(f) => f,
        Err(e) => {
            return err_response(StatusCode::BAD_REQUEST, format!("解析请求失败: {e}"));
        }
    } {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "filename" => filename = field.text().await.ok(),
            "fileId" => file_id = field.text().await.ok(),
            "index" => index = field.text().await.ok().and_then(|s| s.parse().ok()),
            "total" => total = field.text().await.ok().and_then(|s| s.parse().ok()),
            "file" => {
                data = match field.bytes().await {
                    Ok(b) => Some(b),
                    Err(e) => {
                        return err_response(StatusCode::BAD_REQUEST, format!("读取文件分片失败: {e}"));
                    }
                }
            }
            _ => {}
        }
    }

    let (filename, file_id, index, total, data) = match (filename, file_id, index, total, data) {
        (Some(f), Some(id), Some(i), Some(t), Some(d)) => (f, id, i, t, d),
        _ => return err_response(StatusCode::BAD_REQUEST, "缺少必要的上传字段".to_string()),
    };
    if total == 0 || index >= total {
        return err_response(StatusCode::BAD_REQUEST, "分片参数无效".to_string());
    }

    match save_chunk(&ctx.shared, &ctx.tmp_dir, &filename, &file_id, index, total, &data).await {
        Ok(Some(record)) => {
            let _ = ctx.app.emit("file-received", &record);
            (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "saved": true,
                    "name": record.name,
                    "path": record.saved_to,
                })),
            )
        }
        Ok(None) => (StatusCode::OK, Json(json!({ "ok": true, "saved": false }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e })),
        ),
    }
}

fn err_response(status: StatusCode, msg: String) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(json!({ "ok": false, "error": msg })))
}

/// 保存分片；当所有分片到齐后合并为完整文件，返回文件记录
async fn save_chunk(
    shared: &Shared,
    tmp_dir: &Path,
    filename: &str,
    file_id: &str,
    index: usize,
    total: usize,
    data: &Bytes,
) -> Result<Option<FileRecord>, String> {
    let name = sanitize_filename(filename);
    let chunk_dir = tmp_dir.join(file_id);
    tokio::fs::create_dir_all(&chunk_dir)
        .await
        .map_err(|e| format!("创建临时目录失败: {e}"))?;

    let part_path = chunk_dir.join(format!("{:08}.part", index));
    tokio::fs::write(&part_path, data)
        .await
        .map_err(|e| format!("写入分片失败: {e}"))?;

    // 统计已接收的分片数量
    let mut parts: Vec<(usize, PathBuf)> = Vec::new();
    let mut rd = tokio::fs::read_dir(&chunk_dir)
        .await
        .map_err(|e| e.to_string())?;
    while let Some(entry) = rd.next_entry().await.map_err(|e| e.to_string())? {
        let path = entry.path();
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if let Ok(n) = stem.parse::<usize>() {
                parts.push((n, path));
            }
        }
    }

    if parts.len() < total {
        return Ok(None); // 还有分片未到达
    }

    // 所有分片到齐，按顺序合并
    parts.sort_by_key(|(n, _)| *n);
    let save_dir = shared.save_dir.lock().await.clone();
    tokio::fs::create_dir_all(&save_dir)
        .await
        .map_err(|e| format!("创建保存目录失败: {e}"))?;
    let dest = unique_path(&save_dir, &name);

    let mut out = tokio::fs::File::create(&dest)
        .await
        .map_err(|e| format!("创建文件失败: {e}"))?;
    let mut size = 0u64;
    for (_, path) in &parts {
        let bytes = tokio::fs::read(path).await.map_err(|e| e.to_string())?;
        out.write_all(&bytes).await.map_err(|e| e.to_string())?;
        size += bytes.len() as u64;
    }
    out.flush().await.map_err(|e| e.to_string())?;
    out.sync_all().await.map_err(|e| e.to_string())?;
    drop(out);

    let _ = tokio::fs::remove_dir_all(&chunk_dir).await;

    let record = FileRecord {
        name: name.clone(),
        size,
        saved_to: dest.display().to_string(),
        time: now_ms(),
    };
    shared.history.lock().await.push(record.clone());
    Ok(Some(record))
}

/// 清洗文件名：去除路径、非法字符，防止路径穿越
fn sanitize_filename(raw: &str) -> String {
    let base = raw
        .replace('\\', "/")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "unnamed".to_string()
    } else {
        cleaned
    }
}

/// 提取文本预览（截断）
fn text_preview(text: &str, max: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max {
        text.to_string()
    } else {
        let mut s: String = chars[..max].iter().collect();
        s.push('…');
        s
    }
}

/// 转为 ASCII 安全文件名（用于 Content-Disposition 回退名）
fn ascii_filename(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// 目标文件已存在时自动追加序号，如 `a.txt` -> `a (1).txt`
fn unique_path(dir: &Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let stem = Path::new(name)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| name.to_string());
    let ext = Path::new(name)
        .extension()
        .map(|s| s.to_string_lossy().to_string());
    let mut i = 1;
    loop {
        let new_name = match &ext {
            Some(e) if !e.is_empty() => format!("{stem} ({i}).{e}"),
            _ => format!("{stem} ({i})"),
        };
        let candidate = dir.join(new_name);
        if !candidate.exists() {
            return candidate;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use tauri::test::mock_app;
    use tokio::sync::Mutex;

    async fn test_state(tag: &str) -> (AppState, PathBuf, PathBuf) {
        let save_dir = std::env::temp_dir().join(format!("iconnect-test-save-{}-{tag}", std::process::id()));
        let tmp_dir = std::env::temp_dir().join(format!("iconnect-test-tmp-{}-{tag}", std::process::id()));
        let outbox_dir =
            std::env::temp_dir().join(format!("iconnect-test-outbox-{}-{tag}", std::process::id()));
        let state = AppState {
            server: Mutex::new(None),
            shared: Shared::default(),
            tmp_dir: tmp_dir.clone(),
            outbox_dir: outbox_dir.clone(),
        };
        *state.shared.save_dir.lock().await = save_dir.clone();
        (state, save_dir, tmp_dir)
    }

    #[test]
    fn chunked_upload_roundtrip() {
        tauri::async_runtime::block_on(async {
            let app = mock_app();
            let handle = app.handle().clone();
            let (state, save_dir, tmp_dir) = test_state("chunk").await;

            let info = start(handle, &state).await.expect("服务启动失败");
            assert!(info.url.starts_with("http://"), "服务地址无效: {}", info.url);

            let client = reqwest::Client::new();
            let base = &info.url;

            // 分片上传一个含中文文件名的文件（3 个分片）
            let chunks: [&[u8]; 3] = [b"hello ", b"world ", b"!"];
            for (i, chunk) in chunks.iter().enumerate() {
                let form = reqwest::multipart::Form::new()
                    .text("filename", "测试.txt")
                    .text("fileId", "test-file-1")
                    .text("index", i.to_string())
                    .text("total", "3")
                    .part(
                        "file",
                        reqwest::multipart::Part::bytes(chunk.to_vec()).file_name("测试.txt"),
                    );
                let resp = client
                    .post(format!("{base}/upload"))
                    .multipart(form)
                    .send()
                    .await
                    .expect("上传请求失败");
                assert_eq!(resp.status(), 200, "分片 {i} 上传失败");
            }

            // 校验合并结果与历史记录
            let content =
                tokio::fs::read(save_dir.join("测试.txt")).await.expect("保存的文件不存在");
            assert_eq!(content, b"hello world !");

            let history = state.shared.history.lock().await;
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].name, "测试.txt");
            assert_eq!(history[0].size, 13);
            drop(history);

            stop(&state).await.expect("停止服务失败");
            let _ = tokio::fs::remove_dir_all(&save_dir).await;
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            let _ = tokio::fs::remove_dir_all(&state.outbox_dir).await;
        });
    }

    #[test]
    fn outbox_download_roundtrip() {
        tauri::async_runtime::block_on(async {
            let app = mock_app();
            let handle = app.handle().clone();
            let (state, save_dir, _tmp_dir) = test_state("outbox").await;

            // 准备源文件（放在保存目录，start 不会清理它）
            let src = save_dir.join("source.txt");
            tokio::fs::create_dir_all(&save_dir).await.unwrap();
            tokio::fs::write(&src, b"pc -> phone content").await.unwrap();

            // 先启动服务（会清理 outbox 遗留），再发送文本与文件
            let info = start(handle, &state).await.expect("服务启动失败");
            let client = reqwest::Client::new();

            let text_item = send_text(&state.shared, "你好，这是测试文本".to_string())
                .await
                .expect("发送文本失败");
            assert_eq!(text_item.kind, "text");
            let file_item = send_file(&state.shared, &state.outbox_dir, src.clone())
                .await
                .expect("发送文件失败");
            assert_eq!(file_item.kind, "file");
            assert_eq!(file_item.name, "source.txt");
            assert_eq!(file_item.size, Some(19));

            // 验证 outbox 列表与下载

            let list: Vec<OutboxItem> = client
                .get(format!("{}/api/outbox", info.url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(list.len(), 2);

            let resp = client
                .get(format!("{}/api/download/{}", info.url, file_item.id))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let bytes = resp.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), b"pc -> phone content");

            stop(&state).await.expect("停止服务失败");
            let _ = tokio::fs::remove_dir_all(&src).await;
            let _ = tokio::fs::remove_dir_all(&save_dir).await;
            let _ = tokio::fs::remove_dir_all(&state.outbox_dir).await;
        });
    }

    #[test]
    fn text_receive_roundtrip() {
        tauri::async_runtime::block_on(async {
            let app = mock_app();
            let handle = app.handle().clone();
            let (state, _save_dir, _tmp_dir) = test_state("text").await;

            let info = start(handle, &state).await.expect("服务启动失败");
            let client = reqwest::Client::new();

            // 空文本应被拒绝
            let resp = client
                .post(format!("{}/api/text", info.url))
                .json(&json!({ "text": "   " }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400);

            // 正常发送文本
            let resp = client
                .post(format!("{}/api/text", info.url))
                .json(&json!({ "text": "你好，局域网传输" }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);

            let texts = state.shared.texts.lock().await;
            assert_eq!(texts.len(), 1);
            assert_eq!(texts[0].text, "你好，局域网传输");
            drop(texts);

            stop(&state).await.expect("停止服务失败");
            let _ = tokio::fs::remove_dir_all(&state.outbox_dir).await;
        });
    }

    #[test]
    fn filename_sanitization() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("a<b>:c|d?.txt"), "a_b__c_d_.txt");
        assert_eq!(sanitize_filename("  正常 文件.txt  "), "正常 文件.txt");
        assert_eq!(sanitize_filename(".."), "unnamed");
        assert_eq!(text_preview("1234567890", 5), "12345…");
        assert_eq!(text_preview("短文本", 20), "短文本");
    }

    #[test]
    fn qr_svg_generation() {
        let svg = qr_svg("http://192.168.1.100:8080").expect("二维码生成失败");
        assert!(svg.contains("<svg"), "输出应为 SVG");
        assert!(svg.contains("<path"), "SVG 应包含二维码模块");
    }

    #[test]
    fn unique_path_naming() {
        let dir = std::env::temp_dir().join(format!("iconnect-test-name-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"1").unwrap();
        assert_eq!(unique_path(&dir, "a.txt").file_name().unwrap(), "a (1).txt");
        assert_eq!(unique_path(&dir, "b.txt").file_name().unwrap(), "b.txt");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

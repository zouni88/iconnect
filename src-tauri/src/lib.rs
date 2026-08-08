mod server;

use std::path::PathBuf;

use tauri::{AppHandle, State};
use tokio::sync::Mutex;

use crate::server::{FileRecord, OutboxItem, ServerInfo, Shared, TextRecord};

/// 应用全局状态
pub struct AppState {
    pub server: Mutex<Option<server::RunningServer>>,
    pub shared: Shared,
    pub tmp_dir: PathBuf,
    pub outbox_dir: PathBuf,
}

/// 启动 HTTP 服务（幂等：已在运行时直接返回当前信息）
#[tauri::command]
async fn start_server(
    app: AppHandle,
    state: State<'_, AppState>,
    save_dir: Option<String>,
) -> Result<ServerInfo, String> {
    if let Some(dir) = save_dir {
        if !dir.trim().is_empty() {
            *state.shared.save_dir.lock().await = PathBuf::from(dir);
        }
    }
    server::start(app, state.inner()).await
}

/// 停止 HTTP 服务
#[tauri::command]
async fn stop_server(state: State<'_, AppState>) -> Result<(), String> {
    server::stop(state.inner()).await
}

/// 查询服务状态
#[tauri::command]
async fn server_status(state: State<'_, AppState>) -> Result<Option<ServerInfo>, String> {
    Ok(state.server.lock().await.as_ref().map(|s| s.info.clone()))
}

/// 获取默认保存目录
#[tauri::command]
fn default_save_dir() -> Result<String, String> {
    Ok(server::default_save_dir().display().to_string())
}

/// 弹出系统目录选择框
#[tauri::command]
async fn choose_dir(app: AppHandle) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::{DialogExt, FilePath};
    let picked = app.dialog().file().blocking_pick_folder();
    Ok(picked.map(|p| match p {
        FilePath::Path(p) => p.display().to_string(),
        FilePath::Url(u) => u.to_string(),
    }))
}

/// 弹出系统文件选择框（用于发送到手机）；image=true 时仅显示常见图片格式
#[tauri::command]
async fn pick_file(app: AppHandle, image: Option<bool>) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::{DialogExt, FilePath};
    let mut picker = app.dialog().file();
    if image.unwrap_or(false) {
        picker = picker.add_filter(
            "图片",
            &["png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico"],
        );
    }
    let picked = picker.blocking_pick_file();
    Ok(picked.map(|p| match p {
        FilePath::Path(p) => p.display().to_string(),
        FilePath::Url(u) => u.to_string(),
    }))
}

/// 发送文本到手机
#[tauri::command]
async fn send_text(state: State<'_, AppState>, text: String) -> Result<OutboxItem, String> {
    server::send_text(&state.shared, text).await
}

/// 发送文件到手机
#[tauri::command]
async fn send_file(state: State<'_, AppState>, path: String) -> Result<OutboxItem, String> {
    server::send_file(&state.shared, &state.outbox_dir, PathBuf::from(path)).await
}

/// 获取已发送到手机的列表（最新的在前）
#[tauri::command]
async fn get_outbox(state: State<'_, AppState>) -> Result<Vec<OutboxItem>, String> {
    let outbox = state.shared.outbox.lock().await;
    Ok(outbox.iter().rev().cloned().collect())
}

/// 获取手机发来的文本（最新的在前）
#[tauri::command]
async fn get_texts(state: State<'_, AppState>) -> Result<Vec<TextRecord>, String> {
    let texts = state.shared.texts.lock().await;
    Ok(texts.iter().rev().cloned().collect())
}

/// 获取传输记录（最新的在前）
#[tauri::command]
async fn get_history(state: State<'_, AppState>) -> Result<Vec<FileRecord>, String> {
    let history = state.shared.history.lock().await;
    Ok(history.iter().rev().cloned().collect())
}

/// 在系统文件管理器中打开指定路径
#[tauri::command]
fn open_in_explorer(path: String) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    let cmd = "explorer";
    #[cfg(target_os = "macos")]
    let cmd = "open";
    #[cfg(target_os = "linux")]
    let cmd = "xdg-open";
    std::process::Command::new(cmd)
        .arg(&path)
        .spawn()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// 生成二维码 SVG
#[tauri::command]
fn qr_code(url: String) -> Result<String, String> {
    server::qr_svg(&url)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState {
            server: Mutex::new(None),
            shared: Shared::default(),
            tmp_dir: std::env::temp_dir().join("iconnect-upload"),
            outbox_dir: std::env::temp_dir().join("iconnect-outbox"),
        })
        .invoke_handler(tauri::generate_handler![
            start_server,
            stop_server,
            server_status,
            default_save_dir,
            choose_dir,
            pick_file,
            send_text,
            send_file,
            get_outbox,
            get_texts,
            get_history,
            open_in_explorer,
            qr_code,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

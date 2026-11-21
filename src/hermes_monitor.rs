//! Hermes 定时调度与 Vikunja 待办监控集成模块。
//! 负责从 Hermes Bridge 服务 (默认 http://10.10.10.103:8686) 定时拉取
//! Hermes 19项定时任务健康度、DMS 保活状态以及 Vikunja 待办清单。

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

/// 客户端配置，持久化在 %APPDATA%\Daozai\hermes_bridge.json
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HermesBridgeConfig {
    pub enabled: bool,
    pub server_url: String,
    pub poll_interval_secs: u64,
    pub alert_on_error: bool,
}

impl Default for HermesBridgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_url: "http://10.10.10.103:8686".to_string(),
            poll_interval_secs: 30,
            alert_on_error: true,
        }
    }
}

impl HermesBridgeConfig {
    pub fn config_path() -> PathBuf {
        crate::model::config_dir().join("hermes_bridge.json")
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(cfg) = serde_json::from_str::<Self>(&content) {
                return cfg;
            }
        }
        let default_cfg = Self::default();
        let _ = default_cfg.save();
        default_cfg
    }

    pub fn save(&self) -> Result<(), String> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let json = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        fs::write(&path, json).map_err(|e| e.to_string())
    }
}

// ==================== 接口响应结构体 ====================

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DmsSummary {
    pub id: Option<String>,
    pub name: Option<String>,
    pub status: Option<String>,
    pub state: Option<String>,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
    pub last_error: Option<String>,
    pub schedule: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HermesFailedJob {
    pub id: Option<String>,
    pub name: Option<String>,
    pub last_run_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HermesJobItem {
    pub id: Option<String>,
    pub name: Option<String>,
    pub state: Option<String>,
    pub schedule: Option<String>,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
    pub last_status: Option<String>,
    pub has_error: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HermesData {
    pub total_jobs: usize,
    pub active_jobs: usize,
    pub paused_jobs: usize,
    pub completed_jobs: usize,
    pub has_error: bool,
    pub failed_count: usize,
    #[serde(default)]
    pub failed_jobs: Vec<HermesFailedJob>,
    pub dms: Option<DmsSummary>,
    #[serde(default)]
    pub jobs: Vec<HermesJobItem>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VikunjaTaskItem {
    pub id: i64,
    pub title: String,
    pub due_date: Option<String>,
    pub priority: Option<i32>,
    pub project_id: Option<i64>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TodosData {
    pub total_unfinished: usize,
    #[serde(default)]
    pub items: Vec<VikunjaTaskItem>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BridgeOverview {
    pub server_time: Option<String>,
    pub hermes: HermesData,
    pub todos: TodosData,
}

// ==================== 客户端后台轮询服务 ====================

pub struct HermesService {
    pub config: HermesBridgeConfig,
    pub latest_data: Arc<Mutex<Option<BridgeOverview>>>,
    pub last_poll_time: Option<Instant>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub is_busy: Arc<AtomicBool>,
    trigger_tx: Sender<()>,
    // 异步测试连接通道
    test_rx: Option<Receiver<Result<String, String>>>,
    pub test_result: Option<Result<String, String>>,
}

impl HermesService {
    pub fn new() -> Self {
        let config = HermesBridgeConfig::load();
        let latest_data = Arc::new(Mutex::new(None));
        let last_error = Arc::new(Mutex::new(None));
        let is_busy = Arc::new(AtomicBool::new(false));
        let (trigger_tx, trigger_rx) = channel::<()>();

        let cfg_clone = config.clone();
        let data_store = Arc::clone(&latest_data);
        let error_store = Arc::clone(&last_error);
        let busy_flag = Arc::clone(&is_busy);

        // 启动后台工作线程
        std::thread::Builder::new()
            .name("daozai-hermes-worker".into())
            .spawn(move || {
                let mut current_cfg = cfg_clone;
                loop {
                    if current_cfg.enabled && !current_cfg.server_url.is_empty() {
                        busy_flag.store(true, Ordering::SeqCst);
                        let url = format!("{}/api/overview", current_cfg.server_url.trim_end_matches('/'));
                        let res = ureq::get(&url)
                            .timeout(Duration::from_secs(4))
                            .call();

                        match res {
                            Ok(resp) => {
                                match serde_json::from_reader::<_, BridgeOverview>(resp.into_reader().take(2 * 1024 * 1024)) {
                                    Ok(overview) => {
                                        *data_store.lock().unwrap() = Some(overview);
                                        *error_store.lock().unwrap() = None;
                                    }
                                    Err(e) => {
                                        *error_store.lock().unwrap() = Some(format!("JSON解析失败: {e}"));
                                    }
                                }
                            }
                            Err(e) => {
                                *error_store.lock().unwrap() = Some(format!("请求失败: {e}"));
                            }
                        }
                        busy_flag.store(false, Ordering::SeqCst);
                    }

                    // 等待下一次轮询或手动刷新触发
                    let interval = Duration::from_secs(current_cfg.poll_interval_secs.max(5));
                    let _ = trigger_rx.recv_timeout(interval);
                    // 重新加载配置（支持热更新）
                    current_cfg = HermesBridgeConfig::load();
                }
            })
            .expect("Failed to spawn hermes worker thread");

        Self {
            config,
            latest_data,
            last_poll_time: None,
            last_error,
            is_busy,
            trigger_tx,
            test_rx: None,
            test_result: None,
        }
    }

    /// 手动触发一次立即拉取
    pub fn trigger_refresh(&self) {
        let _ = self.trigger_tx.send(());
    }

    /// 获取最新的数据快照
    pub fn snapshot(&self) -> Option<BridgeOverview> {
        self.latest_data.lock().unwrap().clone()
    }

    /// 获取最近的错误信息
    pub fn error_msg(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    /// 触发测试连接
    pub fn test_connection(&mut self, url: &str) {
        let (tx, rx) = channel();
        self.test_rx = Some(rx);
        self.test_result = None;
        let test_url = format!("{}/api/overview", url.trim_end_matches('/'));

        std::thread::spawn(move || {
            let res = ureq::get(&test_url)
                .timeout(Duration::from_secs(3))
                .call();
            match res {
                Ok(resp) => {
                    match serde_json::from_reader::<_, BridgeOverview>(resp.into_reader().take(2 * 1024 * 1024)) {
                        Ok(data) => {
                            let msg = format!(
                                "连接成功！Hermes任务: {}项, 未完成待办: {}项",
                                data.hermes.total_jobs,
                                data.todos.total_unfinished
                            );
                            let _ = tx.send(Ok(msg));
                        }
                        Err(e) => {
                            let _ = tx.send(Err(format!("响应非预期JSON: {e}")));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(format!("无法连接到服务器: {e}")));
                }
            }
        });
    }

    /// 刷新测试连接结果
    pub fn check_test_result(&mut self) {
        if let Some(rx) = &self.test_rx {
            if let Ok(res) = rx.try_recv() {
                self.test_result = Some(res);
                self.test_rx = None;
            }
        }
    }

    /// 同步未完成的 Vikunja 待办到本地 TodoList
    pub fn sync_into_todos(&self, todos: &mut crate::model::TodoList) -> bool {
        let Some(snapshot) = self.snapshot() else { return false; };
        let mut changed = false;
        for vt in &snapshot.todos.items {
            let prefix_title = vt.title.trim();
            if prefix_title.is_empty() { continue; }

            // 检查是否已存在同名待办
            let exists = todos.items.iter().any(|item| item.text == prefix_title);
            if !exists {
                let mut item = crate::model::TodoItem::new(prefix_title);
                // 转换截止时间格式 2026-12-04T18:00:00+08:00 -> 2026-12-04 18:00
                if let Some(due) = &vt.due_date {
                    if due.len() >= 16 && !due.starts_with("0001-01-01") {
                        let date_part = &due[0..10];
                        let time_part = &due[11..16];
                        item.due = Some(format!("{date_part} {time_part}"));
                    }
                }
                todos.items.push(item);
                changed = true;
            }
        }
        if changed {
            todos.save();
        }
        changed
    }
}

/// 打开系统默认浏览器
pub fn open_url_in_browser(url: &str) {
    #[cfg(windows)]
    {
        use std::process::Command;
        let _ = Command::new("cmd").args(["/c", "start", "", url]).spawn();
    }
    #[cfg(not(windows))]
    {
        use std::process::Command;
        let _ = Command::new("xdg-open").arg(url).spawn();
    }
}

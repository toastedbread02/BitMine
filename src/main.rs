use bevy::prelude::*;
use bevy::text::FontSize;
use num_bigint::BigUint;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ===================== Pool candidates (Solo CKPool regional endpoints) =====================
// No registration needed, wallet address is the username. Note: Solo CKPool's own site
// discourages CPU mining (fine for learning/testing, just don't leave it grinding 24/7).
struct PoolCandidate {
    label: &'static str,
    host: &'static str,
    port: u16,
}

const POOL_CANDIDATES: &[PoolCandidate] = &[
    PoolCandidate { label: "US West", host: "uwsolo.ckpool.org", port: 3333 },
    PoolCandidate { label: "US East", host: "uesolo.ckpool.org", port: 3333 },
    PoolCandidate { label: "Germany", host: "eusolo.ckpool.org", port: 3333 },
    PoolCandidate { label: "Singapore", host: "sgsolo.ckpool.org", port: 3333 },
    PoolCandidate { label: "Australia", host: "ausolo.ckpool.org", port: 3333 },
];

const WORKER_NAME: &str = "rustminer1";

const HISTORY_LEN: usize = 24;
const LOG_LEN: usize = 14;
const PICKAXE_FRAME_COUNT: usize = 11; // 4x3 grid, last cell blank

const BG: Color = Color::srgb(0.06, 0.06, 0.08);
const PANEL: Color = Color::srgb(0.11, 0.11, 0.14);
const SIDEBAR: Color = Color::srgb(0.08, 0.08, 0.1);
const TAB_ACTIVE: Color = Color::srgb(0.18, 0.16, 0.1);
const TAB_INACTIVE: Color = Color::srgb(0.08, 0.08, 0.1);
const GOLD: Color = Color::srgb(0.95, 0.72, 0.25);
const GREEN: Color = Color::srgb(0.35, 0.85, 0.45);
const RED: Color = Color::srgb(0.9, 0.4, 0.4);
const GRAY: Color = Color::srgb(0.55, 0.55, 0.6);
const DIM_BORDER: Color = Color::srgba(0.95, 0.72, 0.25, 0.25);

// ===================== Stratum protocol types =====================

#[derive(Clone)]
struct Job {
    job_id: String,
    prevhash: Vec<u8>,
    coinb1: Vec<u8>,
    coinb2: Vec<u8>,
    merkle_branch: Vec<Vec<u8>>,
    version: Vec<u8>,
    nbits: Vec<u8>,
    ntime: Vec<u8>,
}

struct PoolState {
    job: Mutex<Option<Job>>,
    difficulty: Mutex<f64>,
    extranonce1: Mutex<Vec<u8>>,
    extranonce2_size: Mutex<usize>,
}

enum MinerEvent {
    Connected(String),
    Disconnected,
    Difficulty(f64),
    NewJob(String),
    HashUpdate { hashes: u64, hashrate: u64 },
    ShareAccepted,
    ShareRejected(String),
    Log(String),
}

// ===================== Bevy resources / components =====================

#[derive(Resource)]
struct EventReceiver(Mutex<Receiver<MinerEvent>>);

#[derive(Resource, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Dashboard,
    Log,
    Pool,
}

#[derive(Resource)]
struct ActiveTab(Tab);

#[derive(Resource, Default)]
struct HashHistory(VecDeque<u64>);

#[derive(Resource, Default)]
struct ShareLog(VecDeque<String>);

#[derive(Resource, Default)]
struct PoolStatus {
    connected: bool,
    pool_addr: String,
    hashes: u64,
    hashrate: u64,
    difficulty: f64,
    shares_accepted: u64,
    shares_rejected: u64,
    job_id: String,
}

#[derive(Component)]
struct TabButton(Tab);
#[derive(Component)]
struct TabButtonLabel(Tab);
#[derive(Component)]
struct TabPanel(Tab);

#[derive(Component, Clone, Copy, PartialEq, Eq)]
enum DashField {
    Status,
    Hashes,
    Hashrate,
    Shares,
    Difficulty,
}
#[derive(Component)]
struct WorkersValue;
#[derive(Component)]
struct ActivityFill;
#[derive(Component)]
struct HistoryBar(usize);
#[derive(Component)]
struct LogText;

#[derive(Component, Clone, Copy, PartialEq, Eq)]
enum PoolField {
    Conn,
    Addr,
    Job,
    Accepted,
    Rejected,
}

#[derive(Component)]
struct PickaxeAnim {
    timer: Timer,
}

fn main() {
    let demo_mode = std::env::args().any(|a| a == "--demo");

    let wallet_address = if demo_mode {
        println!("Running in --demo mode: no address needed, no pool connection made.");
        "demo".to_string()
    } else {
        load_or_prompt_address()
    };

    let (tx, rx) = channel::<MinerEvent>();

    if demo_mode {
        let tx = tx.clone();
        thread::spawn(move || run_demo(tx));
    } else {
        println!("Probing pool endpoints for lowest latency...");
        let (host, port, label) = pick_best_pool(POOL_CANDIDATES);
        println!("Selected pool: {label} ({host}:{port})");
        let tx = tx.clone();
        let host = host.clone();
        let wallet_address = wallet_address.clone();
        thread::spawn(move || run_stratum(tx, host, port, label.to_string(), wallet_address));
    }

    App::new()
        .add_plugins(DefaultPlugins)
        .insert_resource(EventReceiver(Mutex::new(rx)))
        .insert_resource(ActiveTab(Tab::Dashboard))
        .insert_resource(HashHistory::default())
        .insert_resource(ShareLog::default())
        .insert_resource(PoolStatus::default())
        .add_systems(Startup, setup)
        .add_systems(
            Update,
            (
                receive_events,
                update_dashboard_text,
                update_pool_text,
                update_history_bars,
                update_log_text,
                handle_tab_clicks,
                apply_tab_visibility,
                animate_pickaxe,
            ),
        )
        .run();
}

// ===================== Wallet address: prompt + save =====================

fn config_path() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".rust_miner_config.json")
}

fn looks_like_btc_address(addr: &str) -> bool {
    let addr = addr.trim();
    let len_ok = addr.len() >= 26 && addr.len() <= 62;
    let prefix_ok = addr.starts_with('1') || addr.starts_with('3') || addr.starts_with("bc1");
    len_ok && prefix_ok
}

fn load_or_prompt_address() -> String {
    let path = config_path();
    if let Ok(contents) = fs::read_to_string(&path) {
        if let Ok(v) = serde_json::from_str::<Value>(&contents) {
            if let Some(addr) = v.get("address").and_then(|a| a.as_str()) {
                if looks_like_btc_address(addr) {
                    println!("Using saved address from {}", path.display());
                    return addr.to_string();
                }
            }
        }
    }

    loop {
        print!("Enter your Bitcoin address: ");
        io::stdout().flush().ok();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            continue;
        }
        let addr = input.trim().to_string();
        if !looks_like_btc_address(&addr) {
            println!("That does not look like a valid Bitcoin address (expected to start with 1, 3, or bc1, and be 26-62 characters). Try again.");
            continue;
        }
        let payload = serde_json::json!({ "address": addr });
        if let Err(e) = fs::write(&path, serde_json::to_string_pretty(&payload).unwrap()) {
            println!("Warning: could not save address to {}: {e}", path.display());
        } else {
            println!("Saved to {} - you will not be asked again.", path.display());
        }
        return addr;
    }
}

// ===================== Pool selection: latency probe =====================

fn pick_best_pool(candidates: &'static [PoolCandidate]) -> (String, u16, &'static str) {
    let mut best: Option<(Duration, &PoolCandidate)> = None;
    for candidate in candidates {
        let start = Instant::now();
        let addr = resolve_first(candidate.host, candidate.port);
        let result = TcpStream::connect_timeout(&addr, Duration::from_secs(2));
        match result {
            Ok(_) => {
                let elapsed = start.elapsed();
                println!("  {} ({}): {:?}", candidate.label, candidate.host, elapsed);
                if best.as_ref().map(|(d, _)| elapsed < *d).unwrap_or(true) {
                    best = Some((elapsed, candidate));
                }
            }
            Err(e) => println!("  {} ({}): unreachable ({e})", candidate.label, candidate.host),
        }
    }
    match best {
        Some((_, c)) => (c.host.to_string(), c.port, c.label),
        None => (candidates[0].host.to_string(), candidates[0].port, candidates[0].label),
    }
}

fn resolve_first(host: &str, port: u16) -> std::net::SocketAddr {
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .unwrap_or_else(|| "127.0.0.1:3333".parse().unwrap())
}

fn setup(
    mut commands: Commands,
    asset_server: Res<AssetServer>,
    mut layouts: ResMut<Assets<TextureAtlasLayout>>,
) {
    commands.spawn(Camera2d);

    let image = asset_server.load("sprites/Sprite-0001-Sheet.png");
    let layout = layouts.add(TextureAtlasLayout::from_grid(UVec2::new(64, 64), 4, 3, None, None));

    commands
        .spawn((
            Node {
                width: Val::Percent(100.0),
                height: Val::Percent(100.0),
                justify_content: JustifyContent::Center,
                align_items: AlignItems::Center,
                ..default()
            },
            BackgroundColor(BG),
        ))
        .with_children(|root| {
            root.spawn((
                Node {
                    width: Val::Px(640.0),
                    height: Val::Px(440.0),
                    flex_direction: FlexDirection::Row,
                    border: UiRect::all(Val::Px(1.0)),
                    border_radius: BorderRadius::all(Val::Px(10.0)),
                    overflow: Overflow::clip(),
                    ..default()
                },
                BackgroundColor(PANEL),
                BorderColor::all(DIM_BORDER),
            ))
            .with_children(|window| {
                window
                    .spawn((
                        Node {
                            width: Val::Px(150.0),
                            height: Val::Percent(100.0),
                            flex_direction: FlexDirection::Column,
                            padding: UiRect::all(Val::Px(12.0)),
                            row_gap: Val::Px(6.0),
                            border: UiRect::right(Val::Px(1.0)),
                            ..default()
                        },
                        BackgroundColor(SIDEBAR),
                        BorderColor::all(DIM_BORDER),
                    ))
                    .with_children(|sidebar| {
                        sidebar.spawn((
                            Text::new("RUST MINER"),
                            TextFont { font_size: FontSize::Px(18.0), ..default() },
                            TextColor(GOLD),
                            Node { margin: UiRect::bottom(Val::Px(14.0)), ..default() },
                        ));
                        spawn_tab_button(sidebar, Tab::Dashboard, "DASHBOARD");
                        spawn_tab_button(sidebar, Tab::Log, "LOG");
                        spawn_tab_button(sidebar, Tab::Pool, "POOL");
                    });

                window
                    .spawn((Node {
                        flex_grow: 1.0,
                        height: Val::Percent(100.0),
                        flex_direction: FlexDirection::Column,
                        padding: UiRect::all(Val::Px(22.0)),
                        row_gap: Val::Px(12.0),
                        ..default()
                    },))
                    .with_children(|content| {
                        spawn_dashboard_panel(content, &image, &layout);
                        spawn_log_panel(content);
                        spawn_pool_panel(content);
                    });
            });
        });
}

fn spawn_tab_button(sidebar: &mut ChildSpawnerCommands, tab: Tab, label: &str) {
    sidebar
        .spawn((
            Button,
            Node {
                width: Val::Percent(100.0),
                padding: UiRect::vertical(Val::Px(10.0)).with_left(Val::Px(10.0)),
                border_radius: BorderRadius::all(Val::Px(6.0)),
                ..default()
            },
            BackgroundColor(if matches!(tab, Tab::Dashboard) { TAB_ACTIVE } else { TAB_INACTIVE }),
            TabButton(tab),
        ))
        .with_children(|b| {
            b.spawn((
                Text::new(label),
                TextFont { font_size: FontSize::Px(12.0), ..default() },
                TextColor(if matches!(tab, Tab::Dashboard) { GOLD } else { GRAY }),
                TabButtonLabel(tab),
            ));
        });
}

fn spawn_dashboard_panel(
    content: &mut ChildSpawnerCommands,
    image: &Handle<Image>,
    layout: &Handle<TextureAtlasLayout>,
) {
    content
        .spawn((
            Node { flex_direction: FlexDirection::Column, row_gap: Val::Px(10.0), ..default() },
            TabPanel(Tab::Dashboard),
        ))
        .with_children(|panel| {
            stat_row(panel, "STATUS", |row| {
                row.spawn((Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(8.0),
                    ..default()
                },))
                .with_children(|wrap| {
                    wrap.spawn((
                        ImageNode::from_atlas_image(
                            image.clone(),
                            TextureAtlas { layout: layout.clone(), index: 0 },
                        ),
                        Node { width: Val::Px(40.0), height: Val::Px(40.0), ..default() },
                        PickaxeAnim { timer: Timer::from_seconds(0.3, TimerMode::Repeating) },
                    ));
                    wrap.spawn((
                        Text::new("CONNECTING"),
                        TextFont { font_size: FontSize::Px(16.0), ..default() },
                        TextColor(GRAY),
                        DashField::Status,
                    ));
                });
            });

            stat_row_value(panel, "HASHES", "0", DashField::Hashes);
            stat_row_value(panel, "HASHRATE", "0 H/s", DashField::Hashrate);
            stat_row_value(panel, "SHARES ACCEPTED", "0", DashField::Shares);
            stat_row_value(panel, "DIFFICULTY", "-", DashField::Difficulty);
            stat_row_value(panel, "WORKERS", &num_cpus::get().saturating_sub(1).max(1).to_string(), WorkersValue);

            panel
                .spawn((
                    Node {
                        width: Val::Percent(100.0),
                        height: Val::Px(8.0),
                        margin: UiRect::top(Val::Px(4.0)),
                        border_radius: BorderRadius::all(Val::Px(4.0)),
                        ..default()
                    },
                    BackgroundColor(Color::srgb(0.18, 0.18, 0.22)),
                ))
                .with_children(|bar| {
                    bar.spawn((
                        Node {
                            width: Val::Percent(0.0),
                            height: Val::Percent(100.0),
                            border_radius: BorderRadius::all(Val::Px(4.0)),
                            ..default()
                        },
                        BackgroundColor(GOLD),
                        ActivityFill,
                    ));
                });

            panel.spawn((
                Text::new("HASHRATE HISTORY"),
                TextFont { font_size: FontSize::Px(11.0), ..default() },
                TextColor(GRAY),
                Node { margin: UiRect::top(Val::Px(6.0)), ..default() },
            ));
            panel
                .spawn((Node {
                    width: Val::Percent(100.0),
                    height: Val::Px(70.0),
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::FlexEnd,
                    column_gap: Val::Px(3.0),
                    ..default()
                },))
                .with_children(|graph| {
                    for i in 0..HISTORY_LEN {
                        graph.spawn((
                            Node {
                                flex_grow: 1.0,
                                height: Val::Percent(2.0),
                                border_radius: BorderRadius::top(Val::Px(2.0)),
                                ..default()
                            },
                            BackgroundColor(GOLD),
                            HistoryBar(i),
                        ));
                    }
                });
        });
}

fn spawn_log_panel(content: &mut ChildSpawnerCommands) {
    content
        .spawn((
            Node { flex_direction: FlexDirection::Column, display: Display::None, ..default() },
            TabPanel(Tab::Log),
        ))
        .with_children(|panel| {
            panel.spawn((
                Text::new("ACTIVITY LOG"),
                TextFont { font_size: FontSize::Px(12.0), ..default() },
                TextColor(GRAY),
                Node { margin: UiRect::bottom(Val::Px(8.0)), ..default() },
            ));
            panel.spawn((
                Text::new("Waiting for pool connection..."),
                TextFont { font_size: FontSize::Px(12.0), ..default() },
                TextColor(Color::WHITE),
                LogText,
            ));
        });
}

fn spawn_pool_panel(content: &mut ChildSpawnerCommands) {
    content
        .spawn((
            Node { flex_direction: FlexDirection::Column, row_gap: Val::Px(10.0), display: Display::None, ..default() },
            TabPanel(Tab::Pool),
        ))
        .with_children(|panel| {
            panel.spawn((
                Text::new("POOL CONNECTION"),
                TextFont { font_size: FontSize::Px(12.0), ..default() },
                TextColor(GRAY),
                Node { margin: UiRect::bottom(Val::Px(4.0)), ..default() },
            ));
            stat_row_value(panel, "STATUS", "CONNECTING", PoolField::Conn);
            stat_row_value(panel, "ADDRESS", "-", PoolField::Addr);
            stat_row_value(panel, "CURRENT JOB", "-", PoolField::Job);
            stat_row_value(panel, "SHARES ACCEPTED", "0", PoolField::Accepted);
            stat_row_value(panel, "SHARES REJECTED", "0", PoolField::Rejected);
        });
}

fn stat_row(
    parent: &mut ChildSpawnerCommands,
    label: &str,
    value_builder: impl FnOnce(&mut ChildSpawnerCommands),
) {
    parent
        .spawn(Node {
            width: Val::Percent(100.0),
            flex_direction: FlexDirection::Row,
            justify_content: JustifyContent::SpaceBetween,
            align_items: AlignItems::Center,
            ..default()
        })
        .with_children(|row| {
            row.spawn((
                Text::new(label),
                TextFont { font_size: FontSize::Px(13.0), ..default() },
                TextColor(GRAY),
            ));
            value_builder(row);
        });
}

fn stat_row_value(parent: &mut ChildSpawnerCommands, label: &str, initial: &str, marker: impl Component) {
    stat_row(parent, label, |row| {
        row.spawn((
            Text::new(initial),
            TextFont { font_size: FontSize::Px(15.0), ..default() },
            TextColor(Color::WHITE),
            marker,
        ));
    });
}

// ===================== Event handling / UI update =====================

fn receive_events(
    receiver: Res<EventReceiver>,
    mut status: ResMut<PoolStatus>,
    mut history: ResMut<HashHistory>,
    mut log: ResMut<ShareLog>,
) {
    if let Ok(rx) = receiver.0.lock() {
        for event in rx.try_iter() {
            match event {
                MinerEvent::Connected(addr) => {
                    status.connected = true;
                    status.pool_addr = addr.clone();
                    log.0.push_front(format!("connected to {addr}"));
                }
                MinerEvent::Disconnected => {
                    status.connected = false;
                    log.0.push_front("disconnected from pool".to_string());
                }
                MinerEvent::Difficulty(d) => status.difficulty = d,
                MinerEvent::NewJob(id) => status.job_id = id,
                MinerEvent::HashUpdate { hashes, hashrate } => {
                    status.hashes = hashes;
                    status.hashrate = hashrate;
                    history.0.push_back(hashrate);
                    if history.0.len() > HISTORY_LEN {
                        history.0.pop_front();
                    }
                }
                MinerEvent::ShareAccepted => {
                    status.shares_accepted += 1;
                    log.0.push_front("share accepted".to_string());
                }
                MinerEvent::ShareRejected(reason) => {
                    status.shares_rejected += 1;
                    log.0.push_front(format!("share rejected: {reason}"));
                }
                MinerEvent::Log(msg) => log.0.push_front(msg),
            }
            if log.0.len() > LOG_LEN {
                log.0.truncate(LOG_LEN);
            }
        }
    }
}

fn update_dashboard_text(
    status: Res<PoolStatus>,
    mut fields_q: Query<(&DashField, &mut Text, Option<&mut TextColor>)>,
    mut fill_q: Query<&mut Node, With<ActivityFill>>,
) {
    if !status.is_changed() {
        return;
    }
    for (field, mut text, color) in &mut fields_q {
        match field {
            DashField::Status => {
                if status.connected {
                    **text = "MINING".to_string();
                    if let Some(mut c) = color {
                        *c = TextColor(GREEN);
                    }
                } else {
                    **text = "OFFLINE".to_string();
                    if let Some(mut c) = color {
                        *c = TextColor(RED);
                    }
                }
            }
            DashField::Hashes => **text = format_num(status.hashes),
            DashField::Hashrate => **text = format!("{}/s", format_num(status.hashrate)),
            DashField::Shares => **text = status.shares_accepted.to_string(),
            DashField::Difficulty => {
                **text = if status.difficulty > 0.0 { format!("{:.4}", status.difficulty) } else { "-".to_string() };
            }
        }
    }
    let pct = ((status.hashrate as f32 / 2_000_000.0) * 100.0).clamp(2.0, 100.0);
    for mut node in &mut fill_q {
        node.width = Val::Percent(pct);
    }
}

fn update_pool_text(status: Res<PoolStatus>, mut fields_q: Query<(&PoolField, &mut Text, Option<&mut TextColor>)>) {
    if !status.is_changed() {
        return;
    }
    for (field, mut text, color) in &mut fields_q {
        match field {
            PoolField::Conn => {
                if status.connected {
                    **text = "CONNECTED".to_string();
                    if let Some(mut c) = color {
                        *c = TextColor(GREEN);
                    }
                } else {
                    **text = "DISCONNECTED".to_string();
                    if let Some(mut c) = color {
                        *c = TextColor(RED);
                    }
                }
            }
            PoolField::Addr => {
                **text = if status.pool_addr.is_empty() { "-".to_string() } else { status.pool_addr.clone() };
            }
            PoolField::Job => {
                **text = if status.job_id.is_empty() { "-".to_string() } else { status.job_id.clone() };
            }
            PoolField::Accepted => **text = status.shares_accepted.to_string(),
            PoolField::Rejected => **text = status.shares_rejected.to_string(),
        }
    }
}

fn update_history_bars(history: Res<HashHistory>, mut bars_q: Query<(&HistoryBar, &mut Node)>) {
    if !history.is_changed() {
        return;
    }
    let max = history.0.iter().copied().max().unwrap_or(1).max(1);
    for (bar, mut node) in &mut bars_q {
        let value = history.0.get(bar.0).copied().unwrap_or(0);
        let pct = ((value as f32 / max as f32) * 100.0).clamp(2.0, 100.0);
        node.height = Val::Percent(pct);
    }
}

fn update_log_text(log: Res<ShareLog>, mut text_q: Query<&mut Text, With<LogText>>) {
    if !log.is_changed() {
        return;
    }
    for mut t in &mut text_q {
        **t = if log.0.is_empty() {
            "Waiting for pool connection...".to_string()
        } else {
            log.0.iter().cloned().collect::<Vec<_>>().join("\n")
        };
    }
}

fn handle_tab_clicks(mut active: ResMut<ActiveTab>, query: Query<(&Interaction, &TabButton), Changed<Interaction>>) {
    for (interaction, button) in &query {
        if *interaction == Interaction::Pressed {
            active.0 = button.0;
        }
    }
}

fn apply_tab_visibility(
    active: Res<ActiveTab>,
    mut panels: Query<(&TabPanel, &mut Node)>,
    mut buttons: Query<(&TabButton, &mut BackgroundColor)>,
    mut labels: Query<(&TabButtonLabel, &mut TextColor)>,
) {
    if !active.is_changed() {
        return;
    }
    for (panel, mut node) in &mut panels {
        node.display = if panel.0 == active.0 { Display::Flex } else { Display::None };
    }
    for (button, mut bg) in &mut buttons {
        *bg = BackgroundColor(if button.0 == active.0 { TAB_ACTIVE } else { TAB_INACTIVE });
    }
    for (label, mut color) in &mut labels {
        *color = TextColor(if label.0 == active.0 { GOLD } else { GRAY });
    }
}

fn animate_pickaxe(time: Res<Time>, mut query: Query<(&mut PickaxeAnim, &mut ImageNode)>) {
    for (mut anim, mut image_node) in &mut query {
        anim.timer.tick(time.delta());
        if anim.timer.just_finished() {
            if let Some(atlas) = &mut image_node.texture_atlas {
                atlas.index = (atlas.index + 1) % PICKAXE_FRAME_COUNT;
            }
        }
    }
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

// ===================== Stratum networking =====================

fn run_stratum(tx: Sender<MinerEvent>, host: String, port: u16, label: String, wallet_address: String) {
    let stream = match TcpStream::connect((host.as_str(), port)) {
        Ok(s) => s,
        Err(e) => {
            tx.send(MinerEvent::Log(format!("pool connect failed: {e}"))).ok();
            tx.send(MinerEvent::Disconnected).ok();
            return;
        }
    };
    stream.set_nodelay(true).ok();
    let mut writer = stream.try_clone().expect("clone stream for writing");
    let mut reader = BufReader::new(stream);

    send_line(&mut writer, r#"{"id":1,"method":"mining.subscribe","params":["rust-miner/0.1"]}"#);

    let mut extranonce1 = Vec::new();
    let mut extranonce2_size = 4usize;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            tx.send(MinerEvent::Disconnected).ok();
            return;
        }
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            if v.get("id").and_then(|i| i.as_u64()) == Some(1) {
                if let Some(result) = v.get("result").and_then(|r| r.as_array()) {
                    extranonce1 = hex::decode(result.get(1).and_then(|x| x.as_str()).unwrap_or("")).unwrap_or_default();
                    extranonce2_size = result.get(2).and_then(|x| x.as_u64()).unwrap_or(4) as usize;
                }
                break;
            }
        }
    }

    let auth_msg = format!(
        r#"{{"id":2,"method":"mining.authorize","params":["{}.{}","x"]}}"#,
        wallet_address, WORKER_NAME
    );
    send_line(&mut writer, &auth_msg);
    tx.send(MinerEvent::Connected(format!("{label} ({host}:{port})"))).ok();

    let pool_state = Arc::new(PoolState {
        job: Mutex::new(None),
        difficulty: Mutex::new(1.0),
        extranonce1: Mutex::new(extranonce1),
        extranonce2_size: Mutex::new(extranonce2_size),
    });

    let (submit_tx, submit_rx) = channel::<String>();
    {
        let mut writer2 = writer.try_clone().expect("clone for submit thread");
        thread::spawn(move || {
            for msg in submit_rx {
                send_line(&mut writer2, &msg);
            }
        });
    }

    {
        let pool_state = pool_state.clone();
        let submit_tx = submit_tx.clone();
        let tx = tx.clone();
        let wallet_address = wallet_address.clone();
        thread::spawn(move || mine_worker(pool_state, submit_tx, tx, wallet_address));
    }

    let next_id = AtomicU64::new(100);
    let _ = next_id.load(Ordering::Relaxed);

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                tx.send(MinerEvent::Disconnected).ok();
                return;
            }
            Ok(_) => handle_pool_message(&line, &pool_state, &tx),
            Err(_) => {
                tx.send(MinerEvent::Disconnected).ok();
                return;
            }
        }
    }
}

fn send_line(writer: &mut TcpStream, msg: &str) {
    let _ = writer.write_all(msg.as_bytes());
    let _ = writer.write_all(b"\n");
}

fn handle_pool_message(line: &str, pool_state: &PoolState, tx: &Sender<MinerEvent>) {
    let v: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return,
    };

    if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
        let params = &v["params"];
        match method {
            "mining.notify" => {
                let job_id = params[0].as_str().unwrap_or("").to_string();
                let prevhash = hex::decode(params[1].as_str().unwrap_or("")).unwrap_or_default();
                let coinb1 = hex::decode(params[2].as_str().unwrap_or("")).unwrap_or_default();
                let coinb2 = hex::decode(params[3].as_str().unwrap_or("")).unwrap_or_default();
                let merkle_branch = params[4]
                    .as_array()
                    .map(|a| a.iter().filter_map(|h| hex::decode(h.as_str().unwrap_or("")).ok()).collect())
                    .unwrap_or_default();
                let version = hex::decode(params[5].as_str().unwrap_or("")).unwrap_or_default();
                let nbits = hex::decode(params[6].as_str().unwrap_or("")).unwrap_or_default();
                let ntime = hex::decode(params[7].as_str().unwrap_or("")).unwrap_or_default();

                tx.send(MinerEvent::NewJob(job_id.clone())).ok();
                *pool_state.job.lock().unwrap() = Some(Job {
                    job_id,
                    prevhash,
                    coinb1,
                    coinb2,
                    merkle_branch,
                    version,
                    nbits,
                    ntime,
                });
            }
            "mining.set_difficulty" => {
                if let Some(d) = params[0].as_f64() {
                    *pool_state.difficulty.lock().unwrap() = d;
                    tx.send(MinerEvent::Difficulty(d)).ok();
                }
            }
            _ => {}
        }
        return;
    }

    if let Some(result) = v.get("result") {
        match result {
            Value::Bool(true) => {}
            Value::Bool(false) => {
                let reason = v.get("error").map(|e| e.to_string()).unwrap_or_else(|| "rejected".to_string());
                tx.send(MinerEvent::ShareRejected(reason)).ok();
            }
            _ => {}
        }
    }
    if let Some(err) = v.get("error") {
        if !err.is_null() {
            tx.send(MinerEvent::ShareRejected(err.to_string())).ok();
        } else if v.get("result").and_then(|r| r.as_bool()) == Some(true) {
            tx.send(MinerEvent::ShareAccepted).ok();
        }
    }
}

fn mine_worker(pool_state: Arc<PoolState>, submit_tx: Sender<String>, tx: Sender<MinerEvent>, wallet_address: String) {
    let mut hashes: u64 = 0;
    let mut last_report = Instant::now();
    let mut last_hashes: u64 = 0;
    let mut extranonce2: u64 = 1;

    loop {
        let job_opt = pool_state.job.lock().unwrap().clone();
        let Some(job) = job_opt else {
            thread::sleep(Duration::from_millis(200));
            continue;
        };
        let difficulty = *pool_state.difficulty.lock().unwrap();
        let target = difficulty_to_target(difficulty);
        let extranonce1 = pool_state.extranonce1.lock().unwrap().clone();
        let extranonce2_size = *pool_state.extranonce2_size.lock().unwrap();

        extranonce2 = extranonce2.wrapping_add(1);
        let full = extranonce2.to_be_bytes();
        let take = extranonce2_size.min(8);
        let extranonce2_bytes = full[8 - take..].to_vec();

        let mut coinbase = Vec::with_capacity(job.coinb1.len() + extranonce1.len() + extranonce2_bytes.len() + job.coinb2.len());
        coinbase.extend_from_slice(&job.coinb1);
        coinbase.extend_from_slice(&extranonce1);
        coinbase.extend_from_slice(&extranonce2_bytes);
        coinbase.extend_from_slice(&job.coinb2);

        let mut merkle_root = sha256d(&coinbase);
        for branch in &job.merkle_branch {
            let mut buf = Vec::with_capacity(64);
            buf.extend_from_slice(&merkle_root);
            buf.extend_from_slice(branch);
            merkle_root = sha256d(&buf);
        }

        let prevhash_swapped = swap_word_order(&job.prevhash);

        for nonce in 0u32..100_000 {
            if pool_state.job.lock().unwrap().as_ref().map(|j| j.job_id != job.job_id).unwrap_or(true) {
                break;
            }

            let mut header = Vec::with_capacity(80);
            header.extend_from_slice(&job.version);
            header.extend_from_slice(&prevhash_swapped);
            header.extend_from_slice(&merkle_root);
            header.extend_from_slice(&job.ntime);
            header.extend_from_slice(&job.nbits);
            header.extend_from_slice(&nonce.to_le_bytes());

            let hash = sha256d(&header);
            hashes += 1;

            let mut hash_be = hash.clone();
            hash_be.reverse();
            let hash_num = BigUint::from_bytes_be(&hash_be);

            if hash_num <= target {
                let submit_msg = format!(
                    r#"{{"id":100,"method":"mining.submit","params":["{}.{}","{}","{}","{}","{}"]}}"#,
                    wallet_address,
                    WORKER_NAME,
                    job.job_id,
                    hex::encode(&extranonce2_bytes),
                    hex::encode(&job.ntime),
                    hex::encode(nonce.to_le_bytes())
                );
                submit_tx.send(submit_msg).ok();
                tx.send(MinerEvent::Log(format!("share submitted (job {})", job.job_id))).ok();
            }

            if last_report.elapsed() > Duration::from_millis(500) {
                let rate = (hashes - last_hashes) * 2;
                tx.send(MinerEvent::HashUpdate { hashes, hashrate: rate }).ok();
                last_hashes = hashes;
                last_report = Instant::now();
            }
        }
    }
}

// ===================== Demo mode: fake data for UI testing =====================

fn run_demo(tx: Sender<MinerEvent>) {
    thread::sleep(Duration::from_millis(600));
    tx.send(MinerEvent::Connected("DEMO MODE (no real pool)".to_string())).ok();
    tx.send(MinerEvent::Difficulty(0.001)).ok();
    tx.send(MinerEvent::NewJob("demo-job-0001".to_string())).ok();
    tx.send(MinerEvent::Log("this is fake data, no real mining is happening".to_string())).ok();

    let mut hashes: u64 = 0;
    let mut tick: u64 = 0;
    let mut job_num = 1u32;

    loop {
        tick += 1;
        // cheap deterministic wander so the hashrate/history graph has movement without a rand crate
        let phase = (tick as f64 * 0.3).sin() * 0.5 + 0.5; // 0..1
        let hashrate = (400_000.0 + phase * 900_000.0) as u64;
        hashes += hashrate / 2; // matches the 500ms tick used elsewhere
        tx.send(MinerEvent::HashUpdate { hashes, hashrate }).ok();

        if tick % 6 == 0 {
            tx.send(MinerEvent::ShareAccepted).ok();
            tx.send(MinerEvent::Log(format!("[demo] fake share accepted (job demo-job-{job_num:04})"))).ok();
        }
        if tick % 17 == 0 {
            tx.send(MinerEvent::ShareRejected("demo: simulated stale share".to_string())).ok();
        }
        if tick % 10 == 0 {
            job_num += 1;
            let job_id = format!("demo-job-{job_num:04}");
            tx.send(MinerEvent::NewJob(job_id)).ok();
        }

        thread::sleep(Duration::from_millis(500));
    }
}

fn sha256d(data: &[u8]) -> Vec<u8> {
    let first = Sha256::digest(data);
    Sha256::digest(first).to_vec()
}

fn swap_word_order(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    for chunk in bytes.chunks(4) {
        out.extend(chunk.iter().rev());
    }
    out
}

fn difficulty_to_target(diff: f64) -> BigUint {
    let diff1 = BigUint::from(0xFFFFu64) << 208;
    let diff = if diff <= 0.0 { 1.0 } else { diff };
    let scale = 1_000_000u64;
    let diff_scaled = (diff * scale as f64).round().max(1.0) as u64;
    (diff1 * BigUint::from(scale)) / BigUint::from(diff_scaled)
}

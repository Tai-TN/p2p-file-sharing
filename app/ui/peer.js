/* Giao diện app Peer: điều hướng, vẽ lại trạng thái mỗi giây, xử lý thao tác người dùng. */
"use strict";

const ui = {
  view: "files",
  selected: null,        // info_hash đang chọn ở "File của tôi"
  lastLogId: 0,
  logs: [],
  state: null,           // kết quả get_state gần nhất
  preview: null,         // { preview, meta_path } ở màn Tải file
  previewDir: "",
  previewLoading: false,
  settingsDirty: false,
};

const $ = (sel) => document.querySelector(sel);

/* ── Điều hướng ─────────────────────────────── */

function showView(name) {
  ui.view = name;
  document.querySelectorAll(".nav-item").forEach((b) => b.classList.toggle("active", b.dataset.view === name));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === `view-${name}`));
  if (name === "settings") {
    showAdvanced(false);
    loadSettingsForm();
  }
  if (name === "logs") renderLogs(true);
  render();
}

function showAdvanced(on) {
  $("#settings-main").classList.toggle("hidden", on);
  $("#settings-advanced").classList.toggle("hidden", !on);
}

/* ── Vòng lặp cập nhật ──────────────────────── */

async function poll() {
  try {
    const st = await api("get_state", ui.lastLogId);
    ui.state = st;
    if (st.logs.length) {
      ui.logs.push(...st.logs);
      if (ui.logs.length > 1000) ui.logs.splice(0, ui.logs.length - 1000);
      ui.lastLogId = st.logs[st.logs.length - 1].id;
      appendLogs(st.logs);
    }
    render();
  } catch (e) {
    console.error(e);
  } finally {
    setTimeout(poll, 1000);
  }
}

function render() {
  const st = ui.state;
  if (!st) return;
  renderHeader(st);
  renderNotice(st);
  if (ui.view === "files") renderFiles(st);
  if (ui.view === "share") renderJobs(st);
}

function renderHeader(st) {
  const dot = $("#tracker-dot");
  const text = $("#status-text");
  const c = st.client;
  if (!c) {
    dot.className = "dot bad";
    text.textContent = "Chưa khởi động được";
    return;
  }
  const where = `· ${c.peer_id} · cổng ${c.port}`;
  if (c.tracker_ok === true) {
    dot.className = "dot ok";
    text.textContent = `Đã kết nối tracker ${c.tracker} ${where}`;
  } else if (c.tracker_ok === false) {
    dot.className = "dot bad";
    text.textContent = `Không kết nối được tracker ${c.tracker} ${where}`;
  } else {
    dot.className = "dot wait";
    text.textContent = `Đang kiểm tra tracker ${c.tracker} ${where}`;
  }
  $("#total-down").textContent = fmtRate(c.download_rate);
  $("#total-up").textContent = fmtRate(c.upload_rate);
}

function renderNotice(st) {
  const box = $("#global-notice");
  let html = "";
  if (st.start_error) {
    html = `<div class="notice red" style="margin-bottom:14px">${icon("alert")}<span>${esc(st.start_error)}</span></div>`;
  } else if (st.needs_restart) {
    html = `<div class="notice blue" style="margin-bottom:14px;align-items:center">${icon("info")}
      <span class="grow">Cài đặt mới sẽ có hiệu lực sau khi khởi động lại app.</span>
      <button class="btn sm primary" data-act="restart" data-icon="refresh">Khởi động lại</button></div>`;
  }
  if (setHtml(box, html)) fillIcons(box);
}

/* ── File của tôi ───────────────────────────── */

const STATE_LABEL = {
  downloading: ["Đang tải", "blue"],
  paused: ["Tạm dừng", "amber"],
  seeding: ["Đang chia sẻ", "green"],
  error: ["Lỗi", "red"],
};

function taskPill(t) {
  if (t.blocked) return `<span class="pill red">Bị chặn</span>`;
  const [label, color] = STATE_LABEL[t.state] || [t.status, "gray"];
  return `<span class="pill ${color}">${esc(label)}</span>`;
}

function taskSpeed(t) {
  if (t.state === "downloading") return fmtRate(t.download_rate);
  if (t.state === "seeding") return t.upload_rate > 0 ? fmtRate(t.upload_rate) : `<span class="muted">0 B/s</span>`;
  return `<span class="muted">—</span>`;
}

function barClass(t) {
  if (t.state === "seeding") return "green";
  if (t.state === "paused" || t.state === "error") return "amber";
  return "";
}

function renderFiles(st) {
  const list = $("#files-list");
  const detail = $("#file-detail");
  const tasks = st.client ? st.client.tasks : [];

  if (!tasks.length) {
    setHtml(detail, "");
    setHtml(list, `<div class="card empty">
      <h3>Chưa có file nào</h3>
      <div class="small">Chia sẻ một file trên máy bạn, hoặc tải file người khác chia sẻ bằng file .meta.json.</div>
      <div class="actions">
        <button class="btn primary" data-goto="share">${icon("upload")}Chia sẻ file</button>
        <button class="btn" data-goto="download">${icon("download")}Tải từ .meta.json</button>
      </div></div>`);
    return;
  }

  if (!tasks.some((t) => t.info_hash === ui.selected)) ui.selected = tasks[0].info_hash;

  setHtml(list, `<div class="table">
    <div class="tr head files-grid"><span>Tên file</span><span>Trạng thái</span><span>Tiến độ</span><span>Tốc độ</span></div>
    ${tasks.map((t) => `
      <div class="tr clickable files-grid ${t.info_hash === ui.selected ? "selected" : ""}" data-select="${t.info_hash}">
        <span class="row-flex ellipsis" title="${esc(t.name)}">${icon("file")}<span class="ellipsis">${esc(t.name)}</span></span>
        <span>${taskPill(t)}</span>
        <span><div class="bar ${barClass(t)}"><div style="width:${t.progress}%"></div></div><span class="tiny sec">${t.progress}% · ${fmtBytes(t.file_size)}</span></span>
        <span>${taskSpeed(t)}</span>
      </div>`).join("")}
  </div>`);

  const t = tasks.find((x) => x.info_hash === ui.selected);
  setHtml(detail, t ? renderDetail(t) : "");
}

function renderDetail(t) {
  const dir = t.file_path ? t.file_path.replace(/[\\/][^\\/]*$/, "") : "";
  const commonBtns = `
    <button class="btn sm" data-act="open" data-path="${esc(t.file_path)}">${icon("folder")}Mở thư mục</button>
    <button class="btn sm icon danger" data-act="remove" data-hash="${t.info_hash}" title="Gỡ khỏi danh sách" aria-label="Gỡ khỏi danh sách">${icon("trash")}</button>`;

  let title, buttons, stats, body = "";

  if (t.state === "downloading") {
    title = `${icon("download")}<span>Đang tải: ${esc(t.name)}</span>`;
    buttons = `<button class="btn sm" data-act="pause" data-hash="${t.info_hash}">${icon("pause")}Tạm dừng</button>${commonBtns}`;
    const eta = t.eta_seconds != null ? `~${fmtDuration(t.eta_seconds)}` : (t.download_peers.length ? "đang đo…" : "chờ peer");
    stats = [
      ["Đã tải", `${fmtBytes(t.downloaded_bytes)} / ${fmtBytes(t.file_size)}`],
      ["Tốc độ", fmtRate(t.download_rate)],
      ["Còn lại", eta],
      ["Lưu tại", dir, dir],
    ];
    body += t.download_peers.length
      ? peerTable(`Đang tải từ ${t.download_peers.length} peer`,
          ["Peer", "Có mảnh", "Tốc độ", "Đã nhận"],
          t.download_peers.map((p) => [
            `${esc(p.peer_id)} <span class="muted">· ${esc(p.ip)}</span>`,
            `${p.have}/${t.num_pieces}`,
            p.rate > 0 ? fmtRate(p.rate) : `<span class="muted">chờ</span>`,
            fmtBytes(p.received),
          ]))
      : `<div class="notice amber mt">${icon("info")}<span>Chưa tìm thấy peer nào có file này. App sẽ tự hỏi lại tracker.</span></div>`;
  } else if (t.state === "paused") {
    title = `${icon("pause")}<span>Tạm dừng: ${esc(t.name)}</span>`;
    buttons = `<button class="btn sm primary" data-act="resume" data-hash="${t.info_hash}">${icon("play")}Tiếp tục</button>${commonBtns}`;
    stats = [
      ["Đã tải", `${fmtBytes(t.downloaded_bytes || t.file_size * t.progress / 100)} / ${fmtBytes(t.file_size)}`],
      ["Tiến độ", `${t.progress}%`],
      ["Số mảnh", `${t.have_pieces} / ${t.num_pieces}`],
      ["Lưu tại", dir, dir],
    ];
  } else if (t.state === "seeding") {
    title = `${icon("upload")}<span>Đang chia sẻ: ${esc(t.name)}</span>`;
    buttons = `<button class="btn sm" data-act="copy" data-text="${esc(t.meta_path)}">${icon("copy")}Sao chép đường dẫn .meta.json</button>${commonBtns}`;
    stats = [
      ["Dung lượng", fmtBytes(t.file_size)],
      ["Đã gửi đi", fmtBytes(t.uploaded_bytes)],
      ["Tốc độ gửi", fmtRate(t.upload_rate)],
      ["Peer đang tải", String(t.upload_peers.length)],
    ];
    body += `<div class="small sec mt row-flex">${icon("file")}<span class="ellipsis mono tiny" title="${esc(t.meta_path)}">${esc(t.meta_path)}</span></div>`;
  } else {
    title = `${icon("alert")}<span>${esc(t.name)}</span>`;
    buttons = (t.type === "Download"
      ? `<button class="btn sm primary" data-act="resume" data-hash="${t.info_hash}">${icon("refresh")}Thử lại</button>` : "") + commonBtns;
    stats = [
      ["Tiến độ", `${t.progress}%`],
      ["Số mảnh", `${t.have_pieces} / ${t.num_pieces}`],
      ["Dung lượng", fmtBytes(t.file_size)],
      ["Lưu tại", dir, dir],
    ];
    body += `<div class="notice red mt">${icon("alert")}<span>${esc(t.error || t.status)}</span></div>`;
  }

  if (t.blocked) {
    body = `<div class="notice red mt">${icon("ban")}<span>Tracker đã chặn peer này. Liên hệ người quản trị tracker.</span></div>` + body;
  }

  if (t.upload_peers.length && t.state !== "paused") {
    const CHOKE = { unchoked: ["Unchoke", "green"], optimistic: ["Optimistic", "amber"], choked: ["Choke", "gray"] };
    body += peerTable(t.state === "seeding" ? `Peer đang tải từ bạn (${t.upload_peers.length})` : `Peer đang tải từ bạn (${t.upload_peers.length})`,
      ["Peer", "Tốc độ", "Đã gửi", "Trạng thái"],
      t.upload_peers.map((p) => {
        const [label, color] = CHOKE[p.choke_state] || [p.choke_state, "gray"];
        return [
          `${esc(p.peer_id)} <span class="muted">· ${esc(p.ip)}</span>`,
          fmtRate(p.rate),
          fmtBytes(p.sent),
          `<span class="pill ${color}">${label}</span>`,
        ];
      }));
  } else if (t.state === "seeding") {
    body += `<div class="small muted mt">Chưa có peer nào đang tải file này từ bạn.</div>`;
  }

  return `<div class="card">
    <div class="card-head"><div class="card-title">${title}</div><div class="row-flex">${buttons}</div></div>
    <div class="stats">${stats.map(([k, v, tip]) => `<div class="stat"><div class="k">${k}</div><div class="v" ${tip ? `title="${esc(tip)}"` : ""}>${esc(v)}</div></div>`).join("")}</div>
    ${body}
  </div>`;
}

function peerTable(caption, headers, rows) {
  return `<div class="small sec mt" style="margin-bottom:2px">${esc(caption)}</div>
    <div class="table">
      <div class="tr head peers-grid">${headers.map((h) => `<span>${h}</span>`).join("")}</div>
      ${rows.map((r) => `<div class="tr peers-grid small">${r.map((c, i) => `<span class="${i === 0 ? "ellipsis" : ""}">${c}</span>`).join("")}</div>`).join("")}
    </div>`;
}

async function removeTask(hash) {
  const t = ui.state.client.tasks.find((x) => x.info_hash === hash);
  if (!t) return;
  const partial = t.type === "Download" && t.progress < 100;
  const res = await confirmDialog({
    title: `Gỡ "${t.name}"?`,
    body: t.state === "seeding"
      ? "App sẽ ngừng chia sẻ file này. File trên máy bạn vẫn được giữ nguyên."
      : "App sẽ ngừng tải file này.",
    okText: "Gỡ",
    danger: true,
    checkbox: partial ? { label: "Xóa luôn phần dữ liệu đã tải dở", checked: false } : null,
  });
  if (!res.ok) return;
  const r = await api("remove", hash, res.checked);
  toast(r.ok ? `Đã gỡ ${t.name}` : r.error, r.ok ? "" : "bad");
  poll_once();
}

/* ── Chia sẻ file ───────────────────────────── */

async function startShare(path) {
  if (!path) return;
  const r = await api("share", path);
  if (!r.ok) toast(r.error, "bad");
  poll_once();
}

function renderJobs(st) {
  const box = $("#share-jobs");
  const html = st.jobs.slice().reverse().map((j) => {
    if (j.state === "running") {
      const pct = j.total ? Math.round((j.done / j.total) * 100) : 0;
      const label = j.stage === "verify" ? "Đang kiểm tra file nguồn (SHA-256)…" : "Đang tính SHA-256 từng mảnh…";
      return `<div class="card">
        <div class="card-head"><div class="card-title">${icon("file")}<span>${esc(j.name)}</span><span class="muted small">· ${fmtBytes(j.size)}</span></div></div>
        <div class="kv"><span>Kích thước mảnh</span><span>512 KB${j.total ? ` → ${j.total} mảnh` : ""}</span></div>
        <div class="small sec mt">${label}</div>
        <div class="bar" style="margin-top:4px"><div style="width:${pct}%"></div></div>
        <div class="tiny muted" style="margin-top:4px">${j.done} / ${j.total || "?"} mảnh</div>
      </div>`;
    }
    if (j.state === "done") {
      return `<div class="card ok">
        <div class="card-title" style="color:var(--ok-text);margin-bottom:6px">${icon("check")}<span>Đang chia sẻ ${esc(j.name)}</span></div>
        <div class="small sec" style="margin-bottom:8px">Gửi file .meta.json này cho người muốn tải.</div>
        <div class="row-flex" style="flex-wrap:wrap">
          <input class="input mono grow" readonly value="${esc(j.meta_path)}">
          <button class="btn sm" data-act="open" data-path="${esc(j.meta_path)}">${icon("folder")}Mở thư mục</button>
          <button class="btn sm" data-act="copy" data-text="${esc(j.meta_path)}">${icon("copy")}Sao chép đường dẫn</button>
        </div>
      </div>`;
    }
    return `<div class="card bad">
      <div class="card-title" style="color:var(--bad-text)">${icon("alert")}<span>Không chia sẻ được ${esc(j.name)}</span></div>
      <div class="small sec" style="margin-top:4px">${esc(j.error)}</div>
    </div>`;
  }).join("");
  setHtml(box, html);
}

/* ── Tải file ───────────────────────────────── */

async function loadPreview(metaPath, dir) {
  if (!metaPath) return;
  ui.previewLoading = true;
  $("#download-preview").innerHTML = `<div class="card small sec">Đang đọc file và hỏi tracker…</div>`;
  const r = await api("preview", metaPath, dir || ui.previewDir || "");
  ui.previewLoading = false;
  if (!r.ok) {
    ui.preview = null;
    $("#download-preview").innerHTML = `<div class="notice red">${icon("alert")}<span>${esc(r.error)}</span></div>`;
    return;
  }
  ui.preview = r;
  if (!ui.previewDir) ui.previewDir = ui.state?.config?.download_dir || "";
  renderPreview();
}

function renderPreview() {
  const box = $("#download-preview");
  if (!ui.preview) { box.innerHTML = ""; return; }
  const p = ui.preview.preview;
  const notes = [];
  if (p.already_added) notes.push(["blue", "info", "File này đã có trong danh sách của bạn."]);
  if (!p.tracker_ok) notes.push(["red", "alert", "Không kết nối được tracker nên chưa biết ai đang có file này."]);
  else if (p.peer_count === 0) notes.push(["amber", "alert", "Chưa có peer nào chia sẻ file này. Bạn vẫn có thể bắt đầu, app sẽ chờ peer."]);
  if (p.resume_progress != null) notes.push(["amber", "history", `Đã tải dở ${p.resume_progress}% trước đó, sẽ tải tiếp phần còn thiếu.`]);
  else if (p.target_exists) notes.push(["amber", "info", "Thư mục lưu đã có file trùng tên. App sẽ kiểm tra và giữ lại các mảnh đúng."]);

  const peers = p.tracker_ok
    ? (p.peer_count ? `<span style="color:var(--ok)">${p.peer_count} peer</span> <span class="muted">(${p.seeder_count} có đủ file)</span>` : `<span class="muted">0 peer</span>`)
    : `<span class="muted">không rõ</span>`;

  box.innerHTML = `<div class="card">
    <div class="card-head"><div class="card-title">${icon("file")}<span>${esc(p.name)}</span></div></div>
    <div class="kv">
      <span>Dung lượng</span><span>${fmtBytes(p.file_size)} · ${p.num_pieces} mảnh × ${fmtBytes(p.piece_length)}</span>
      <span>Tracker</span><span>${esc(p.tracker || "—")}</span>
      <span>Mã file</span><span class="mono tiny" title="${esc(p.info_hash)}">${esc(p.info_hash.slice(0, 16))}…</span>
      <span>Peer đang có</span><span>${peers}</span>
    </div>
    <div class="small sec" style="margin:12px 0 4px">Lưu vào</div>
    <div class="row-flex"><input class="input grow" readonly value="${esc(ui.previewDir)}"><button class="btn sm" data-act="preview-dir">Đổi…</button></div>
    ${notes.map(([c, ic, text]) => `<div class="notice ${c} mt">${icon(ic)}<span>${esc(text)}</span></div>`).join("")}
    <div class="row-flex mt" style="justify-content:flex-end">
      <button class="btn" data-act="preview-cancel">Hủy</button>
      <button class="btn primary" data-act="start-download" ${p.already_added ? "disabled" : ""}>${icon("download")}Bắt đầu tải</button>
    </div>
  </div>`;
}

async function startDownload() {
  if (!ui.preview) return;
  const r = await api("download", ui.preview.meta_path, ui.previewDir);
  if (!r.ok) { toast(r.error, "bad"); return; }
  toast(`Bắt đầu tải ${ui.preview.preview.name}`, "ok");
  ui.selected = r.info_hash;
  ui.preview = null;
  renderPreview();
  showView("files");
  poll_once();
}

/* ── Cài đặt ────────────────────────────────── */

const SETTING_FIELDS = {
  tracker_host: "s-tracker-host", tracker_port: "s-tracker-port", listen_port: "s-listen-port",
  peer_name: "s-peer-name", download_dir: "s-download-dir", meta_dir: "s-meta-dir",
  max_workers: "s-max-workers", regular_slots: "s-regular-slots",
  optimistic_slots: "s-optimistic-slots", choking_interval: "s-choking-interval",
};

function loadSettingsForm() {
  const cfg = ui.state?.config;
  if (!cfg) return;
  for (const [key, id] of Object.entries(SETTING_FIELDS)) $("#" + id).value = cfg[key] ?? "";
  $("#s-auto-resume").checked = !!cfg.auto_resume;
  $("#settings-error").textContent = "";
  $("#tracker-test-result").textContent = "";
}

async function saveSettings() {
  const values = {};
  for (const [key, id] of Object.entries(SETTING_FIELDS)) values[key] = $("#" + id).value;
  values.auto_resume = $("#s-auto-resume").checked;
  const r = await api("save_settings", values);
  if (!r.ok) {
    $("#settings-error").textContent = r.error;
    return;
  }
  $("#settings-error").textContent = "";
  toast(r.needs_restart ? "Đã lưu. Khởi động lại app để áp dụng." : "Đã lưu cài đặt", "ok");
  poll_once();
}

async function testTracker() {
  const out = $("#tracker-test-result");
  out.innerHTML = `<span class="muted">Đang kiểm tra…</span>`;
  const r = await api("test_tracker", $("#s-tracker-host").value, $("#s-tracker-port").value);
  out.innerHTML = r.ok
    ? `<span style="color:var(--ok)">Kết nối được tracker.</span>`
    : `<span style="color:var(--bad)">${esc(r.error)}</span>`;
}

/* ── Nhật ký ────────────────────────────────── */

function logMatches(line) {
  const level = $("#log-level").value;
  const q = $("#log-search").value.trim().toLowerCase();
  return (!level || line.level === level) && (!q || line.text.toLowerCase().includes(q));
}

function logHtml(line) {
  return `<div class="log-line"><span class="t">${fmtClock(line.time)}</span><span class="lv-${line.level}">${line.level}</span><span class="msg">${esc(line.text)}</span></div>`;
}

function renderLogs(scrollToEnd = false) {
  const box = $("#log-box");
  const lines = ui.logs.filter(logMatches);
  box.innerHTML = lines.length ? lines.map(logHtml).join("") : `<div class="muted">Chưa có dòng nhật ký nào.</div>`;
  if (scrollToEnd) box.scrollTop = box.scrollHeight;
}

function appendLogs(lines) {
  const box = $("#log-box");
  const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  const matching = lines.filter(logMatches);
  if (!matching.length) return;
  if (box.firstElementChild && box.firstElementChild.classList.contains("muted")) box.innerHTML = "";
  box.insertAdjacentHTML("beforeend", matching.map(logHtml).join(""));
  while (box.childElementCount > 1000) box.firstElementChild.remove();
  if (nearBottom) box.scrollTop = box.scrollHeight;
}

/* ── Kéo thả (Python gửi đường dẫn đầy đủ qua window.onNativeDrop) ── */

window.onNativeDrop = (path) => {
  document.querySelectorAll(".drop").forEach((d) => d.classList.remove("over"));
  const isMeta = /\.json$/i.test(path);
  if (ui.view === "download" || (ui.view !== "share" && isMeta)) {
    if (ui.view !== "download") showView("download");
    loadPreview(path);
  } else {
    if (ui.view !== "share") showView("share");
    startShare(path);
  }
};

function setupDragHighlight() {
  ["dragenter", "dragover"].forEach((ev) => document.addEventListener(ev, (e) => {
    e.preventDefault();
    const zone = e.target.closest && e.target.closest(".drop");
    document.querySelectorAll(".drop").forEach((d) => d.classList.toggle("over", d === zone));
  }));
  document.addEventListener("dragleave", (e) => {
    if (!e.relatedTarget) document.querySelectorAll(".drop").forEach((d) => d.classList.remove("over"));
  });
  document.addEventListener("drop", (e) => {
    e.preventDefault();
    document.querySelectorAll(".drop").forEach((d) => d.classList.remove("over"));
  });
}

/* ── Sự kiện ────────────────────────────────── */

let pollTimer = null;
function poll_once() {
  clearTimeout(pollTimer);
  pollTimer = setTimeout(async () => {
    try {
      ui.state = await api("get_state", ui.lastLogId);
      if (ui.state.logs.length) {
        ui.logs.push(...ui.state.logs);
        ui.lastLogId = ui.state.logs[ui.state.logs.length - 1].id;
        appendLogs(ui.state.logs);
      }
      render();
    } catch (e) { console.error(e); }
  }, 150);
}

document.addEventListener("click", async (e) => {
  const nav = e.target.closest("[data-view]");
  if (nav) { showView(nav.dataset.view); return; }

  const go = e.target.closest("[data-goto]");
  if (go) { showView(go.dataset.goto); return; }

  const row = e.target.closest("[data-select]");
  if (row && !e.target.closest("[data-act]")) {
    ui.selected = row.dataset.select;
    render();
    return;
  }

  const pickDir = e.target.closest("[data-pick-dir]");
  if (pickDir) {
    const dir = await api("pick_folder");
    if (dir) $("#" + pickDir.dataset.pickDir).value = dir;
    return;
  }

  const btn = e.target.closest("[data-act]");
  if (!btn || btn.disabled) return;
  const act = btn.dataset.act;
  const hash = btn.dataset.hash;

  if (act === "pause" || act === "resume") {
    const r = await api(act, hash);
    if (!r.ok) toast(r.error, "bad");
    poll_once();
  } else if (act === "remove") {
    removeTask(hash);
  } else if (act === "open") {
    const r = await api("open_folder", btn.dataset.path);
    if (!r.ok) toast(r.error, "bad");
  } else if (act === "copy") {
    toast((await copyText(btn.dataset.text)) ? "Đã sao chép đường dẫn" : "Không sao chép được", "");
  } else if (act === "restart") {
    const res = await confirmDialog({ title: "Khởi động lại app?", body: "Các file đang tải sẽ được tải tiếp sau khi app mở lại.", okText: "Khởi động lại" });
    if (res.ok) api("restart_app");
  } else if (act === "preview-dir") {
    const dir = await api("pick_folder");
    if (dir) { ui.previewDir = dir; loadPreview(ui.preview.meta_path, dir); }
  } else if (act === "preview-cancel") {
    ui.preview = null;
    renderPreview();
  } else if (act === "start-download") {
    startDownload();
  }
});

$("#btn-pick-share").addEventListener("click", async () => startShare(await api("pick_file")));
$("#btn-pick-meta").addEventListener("click", async () => {
  const path = await api("pick_meta_file");
  if (path) loadPreview(path);
});
$("#btn-advanced").addEventListener("click", () => showAdvanced(true));
$("#btn-back-settings").addEventListener("click", () => showAdvanced(false));
$("#btn-save-settings").addEventListener("click", saveSettings);
$("#btn-test-tracker").addEventListener("click", testTracker);
$("#log-search").addEventListener("input", () => renderLogs());
$("#log-level").addEventListener("change", () => renderLogs());
$("#btn-clear-logs").addEventListener("click", async () => {
  await api("clear_logs");
  ui.logs = [];
  renderLogs();
});

fillIcons();
setupDragHighlight();
poll();

/* Giao diện quản trị Tracker: xem swarm, xem peer, xóa / chặn / bỏ chặn peer. */
"use strict";

const admin = { state: null, selected: null, lastLogId: 0 };
const $ = (sel) => document.querySelector(sel);

async function poll() {
  try {
    admin.state = await api("get_state", admin.lastLogId);
    const logs = admin.state.logs;
    if (logs.length) admin.lastLogId = logs[logs.length - 1].id;
    render();
  } catch (e) {
    console.error(e);
  } finally {
    setTimeout(poll, 1000);
  }
}

function render() {
  const st = admin.state;
  if (!st) return;
  const swarms = st.state.swarms;

  // Thanh trên cùng
  const dot = $("#run-dot"), text = $("#run-text"), toggle = $("#btn-toggle");
  if (st.running) {
    dot.className = "dot ok";
    text.textContent = `Đang chạy ${st.host}:${st.port} · ${fmtDuration(st.uptime)}`;
    setHtml(toggle, `${icon("stop")}Dừng`);
    $("#port-box").classList.add("hidden");
  } else {
    dot.className = "dot bad";
    text.textContent = "Đã dừng";
    setHtml(toggle, `${icon("play")}Chạy`);
    const box = $("#port-box");
    if (box.classList.contains("hidden")) {
      box.classList.remove("hidden");
      $("#port-input").value = st.port;
    }
  }
  setHtml($("#error-box"), st.error
    ? `<div class="notice red" style="margin-bottom:12px">${icon("alert")}<span>${esc(st.error)}</span></div>` : "");

  // Số liệu tổng quan
  const allPeers = swarms.flatMap((s) => s.peers);
  const uniquePeers = new Set(allPeers.map((p) => p.peer_id));
  $("#m-swarms").textContent = swarms.length;
  $("#m-peers").textContent = uniquePeers.size;
  $("#m-roles").textContent = `${swarms.reduce((a, s) => a + s.seeders, 0)} / ${swarms.reduce((a, s) => a + s.leechers, 0)}`;
  const reported = new Set(allPeers.filter((p) => p.reports > 0).map((p) => p.peer_id));
  $("#m-reported").textContent = reported.size;
  $("#m-reported").style.color = reported.size ? "var(--bad)" : "";

  // Danh sách swarm
  if (!swarms.some((s) => s.info_hash === admin.selected)) admin.selected = swarms.length ? swarms[0].info_hash : null;
  setHtml($("#swarm-list"), swarms.length
    ? `<div class="tr head swarm-grid"><span>File</span><span>Peer</span><span>Seed</span><span>Leech</span></div>` +
      swarms.map((s) => `
        <div class="tr clickable swarm-grid small ${s.info_hash === admin.selected ? "selected" : ""}" data-swarm="${s.info_hash}">
          <span class="ellipsis" title="${esc(s.name)}">${esc(s.name)}</span>
          <span>${s.peers.length}</span><span>${s.seeders}</span><span>${s.leechers}</span>
        </div>`).join("")
    : `<div class="empty small">Chưa có peer nào announce lên tracker.</div>`);

  // Peer trong swarm đang chọn
  const swarm = swarms.find((s) => s.info_hash === admin.selected);
  $("#peers-title").textContent = swarm ? `Peer trong ${swarm.name}` : "Peer";
  $("#peers-hash").textContent = swarm ? `${swarm.info_hash.slice(0, 12)}…` : "";
  setHtml($("#peer-list"), swarm
    ? `<div class="tr head tpeer-grid"><span>Peer</span><span>Địa chỉ</span><span>Tiến độ</span><span>Lần cuối</span><span></span></div>` +
      swarm.peers.map((p) => {
        const late = p.last_seen_ago > 60;
        return `<div class="tr tpeer-grid small ${p.reports ? "flagged" : ""}">
          <span class="ellipsis" title="${esc(p.peer_id)}">${esc(p.peer_id)}${p.reports ? ` <span class="pill red" title="Số peer báo lỗi">${p.reports} báo lỗi</span>` : ""}</span>
          <span class="mono tiny">${esc(p.ip)}:${p.port}</span>
          <span>${p.is_seeder ? `<span class="pill green">Seeder</span>`
            : `<div class="bar"><div style="width:${p.progress}%"></div></div><span class="tiny sec">${p.progress}%</span>`}</span>
          <span class="${late ? "" : "sec"}" style="${late ? "color:var(--bad)" : ""}">${fmtDuration(p.last_seen_ago)}</span>
          <span class="row-flex" style="gap:4px">
            <button class="btn icon" data-act="remove" data-peer="${esc(p.peer_id)}" title="Xóa khỏi swarm" aria-label="Xóa ${esc(p.peer_id)} khỏi swarm">${icon("userx")}</button>
            <button class="btn icon danger" data-act="block" data-peer="${esc(p.peer_id)}" data-ip="${esc(p.ip)}" title="Chặn peer" aria-label="Chặn ${esc(p.peer_id)}">${icon("ban")}</button>
          </span>
        </div>`;
      }).join("")
    : `<div class="empty small">Chọn một file ở bên trái.</div>`);

  // Danh sách chặn
  const blocked = [
    ...st.state.blocked_peer_ids.map((id) => ["Peer", id, "peer"]),
    ...st.state.blocked_ips.map((ip) => ["IP", ip, "ip"]),
  ];
  setHtml($("#block-list"), blocked.length
    ? blocked.map(([kind, value, type]) => `
        <div class="tr block-grid small">
          <span class="pill ${type === "ip" ? "amber" : "red"}">${kind}</span>
          <span class="mono ellipsis">${esc(value)}</span>
          <button class="btn sm" data-act="unblock" data-type="${type}" data-value="${esc(value)}">Bỏ chặn</button>
        </div>`).join("")
    : `<div class="empty small">Chưa chặn peer nào.</div>`);

  // Hoạt động gần đây
  const events = st.state.events;
  setHtml($("#events"), events.length
    ? events.map((ev) => `<div><span class="muted">${fmtClock(ev.time)}</span> <span class="lv lv-${ev.kind}">${ev.kind}</span> ${esc(ev.text)}</div>`).join("")
    : `<div class="muted">Chưa có hoạt động nào.</div>`);
}

document.addEventListener("click", async (e) => {
  const row = e.target.closest("[data-swarm]");
  if (row) { admin.selected = row.dataset.swarm; render(); return; }

  const btn = e.target.closest("[data-act]");
  if (!btn) return;
  const act = btn.dataset.act;

  if (act === "remove") {
    const peer = btn.dataset.peer;
    const res = await confirmDialog({
      title: `Xóa ${peer} khỏi swarm?`,
      body: "Peer sẽ biến mất khỏi danh sách nhưng có thể quay lại ở lần announce tiếp theo. Dùng Chặn nếu muốn chặn hẳn.",
      okText: "Xóa",
    });
    if (!res.ok) return;
    const r = await api("remove_peer", admin.selected, peer);
    toast(r.ok ? `Đã xóa ${peer}` : r.error, r.ok ? "" : "bad");
  } else if (act === "block") {
    const peer = btn.dataset.peer, ip = btn.dataset.ip;
    const res = await confirmDialog({
      title: `Chặn ${peer}?`,
      body: "Peer bị gỡ khỏi mọi swarm, không thể announce lại và không nhận được danh sách peer.",
      okText: "Chặn",
      danger: true,
      checkbox: { label: `Chặn luôn địa chỉ IP ${ip} (mọi peer từ máy này)`, checked: false },
    });
    if (!res.ok) return;
    const r = await api("block_peer", peer, res.checked ? ip : "");
    toast(r.ok ? `Đã chặn ${peer}` : r.error, r.ok ? "" : "bad");
  } else if (act === "unblock") {
    const type = btn.dataset.type, value = btn.dataset.value;
    const r = await api("unblock", type === "peer" ? value : "", type === "ip" ? value : "");
    toast(r.ok ? `Đã bỏ chặn ${value}` : r.error, r.ok ? "" : "bad");
  }
  admin.state = await api("get_state", admin.lastLogId);
  render();
});

$("#btn-toggle").addEventListener("click", async () => {
  if (!admin.state) return;
  if (admin.state.running) {
    const res = await confirmDialog({
      title: "Dừng tracker?",
      body: "Peer sẽ không tìm được nhau cho đến khi tracker chạy lại. Danh sách swarm hiện tại được giữ nguyên.",
      okText: "Dừng",
      danger: true,
    });
    if (!res.ok) return;
    await api("stop_server");
  } else {
    const r = await api("start_server", $("#port-input").value);
    if (!r.ok) toast(r.error, "bad");
  }
  admin.state = await api("get_state", admin.lastLogId);
  render();
});

fillIcons();
poll();

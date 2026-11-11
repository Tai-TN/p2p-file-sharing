/* Tiện ích dùng chung cho app Peer và app Tracker: icon, định dạng số, gọi API, hộp thoại. */
"use strict";

const ICONS = {
  share: '<circle cx="6" cy="12" r="3"/><circle cx="18" cy="6" r="3"/><circle cx="18" cy="18" r="3"/><path d="M8.7 10.7l6.6-3.4"/><path d="M8.7 13.3l6.6 3.4"/>',
  files: '<path d="M15 3v4a1 1 0 0 0 1 1h4"/><path d="M18 17h-7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4l5 5v7a2 2 0 0 1-2 2z"/><path d="M16 17v2a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h2"/>',
  upload: '<path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/><path d="M7 9l5-5 5 5"/><path d="M12 4v12"/>',
  download: '<path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/><path d="M7 11l5 5 5-5"/><path d="M12 4v12"/>',
  settings: '<path d="M4 6h8"/><path d="M16 6h4"/><circle cx="14" cy="6" r="2"/><path d="M4 12h2"/><path d="M10 12h10"/><circle cx="8" cy="12" r="2"/><path d="M4 18h10"/><path d="M18 18h2"/><circle cx="16" cy="18" r="2"/>',
  terminal: '<path d="M5 7l5 5-5 5"/><path d="M12 19h7"/>',
  folder: '<path d="M5 4h4l3 3h7a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2"/>',
  pause: '<rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/>',
  play: '<path d="M7 4v16l13-8z"/>',
  trash: '<path d="M4 7h16"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2l1-12"/><path d="M9 7V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v3"/>',
  copy: '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/>',
  check: '<circle cx="12" cy="12" r="9"/><path d="M9 12l2 2 4-4"/>',
  alert: '<path d="M12 9v4"/><path d="M10.4 3.9L2.6 17a2 2 0 0 0 1.7 3h15.4a2 2 0 0 0 1.7-3L13.6 3.9a2 2 0 0 0-3.2 0z"/><path d="M12 16h.01"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 8h.01"/><path d="M11 12h1v4h1"/>',
  history: '<path d="M12 8v4l2 2"/><path d="M3.05 11a9 9 0 1 1 .5 4m-.5 5v-5h5"/>',
  search: '<circle cx="10" cy="10" r="7"/><path d="M21 21l-6-6"/>',
  down: '<path d="M12 5v14"/><path d="M18 13l-6 6-6-6"/>',
  up: '<path d="M12 19V5"/><path d="M18 11l-6-6-6 6"/>',
  right: '<path d="M9 6l6 6-6 6"/>',
  left: '<path d="M15 6l-6 6 6 6"/>',
  server: '<rect x="3" y="4" width="18" height="7" rx="2"/><rect x="3" y="13" width="18" height="7" rx="2"/><path d="M7 8h.01"/><path d="M7 17h.01"/>',
  ban: '<circle cx="12" cy="12" r="9"/><path d="M5.7 5.7l12.6 12.6"/>',
  userx: '<circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><path d="M17 9l4 4m0-4l-4 4"/>',
  x: '<path d="M18 6L6 18M6 6l12 12"/>',
  stop: '<rect x="6" y="6" width="12" height="12" rx="2"/>',
  refresh: '<path d="M20 11A8.1 8.1 0 0 0 4.5 9M4 5v4h4"/><path d="M4 13a8.1 8.1 0 0 0 15.5 2m.5 4v-4h-4"/>',
  file: '<path d="M14 3v4a1 1 0 0 0 1 1h4"/><path d="M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z"/>',
  cloud: '<path d="M7 18a4.6 4.4 0 0 1 0-9 5 4.5 0 0 1 11 2h1a3.5 3.5 0 0 1 0 7h-1"/><path d="M9 15l3-3 3 3"/><path d="M12 12v9"/>',
};

function icon(name, cls = "") {
  return `<svg class="i ${cls}" viewBox="0 0 24 24" aria-hidden="true">${ICONS[name] || ""}</svg>`;
}

function fillIcons(root = document) {
  root.querySelectorAll("[data-icon]").forEach((el) => {
    el.insertAdjacentHTML("afterbegin", icon(el.dataset.icon, el.dataset.iconClass || ""));
    el.removeAttribute("data-icon");
  });
}

/*
 * Giao diện vẽ lại mỗi giây. Nếu một nút bị thay bằng phần tử mới đúng lúc người
 * dùng đang nhấn chuột thì trình duyệt bỏ mất cú click. Vì vậy: không vẽ lại khi
 * đang nhấn chuột, và chỉ thay HTML khi nội dung thực sự thay đổi.
 */
let pointerIsDown = false;
document.addEventListener("pointerdown", () => { pointerIsDown = true; }, true);
document.addEventListener("pointerup", () => setTimeout(() => { pointerIsDown = false; }, 0), true);
document.addEventListener("pointercancel", () => { pointerIsDown = false; }, true);

function setHtml(el, html) {
  if (pointerIsDown || el.dataset.html === html) return false;
  el.innerHTML = html;
  el.dataset.html = html;
  return true;
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

function fmtBytes(n) {
  n = Number(n) || 0;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  const digits = i === 0 ? 0 : n >= 100 ? 0 : 1;
  return `${n.toFixed(digits)} ${units[i]}`;
}

function fmtRate(n) {
  return `${fmtBytes(n)}/s`;
}

function fmtDuration(sec) {
  if (sec == null || !isFinite(sec)) return "—";
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return `${sec} giây`;
  const m = Math.floor(sec / 60), s = sec % 60;
  if (m < 60) return s ? `${m} phút ${s} giây` : `${m} phút`;
  const h = Math.floor(m / 60);
  return `${h} giờ ${m % 60} phút`;
}

function fmtClock(ts) {
  const d = new Date(ts * 1000);
  return [d.getHours(), d.getMinutes(), d.getSeconds()].map((x) => String(x).padStart(2, "0")).join(":");
}

/* Gọi hàm Python qua pywebview. Luôn trả về Promise; ném lỗi nếu Python ném lỗi. */
const pywebviewReady = new Promise((resolve) => {
  if (window.pywebview && window.pywebview.api) resolve();
  else window.addEventListener("pywebviewready", resolve, { once: true });
});

async function api(name, ...args) {
  await pywebviewReady;
  return window.pywebview.api[name](...args);
}

function toast(message, kind = "") {
  let box = document.querySelector(".toasts");
  if (!box) {
    box = document.createElement("div");
    box.className = "toasts";
    document.body.appendChild(box);
  }
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  box.appendChild(el);
  setTimeout(() => el.remove(), kind === "bad" ? 6000 : 3500);
}

/* Hộp thoại xác nhận. Trả về Promise<{ok, checked}>. */
function confirmDialog({ title, body = "", okText = "Đồng ý", danger = false, checkbox = null }) {
  return new Promise((resolve) => {
    const back = document.createElement("div");
    back.className = "modal-back";
    back.innerHTML = `
      <div class="modal" role="dialog" aria-modal="true">
        <h2>${esc(title)}</h2>
        ${body ? `<p>${body}</p>` : ""}
        ${checkbox ? `<label class="check"><input type="checkbox" id="dlg-check" ${checkbox.checked ? "checked" : ""}> <span>${esc(checkbox.label)}</span></label>` : ""}
        <div class="actions">
          <button class="btn" data-act="cancel">Hủy</button>
          <button class="btn ${danger ? "danger-fill" : "primary"}" data-act="ok">${esc(okText)}</button>
        </div>
      </div>`;
    const close = (ok) => {
      const checked = !!back.querySelector("#dlg-check")?.checked;
      back.remove();
      document.removeEventListener("keydown", onKey);
      resolve({ ok, checked });
    };
    const onKey = (e) => {
      if (e.key === "Escape") close(false);
      if (e.key === "Enter") close(true);
    };
    back.addEventListener("click", (e) => {
      if (e.target === back) close(false);
      const act = e.target.closest("[data-act]")?.dataset.act;
      if (act) close(act === "ok");
    });
    document.addEventListener("keydown", onKey);
    document.body.appendChild(back);
    back.querySelector('[data-act="ok"]').focus();
  });
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (_) {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    let ok = false;
    try { ok = document.execCommand("copy"); } catch (_) { ok = false; }
    ta.remove();
    return ok;
  }
}

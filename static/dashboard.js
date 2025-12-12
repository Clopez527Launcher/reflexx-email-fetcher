if (window.__DASHBOARD_BUNDLE_LOADED__) {
  console.warn('dashboard.js loaded twice, skipping second include');
  throw new Error('skip_second_include');
}
window.__DASHBOARD_BUNDLE_LOADED__ = true;

/* ========= Helpers for the new date-range flow (top bar) ========= */

// Convert "mm/dd/yyyy" -> "yyyy-mm-dd"
function toYMD(mdY) {
  if (!mdY) return "";
  const [m, d, y] = mdY.split("/").map(s => s.trim());
  return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
}

// simple HTML escape so inline templates don't break
function esc(val) {
  return String(val == null ? "" : val)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// --- Call-stats ownership guard -------------------------------------------
// The HTML must set window.__CALLSTATS_SINGLETON = 'inline' BEFORE this file loads.
// When it's 'inline', the INLINE script owns the call-stats cells and this file
// must NOT write them. When absent or 'bundle', this file may write them.
const CALLSTATS_OWNER = (window.__CALLSTATS_SINGLETON || 'bundle');
const canWriteCallStats = () => CALLSTATS_OWNER === 'bundle';

// Safe text setter (no crash if element is missing)
function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

// Update the 4 call stat labels (bundle only writes if it's the owner)
function setCallStats({ inbound_count, outbound_count, inbound_duration, outbound_duration }) {
  if (!canWriteCallStats()) return; // ← important guard
  setText("inboundCalls", Number(inbound_count || 0).toLocaleString());
  setText("outboundCalls", Number(outbound_count || 0).toLocaleString());
  setText("inboundDuration", (inbound_duration || "00:00:00").replace(/^0+:/, ""));
  setText("outboundDuration", (outbound_duration || "00:00:00").replace(/^0+:/, ""));
}

/* ------------ TIMEOUT SAFETY ------------ */
async function safeFetchJSON(url, options = {}) {
  const res = await fetch(url, {
    credentials: 'same-origin',
    headers: { 'Accept': 'application/json', ...(options.headers || {}) },
    ...options
  });

  const contentType = res.headers.get('content-type') || '';
  const text = await res.text();

  // HTML means login page or server error page; treat as session expired
  if (!contentType.includes('application/json')) {
    const err = new Error('Non-JSON response');
    err.code = 'NON_JSON';
    err.status = res.status;
    err.bodyPreview = text.slice(0, 200);
    throw err;
  }

  let data;
  try { data = JSON.parse(text); }
  catch {
    const err = new Error('Bad JSON');
    err.code = 'BAD_JSON';
    err.status = res.status;
    err.bodyPreview = text.slice(0, 200);
    throw err;
  }

  if (!res.ok) {
    const err = new Error(data?.message || 'HTTP error');
    err.code = 'HTTP_ERROR';
    err.status = res.status;
    err.data = data;
    throw err;
  }

  return data;
}

function handleSessionMaybe(err) {
  if (err && (err.status === 401 || err.code === 'NON_JSON')) {
    alert('Your session expired. Please log in again.');
    window.location.href = `/login?next=${encodeURIComponent(location.pathname)}`;
    return true;
  }
  return false;
}
/* ------------ END TIMEOUT SAFETY ------------ */


/* ========= Call metrics (range picker) ========= */
async function applyDateRangeFetch() {
  if (typeof canWriteCallStats === "function" && !canWriteCallStats()) return;

  const startRaw = document.getElementById("cm-start")?.value; // mm/dd/yyyy
  const endRaw   = document.getElementById("cm-end")?.value;
  const errEl    = document.getElementById("cm-error");
  const employeeId = document.getElementById("employeeSelect")?.value || "all";

  if (!startRaw || !endRaw) return;

  const startYMD = toYMD(startRaw);
  const endYMD   = toYMD(endRaw);

  if (endYMD < startYMD) {
    if (errEl) { errEl.textContent = "End date must be after start date."; errEl.style.display = "inline"; }
    return;
  }
  if (errEl) errEl.style.display = "none";

  try {
    // ✅ manager_id is server-scoped via session
    const url = `/api/call-metrics/range`
      + `?employee_id=${encodeURIComponent(employeeId)}`
      + `&start=${encodeURIComponent(startYMD)}`
      + `&end=${encodeURIComponent(endYMD)}`;

    const data = await safeFetchJSON(url);
    if (data.error) throw new Error(data.error);
    setCallStats(data);
  } catch (e) {
    if (handleSessionMaybe?.(e)) return;
    console.error("❌ Range fetch failed:", e);
    if (errEl) {
      errEl.textContent = "Failed to load call metrics.";
      errEl.style.display = "inline";
    }
  }
}

// 🔗 Minimal wiring so changes auto-fetch
document.getElementById("cm-apply")?.addEventListener("click", applyDateRangeFetch);
document.getElementById("employeeSelect")?.addEventListener("change", applyDateRangeFetch);
["cm-start","cm-end"].forEach(id => {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener("change", applyDateRangeFetch);
  el.addEventListener("keydown", e => { if (e.key === "Enter") applyDateRangeFetch(); });
});



/* ========= Weblogs “No data” overlay ========= */
function toggleNoDataOverlay(canvasId, overlayId, show){
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const wrap = canvas.parentElement;
  if (!wrap) return;

  // create overlay once
  let msg = document.getElementById(overlayId);
  if (!msg){
    msg = document.createElement('div');
    msg.id = overlayId;
    msg.textContent = 'No Data to Display Today';
    msg.style.cssText = 'position:absolute;inset:0;display:none;align-items:center;justify-content:center;font-weight:600;font-size:1.05rem;color:#00ffff;background:rgba(0,0,0,0.18);border-radius:10px;text-align:center;';
    wrap.style.position = 'relative';
    wrap.appendChild(msg);
  }

  msg.style.display    = show ? 'flex' : 'none';
  canvas.style.display = show ? 'none' : 'block';
}


/* ========= Main dashboard logic ========= */

document.addEventListener("DOMContentLoaded", function () {
  // Format seconds or "HH:MM:SS" into "Hh Mm Ss" / "Mm Ss" / "Ss"
  function formatDurationSmart(secOrHms) {
    if (secOrHms == null) return "0s";
    // If already "HH:MM:SS"
    if (typeof secOrHms === "string" && secOrHms.includes(":")) {
      const [hh, mm, ss] = secOrHms.split(":").map(Number);
      if (Number.isFinite(hh) && Number.isFinite(mm) && Number.isFinite(ss)) {
        if (hh > 0) return `${hh}h ${mm}m ${ss}s`;
        if (mm > 0) return `${mm}m ${ss}s`;
        return `${ss}s`;
      }
    }
    // Otherwise treat as seconds
    let total = Math.max(0, Math.floor(Number(secOrHms) || 0));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  // 🔧 Helper to format HH:MM:SS → "1h 23m"
  function prettifyTime(timeStr) {
    if (!timeStr || timeStr === "00:00:00") return "0m";
    const [hours, minutes, seconds] = timeStr.split(":").map(Number);
    let result = "";
    if (hours > 0) result += `${hours}h `;
    if (minutes > 0 || hours > 0) result += `${minutes}m`;
    if (!result) result = `${seconds}s`;
    return result.trim();
  }

  const employeeSelect  = document.getElementById("employeeSelect");
  const cmApplyBtn      = document.getElementById("cm-apply");
  
  const startEl = document.getElementById("cm-start");  // From date
  const endEl   = document.getElementById("cm-end");    // To date

  // 🔑 Resolve MANAGER_ID dynamically with sane fallbacks
  const MANAGER_ID = (() => {
    const fromGlobal = Number(window.MANAGER_ID);                       // inline <script> sets this
    const fromDom    = Number(document.body?.dataset?.managerId);       // <body data-manager-id="4">
    const fromLS     = Number(localStorage.getItem('MANAGER_ID'));      // cache once found
    const id = [fromGlobal, fromDom, fromLS].find(v => Number.isFinite(v) && v > 0);
    if (id) { localStorage.setItem('MANAGER_ID', String(id)); return id; }
    return 4; // final fallback so nothing crashes
  })();


  let weblogsChartInstance = null;  // 🔁 Chart instance for reuse

  // ✅ Session-scoped employees list (nickname-first; smart fallback from email)
  async function fetchEmployees() {
    try {
      const employees = await safeFetchJSON(`/manager-employees`);
      if (!employeeSelect) return;

      employeeSelect.innerHTML = `<option value="all">All Employees</option>`;

      // Build a nice label:
      // 1) nickname
      // 2) name/display_name/full_name
      // 3) derived from email local-part (e.g., "eman.nasr" -> "Eman Nasr")
      // 4) raw email
      const labelFrom = (e) => {
        const direct =
          (e.nickname && e.nickname.trim()) ||
          (e.name && e.name.trim()) ||
          (e.display_name && e.display_name.trim()) ||
          (e.full_name && e.full_name.trim());
        if (direct) return direct;

        const email = (e.email || "").trim();
        if (email.includes("@")) {
          const local = email.split("@")[0]
            .replace(/[._]+/g, " ")
            .replace(/\s+/g, " ")
            .trim()
            .toLowerCase()
            .replace(/\b\w/g, (c) => c.toUpperCase()); // Title Case
          if (local) return local;
        }
        return email || "Unknown";
      };

      // Sort by the label we’ll show
      employees
        .slice()
        .sort((a, b) => labelFrom(a).localeCompare(labelFrom(b)))
        .forEach((e) => {
          const opt = document.createElement("option");
          opt.value = e.id;
          opt.textContent = labelFrom(e);
          employeeSelect.appendChild(opt);
        });
    } catch (err) {
      if (handleSessionMaybe?.(err)) return;
      console.error("❌ Error fetching employees:", err);
    }
  }

  // ✅ Fetch Employee Data and Update Numeric Widgets (Raw Values)
  async function updateDashboard() {
    if (!employeeSelect) return;
    const employeeId = employeeSelect.value;
    const timeRange = "today"; // until API supports start/end for this widget

    try {
      const url = `/api/get_employee_data?employee_id=${encodeURIComponent(employeeId)}&time_range=${encodeURIComponent(timeRange)}`;
      const data = await safeFetchJSON(url);
      if (!data) return;

      // Safe helpers
      const safeNumber = (val) => {
        const num = Number(val);
        return isNaN(num) ? 0 : num.toFixed(2);
      };
      const safeTime = (val) => (typeof val === "string" && val.includes(":") ? val : "00:00:00");

      setText("mouse-distance", safeNumber(data.total_mouse_distance));
      setText("mouse-distance-avg", `Daily Avg: ${safeNumber(data.daily_avg_mouse_distance)}`);

      setText("keystrokes", safeNumber(data.total_keystrokes));
      setText("keystrokes-avg", `Daily Avg: ${safeNumber(data.daily_avg_keystrokes)}`);

      setText("clicks", safeNumber(data.total_mouse_clicks));
      setText("clicks-avg", `Daily Avg: ${safeNumber(data.daily_avg_mouse_clicks)}`);

      setText("idle-count", safeTime(data.total_idle_count_formatted));
      setText("idle-count-avg", `Daily Avg: ${safeTime(data.daily_avg_idle_count)}`);
    } catch (error) {
      if (handleSessionMaybe(error)) return;
      console.error("❌ Error fetching dashboard data:", error);
    }
  }

  // ✅ Fetch Web Usage % for calendar range and update chart.
		async function updateWeblogsChart() {
				const employeeId = document.getElementById("employeeSelect")?.value || "all";
				const startRaw   = document.getElementById("cm-start")?.value; // mm/dd/yyyy
				const endRaw     = document.getElementById("cm-end")?.value;
				if (!startRaw || !endRaw) return;

				const startYMD = toYMD(startRaw);
				const endYMD   = toYMD(endRaw);

				// guard on invalid range
				const errEl = document.getElementById("cm-error");
				if (endYMD < startYMD) {
						if (errEl) { errEl.textContent = "End date must be after start date."; errEl.style.display = "inline"; }
						return;
				} else if (errEl) errEl.style.display = "none";

				try {
						const params = new URLSearchParams({ start: startYMD, end: endYMD, employee: employeeId });
						if (employeeId === "all") params.set("manager_id", MANAGER_ID);

						const payload = await safeFetchJSON(`/api/web-usage?${params.toString()}`);
						console.log("WEB-USAGE payload:", payload?.data?.[0], payload);

						// shape → { data: [{ label, percent }, ...] }
						const labels   = (payload.data || []).map(d => d.label);
						const percents = (payload.data || []).map(d => Number(d.percent || 0));
						const hasData  = labels.length && percents.some(v => v > 0);
						// Also grab time for tooltip (try common server field names)
						const secondsOrHms = (payload.data || []).map(d => Number(d.seconds || 0));

						if (!hasData) {
								if (weblogsChartInstance) { weblogsChartInstance.destroy(); weblogsChartInstance = null; }
								toggleNoDataOverlay('weblogsChart','weblogsNoData', true);
								return;
						}
						toggleNoDataOverlay('weblogsChart','weblogsNoData', false);

						// 🔹 figure out the biggest % so the longest bar hits the right edge
						const maxPercent = Math.max(...percents);
						const axisMax    = maxPercent > 0 ? maxPercent * 1.05 : 1;  // small 5% buffer

						const canvas = document.getElementById("weblogsChart");
						if (!canvas) return;
						const ctx = canvas.getContext("2d");
						if (weblogsChartInstance) weblogsChartInstance.destroy();

						weblogsChartInstance = new Chart(ctx, {
								type: "bar",
								data: {
										labels,
										datasets: [{
												label: "Web Usage %",
												data: percents,
												backgroundColor: (ctx) => {
														const chart = ctx.chart;
														const { left, right } = chart.chartArea || {};
														if (!left || !right) return "#00f0ff";
														const gradient = chart.ctx.createLinearGradient(left, 0, right, 0);
														gradient.addColorStop(0.00, "rgba(0, 255, 150, 1)");
														gradient.addColorStop(0.15, "rgba(0, 200, 200, 1)");
														gradient.addColorStop(1.00, "rgba(0, 100, 255, 1)");
														return gradient;
												},
												borderRadius: 10,
												barThickness: 20,
												metaTime: secondsOrHms
										}]
								},
								options: {
										indexAxis: "y",
										scales: {
												x: {
														beginAtZero: true,
														max: axisMax,                          // 🔥 dynamic max instead of 100
														ticks: { callback: v => v + "%", color: "#ccc" },
														grid: { color: "rgba(255,255,255,0.1)" }
												},
												y: {
														ticks: { color: "#fff", font: { weight: "bold" } },
														grid: { display: false }
												}
										},
										plugins: {
												legend: { display: false },
												tooltip: {
														enabled: true,
														backgroundColor: "#1a1a1a",
														titleColor: "#00f0ff",
														bodyColor: "#ffffff",
														callbacks: {
																label: function (ctx) {
																		const label = ctx.label || '';
																		const pct   = typeof ctx.raw === 'number' ? ctx.raw : Number(ctx.raw || 0);
																		const tRaw  = (ctx.dataset.metaTime && ctx.dataset.metaTime[ctx.dataIndex]) || 0;
																		return `${label}: ${pct.toFixed(2)}% • ${formatDurationSmart(tRaw)}`;
																}
														}
												}
										},
										maintainAspectRatio: false
								}
						});
				} catch (error) {
						if (handleSessionMaybe(error)) return;
						console.error("❌ Error updating weblogs chart:", error);
						if (weblogsChartInstance) { weblogsChartInstance.destroy(); weblogsChartInstance = null; }
						toggleNoDataOverlay('weblogsChart','weblogsNoData', true);
				}
		}


  // ✅ Fetch Call Stats Summary for Dashboard Widget (old dropdown flow)
  async function fetchCallMetricsSummary() {
    if (!canWriteCallStats()) return; // bundle must not write call stats if inline owns them
    if (!employeeSelect) return;
    const employeeId = employeeSelect.value;
    const dateRange = "today";

    try {
      const data = await safeFetchJSON("/api/call-metrics-summary", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          manager_id: MANAGER_ID,
          employee_id: employeeId,
          date_range: dateRange
        })
      });

      setText("inboundCalls", data.inbound_count);
      setText("outboundCalls", data.outbound_count);
      setText("inboundDuration", prettifyTime(data.inbound_duration));
      setText("outboundDuration", prettifyTime(data.outbound_duration));
    } catch (error) {
      if (handleSessionMaybe(error)) return;
      console.warn("Call metrics fetch error:", error);
    }
  }

  // ✅ Refresh both parts of the dashboard
  function refreshAll() {
    updateDashboard();
    updateWeblogsChart();

    // Only the owner writes call stats (range or summary)
    if (!canWriteCallStats()) return;

    const hasRange =
      document.getElementById("cm-start")?.value &&
      document.getElementById("cm-end")?.value;

    if (hasRange) {
      applyDateRangeFetch();   // new calendars
    } else {
      fetchCallMetricsSummary(); // fallback
    }
  }

  // Events

  // Debounce so we don't spam requests while typing/picking
  const debounce = (fn, ms = 250) => {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  };
  const onDatesChanged = debounce(() => {
    // Only update the Web Usage chart right away (your goal)
    updateWeblogsChart();
    // You can also refresh call stats here if you want them to live-update:
    // if (canWriteCallStats()) applyDateRangeFetch();
  }, 250);

  if (employeeSelect) {
    employeeSelect.addEventListener("change", refreshAll);
  }

  // Keep the Apply button as a fallback/manual refresh
  if (cmApplyBtn) {
    cmApplyBtn.addEventListener("click", (e) => {
      e.preventDefault();
      applyDateRangeFetch();   // call stats (if your bundle owns it)
      updateWeblogsChart();    // web usage
    });
  }

  // Auto-refresh Web Usage when dates change
  [startEl, endEl].forEach(el => {
    if (!el) return;
    el.addEventListener("change", onDatesChanged);
    el.addEventListener("input",  onDatesChanged);
    el.addEventListener("keydown", e => { if (e.key === "Enter") onDatesChanged(); });
    // Flatpickr support (if used)
    if (el._flatpickr && el._flatpickr.config && Array.isArray(el._flatpickr.config.onChange)) {
      el._flatpickr.config.onChange.push(onDatesChanged);
    }
  });

  // Init
  fetchEmployees().then(() => {
    if (employeeSelect) employeeSelect.value = "all";
    refreshAll();
    // If Flatpickr set defaults in HTML, this will pull the range right away
    setTimeout(applyDateRangeFetch, 200);
    setTimeout(updateWeblogsChart, 250);
  });
}); // DOMContentLoaded closes


/* ========= Other global functions you already had ========= */

function updateDial(dialId, total, average) {
  const totalValue = typeof total === 'string' ? Number(total) : total;
  const averageValue = typeof average === 'string' ? Number(average) : average;
  const percent = averageValue > 0 ? Math.min((totalValue / averageValue) * 100, 100) : 0;

  const dial = document.querySelector(`#${dialId} .meter`);
  const label = document.querySelector(`#${dialId} .label`);

  if (dial && label) {
    dial.setAttribute("stroke-dasharray", `${percent}, 100`);
    label.textContent = `${Math.round(percent)}%`;
  } else {
    console.warn(`⚠️ Could not find dial or label for ${dialId}`);
  }
}

async function askAI() {
  const question = document.getElementById("ai-input")?.value || "";

  const dashboardData = {
    calls: {
      inbound: document.getElementById("inboundCalls")?.innerText || "0",
      outbound: document.getElementById("outboundCalls")?.innerText || "0",
      inboundTalkTime: document.getElementById("inboundDuration")?.innerText || "0m",
      outboundTalkTime: document.getElementById("outboundDuration")?.innerText || "0m"
    }
  };

  let businessMetrics = [];
  try {
    businessMetrics = await safeFetchJSON("/api/ai/business-metrics");
    console.log("✅ Business metrics loaded:", businessMetrics);
  } catch (error) {
    if (handleSessionMaybe(error)) return;
    console.warn("⚠️ Could not fetch business metrics:", error);
  }

  try {
    const result = await safeFetchJSON("/api/ai_ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        question,
        dashboard_data: dashboardData,
        business_metrics: businessMetrics
      })
    });
    const out = document.getElementById("ai-response");
    if (out) out.innerText = result.response || "Error: " + result.error;
  } catch (error) {
    if (handleSessionMaybe(error)) return;
    const out = document.getElementById("ai-response");
    if (out) out.innerText = "Error talking to AI.";
  }
}

// ✅ Fetch + render Scorecard using nickname
function fetchScorecard() {
  const dateInput = document.getElementById("scorecard-date");
  if (!dateInput) return;

  const selectedDate = dateInput.value || new Date().toISOString().slice(0, 10);

  safeFetchJSON(`/api/scorecard?date=${encodeURIComponent(selectedDate)}`)
    .then(data => {
      const tbody = document.getElementById("scorecard-table-body");
      if (!tbody) return;
      tbody.innerHTML = "";

      if (!Array.isArray(data) || data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;color:#888;">No users</td></tr>`;
        return;
      }

      data.forEach(user => {
        // pick a label
        const label =
          (user.nickname && user.nickname.trim()) ||
          (user.label && user.label.trim()) ||
          user.email ||
          "Unknown";

        const userId = user.user_id || user.id || "";
        const grade = user.grade ?? "—";
        const color = user.color || "#9ab";

        // 👇 this is what we added from backend
        const isInactive = user.is_inactive === 1
										|| user.is_inactive === true
										|| user.is_inactive === "1";


        // normalize score (backend might send score or total_score)
        let rawScore = 0;
        if (Number.isFinite(user.score)) {
          rawScore = user.score;
        } else if (Number.isFinite(user.total_score)) {
          rawScore = user.total_score;
        }

        const tr = document.createElement("tr");

        // build user link
        const userLink = `
          <a href="#"
             class="user-metrics-link"
             data-user-id="${userId}"
             data-user-email="${user.email || ""}"
             data-user-label="${label}"
             data-date="${selectedDate}"
             style="color:#00f0ff;">
            ${label}
          </a>
        `;

        if (isInactive) {
          // gray + dashes
          tr.innerHTML = `
            <td class="scorecard-muted">${userLink}</td>
            <td class="scorecard-muted" style="text-align:center;">—</td>
            <td class="scorecard-muted" style="text-align:right;">—</td>
          `;
        } else {
          tr.innerHTML = `
            <td>${userLink}</td>
            <td style="text-align:center;color:${color};">${grade}</td>
            <td style="text-align:right;color:${color};">${rawScore}%</td>
          `;
        }

        tbody.appendChild(tr);
      });
    })
    .catch(error => {
      // keep your existing error UI
      const tbody = document.getElementById("scorecard-table-body");
      if (tbody)
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;color:#f66;">Failed to load scorecard</td></tr>`;
      console.error("Scorecard API error:", error);
    });
}

// ✅ Modal drill-down: prefer user_id, fall back to email
document.addEventListener("click", function (e) {
  const el = e.target.closest(".user-metrics-link");
  if (!el) return;

  e.preventDefault();
  const date      = el.dataset.date;
  const userId    = el.dataset.userId;
  const userEmail = el.dataset.userEmail;
  const userLabel = el.dataset.userLabel || userEmail;

  const query = userId
    ? `user_id=${encodeURIComponent(userId)}&date=${encodeURIComponent(date)}`
    : `user=${encodeURIComponent(userEmail)}&date=${encodeURIComponent(date)}`;

  safeFetchJSON(`/api/user-metrics?${query}`)
    .then(data => {
      const fmtNum = (n) => (n != null ? Number(n).toLocaleString() : "N/A");
      const fmtMin = (mins) => {
        if (mins == null || isNaN(mins)) return "N/A";
        const h = Math.floor(mins / 60), m = Math.round(mins % 60);
        return `${h} hr ${m} min`;
      };

      const html = `
        <h3 style="color:#00f0ff;">${esc(userLabel)} – ${esc(date)}</h3>
        ${userEmail ? `<div style="margin:6px 0 10px;"><a href="mailto:${esc(userEmail)}" style="color:#9ab;">${esc(userEmail)}</a></div>` : ""}
        <ul style="margin-top: 10px;">
          <li>Mouse Distance: ${data.mouse_distance != null ? Number(data.mouse_distance).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : 'N/A'}</li>
          <li>Keystrokes: ${fmtNum(data.keystrokes)}</li>
          <li>Mouse Clicks: ${fmtNum(data.mouse_clicks)}</li>
          <li>Idle Time: ${fmtMin(data.idle_time)}</li>
          <li>Inbound Calls: ${fmtNum(data.inbound)}</li>
          <li>Outbound Calls: ${fmtNum(data.outbound)}</li>
          <li>Talk Time In: ${fmtMin(data.in_talk)}</li>
          <li>Talk Time Out: ${fmtMin(data.out_talk)}</li>
        </ul>
      `;

      const modal = document.getElementById("userMetricsModal");
      const content = document.getElementById("userMetricsContent");
      if (content) content.innerHTML = html;
      if (modal) modal.style.display = "block";
    })
    .catch(err => {
      if (handleSessionMaybe(err)) return;
      const modal = document.getElementById("userMetricsModal");
      const content = document.getElementById("userMetricsContent");
      if (content) content.innerHTML = "<p style='color:red;'>Error loading data</p>";
      if (modal) modal.style.display = "block";
      console.error(err);
    });
});

// === Scorecard date input init ===
document.addEventListener("DOMContentLoaded", function () {
  const dateInput = document.getElementById("scorecard-date");
  if (dateInput) {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Los_Angeles",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });

    const parts = formatter.formatToParts(new Date());
    const year = parts.find((p) => p.type === "year").value;
    const month = parts.find((p) => p.type === "month").value;
    const day = parts.find((p) => p.type === "day").value;

    const today = new Date(`${year}-${month}-${day}T00:00:00`);
    dateInput.value = today.toISOString().split("T")[0];

    fetchScorecard();
    dateInput.addEventListener("change", fetchScorecard);
  }
});

// ===== Reflexx Dashboard: Team Score IN the existing ring =====
document.addEventListener('DOMContentLoaded', () => {
  // --- DOM hooks (from your HTML) ---
  const ring       = document.querySelector('#baselineRingWidget .ring-progress');
  const centerText = document.querySelector('#baselineRingWidget #kpiCenter');
  const baseEl     = document.getElementById('kpiBaseline');
  const currEl     = document.getElementById('kpiCurrent');
  const deltaEl    = document.getElementById('kpiDelta');

  // top filters
  const startInput = document.getElementById('cm-start') || document.getElementById('scorecard-date');
  const endInput   = document.getElementById('cm-end');
  const applyBtn   = document.getElementById('cm-apply');

  // ring geometry
  const R = 50;
  const CIRC = 2 * Math.PI * R;

  // ---- helper: normalize date ----
  function toYMD(raw) {
    if (!raw) return '';
    // already yyyy-mm-dd
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    // mm/dd/yyyy
    const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) {
      const mm = m[1].padStart(2,'0');
      const dd = m[2].padStart(2,'0');
      return `${m[3]}-${mm}-${dd}`;
    }
    return '';
  }

  // ---- main refresh (TEMP DISABLED) ----
		async function refreshTeamScoreRing() {
				// Team Score ring is disabled for now so /api/team-score
				// stops throwing s.total_score errors.
				console.log("[Team Score Ring] disabled for now.");
				return;
		}

  // run once
  refreshTeamScoreRing().catch(console.error);

  // re-run on changes
  [startInput, endInput, applyBtn, document.getElementById('scorecard-date')].forEach(el => {
    if (!el) return;
    el.addEventListener('change', () => refreshTeamScoreRing().catch(console.error));
    el.addEventListener('click',  () => refreshTeamScoreRing().catch(console.error));
    el.addEventListener?.('keydown', e => {
      if (e.key === 'Enter') refreshTeamScoreRing().catch(console.error);
    });
  });
});

function esc(val) {
  return String(val == null ? "" : val)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/* 网页控制台：只消费仓库 /api（设备 CLI 的 show/tasks/devices 也是同一接口）。 */
(function () {
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
    (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
  const short = (h) => h ? String(h).replace(/^sha256:/, "").slice(0, 12) : "";

  $("base").value = location.origin;
  const api = async (path) => {
    const r = await fetch($("base").value.replace(/\/$/, "") + path);
    if (!r.ok) {
      const b = await json_or(r);
      throw new Error(b?.error?.code || ("HTTP " + r.status));
    }
    return r.json();
  };
  const json_or = async (r) => { try { return await r.json(); } catch { return {}; } };

  const badge = (s) => `<span class="badge ${esc(s)}">${esc(s)}</span>`;
  const bar = (c, t) =>
    `<span class="bar"><i style="width:${t ? Math.round(100 * c / t) : 0}%"></i></span>
     <span class="mono muted">${c}/${t}</span>`;

  async function renderDevices() {
    const { devices } = await api("/api/devices");
    $("devices").innerHTML = devices.length ? devices.map((d) => `
      <div class="device">
        <div class="row">
          <strong>${esc(d.device_id)}</strong>
          <span class="chip">当前：<span class="mono">${
            esc(d.current_manifest_id || "—（未切换）")}</span>
            <span class="muted">gen ${d.current_generation}</span></span>
          <span class="chip">待切换：<span class="mono">${
            esc(d.pending_task_id || "—（槽空闲）")}</span></span>
          ${d.last_switched_at ? `<span class="chip muted">${
            esc(d.last_switched_at)}</span>` : ""}
        </div>
        <div style="margin-top:8px">
          <table><tbody>
          ${d.tasks.map((t) => `
            <tr class="task" data-task="${esc(t.task_id)}">
              <td class="mono">${esc(t.task_id)}</td>
              <td>gen ${t.generation}</td>
              <td class="mono">${esc(t.manifest_id)}</td>
              <td>${badge(t.status)}</td>
              <td>${bar(t.confirmed_blocks, t.total_blocks)}</td>
              <td class="muted">${t.attempt}</td>
              <td class="reason">${esc(t.reason_code || "")}
                ${t.reason ? "— " + esc(t.reason) : ""}</td>
            </tr>`).join("")}
          </tbody></table>
        </div>
      </div>`).join("") : `<p class="muted">暂无设备任务</p>`;
  }

  async function renderManifests() {
    const { manifests } = await api("/api/manifests");
    $("manifests").innerHTML = manifests.map((m) => `
      <tr>
        <td class="mono">${esc(m.manifest_id)}</td>
        <td class="mono">${esc(m.snapshot_id)}</td>
        <td class="mono muted" title="${esc(m.root_sha256)}">${short(m.root_sha256)}</td>
        <td class="muted">${esc(m.created_at)}</td>
        <td>${esc(m.note || "")}</td>
      </tr>`).join("") || `<tr><td colspan="5" class="muted">暂无清单</td></tr>`;
  }

  async function renderTasks() {
    const { tasks } = await api("/api/tasks");
    $("tasks").innerHTML = tasks.map((t) => `
      <tr class="task" data-task="${esc(t.task_id)}">
        <td>${esc(t.device_id)}</td>
        <td>gen ${t.generation}</td>
        <td class="mono">${esc(t.manifest_id)}</td>
        <td>${badge(t.status)}</td>
        <td>${bar(t.confirmed_blocks, t.total_blocks)}</td>
        <td class="muted">${t.attempt}</td>
        <td class="reason">${esc(t.reason_code || "")}
          ${t.reason ? "— " + esc(t.reason) : ""}</td>
      </tr>`).join("") || `<tr><td colspan="7" class="muted">暂无任务</td></tr>`;
  }

  async function showDetail(taskId) {
    const [task, { events }] = await Promise.all([
      api(`/api/tasks/${taskId}`),
      api(`/api/tasks/${taskId}/events`)]);
    $("detail-panel").style.display = "";
    $("detail-id").textContent = taskId;
    const frozen = task.frozen_result;
    $("detail-summary").innerHTML = `
      <div class="row" style="display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px">
        <span class="chip">设备 ${esc(task.device_id)}</span>
        <span class="chip">gen ${task.generation}</span>
        <span class="chip">${badge(task.status)}</span>
        <span class="chip mono">${esc(task.manifest_id)}</span>
        <span class="chip">${bar(task.confirmed_blocks, task.total_blocks)}</span>
      </div>
      ${frozen ? `<p class="reason">终态冻结（重试同一清单返回第一次结果）：
        <code>${esc(frozen.outcome)}</code>
        ${frozen.code ? "· " + esc(frozen.code) : ""}
        ${frozen.message ? "— " + esc(frozen.message) : ""}
        （${esc(frozen.frozen_at)}）</p>` : ""}
      ${task.last_error && task.status === "PULLING" ?
        `<p class="reason">${esc(task.last_error)}</p>` : ""}`;
    $("events").innerHTML = events.map((e) => `
      <div class="ev">
        <span class="t mono">${esc(e.ts)}</span>
        <span class="badge ${terminalBadge(e.event)}">${esc(e.event)}</span>
        ${e.attempt != null ? `<span class="muted">第 ${e.attempt} 次尝试</span>` : ""}
        ${e.confirmed_blocks != null ? bar(e.confirmed_blocks, e.total_blocks || 0) : ""}
        ${e.code ? `<span class="reason">${esc(e.code)}</span>` : ""}
        ${e.message ? `<span>${esc(e.message)}</span>` : ""}
      </div>`).join("") || `<p class="muted">尚无事件</p>`;
  }

  const terminalBadge = (ev) => ({
    switch_succeeded: "SWITCHED", rejected: "REJECTED", failed: "FAILED",
    stage_completed: "STAGED", pull_completed: "PULLED",
    pull_paused: "PULLING", pull_progress: "PULLING",
  }[ev] || "NEW");

  document.addEventListener("click", async (e) => {
    const tr = e.target.closest("tr.task");
    if (!tr) return;
    try { await showDetail(tr.dataset.task); } catch (err) { alert(err.message); }
  });

  let busy = false;
  async function tick() {
    if (busy) return;
    busy = true;
    $("spin").textContent = "刷新中…";
    try {
      await Promise.all([renderDevices(), renderManifests(), renderTasks()]);
      $("spin").textContent = "已更新 " + new Date().toLocaleTimeString();
    } catch (e) {
      $("spin").textContent = "仓库不可达：" + e.message;
    } finally {
      busy = false;
    }
  }
  tick();
  setInterval(tick, 2000);
})();

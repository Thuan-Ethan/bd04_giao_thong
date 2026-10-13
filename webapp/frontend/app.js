// app.js - logic cho ca 5 man hinh, vanilla JS, khong build step.

// ---------- Dieu huong tab ----------
document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-pane").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
    if (btn.dataset.tab === "ban-do" && !_banDoKhoiTaoXong) khoiTaoBanDo();
    if (btn.dataset.tab === "he-thong") taiThongKeHeThong();
  });
});

document.querySelectorAll(".sub-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".sub-btn").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".sub-pane").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("dp-sub-" + btn.dataset.sub).classList.add("active");
  });
});

function dinhDangGio(iso) {
  const d = new Date(iso);
  return d.toLocaleString("vi-VN", { hour12: false });
}

// =====================================================================================
// Man hinh 1 - Ban do nhiet
// =====================================================================================

let _map, _geojsonLayer, _banDoKhoiTaoXong = false;

async function khoiTaoBanDo() {
  _banDoKhoiTaoXong = true;
  document.getElementById("bd-trang-thai").textContent = "Dang tai...";

  _map = L.map("ban-do-map").setView([40.73, -73.95], 10.3);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
    maxZoom: 18,
  }).addTo(_map);

  const [geojson, khoang] = await Promise.all([
    fetch("/api/vung-geojson").then((r) => r.json()),
    fetch("/api/khung-gio-khoang").then((r) => r.json()),
  ]);

  const sel = document.getElementById("bd-khung-gio");
  const min = new Date(khoang.thuc_te.min);
  const max = new Date(khoang.thuc_te.max);
  // chi liet ke moi gio (toi da 400 lua chon de khong qua tai select)
  const options = [];
  let cur = new Date(max);
  let dem = 0;
  while (cur >= min && dem < 400) {
    options.push(new Date(cur));
    cur = new Date(cur.getTime() - 3600 * 1000);
    dem++;
  }
  sel.innerHTML = options
    .map((d) => `<option value="${d.toISOString().slice(0, 19)}">${dinhDangGio(d)}</option>`)
    .join("");

  _geojsonLayer = L.geoJSON(geojson, {
    style: () => ({ fillColor: "#eeeeee", weight: 1, color: "#999", fillOpacity: 0.75 }),
    onEachFeature: (feature, layer) => {
      layer.bindTooltip(`Vung ${feature.properties.LocationID}: ${feature.properties.zone}`);
    },
  }).addTo(_map);

  document.getElementById("bd-khung-gio").addEventListener("change", capNhatBanDo);
  document.getElementById("bd-che-do").addEventListener("change", capNhatBanDo);
  await capNhatBanDo();
}

function mauTheoGiaTri(giaTri, min, max, cheDoSaiSo) {
  if (giaTri === undefined || giaTri === null) return "#eeeeee";
  if (cheDoSaiSo) {
    // sai so: am (du bao qua cao, do), duong (du bao qua thap, xanh duong), gan 0 (trang)
    const bien = Math.max(Math.abs(min), Math.abs(max), 1);
    const ty = giaTri / bien;
    if (ty > 0) return `rgba(220, 50, 50, ${Math.min(Math.abs(ty), 1)})`;
    return `rgba(50, 90, 220, ${Math.min(Math.abs(ty), 1)})`;
  }
  const ty = max > min ? (giaTri - min) / (max - min) : 0;
  const r = Math.round(255 * ty);
  const g = Math.round(200 * (1 - ty));
  return `rgb(${r}, ${g}, 40)`;
}

async function capNhatBanDo() {
  const khungGio = document.getElementById("bd-khung-gio").value;
  const cheDo = document.getElementById("bd-che-do").value;
  if (!khungGio) return;
  document.getElementById("bd-trang-thai").textContent = "Dang tai...";

  const res = await fetch(`/api/nhu-cau?khung_gio=${khungGio}&che_do=${cheDo}`);
  if (!res.ok) {
    document.getElementById("bd-trang-thai").textContent = "Loi tai du lieu (" + res.status + ")";
    return;
  }
  const data = await res.json();
  const theoVung = {};
  data.du_lieu.forEach((d) => (theoVung[d.vung] = d.gia_tri));

  const giaTris = Object.values(theoVung);
  const min = giaTris.length ? Math.min(...giaTris) : 0;
  const max = giaTris.length ? Math.max(...giaTris) : 1;

  _geojsonLayer.setStyle((feature) => {
    const v = theoVung[feature.properties.LocationID];
    return {
      fillColor: mauTheoGiaTri(v, min, max, cheDo === "sai_so"),
      weight: 1, color: "#999", fillOpacity: 0.8,
    };
  });
  _geojsonLayer.eachLayer((layer) => {
    const v = theoVung[layer.feature.properties.LocationID];
    layer.setTooltipContent(
      `Vung ${layer.feature.properties.LocationID}: ${layer.feature.properties.zone}<br>${cheDo}: ${v ?? "khong co du lieu"}`
    );
  });

  document.getElementById("bd-trang-thai").textContent =
    `${data.du_lieu.length} vung co du lieu (min=${min}, max=${max})`;
  document.getElementById("bd-legend").textContent =
    cheDo === "sai_so"
      ? "Do = thuc te thap hon du bao, Xanh duong = thuc te cao hon du bao"
      : "Vang -> Do: nhu cau thap -> cao";
}

// =====================================================================================
// Man hinh 2 - Chi tiet vung
// =====================================================================================

let _chartLichSu = null;

document.getElementById("ctv-xem").addEventListener("click", taiChiTietVung);

async function taiChiTietVung() {
  const id = document.getElementById("ctv-vung-id").value;
  document.getElementById("ctv-trang-thai").textContent = "Dang tai...";
  const res = await fetch(`/api/vung/${id}`);
  if (!res.ok) {
    document.getElementById("ctv-trang-thai").textContent = "Loi (" + res.status + ")";
    return;
  }
  const d = await res.json();
  document.getElementById("ctv-trang-thai").textContent = "";
  document.getElementById("ctv-ten-vung").textContent =
    `Vung ${d.vung}: ${d.ten_vung} (${d.borough ?? "?"})`;

  const ctx = document.getElementById("ctv-chart-lich-su").getContext("2d");
  if (_chartLichSu) _chartLichSu.destroy();
  _chartLichSu = new Chart(ctx, {
    type: "line",
    data: {
      labels: d.lich_su_7_ngay.map((r) => dinhDangGio(r.khung_gio)),
      datasets: [{ label: "So chuyen", data: d.lich_su_7_ngay.map((r) => r.so_chuyen),
        borderColor: "#0b3d66", backgroundColor: "rgba(11,61,102,0.15)", fill: true, pointRadius: 0 }],
    },
    options: { scales: { x: { ticks: { maxTicksLimit: 8 } } } },
  });

  const tbodyDuBao = document.querySelector("#ctv-bang-du-bao tbody");
  tbodyDuBao.innerHTML = d.du_bao_2h_toi
    .map((r) => `<tr><td>${dinhDangGio(r.khung_gio_muc_tieu)}</td><td>${r.horizon_gio}h</td><td>${r.so_chuyen_du_bao}</td></tr>`)
    .join("") || "<tr><td colspan='3'>Chua co du bao</td></tr>";

  document.getElementById("ctv-tgcb").textContent = d.thoi_gian_chuyen_tb_phut ?? "-";

  const tbodyTop5 = document.querySelector("#ctv-bang-top5 tbody");
  tbodyTop5.innerHTML = d.top_5_diem_den
    .map((r) => `<tr><td>${r.vung_tra}</td><td>${r.ten_vung}</td><td>${r.so_chuyen}</td></tr>`)
    .join("") || "<tr><td colspan='3'>Khong co du lieu</td></tr>";
}

// =====================================================================================
// Man hinh 3 - Ma tran OD
// =====================================================================================

document.getElementById("od-xem").addEventListener("click", taiMaTranOD);

async function taiMaTranOD() {
  const gio = document.getElementById("od-gio").value;
  const thu = document.getElementById("od-thu").value;
  const dayDu = document.getElementById("od-day-du").checked;
  document.getElementById("od-trang-thai").textContent = "Dang tai...";

  const res = await fetch(`/api/od?gio=${gio}&thu=${thu}&day_du=${dayDu}`);
  if (!res.ok) {
    document.getElementById("od-trang-thai").textContent = "Loi (" + res.status + ")";
    return;
  }
  const d = await res.json();
  document.getElementById("od-trang-thai").textContent = `${d.du_lieu.length} o du lieu${d.rut_gon ? " (rut gon top 30 vung)" : ""}`;

  const vungDon = [...new Set(d.du_lieu.map((r) => r.vung_don))].sort((a, b) => a - b);
  const vungTra = [...new Set(d.du_lieu.map((r) => r.vung_tra))].sort((a, b) => a - b);
  const o = {};
  d.du_lieu.forEach((r) => (o[`${r.vung_don}_${r.vung_tra}`] = r));
  const max = Math.max(1, ...d.du_lieu.map((r) => r.so_chuyen));

  let html = "<table><thead><tr><th>don\\tra</th>" + vungTra.map((v) => `<th>${v}</th>`).join("") + "</tr></thead><tbody>";
  vungDon.forEach((vd) => {
    html += `<tr><th>${vd}</th>`;
    vungTra.forEach((vt) => {
      const r = o[`${vd}_${vt}`];
      const sc = r ? r.so_chuyen : 0;
      const ty = sc / max;
      const bg = `rgba(11,61,102,${ty.toFixed(2)})`;
      html += `<td style="background:${bg}" data-vd="${vd}" data-vt="${vt}" title="${sc} chuyen">${sc || ""}</td>`;
    });
    html += "</tr>";
  });
  html += "</tbody></table>";
  document.getElementById("od-heatmap").innerHTML = html;

  document.querySelectorAll("#od-heatmap td[data-vd]").forEach((td) => {
    td.addEventListener("click", () => xemChiTietCapOD(td.dataset.vd, td.dataset.vt));
  });
}

async function xemChiTietCapOD(vd, vt) {
  const res = await fetch(`/api/od/${vd}/${vt}`);
  const el = document.getElementById("od-chi-tiet-cap");
  if (!res.ok) { el.textContent = "Khong co du lieu cho cap nay."; return; }
  const d = await res.json();
  el.innerHTML = `<h3>${d.ten_vung_don} (${vd}) -> ${d.ten_vung_tra} (${vt})</h3>` +
    `<p>${d.chi_tiet.length} to hop gio/thu co du lieu. Thoi gian trung vi trung binh: ` +
    (d.chi_tiet.reduce((s, r) => s + (r.thoi_gian_trung_vi || 0), 0) / d.chi_tiet.length).toFixed(1) + " phut</p>";
}

// =====================================================================================
// Man hinh 4 - Dieu phoi
// =====================================================================================

document.getElementById("dp-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = {
    vung: parseInt(document.getElementById("dp-vung").value, 10),
    khung_gio_ap_dung: document.getElementById("dp-khung-gio").value,
    so_xe_dieu_den: parseInt(document.getElementById("dp-so-xe").value, 10),
    ghi_chu: document.getElementById("dp-ghi-chu").value,
  };
  const res = await fetch("/api/dieu-phoi", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  const d = await res.json();
  document.getElementById("dp-ket-qua").textContent = res.ok
    ? `Da ghi ke hoach (id=${d.id_ke_hoach})`
    : `Loi: ${JSON.stringify(d)}`;
});

document.getElementById("dc-xem").addEventListener("click", async () => {
  const khungGio = document.getElementById("dc-khung-gio").value;
  if (!khungGio) return;
  const res = await fetch(`/api/dieu-phoi?khung_gio=${khungGio}`);
  const d = await res.json();
  const tbody = document.querySelector("#dc-bang tbody");
  tbody.innerHTML = d.du_lieu
    .map((r) => `<tr><td>${r.vung}</td><td>${r.ten_vung}</td><td>${r.so_xe_dieu_den}</td><td>${r.ghi_chu ?? ""}</td><td>${r.so_chuyen_thuc_te ?? "chua co"}</td></tr>`)
    .join("") || "<tr><td colspan='5'>Khong co ke hoach nao cho khung gio nay</td></tr>";
});

// =====================================================================================
// Man hinh 5 - Hieu nang he thong
// =====================================================================================

document.getElementById("ht-tai").addEventListener("click", taiThongKeHeThong);

async function taiThongKeHeThong() {
  const res = await fetch("/api/he-thong/thong-ke");
  const d = await res.json();

  const tbody = document.querySelector("#ht-bang-dung-luong tbody");
  tbody.innerHTML = Object.entries(d.dung_luong_tang)
    .filter(([k]) => k !== "loi")
    .map(([tang, info]) => `<tr><td>${tang}</td><td>${(info.so_byte / 1e9).toFixed(2)} GB</td></tr>`)
    .join("") || "<tr><td colspan='2'>Khong doc duoc</td></tr>";

  const dagDiv = document.getElementById("ht-dag-runs");
  dagDiv.innerHTML = Object.entries(d.lich_su_dag)
    .map(([dagId, runs]) => {
      if (!Array.isArray(runs)) return `<p>${dagId}: loi</p>`;
      const rows = runs.map((r) => `<tr><td>${r.run_id ?? ""}</td><td>${r.state ?? ""}</td></tr>`).join("");
      return `<h4>${dagId}</h4><table><thead><tr><th>Run</th><th>Trang thai</th></tr></thead><tbody>${rows}</tbody></table>`;
    })
    .join("");

  document.getElementById("ht-thi-nghiem").textContent =
    `${d.thi_nghiem_hieu_nang.trang_thai}: ${d.thi_nghiem_hieu_nang.ghi_chu}`;
}

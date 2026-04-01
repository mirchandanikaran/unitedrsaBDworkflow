const state = {
  filters: { country: "", keyword: "", status: "", source: "" },
  tenders: [],
  pagination: {
    page: 1,
    pageSize: 20,
    total: 0,
  },
};

const ids = {
  form: document.getElementById("filter-form"),
  country: document.getElementById("country"),
  keyword: document.getElementById("keyword"),
  source: document.getElementById("source"),
  status: document.getElementById("status"),
  clearFilters: document.getElementById("clear-filters"),
  ingestBtn: document.getElementById("ingest-btn"),
  prevPage: document.getElementById("prev-page"),
  nextPage: document.getElementById("next-page"),
  pageInfo: document.getElementById("page-info"),
  totalCount: document.getElementById("total-count"),
  openCount: document.getElementById("open-count"),
  closingSoonCount: document.getElementById("closing-soon-count"),
  sourceBreakdown: document.getElementById("source-breakdown"),
  tendersBody: document.getElementById("tenders-body"),
  statusLine: document.getElementById("status-line"),
  loadingOverlay: document.getElementById("loading-overlay"),
  loadingText: document.getElementById("loading-text"),
};

let loadingCount = 0;
function setLoading(isLoading, text = "Refreshing data...") {
  if (!ids.loadingOverlay) return;
  if (isLoading) {
    loadingCount += 1;
    ids.loadingOverlay.classList.add("show");
    if (ids.loadingText) ids.loadingText.textContent = text;
    return;
  }
  loadingCount = Math.max(0, loadingCount - 1);
  if (loadingCount === 0) ids.loadingOverlay.classList.remove("show");
}

function toQuery(params) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value) search.set(key, value);
  });
  return search.toString();
}

function getTotalPages() {
  return Math.max(1, Math.ceil(state.pagination.total / state.pagination.pageSize));
}

function renderPagination() {
  const totalPages = getTotalPages();
  state.pagination.page = Math.min(state.pagination.page, totalPages);
  ids.pageInfo.textContent = `Page ${state.pagination.page} of ${totalPages}`;
  ids.prevPage.disabled = state.pagination.page <= 1;
  ids.nextPage.disabled = state.pagination.page >= totalPages;
}

function fmtDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function normalizeStatus(status, closingDate) {
  const raw = (status || "").toLowerCase().trim();
  if (raw === "open" || raw === "closed" || raw === "awarded") return raw;
  if (closingDate) {
    const closing = new Date(closingDate);
    if (!Number.isNaN(closing.getTime()) && closing.getTime() < Date.now()) return "closed";
  }
  return "open";
}

function statusBadge(status, closingDate) {
  const value = normalizeStatus(status, closingDate);
  return `<span class="status-badge status-${value}">${value}</span>`;
}

function renderInsights() {
  ids.totalCount.textContent = String(state.pagination.total);
}

function renderTable() {
  ids.tendersBody.innerHTML = "";
  if (!state.tenders.length) {
    const row = document.createElement("tr");
    row.innerHTML = '<td colspan="7">No tenders found for current filters.</td>';
    ids.tendersBody.appendChild(row);
    return;
  }

  state.tenders.forEach((tender) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${tender.title || "-"}</td>
      <td>${tender.organization || "-"}</td>
      <td>${tender.country || "-"}</td>
      <td>${statusBadge(tender.status, tender.closing_date)}</td>
      <td>${fmtDate(tender.closing_date)}</td>
      <td>${tender.source || "-"}</td>
      <td><a href="${tender.url}" target="_blank" rel="noreferrer" title="Open original tender source page">Open</a></td>
    `;
    ids.tendersBody.appendChild(row);
  });
}

async function loadClosingSoonCount() {
  const response = await fetch("/tenders/closing-soon");
  if (!response.ok) throw new Error("Failed to fetch closing soon tenders.");
  const rows = await response.json();
  ids.closingSoonCount.textContent = String(rows.length);
}

async function loadTenders() {
  ids.statusLine.textContent = "Loading tenders...";
  const query = toQuery(state.filters);
  const offset = (state.pagination.page - 1) * state.pagination.pageSize;

  const openQuery = toQuery({ ...state.filters, status: "open" });
  const [countResponse, openCountResponse, tendersResponse] = await Promise.all([
    fetch(`/tenders/count?${query}`),
    fetch(`/tenders/count?${openQuery}`),
    fetch(`/tenders?${query}&limit=${state.pagination.pageSize}&offset=${offset}`),
  ]);
  if (!countResponse.ok || !openCountResponse.ok || !tendersResponse.ok) {
    throw new Error("Failed to fetch tenders.");
  }

  const countData = await countResponse.json();
  const openCountData = await openCountResponse.json();
  state.pagination.total = Number(countData.total || 0);
  ids.openCount.textContent = String(openCountData.total || 0);
  state.tenders = await tendersResponse.json();
  ids.statusLine.textContent = `Loaded ${state.tenders.length} tenders on this page.`;
  renderInsights();
  renderTable();
  renderPagination();
}

async function loadSourceOptions() {
  const response = await fetch("/sources");
  if (!response.ok) throw new Error("Failed to fetch sources.");
  const rows = await response.json();
  ids.source.innerHTML = '<option value="">All Sources</option>';
  rows.forEach((row) => {
    const option = document.createElement("option");
    option.value = row.source;
    option.textContent = `${row.source} (${row.count})`;
    ids.source.appendChild(option);
  });
  ids.source.value = state.filters.source || "";
}

async function loadSourceBreakdown() {
  const query = toQuery(state.filters);
  const response = await fetch(`/sources?${query}`);
  if (!response.ok) throw new Error("Failed to fetch source breakdown.");
  const rows = await response.json();
  ids.sourceBreakdown.innerHTML = "";
  rows
    .sort((a, b) => b.count - a.count)
    .forEach((row) => {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = `${row.source}: ${row.count}`;
      ids.sourceBreakdown.appendChild(chip);
    });
}

async function runIngestion() {
  ids.ingestBtn.disabled = true;
  ids.ingestBtn.textContent = "Running...";
  ids.statusLine.textContent = "Ingesting all sources and refreshing workflow data...";
  setLoading(true, "Ingesting from all portals...");
  try {
    const response = await fetch("/ingest-all", { method: "POST" });
    if (!response.ok) throw new Error("Ingestion failed.");
    const result = await response.json();
    ids.statusLine.textContent = `Ingestion complete. Inserted ${result.inserted}, updated ${result.updated_existing}, fetched ${result.total_fetched}.`;
    await Promise.all([loadSourceOptions(), loadData()]);
  } catch (error) {
    ids.statusLine.textContent = String(error);
  } finally {
    setLoading(false);
    ids.ingestBtn.disabled = false;
    ids.ingestBtn.textContent = "Ingest All Sources";
  }
}

async function loadData() {
  setLoading(true, "Refreshing dashboard...");
  try {
    await Promise.all([loadTenders(), loadClosingSoonCount(), loadSourceBreakdown()]);
  } catch (error) {
    ids.statusLine.textContent = String(error);
  } finally {
    setLoading(false);
  }
}

ids.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  state.filters.country = ids.country.value.trim();
  state.filters.keyword = ids.keyword.value.trim();
  state.filters.source = ids.source.value.trim();
  state.filters.status = ids.status.value.trim();
  state.pagination.page = 1;
  await loadData();
});

ids.clearFilters.addEventListener("click", async () => {
  ids.country.value = "";
  ids.keyword.value = "";
  ids.source.value = "";
  ids.status.value = "";
  state.filters = { country: "", keyword: "", status: "", source: "" };
  state.pagination.page = 1;
  await loadData();
});

ids.prevPage.addEventListener("click", async () => {
  if (state.pagination.page <= 1) return;
  state.pagination.page -= 1;
  await loadData();
});

ids.nextPage.addEventListener("click", async () => {
  const totalPages = getTotalPages();
  if (state.pagination.page >= totalPages) return;
  state.pagination.page += 1;
  await loadData();
});

ids.ingestBtn.addEventListener("click", runIngestion);

loadSourceOptions().then(loadData);

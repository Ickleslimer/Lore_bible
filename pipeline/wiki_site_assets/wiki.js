(function () {
  "use strict";

  const DEBOUNCE_MS = 120;
  const MAX_RESULTS = 8;

  function wikiRoot() {
    return document.body.getAttribute("data-wiki-root") || "./";
  }

  function joinRoot(path) {
    const root = wikiRoot();
    if (path.startsWith("http") || path.startsWith("/")) return path;
    return root + path;
  }

  let indexPromise = null;

  function loadIndex() {
    if (!indexPromise) {
      indexPromise = fetch(joinRoot("search-index.json"))
        .then(function (res) {
          if (!res.ok) throw new Error("search-index.json missing");
          return res.json();
        })
        .catch(function () {
          return [];
        });
    }
    return indexPromise;
  }

  function filterIndex(entries, query) {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    return entries
      .filter(function (entry) {
        const name = String(entry.name || "").toLowerCase();
        const excerpt = String(entry.excerpt || "").toLowerCase();
        const type = String(entry.type || "").toLowerCase();
        return name.includes(q) || excerpt.includes(q) || type.includes(q);
      })
      .slice(0, MAX_RESULTS);
  }

  function initSearch() {
    const wrap = document.querySelector("[data-wiki-search]");
    if (!wrap) return;

    const input = wrap.querySelector(".wiki-search-input");
    const panel = wrap.querySelector(".wiki-search-results");
    if (!input || !panel) return;

    let entries = [];
    let activeIndex = -1;
    let debounceTimer = null;

    loadIndex().then(function (data) {
      entries = Array.isArray(data) ? data : [];
    });

    function closePanel() {
      panel.hidden = true;
      input.setAttribute("aria-expanded", "false");
      activeIndex = -1;
    }

    function openPanel() {
      panel.hidden = false;
      input.setAttribute("aria-expanded", "true");
    }

    function renderHits(hits) {
      panel.innerHTML = "";
      if (!hits.length) {
        const empty = document.createElement("div");
        empty.className = "wiki-search-empty";
        empty.textContent = "No matching pages.";
        panel.appendChild(empty);
        openPanel();
        return;
      }

      hits.forEach(function (hit, idx) {
        const a = document.createElement("a");
        a.className = "wiki-search-hit";
        a.href = joinRoot(hit.path);
        a.setAttribute("role", "option");
        a.dataset.index = String(idx);

        const name = document.createElement("span");
        name.className = "wiki-search-hit-name";
        name.textContent = hit.name;

        const meta = document.createElement("span");
        meta.className = "wiki-search-hit-meta";
        const kind = hit.page_kind === "work" ? "Narrative work" : hit.type || "Page";
        meta.textContent = kind;

        const excerpt = document.createElement("span");
        excerpt.className = "wiki-search-hit-excerpt";
        excerpt.textContent = hit.excerpt || "";

        a.appendChild(name);
        a.appendChild(meta);
        if (hit.excerpt) a.appendChild(excerpt);
        panel.appendChild(a);
      });
      openPanel();
    }

    function setActive(idx) {
      const items = panel.querySelectorAll(".wiki-search-hit");
      items.forEach(function (el, i) {
        el.classList.toggle("is-active", i === idx);
      });
      activeIndex = idx;
      if (idx >= 0 && items[idx]) {
        items[idx].scrollIntoView({ block: "nearest" });
      }
    }

    function runSearch() {
      const hits = filterIndex(entries, input.value);
      renderHits(hits);
      setActive(-1);
    }

    input.addEventListener("input", function () {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(runSearch, DEBOUNCE_MS);
    });

    input.addEventListener("focus", function () {
      if (input.value.trim()) runSearch();
    });

    input.addEventListener("keydown", function (ev) {
      const items = panel.querySelectorAll(".wiki-search-hit");
      if (ev.key === "ArrowDown") {
        ev.preventDefault();
        if (panel.hidden) runSearch();
        setActive(Math.min(activeIndex + 1, items.length - 1));
      } else if (ev.key === "ArrowUp") {
        ev.preventDefault();
        setActive(Math.max(activeIndex - 1, 0));
      } else if (ev.key === "Enter" && activeIndex >= 0 && items[activeIndex]) {
        ev.preventDefault();
        window.location.href = items[activeIndex].href;
      } else if (ev.key === "Escape") {
        closePanel();
        input.blur();
      }
    });

    document.addEventListener("click", function (ev) {
      if (!wrap.contains(ev.target)) closePanel();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSearch);
  } else {
    initSearch();
  }
})();

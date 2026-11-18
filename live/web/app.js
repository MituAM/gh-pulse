// gh-pulse live dashboard. Polls two small JSON files the backend writes atomically:
//   api/state.json   — written by the Flink job (bursts + trending board)
//   api/ingest.json  — written by the poller (feed health)
// Everything that came from GitHub is rendered with textContent, never innerHTML: actor and repo
// names are user-controlled strings on a public page.
(() => {
  "use strict";

  const POLL_MS = 5000;
  const STALE_AFTER_MS = 4 * 60 * 1000; // the Flink job rewrites state at least every slide
  // Neutral on purpose: this names real people, from a sampled heuristic, on a public page.
  const BURST_LABEL_HUMAN = "HIGH ACTIVITY";
  const BURST_LABEL_BOT = "BOT";

  const $ = (id) => document.getElementById(id);
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const link = (href, text, cls) => {
    const a = el("a", cls, text);
    a.href = href;
    a.rel = "noopener";
    a.target = "_blank";
    return a;
  };

  const actorUrl = (login) => {
    const app = /^(.*)\[bot\]$/.exec(login);
    return app
      ? `https://github.com/apps/${encodeURIComponent(app[1])}`
      : `https://github.com/${encodeURIComponent(login)}`;
  };
  const repoUrl = (name) =>
    "https://github.com/" + name.split("/").map(encodeURIComponent).join("/");

  const ago = (ms) => {
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 60) return `${s}s ago`;
    const m = Math.round(s / 60);
    if (m < 60) return `${m} min${m === 1 ? "" : "s"} ago`;
    const h = Math.round(m / 60);
    if (h < 48) return `${h} hour${h === 1 ? "" : "s"} ago`;
    return `${Math.round(h / 24)} days ago`;
  };
  const clock = (ms) =>
    new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const fmt = (n) => (Math.round(n * 10) / 10).toLocaleString(undefined, { maximumFractionDigits: 1 });

  // ---- state across polls ------------------------------------------------------------------

  let seenBursts = null; // null until the first render, so the initial list does not all flash
  const shownScore = new Map(); // repo -> score currently on screen, for count-up animation
  let lastState = null;
  let lastIngest = null;

  // ---- bursts ------------------------------------------------------------------------------

  function renderBursts(state) {
    const list = $("bursts");
    // A persistent spammer reports once per cooldown; show their latest burst once, with a count.
    const repeats = new Map();
    for (const b of state.bursts || []) repeats.set(b.actor, (repeats.get(b.actor) || 0) + 1);
    const bursts = (state.bursts || []).filter((b, i, all) => all.findIndex((x) => x.actor === b.actor) === i);
    const s = state.settings || {};
    $("burst-rule").textContent = `${s.burstEvents ?? "?"}+ sampled events in ${s.burstWindowMinutes ?? "?"} min`;
    $("bursts-empty").hidden = bursts.length > 0;

    const key = (b) => `${b.actor}@${b.startedAt}`;
    const nextSeen = new Set(bursts.map(key));
    list.replaceChildren(
      ...bursts.map((b) => {
        const li = el("li", seenBursts && !seenBursts.has(key(b)) ? "fresh" : "");
        li.append(
          el("span", `badge ${b.isBot ? "bot" : "human"}`, b.isBot ? BURST_LABEL_BOT : BURST_LABEL_HUMAN),
        );
        const who = el("span", "who");
        who.append(link(actorUrl(b.actor), b.actor));
        li.append(who, el("span", "when", ago(b.endedAt)));

        const mins = Math.max(1, Math.round((b.endedAt - b.startedAt) / 60000));
        const what = el("span", "what");
        what.append(el("b", "", String(b.events)), ` events in ${mins} min · ${clock(b.startedAt)}–${clock(b.endedAt)}`);
        const n = repeats.get(b.actor);
        if (n > 1) what.append(" · ", el("b", "", `×${n}`), " bursts");
        li.append(what);

        if (b.repos && b.repos.length) {
          const targets = el("span", "targets");
          for (const r of b.repos) {
            const a = link(repoUrl(r.repo), r.events > 1 ? `${r.repo} ×${r.events}` : r.repo);
            targets.append(a);
          }
          li.append(targets);
        }
        return li;
      }),
    );
    seenBursts = nextSeen;
    renderBlips(bursts);
  }

  // One blip per recent burst: angle from a hash of the actor, distance from the centre by age.
  function renderBlips(bursts) {
    const g = $("blips");
    const ns = "http://www.w3.org/2000/svg";
    const now = Date.now();
    g.replaceChildren(
      ...bursts.slice(0, 24).map((b) => {
        let h = 0;
        for (const ch of b.actor) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
        const angle = (h % 360) * (Math.PI / 180);
        const age = Math.min(1, (now - b.endedAt) / (60 * 60 * 1000));
        const r = 18 + age * 74;
        const c = document.createElementNS(ns, "circle");
        c.setAttribute("cx", (100 + r * Math.cos(angle)).toFixed(1));
        c.setAttribute("cy", (100 + r * Math.sin(angle)).toFixed(1));
        c.setAttribute("r", Math.min(6, 2 + b.events / 4).toFixed(1));
        c.setAttribute("class", b.isBot ? "blip bot" : "blip");
        c.style.animationDelay = `${((angle / (2 * Math.PI)) * 3.6).toFixed(2)}s`;
        return c;
      }),
    );
  }

  // ---- leaderboard -------------------------------------------------------------------------

  function renderBoard(state) {
    const t = state.trending || {};
    const repos = t.repos || [];
    const s = state.settings || {};
    $("trending-rule").textContent = t.windowEnd
      ? `last ${s.trendingWindowMinutes} min · as of ${clock(t.windowEnd)} · refresh every ${s.trendingSlideMinutes} min`
      : "";
    $("board-empty").hidden = repos.length > 0;

    const body = $("board");
    const animations = [];
    body.replaceChildren(
      ...repos.map((r) => {
        const before = shownScore.get(r.repo);
        const tr = el("tr", before !== undefined && before !== r.score ? "bump" : "");
        tr.append(el("td", "c-rank", String(r.rank)));

        const move = el("td", "c-move");
        if (r.isNew) move.append(el("span", "new", "NEW"));
        else if (r.previousRank != null && r.previousRank !== r.rank) {
          const up = r.previousRank > r.rank;
          move.append(el("span", up ? "up" : "down", `${up ? "▲" : "▼"}${Math.abs(r.previousRank - r.rank)}`));
        }
        tr.append(move);

        const repo = el("td", "c-repo");
        repo.append(link(repoUrl(r.repo), r.repo));
        tr.append(repo);

        const score = el("td", "c-num");
        const scoreSpan = el("span", "score", fmt(before ?? r.score));
        score.append(scoreSpan);
        tr.append(score);
        if (before !== undefined && before !== r.score) animations.push([scoreSpan, before, r.score]);

        const delta = el("td", "c-num c-delta");
        if (r.previousScore != null) {
          const d = r.score - r.previousScore;
          if (d !== 0) delta.append(el("span", d > 0 ? "up" : "down", `${d > 0 ? "+" : ""}${fmt(d)}`));
        }
        tr.append(delta);

        tr.append(
          el("td", "c-num c-stat", String(r.pushes)),
          el("td", "c-num c-stat", r.merged ? `${r.pullRequests}(${r.merged})` : String(r.pullRequests)),
          el("td", "c-num c-stat", String(r.issues)),
          el("td", "c-num c-stat", String(r.comments)),
          el("td", "c-num c-stat", String(r.stars)),
          el("td", "c-num c-stat", String(r.forks)),
        );
        shownScore.set(r.repo, r.score);
        return tr;
      }),
    );
    for (const [span, from, to] of animations) countUp(span, from, to);
  }

  function countUp(span, from, to) {
    if (matchMedia("(prefers-reduced-motion: reduce)").matches) { span.textContent = fmt(to); return; }
    const start = performance.now();
    const dur = 900;
    const step = (now) => {
      const p = Math.min(1, (now - start) / dur);
      const eased = 1 - Math.pow(1 - p, 3);
      span.textContent = fmt(from + (to - from) * eased);
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  // ---- status line -------------------------------------------------------------------------

  function renderStatus() {
    const status = $("status");
    const label = $("live-label");
    const ingest = $("status-ingest");
    status.classList.remove("live", "stale", "down");

    if (!lastState) {
      status.classList.add("down");
      label.textContent = "OFFLINE";
      ingest.textContent = "no data from the pipeline yet";
      return;
    }
    const age = Date.now() - lastState.generatedAt;
    const live = age < STALE_AFTER_MS;
    status.classList.add(live ? "live" : "stale");
    label.textContent = live ? "LIVE" : "STALE";

    const parts = [`updated ${ago(lastState.generatedAt)}`];
    if (lastIngest) {
      if (lastIngest.eventsTotal != null) parts.unshift(`${lastIngest.eventsTotal.toLocaleString()} events ingested`);
      if (lastIngest.medianLagSeconds != null) parts.push(`feed lag ~${Math.round(lastIngest.medianLagSeconds / 60)} min`);
    }
    ingest.textContent = parts.join(" · ");
  }

  // ---- loop --------------------------------------------------------------------------------

  async function getJson(path) {
    const res = await fetch(path, { headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
    return res.json();
  }

  async function tick() {
    try {
      const state = await getJson("api/state.json");
      if (!lastState || state.generatedAt !== lastState.generatedAt) {
        renderBursts(state);
        renderBoard(state);
      }
      lastState = state;
    } catch (e) {
      console.warn(e);
    }
    try {
      lastIngest = await getJson("api/ingest.json");
    } catch (e) {
      /* the health file is optional */
    }
    renderStatus();
  }

  tick();
  setInterval(tick, POLL_MS);
  // Keep "2 mins ago" honest between data updates.
  setInterval(() => {
    if (lastState) renderBursts(lastState);
    renderStatus();
  }, 30000);
})();

#!/usr/bin/env python3
"""AI radar collector: deterministic, metric-ranked input for the weekly radar.

Collects the last N days from free sources (HN Algolia, Reddit, lab/curator RSS,
Hugging Face trending, GitHub new repos, Lobsters), clusters items that describe
the same event, scores clusters on real metrics and prints a ranked brief.

The LLM is expected to *write up* the ranked clusters, not to pick them.

Every source is optional: a failing source is reported under `source_status`
and never aborts the run. Reddit is opt-in (reddit.enabled must be true) and
disabled pending Reddit Data API approval; anonymous RSS is not permitted.

Stdlib only, so it runs as a cron pre-run script.
"""

from __future__ import annotations

import argparse
import base64
import email.utils
import gzip
import html
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG = HERE / "sources.json"
DEFAULT_STATE = HERE / "state" / "seen.json"

# --------------------------------------------------------------------------- #
# Topic / novelty vocabularies (tune by hand after pilot issues)
# --------------------------------------------------------------------------- #

AI_RE = re.compile(
    r"\b(ai|a\.i\.|llms?|gpt[-\s]?\d*\w*|chatgpt|openai|anthropic|claude|gemini|deepmind|"
    r"mistral|llama|qwen|deepseek|grok|xai|copilot|cursor|codex|agents?|agentic|mcp|"
    r"transformers?|diffusion|inference|fine[-\s]?tun\w*|rag|embeddings?|neural|"
    r"machine learning|ml|model weights|open[-\s]?weights?|reasoning model|benchmark|"
    r"hugging ?face|nvidia|gpu|tpu|vllm|ollama|llama\.cpp|vibe[-\s]?cod\w*|prompt\w*|"
    r"token\w*|context window|multimodal|text[-\s]to[-\s]\w+|speech|voice model|"
    r"robotics?|humanoid|superintelligence|agi|alignment|safety)\b",
    re.I,
)

FRONTIER_RE = re.compile(
    r"\b(gpt[-\s]?\d(\.\d)?\w*|o\d(-\w+)?|claude\s*(opus|sonnet|haiku)?\s*\d(\.\d)?|(?:opus|sonnet|haiku|fable|mythos)\s*\d(\.\d)?|"
    r"gemini\s*\d(\.\d)?\w*|llama\s*\d|qwen\s*\d(\.\d)?|deepseek[-\s]?(v|r)\d|grok\s*\d|"
    r"mistral\s*(large|medium)\s*\d*|kimi\s*k?\d)\b",
    re.I,
)
LAUNCH_RE = re.compile(
    r"\b(introduc\w+|announc\w+|launch\w*|releas\w+|unveil\w*|now available|open[-\s]?sourc\w+|"
    r"new model|state[-\s]of[-\s]the[-\s]art|sota|beats|surpass\w*|breakthrough|first)\b",
    re.I,
)
PATCH_RE = re.compile(r"\b(v?\d+\.\d+\.\d+|patch|hotfix|bugfix|point release|changelog)\b", re.I)
FUNDING_RE = re.compile(
    r"\b(rais\w*\s+(a\s+)?\$|funding round|series [a-f]\b|seed round|valuation|valued at|acquires|acquired|"
    r"acquisition|ipo\b|\$\d+(\.\d+)?\s?(m|b|bn|million|billion)\b)",
    re.I,
)
DATA_ENG_RE = re.compile(
    r"\b(data engineering|dbt|dagster|airflow|iceberg|delta lake|duckdb|bigquery|snowflake|"
    r"databricks|spark|kafka|flink|lakehouse|data warehouse|etl|elt|data quality|"
    r"postgres(ql)?|clickhouse|parquet|arrow|polars)\b",
    re.I,
)

TRACKING_PARAMS = {"ref", "ref_src", "source", "fbclid", "gclid", "mc_cid", "mc_eid", "cmpid"}
STOPWORDS = set(
    "a an the and or of to in on for with by from at as is are was be new its it this that "
    "how why what we our you your i my vs via into now just more than about after over can "
    "will has have show hn ask".split()
)


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #


@dataclass
class Item:
    source: str
    kind: str  # community | lab | curator | news | popularity
    title: str
    url: str
    published: str | None = None
    discussion_url: str | None = None
    metrics: dict[str, float] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class Cluster:
    items: list[Item]
    score: float = 0.0
    section: str = "main"
    reasons: list[str] = field(default_factory=list)
    factors: dict[str, float] = field(default_factory=dict)
    claim_type: str | None = None
    evidence: str = "discussion-only"  # primary | reported | discussion-only
    primary_urls: list[str] = field(default_factory=list)

    @property
    def title(self) -> str:
        # Prefer a primary (lab) title, then the most-discussed item.
        labs = [i for i in self.items if i.kind == "lab"]
        if labs:
            return labs[0].title
        return max(self.items, key=lambda i: i.metrics.get("points", 0) + i.metrics.get("score", 0)).title

    @property
    def urls(self) -> list[str]:
        out: list[str] = []
        for i in self.items:
            for u in (i.url, i.discussion_url):
                if u and u not in out:
                    out.append(u)
        return out


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #


class Http:
    def __init__(self, user_agent: str, timeout: int) -> None:
        self.ua = user_agent
        self.timeout = timeout

    def get(self, url: str, headers: dict[str, str] | None = None, data: bytes | None = None) -> bytes:
        h = {"User-Agent": self.ua, "Accept": "*/*"}
        h.update(headers or {})
        req = urllib.request.Request(url, headers=h, data=data)
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            body = r.read()
            if r.headers.get("Content-Encoding", "").lower() == "gzip" or body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            return body

    def json(self, url: str, headers: dict[str, str] | None = None, data: bytes | None = None) -> Any:
        return json.loads(self.get(url, headers, data).decode("utf-8"))


def canonical_url(url: str) -> str:
    try:
        p = urllib.parse.urlsplit(url.strip())
    except ValueError:
        return url.strip()
    host = p.netloc.lower().removeprefix("www.").removeprefix("m.")
    q = [(k, v) for k, v in urllib.parse.parse_qsl(p.query) if not k.lower().startswith("utm_") and k.lower() not in TRACKING_PARAMS]
    path = p.path.rstrip("/") or "/"
    return urllib.parse.urlunsplit(("https", host, path, urllib.parse.urlencode(q), ""))


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        dt = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        dt = None
    if dt is None:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def strip_html(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", text or "")).strip()


# --------------------------------------------------------------------------- #
# Fetchers — each returns list[Item] and may raise; caller records status.
# --------------------------------------------------------------------------- #


def parse_feed(raw: bytes, name: str, kind: str, since: datetime) -> list[Item]:
    root = ET.fromstring(raw)
    items: list[Item] = []
    atom = "{http://www.w3.org/2005/Atom}"
    # RSS 2.0
    for it in root.iter("item"):
        title = strip_html(it.findtext("title") or "")
        link = (it.findtext("link") or "").strip()
        pub = parse_date(it.findtext("pubDate") or it.findtext("{http://purl.org/dc/elements/1.1/}date"))
        if title and link and (pub is None or pub >= since):
            if pub is None:
                continue  # undated items can't be windowed; drop
            items.append(Item(name, kind, title, link, pub.isoformat()))
    # Atom
    for en in root.iter(f"{atom}entry"):
        title = strip_html(en.findtext(f"{atom}title") or "")
        link_el = next((l for l in en.findall(f"{atom}link") if l.get("rel") in (None, "alternate")), None)
        link = link_el.get("href", "") if link_el is not None else ""
        pub = parse_date(en.findtext(f"{atom}published") or en.findtext(f"{atom}updated"))
        if title and link and pub and pub >= since:
            items.append(Item(name, kind, title, link, pub.isoformat()))
    return items


def fetch_feeds(http: Http, cfg: dict, since: datetime, status: dict) -> list[Item]:
    out: list[Item] = []
    for f in cfg.get("feeds", []):
        key = f"feed:{f['name']}"
        try:
            got = parse_feed(http.get(f["url"]), f["name"], f.get("kind", "news"), since)
            out.extend(got)
            status[key] = f"ok ({len(got)})"
        except Exception as exc:  # noqa: BLE001
            status[key] = f"error: {type(exc).__name__}: {exc}"[:200]
    return out


def fetch_hn(http: Http, cfg: dict, since: datetime) -> list[Item]:
    c = cfg["hn"]
    ts = int(since.timestamp())
    items: list[Item] = []
    seen: set[str] = set()
    per_page = min(int(c.get("max_hits", 200)), 1000)
    for tags in ("story", "show_hn"):
        url = (
            "https://hn.algolia.com/api/v1/search_by_date?"
            f"tags={tags}&numericFilters=created_at_i%3E{ts},points%3E{int(c.get('min_points', 80))}"
            f"&hitsPerPage={per_page}"
        )
        for h in http.json(url).get("hits", []):
            oid = h["objectID"]
            if oid in seen:
                continue
            seen.add(oid)
            disc = f"https://news.ycombinator.com/item?id={oid}"
            title = h.get("title") or ""
            items.append(
                Item(
                    "Hacker News",
                    "community",
                    title,
                    h.get("url") or disc,
                    datetime.fromtimestamp(h["created_at_i"], timezone.utc).isoformat(),
                    disc,
                    {"points": h.get("points") or 0, "comments": h.get("num_comments") or 0},
                    ["show_hn"] if title.lower().startswith("show hn") else [],
                )
            )
    return items


def reddit_oauth_token(http: Http) -> str | None:
    cid, secret = os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET")
    if not (cid and secret):
        return None
    auth = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    tok = http.json("https://www.reddit.com/api/v1/access_token", {"Authorization": f"Basic {auth}"}, data)
    return tok.get("access_token")


def fetch_reddit(http: Http, cfg: dict, since: datetime, status: dict) -> list[Item]:
    """Reddit top-of-week. OAuth when keys are present, else public RSS.

    Without a key Reddit allows roughly one RSS request per minute per IP, so
    subreddits are fetched as a few *multireddits* (r/a+b+c) in one request
    each, with a bounded wait-and-retry on HTTP 429. Never raises.
    """
    c = cfg["reddit"]
    mode = c.get("mode", "auto")
    token = None
    if mode in ("auto", "oauth"):
        try:
            token = reddit_oauth_token(http)
        except Exception as exc:  # noqa: BLE001
            status["reddit:oauth"] = f"error, falling back to RSS: {exc}"[:200]
    status["reddit:mode"] = "oauth (score+comments)" if token else "rss (no key; rank only)"
    groups: list[list[str]] = c.get("groups") or [[s] for s in c.get("subreddits", [])]
    out: list[Item] = []
    for n, group in enumerate(groups):
        key = "reddit:r/" + "+".join(group)
        try:
            if token:
                got = [i for sub in group for i in _reddit_oauth(http, token, sub, int(c.get("limit", 25)))]
            else:
                if n:
                    time.sleep(float(c.get("rss_pause_seconds", 20)))
                got = _with_429_retry(
                    lambda g=group: _reddit_rss(http, g, int(c.get("rss_limit", 100)), since),
                    attempts=int(c.get("rss_retries", 3)),
                    wait=float(c.get("rss_retry_wait_seconds", 30)),
                )
            out.extend(got)
            status[key] = f"ok ({len(got)})"
        except Exception as exc:  # noqa: BLE001
            status[key] = f"error: {type(exc).__name__}: {exc}"[:200]
    return out


def _with_429_retry(fn: Callable[[], list[Item]], attempts: int, wait: float) -> list[Item]:
    for attempt in range(attempts + 1):
        try:
            return fn()
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == attempts:
                raise
            time.sleep(wait)
    return []


def _reddit_oauth(http: Http, token: str, sub: str, limit: int) -> list[Item]:
    data = http.json(
        f"https://oauth.reddit.com/r/{sub}/top?t=week&limit={limit}",
        {"Authorization": f"Bearer {token}"},
    )
    out = []
    for rank, ch in enumerate(data["data"]["children"], 1):
        d = ch["data"]
        disc = "https://www.reddit.com" + d["permalink"]
        url = d.get("url_overridden_by_dest") or disc
        out.append(
            Item(
                f"r/{sub}",
                "community",
                d["title"],
                url,
                datetime.fromtimestamp(d["created_utc"], timezone.utc).isoformat(),
                disc,
                {"score": d.get("score", 0), "comments": d.get("num_comments", 0), "rank": rank},
            )
        )
    return out


def _reddit_rss(http: Http, subs: list[str], limit: int, since: datetime) -> list[Item]:
    raw = http.get(f"https://www.reddit.com/r/{'+'.join(subs)}/top/.rss?t=week&limit={limit}")
    return parse_reddit_atom(raw, since)


def parse_reddit_atom(raw: bytes, since: datetime) -> list[Item]:
    if b"<feed" not in raw[:1000]:
        raise RuntimeError("bot wall / non-feed response")
    atom = "{http://www.w3.org/2005/Atom}"
    root = ET.fromstring(raw)
    out = []
    ranks: dict[str, int] = {}
    for en in root.iter(f"{atom}entry"):
        cat = en.find(f"{atom}category")
        sub = cat.get("term") if cat is not None else "reddit"
        ranks[sub] = ranks.get(sub, 0) + 1  # feed is score-sorted => per-sub rank
        title = strip_html(en.findtext(f"{atom}title") or "")
        link_el = en.find(f"{atom}link")
        disc = link_el.get("href", "") if link_el is not None else ""
        content = html.unescape(en.findtext(f"{atom}content") or "")
        # The "[link]" anchor points to the external target for link posts.
        m = re.search(r'<a href="([^"]+)">\[link\]</a>', content)
        url = m.group(1) if m else disc
        pub = parse_date(en.findtext(f"{atom}published") or en.findtext(f"{atom}updated"))
        if pub and pub < since:
            continue
        out.append(Item(f"r/{sub}", "community", title, url, pub.isoformat() if pub else None, disc, {"rank": ranks[sub]}))
    return out


def fetch_anthropic(http: Http, cfg: dict, since: datetime) -> list[Item]:
    """Anthropic has no RSS; parse the cards on /news (title + date)."""
    page = http.get("https://www.anthropic.com/news").decode("utf-8", "replace")
    out: dict[str, Item] = {}
    for href, body in re.findall(r'href="(/news/[a-z0-9\-]+|/claude-[a-z0-9\-]+)"[^>]*>(.*?)</a>', page, re.S):
        parts = [html.unescape(p).strip() for p in re.split(r"<[^>]+>", body) if p.strip()]
        date = next((d for d in map(_parse_mdy, parts) if d), None)
        # Category chips ("Announcements", "Product") are single words; titles are not.
        cands = [p for p in parts if " " in p and len(p) > 12 and not _parse_mdy(p)]
        if not date or date < since or not cands:
            continue
        out.setdefault(href, Item("Anthropic", "lab", cands[0], "https://www.anthropic.com" + href, date.isoformat()))
    return list(out.values())


def _parse_mdy(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%b %d, %Y").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def fetch_hf(http: Http, cfg: dict, since: datetime) -> list[Item]:
    data = http.json(f"https://huggingface.co/api/models?sort=trendingScore&limit={int(cfg['huggingface'].get('limit', 15))}")
    out = []
    for rank, m in enumerate(data, 1):
        created = parse_date(m.get("createdAt"))
        out.append(
            Item(
                "HF trending",
                "popularity",
                f"{m['id']} ({m.get('pipeline_tag') or 'model'})",
                f"https://huggingface.co/{m['id']}",
                created.isoformat() if created else None,
                None,
                {"likes": m.get("likes", 0), "trending": m.get("trendingScore", 0), "rank": rank},
                ["tool", "new" if created and created >= since else "old"],
            )
        )
    return out


def fetch_github(http: Http, cfg: dict, since: datetime) -> list[Item]:
    c = cfg["github"]
    headers = {"Accept": "application/vnd.github+json"}
    if os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    day = since.date().isoformat()
    out: dict[str, Item] = {}
    for topic in c.get("topics", []):
        q = urllib.parse.quote(f"created:>{day} topic:{topic} stars:>={int(c.get('min_stars', 150))}")
        data = http.json(f"https://api.github.com/search/repositories?q={q}&sort=stars&order=desc&per_page=10", headers)
        for r in data.get("items", []):
            out.setdefault(
                r["full_name"],
                Item(
                    "GitHub new repos",
                    "popularity",
                    f"{r['full_name']}: {(r.get('description') or '').strip()[:140]}",
                    r["html_url"],
                    r.get("created_at"),
                    None,
                    {"stars": r.get("stargazers_count", 0)},
                    ["tool"],
                ),
            )
    return list(out.values())


def fetch_lobsters(http: Http, cfg: dict, since: datetime) -> list[Item]:
    out: dict[str, Item] = {}
    for tag in cfg["lobsters"].get("tags", []):
        for s in http.json(f"https://lobste.rs/t/{tag}.json"):
            pub = parse_date(s.get("created_at"))
            if not pub or pub < since:
                continue
            out.setdefault(
                s["short_id"],
                Item(
                    "Lobsters",
                    "community",
                    s["title"],
                    s.get("url") or s["short_id_url"],
                    pub.isoformat(),
                    s["short_id_url"],
                    {"lobsters_score": s.get("score", 0), "comments": s.get("comment_count", 0)},
                ),
            )
    return list(out.values())


# --------------------------------------------------------------------------- #
# Clustering and scoring
# --------------------------------------------------------------------------- #


def tokens(title: str) -> set[str]:
    words = re.findall(r"[a-z0-9][a-z0-9.\-+]*", title.lower())
    return {w.strip(".-") for w in words if w not in STOPWORDS and len(w) > 2}


def entities(title: str) -> set[str]:
    """Named model entities. A following Capitalised word is kept as the variant
    ("GPT-6 Sol" vs "GPT-6 Astra") so sibling models don't merge."""
    out = set()
    for m in FRONTIER_RE.finditer(title):
        ent = re.sub(r"[\s\-]+", " ", m.group(0).lower()).strip()
        ent = re.sub(r"^(claude )", "", ent)
        nxt = re.match(r"[\s\-]+([A-Z][a-z]+)\b", title[m.end():])
        if nxt and nxt.group(1).lower() not in STOPWORDS | {"is", "and", "benchmarks", "creates", "comes"}:
            ent += " " + nxt.group(1).lower()
        out.add(ent)
    return out


def cluster_items(items: list[Item], threshold: float = 0.5) -> list[Cluster]:
    clusters: list[Cluster] = []
    by_url: dict[str, Cluster] = {}
    sigs: list[tuple[set[str], set[str], Cluster]] = []
    for it in items:
        cu = canonical_url(it.url)
        c = by_url.get(cu)
        if c is None:
            tk, en = tokens(it.title), entities(it.title)
            for stk, sen, sc in sigs:
                inter = len(tk & stk)
                jacc = inter / max(1, len(tk | stk))
                # Same named frontier model + some word overlap => same event.
                # Same named model as the cluster's defining entity => same event.
                if jacc >= threshold or (en and sen and en & sen):
                    c = sc
                    break
        if c is None:
            c = Cluster([])
            clusters.append(c)
            # Only single-entity titles define a cluster's entity; roundups
            # ("Opus 5.5, GPT-6 Sol and a price war") join but don't seed.
            ents = entities(it.title)
            sigs.append((tokens(it.title), ents if len(ents) == 1 else set(), c))
        c.items.append(it)
        by_url.setdefault(cu, c)
    return clusters


def is_ai(c: Cluster) -> bool:
    return any(AI_RE.search(i.title) or DATA_ENG_RE.search(i.title) for i in c.items) or any(i.kind == "lab" for i in c.items)


def score_cluster(c: Cluster) -> None:
    # Per source take the strongest post (many threads in one sub about the
    # same event should not stack); breadth rewards multiple sources separately.
    def best(src_pred, key, default=0):
        per: dict[str, float] = {}
        for i in c.items:
            if src_pred(i.source) and key in i.metrics:
                per[i.source] = max(per.get(i.source, default), i.metrics[key])
        return per

    hn = [i for i in c.items if i.source == "Hacker News"]
    top_hn = max(hn, key=lambda i: i.metrics.get("points", 0), default=None)
    pts = top_hn.metrics.get("points", 0) if top_hn else 0
    hn_comments = top_hn.metrics.get("comments", 0) if top_hn else 0
    is_reddit = lambda s: s.startswith("r/")  # noqa: E731
    reddit_score = sum(best(is_reddit, "score").values())
    reddit_ranks = {s: min(i.metrics["rank"] for i in c.items if i.source == s and "rank" in i.metrics)
                    for s in {i.source for i in c.items if is_reddit(i.source) and "score" not in i.metrics and "rank" in i.metrics}}
    reddit_rank_pts = sum(max(0, 26 - r) for r in reddit_ranks.values())
    lob = sum(best(lambda s: s == "Lobsters", "lobsters_score").values())
    pop = sum(i.metrics.get("stars", 0) + i.metrics.get("likes", 0) for i in c.items)

    discussion = (
        math.log1p(pts)
        + 0.5 * math.log1p(hn_comments)
        + 0.8 * math.log1p(reddit_score)
        + 0.12 * reddit_rank_pts
        + 0.5 * math.log1p(lob)
    )
    sources = {i.source for i in c.items}
    breadth = 1.5 * (len(sources) - 1)
    has_primary = any(i.kind == "lab" for i in c.items)
    text = " ".join(i.title for i in c.items)

    novelty = 0.0
    if FRONTIER_RE.search(text):
        novelty += 3.0
        c.reasons.append("frontier model mentioned")
    if LAUNCH_RE.search(text):
        novelty += 1.5
        c.reasons.append("launch/release language")
    if PATCH_RE.search(text) and not FRONTIER_RE.search(text):
        novelty -= 1.5
        c.reasons.append("looks like a patch/version bump")
    primary = 1.5 if has_primary else 0.0
    popularity = 0.4 * math.log1p(pop)
    money = funding_usd(" ".join(i.title for i in c.items if FUNDING_RE.search(i.title)))
    funding = 0.8 * math.log10(money / 1e6 + 1) * 2 if money else 0.0
    if money:
        c.reasons.append(f"funding ~${money / 1e6:,.0f}M")

    c.factors = {
        "discussion": round(discussion, 2),
        "breadth": round(breadth, 2),
        "novelty": round(novelty, 2),
        "primary_source": primary,
        "popularity": round(popularity, 2),
        "funding": round(funding, 2),
        "hn_points": pts,
        "hn_comments": hn_comments,
        "reddit_score": reddit_score,
        "n_sources": len(sources),
    }
    c.score = round(discussion + breadth + novelty + primary + popularity + funding, 2)
    if pts:
        c.reasons.append(f"HN {pts} pts / {hn_comments} comments")
    if len(sources) > 1:
        c.reasons.append(f"{len(sources)} independent sources")
    if has_primary:
        c.reasons.append("primary source")

    # Evidence rule: factual claims (release/price/funding/court) need an
    # official primary source; otherwise the writer must present them as
    # "reported/discussed", never as established fact. HN/Reddit are signal only.
    c.claim_type = claim_type(text)
    # Lab posts first; github/HF pages are primary only for their own repo/model,
    # so they count only when no lab post is in the cluster.
    primary_urls = [i.url for i in c.items if i.kind == "lab"] or [
        i.url for i in c.items if is_official_url(i.url, allow_hosting=True)]
    c.evidence = "primary" if primary_urls else ("reported" if any(i.kind in ("news", "curator") for i in c.items) else "discussion-only")
    c.primary_urls = list(dict.fromkeys(canonical_url(u) for u in primary_urls))[:3]

    lead = c.title
    if money or FUNDING_RE.search(lead):
        c.section = "startups_funding"
    elif has_primary or FRONTIER_RE.search(lead):
        c.section = "main"
    elif any("tool" in i.tags or "show_hn" in i.tags for i in c.items):
        c.section = "tools"
    elif DATA_ENG_RE.search(text):
        c.section = "data_engineering"
    elif novelty > 0:
        c.section = "main"
    else:
        # High discussion, no new capability/model: what people argue about.
        c.section = "debate"


OFFICIAL_DOMAINS = (
    "openai.com", "anthropic.com", "deepmind.google", "blog.google", "ai.google", "ai.meta.com",
    "mistral.ai", "x.ai", "qwen.ai", "qwenlm.github.io", "deepseek.com", "huggingface.co",
    "github.com", "arxiv.org", "nvidia.com", "microsoft.com", "aws.amazon.com", "cloud.google.com",
    "databricks.com", "snowflake.com", "courtlistener.com", "sec.gov", "supremecourt.gov",
)
CLAIM_PATTERNS = [
    ("funding", FUNDING_RE),
    ("court/regulation", re.compile(r"\b(court|judge|ruling|lawsuit|sues?|sued|ftc|doj|regulator\w*|designation|ban(s|ned)?)\b", re.I)),
    ("pricing", re.compile(r"\b(pric\w+|cheaper|cost\w*|\$\d)", re.I)),
    ("release", re.compile(r"\b(introduc\w+|launch\w*|releas\w+|announc\w+|now available|open[-\s]?sourc\w+)\b", re.I)),
]


def claim_type(text: str) -> str | None:
    return next((name for name, rx in CLAIM_PATTERNS if rx.search(text)), None)


HOSTING_DOMAINS = {"github.com", "huggingface.co", "arxiv.org"}


def is_official_url(url: str, allow_hosting: bool = False) -> bool:
    host = urllib.parse.urlsplit(url).netloc.lower().removeprefix("www.")
    doms = OFFICIAL_DOMAINS if allow_hosting else tuple(d for d in OFFICIAL_DOMAINS if d not in HOSTING_DOMAINS)
    return any(host == d or host.endswith("." + d) for d in doms)


MONEY_RE = re.compile(r"\$\s?(\d+(?:\.\d+)?)\s?(b|bn|billion|m|mn|million)\b", re.I)


def funding_usd(text: str) -> float:
    best = 0.0
    for num, unit in MONEY_RE.findall(text):
        mult = 1e9 if unit.lower().startswith("b") else 1e6
        best = max(best, float(num) * mult)
    return best


# --------------------------------------------------------------------------- #
# History / dedupe
# --------------------------------------------------------------------------- #


def load_history(state_path: Path, extra_files: list[Path]) -> set[str]:
    seen: set[str] = set()
    if state_path.exists():
        try:
            seen.update(json.loads(state_path.read_text()).get("urls", []))
        except (json.JSONDecodeError, OSError):
            pass
    for f in extra_files:
        if f.exists():
            seen.update(canonical_url(u) for u in re.findall(r"https?://[^\s)>\"\]]+", f.read_text()))
    return seen


def save_history(state_path: Path, clusters: list[Cluster], previous: set[str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    urls = set(previous)
    for c in clusters:
        urls.update(canonical_url(u) for u in c.urls)
    state_path.write_text(json.dumps({"updated": datetime.now(timezone.utc).isoformat(), "urls": sorted(urls)}, indent=1))


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #

SECTION_TITLES = {
    "main": "Main signal (models, capabilities, big-lab moves)",
    "debate": "What people are arguing about (high discussion, no new capability)",
    "startups_funding": "Startups & money",
    "tools": "Tools / repos / models to watch",
    "data_engineering": "Data-engineering corner",
}
SECTION_LIMITS = {"main": 10, "debate": 6, "startups_funding": 6, "tools": 8, "data_engineering": 5}


def render_markdown(result: dict) -> str:
    lines = [
        f"# AI radar collector — {result['window']['from'][:10]} → {result['window']['to'][:10]}",
        "",
        "Pre-ranked by script (discussion volume, independent sources, novelty, primary source).",
        "Write up these clusters in rank order; do not re-rank by opinion. Keep fact / discussion / opinion separate.",
        "Evidence rule: state release/price/funding/court claims as fact ONLY when evidence=primary (cite primary URL).",
        "Otherwise phrase as 'сообщается / обсуждают' with the source named. HN/Reddit = discussion metric, never proof.",
        "",
    ]
    for sec, title in SECTION_TITLES.items():
        rows = result["sections"].get(sec, [])
        if not rows:
            continue
        lines += [f"## {title}", ""]
        for n, c in enumerate(rows, 1):
            lines.append(f"{n}. **{c['title']}** — score {c['score']}; why: {', '.join(c['reasons']) or 'n/a'}")
            ev = c["evidence"] + (f" ({c['claim_type']} claim)" if c.get("claim_type") else "")
            lines.append(f"   evidence: {ev}" + (f"; primary: {', '.join(c['primary_urls'])}" if c["primary_urls"] else ""))
            for it in c["items"][:5]:
                m = ", ".join(f"{k}={int(v)}" for k, v in it["metrics"].items() if k != "trending")
                disc = f" | discussion: {it['discussion_url']}" if it.get("discussion_url") and it["discussion_url"] != it["url"] else ""
                lines.append(f"   - [{it['source']}] {it['title']} — {it['url']}{disc}{f' ({m})' if m else ''}")
        lines.append("")
    lines += ["## Source status", ""]
    lines += [f"- {k}: {v}" for k, v in sorted(result["source_status"].items())]
    return "\n".join(lines) + "\n"


def collect(cfg: dict, days: int, now: datetime | None = None) -> tuple[list[Item], dict]:
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    http = Http(cfg.get("user_agent", "ai-radar-collector/0.1"), int(cfg.get("timeout_seconds", 20)))
    status: dict[str, str] = {}
    items: list[Item] = []

    def run(key: str, fn: Callable[[], list[Item]]) -> None:
        try:
            got = fn()
            items.extend(got)
            status[key] = f"ok ({len(got)})"
        except Exception as exc:  # noqa: BLE001 - one source must never kill the run
            status[key] = f"error: {type(exc).__name__}: {exc}"[:200]

    if cfg.get("hn", {}).get("enabled", True):
        run("hn", lambda: fetch_hn(http, cfg, since))
    if cfg.get("lobsters", {}).get("enabled", True):
        run("lobsters", lambda: fetch_lobsters(http, cfg, since))
    if cfg.get("huggingface", {}).get("enabled", True):
        run("huggingface", lambda: fetch_hf(http, cfg, since))
    if cfg.get("github", {}).get("enabled", True):
        run("github", lambda: fetch_github(http, cfg, since))
    if cfg.get("anthropic", {}).get("enabled", True):
        run("anthropic", lambda: fetch_anthropic(http, cfg, since))
    items.extend(fetch_feeds(http, cfg, since, status))
    # Reddit is opt-in (fail-closed): its Developer Terms require approval before any
    # API access, including anonymous RSS/.json. No request is made unless enabled is true.
    reddit_cfg = cfg.get("reddit", {})
    if reddit_cfg.get("enabled", False) is True:
        try:
            items.extend(fetch_reddit(http, cfg, since, status))
        except Exception as exc:  # noqa: BLE001
            status["reddit"] = f"error: {exc}"[:200]
    else:
        status["reddit"] = "disabled: " + str(reddit_cfg.get("disabled_reason", "reddit.enabled is not true"))
    return items, {"since": since, "now": now, "status": status}


def rank(items: list[Item], history: set[str]) -> tuple[dict[str, list[Cluster]], int]:
    clusters = cluster_items(items)
    kept: list[Cluster] = []
    dropped_seen = 0
    for c in clusters:
        if not is_ai(c):
            continue
        if history and all(canonical_url(u) in history for u in c.urls):
            dropped_seen += 1
            continue
        score_cluster(c)
        kept.append(c)
    sections: dict[str, list[Cluster]] = {k: [] for k in SECTION_TITLES}
    for c in sorted(kept, key=lambda c: c.score, reverse=True):
        if len(sections[c.section]) < SECTION_LIMITS[c.section]:
            sections[c.section].append(c)
    return sections, dropped_seen


def to_result(sections: dict[str, list[Cluster]], meta: dict, n_items: int, dropped_seen: int) -> dict:
    return {
        "window": {"from": meta["since"].isoformat(), "to": meta["now"].isoformat()},
        "n_items": n_items,
        "dropped_as_already_sent": dropped_seen,
        "sections": {
            k: [
                {
                    "title": c.title,
                    "score": c.score,
                    "reasons": c.reasons,
                    "factors": c.factors,
                    "claim_type": c.claim_type,
                    "evidence": c.evidence,
                    "primary_urls": c.primary_urls,
                    "items": [asdict(i) for i in sorted(c.items, key=lambda i: i.kind != "lab")],
                }
                for c in v
            ]
            for k, v in sections.items()
        },
        "source_status": meta["status"],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--days", type=int)
    ap.add_argument("--format", choices=["markdown", "json"], default="markdown")
    ap.add_argument("--out", type=Path, help="also write JSON result here")
    ap.add_argument("--state", type=Path, default=DEFAULT_STATE, help="seen-URL state for cross-issue dedupe")
    ap.add_argument("--history-file", type=Path, action="append", default=[], help="text file with previously sent URLs (e.g. a dump of past issues)")
    ap.add_argument("--no-dedupe", action="store_true")
    ap.add_argument("--save-raw", type=Path, help="save raw collected items (for offline re-ranking / weight tuning)")
    ap.add_argument("--replay", type=Path, help="skip network; re-rank items from a --save-raw file")
    ap.add_argument("--commit", action="store_true", help="record this run's URLs into --state (do after the issue is published)")
    args = ap.parse_args(argv)

    cfg = json.loads(args.config.read_text())
    days = args.days or int(cfg.get("window_days", 7))
    if args.replay:
        raw = json.loads(args.replay.read_text())
        items = [Item(**i) for i in raw["items"]]
        meta = {"since": datetime.fromisoformat(raw["since"]), "now": datetime.fromisoformat(raw["now"]), "status": raw["status"]}
    else:
        items, meta = collect(cfg, days)
    if args.save_raw:
        args.save_raw.parent.mkdir(parents=True, exist_ok=True)
        args.save_raw.write_text(json.dumps({"since": meta["since"].isoformat(), "now": meta["now"].isoformat(),
                                             "status": meta["status"], "items": [asdict(i) for i in items]}, ensure_ascii=False))
    history = set() if args.no_dedupe else load_history(args.state, args.history_file)
    sections, dropped = rank(items, history)
    result = to_result(sections, meta, len(items), dropped)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, ensure_ascii=False, indent=1))
    if args.commit:
        save_history(args.state, [c for v in sections.values() for c in v], history)

    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=1) if args.format == "json" else render_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

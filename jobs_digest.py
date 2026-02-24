import os
import json
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import feedparser
import httpx


# ====== Telegram env ======
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL"]

# Buenos Aires fixed offset (UTC-3)
BA_TZ = timezone(timedelta(hours=-3))


# ====== Helpers ======
def now_ba() -> datetime:
    return datetime.now(timezone.utc).astimezone(BA_TZ)


def ru_date(dt: datetime) -> str:
    # DD.MM
    return dt.strftime("%d.%m")


def ru_time(dt: datetime) -> str:
    # HH:MM
    return dt.strftime("%H:%M")


def load_config(path: str = "jobs_sources.json") -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_state(path: str = "state.json") -> Dict:
    if not os.path.exists(path):
        return {"published": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "published" not in data or not isinstance(data["published"], list):
        data["published"] = []
    return data


def save_state(state: Dict, path: str = "state.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def normalize_url(url: str) -> str:
    """Remove common tracking params and fragments for better dedup."""
    if not url:
        return url
    url = url.strip()
    # drop fragment
    url = url.split("#", 1)[0]
    # strip common utm params
    url = re.sub(r"(\?|&)(utm_[^=]+|ref|source|fbclid|gclid)=[^&]+", "", url, flags=re.I)
    # cleanup ?& remnants
    url = url.replace("?&", "?").rstrip("?&")
    return url


def html_escape(s: str) -> str:
    if s is None:
        return ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def text_contains_any(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def clean_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


@dataclass
class Job:
    title: str
    link: str
    source: str
    published_dt: Optional[datetime] = None
    summary: str = ""


# ====== RSS parsing ======
def parse_entry_date(entry) -> Optional[datetime]:
    """
    Try to parse published/updated date from feedparser entry into timezone-aware UTC dt.
    """
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(entry, key, None)
        if t:
            try:
                # feedparser gives time.struct_time in UTC-ish; assume UTC
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def fetch_jobs_from_rss(feed_name: str, url: str) -> List[Job]:
    parsed = feedparser.parse(url)
    jobs: List[Job] = []
    for entry in getattr(parsed, "entries", []) or []:
        title = clean_whitespace(getattr(entry, "title", "") or "")
        link = normalize_url(getattr(entry, "link", "") or "")
        summary = clean_whitespace(getattr(entry, "summary", "") or getattr(entry, "description", "") or "")
        dt = parse_entry_date(entry)
        if title and link:
            jobs.append(Job(title=title, link=link, source=feed_name, published_dt=dt, summary=summary))
    return jobs


# ====== Filtering & scoring ======
def within_lookback(job: Job, lookback_hours: int) -> bool:
    if not job.published_dt:
        # If feed doesn't provide date, allow it (feeds usually contain recent items).
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    return job.published_dt >= cutoff


def build_filter_text(job: Job) -> str:
    # Use title + summary to catch geo/role hints
    return f"{job.title} {job.summary}".lower()


def passes_filters(job: Job, filters: Dict) -> bool:
    text = build_filter_text(job)

    if filters.get("require_remote", True):
        remote_kw = filters.get("remote_keywords", [])
        # Some feeds are remote-only; still good to keep remote requirement lenient:
        # If not found in text, don't drop immediately — but penalize in scoring.
        # Here we do not hard-drop if missing remote keyword.
        pass

    include_kw = filters.get("include_keywords", [])
    exclude_kw = filters.get("exclude_keywords", [])

    # Include: require at least 1 include keyword
    if include_kw and not text_contains_any(text, include_kw):
        return False

    # Exclude: drop if contains any exclude keyword
    if exclude_kw and text_contains_any(text, exclude_kw):
        return False

    # Age
    max_age = int(filters.get("max_age_hours", 168))
    if not within_lookback(job, max_age):
        return False

    return True


def score_job(job: Job, filters: Dict) -> int:
    """
    Simple rule-based scoring:
    - geo priority mentions
    - remote keyword mentions
    - salary presence heuristic
    """
    text = build_filter_text(job)
    score = 0

    # Geo priority
    geo_kw = filters.get("geo_priority_keywords", [])
    if geo_kw and text_contains_any(text, geo_kw):
        score += 3

    # Remote hints
    remote_kw = filters.get("remote_keywords", [])
    if remote_kw and text_contains_any(text, remote_kw):
        score += 2

    # Salary heuristic
    # Detect $ or USD or ranges like 50k, 100k, 4k–6k
    if re.search(r"(\$|usd|\b\d{2,3}\s?k\b|\b\d{4,6}\b)", text, flags=re.I):
        score += 1

    # Seniority heuristic
    if re.search(r"\bsenior\b|\blead\b|\bstaff\b|\bprincipal\b", text, flags=re.I):
        score += 1

    return score


# ====== Dedup ======
def job_key(job: Job) -> str:
    """
    Dedup key:
    - normalized link (best)
    - fallback: normalized title
    """
    link = normalize_url(job.link)
    title = re.sub(r"\s+", " ", job.title.lower()).strip()
    return sha(f"{link}::{title}")


# ====== Tagging (lightweight) ======
def infer_tags(job: Job) -> List[str]:
    t = build_filter_text(job)
    tags = []

    # roles
    if "product manager" in t or re.search(r"\bpm\b", t):
        tags.append("#product")
    if "designer" in t or "ux" in t or "ui" in t:
        tags.append("#design")
    if "backend" in t:
        tags.append("#backend")
    if "frontend" in t or "front-end" in t:
        tags.append("#frontend")
    if "full stack" in t or "fullstack" in t:
        tags.append("#fullstack")
    if "data" in t or "analytics" in t or "analyst" in t:
        tags.append("#data")
    if "machine learning" in t or re.search(r"\bml\b", t) or re.search(r"\bai\b", t):
        tags.append("#ai")

    # geo (basic)
    geo_map = [
        ("mexico", "#mexico"),
        ("brazil", "#brazil"),
        ("argentina", "#argentina"),
        ("chile", "#chile"),
        ("colombia", "#colombia"),
        ("latam", "#latam"),
        ("latin america", "#latam"),
        ("usa", "#usa"),
        ("united states", "#usa"),
    ]
    for kw, tag in geo_map:
        if kw in t and tag not in tags:
            tags.append(tag)

    # keep tags short
    return tags[:5]


# ====== Analytics comment (free, rule-based) ======
def market_signal(jobs: List[Job]) -> str:
    if not jobs:
        return "рынок выглядит спокойным: в выборке мало новых ролей."

    text_all = " ".join(build_filter_text(j) for j in jobs)

    # role counts
    role_buckets = [
        ("product", ["product manager", "product", " pm "]),
        ("design", ["designer", "ux", "ui", "figma"]),
        ("backend", ["backend", "golang", "java", "python", "node"]),
        ("frontend", ["frontend", "react", "vue", "angular"]),
        ("data/ai", ["data", "analytics", "machine learning", " ml ", " ai "]),
        ("devops", ["devops", "sre", "kubernetes", "terraform", "cloud"]),
    ]
    counts: List[Tuple[str, int]] = []
    for name, kws in role_buckets:
        c = 0
        for kw in kws:
            c += text_all.count(kw.strip())
        counts.append((name, c))
    counts.sort(key=lambda x: x[1], reverse=True)

    top1, top1c = counts[0]
    top2, top2c = counts[1] if len(counts) > 1 else (None, 0)

    # salary presence heuristic
    salary_count = 0
    for j in jobs:
        if re.search(r"(\$|usd|\b\d{2,3}\s?k\b|\b\d{4,6}\b)", build_filter_text(j), flags=re.I):
            salary_count += 1

    # geo hints
    geo = []
    for kw, label in [("mexico", "Мексика"), ("brazil", "Бразилия"), ("argentina", "Аргентина"), ("chile", "Чили"), ("colombia", "Колумбия"), ("usa", "США"), ("latam", "LATAM")]:
        if kw in text_all:
            geo.append(label)
    geo = geo[:3]

    parts = []
    if top1c > 0:
        if top2c > 0:
            parts.append(f"в этой подборке чаще встречаются роли {top1} и {top2}")
        else:
            parts.append(f"в этой подборке чаще встречаются роли {top1}")
    if salary_count > 0:
        parts.append(f"вилки/суммы указаны примерно у {salary_count} из {len(jobs)}")
    if geo:
        parts.append("фокус по гео: " + ", ".join(geo))

    if not parts:
        return "вакансии разнотипные, явного доминирующего тренда в этой выборке нет."
    return "; ".join(parts) + "."


# ====== Formatting ======
def build_post(jobs: List[Job], cfg: Dict) -> str:
    fmt = cfg.get("formatting", {})
    meta = cfg.get("meta", {})
    max_per_post = int(fmt.get("max_per_post", meta.get("max_items_per_digest", 10)))

    jobs = jobs[:max_per_post]
    dt = now_ba()
    title = fmt.get("title_template", "💼 Remote LATAM Jobs — {date_ru} • {time_ba} BA").format(
        date_ru=ru_date(dt),
        time_ba=ru_time(dt),
    )

    # Sources short names
    sources_short = []
    short_map = (cfg.get("formatting", {}).get("source_short_names") or {})
    for j in jobs:
        s = j.source
        sources_short.append(short_map.get(s, s))
    sources_short = ", ".join(sorted(set(sources_short))) if sources_short else "RSS"

    header = f"<b>{html_escape(title)}</b>\n\n"
    header += f"Короткая подборка свежих remote-вакансий (фокус: LATAM / worldwide).\n"
    header += f"Отобрано: <b>{len(jobs)}</b> • Источники: <b>{html_escape(sources_short)}</b>\n\n"

    lines = [header]

    for idx, job in enumerate(jobs, 1):
        tags = infer_tags(job)
        tags_inline = " ".join(tags) if tags else "#jobs"

        # Pretty clickable link
        link_html = f'<a href="{html_escape(job.link)}">Откликнуться</a>'

        lines.append(
            f"<b>{idx}. {html_escape(job.title)}</b>\n"
            f"🏷 {html_escape(tags_inline)}\n"
            f"🔗 {link_html}\n"
        )

    signal = market_signal(jobs)
    lines.append(f"\n📌 <b>Сигнал рынка:</b> {html_escape(signal)}\n")

    footer_tags = fmt.get("footer_tags", ["#jobs", "#remote", "#latam"])
    # Add a few inferred tags from all jobs
    inferred = []
    for j in jobs:
        inferred.extend(infer_tags(j))
    # Keep unique + short
    inferred = [t for t in sorted(set(inferred)) if t.startswith("#")]
    inferred = inferred[:8]

    footer = " ".join(footer_tags + inferred)
    lines.append(f"\n{html_escape(footer)}")

    return "\n".join(lines).strip()


# ====== Telegram ======
def tg_send_html(text_html: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = httpx.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


# ====== Main ======
def main() -> None:
    cfg = load_config()
    state = load_state()

    meta = cfg.get("meta", {})
    feeds = cfg.get("feeds", [])
    filters = cfg.get("filters", {})
    max_items = int(meta.get("max_items_per_digest", 10))
    min_items = int(meta.get("min_items_per_digest", 3))
    lookback_hours = int(meta.get("lookback_hours", 72))

    # Collect
    jobs: List[Job] = []
    for f in feeds:
        if not f.get("enabled", True):
            continue
        if f.get("type") != "rss":
            continue
        feed_name = f.get("id") or f.get("name") or "feed"
        url = f.get("url")
        if not url:
            continue
        try:
            jobs.extend(fetch_jobs_from_rss(feed_name, url))
        except Exception as e:
            print(f"[WARN] Failed feed {feed_name}: {e}")

    # Filter + lookback + dedup (state)
    published = set(state.get("published", []))
    fresh: List[Tuple[int, Job, str]] = []  # (score, job, key)

    for job in jobs:
        if not within_lookback(job, lookback_hours):
            continue
        if not passes_filters(job, filters):
            continue

        key = job_key(job)
        if key in published:
            continue

        sc = score_job(job, filters)
        fresh.append((sc, job, key))

    if not fresh:
        print("[INFO] No new jobs after filtering.")
        return

    # Sort: score desc, then date desc if available
    fresh.sort(
        key=lambda x: (
            x[0],
            x[1].published_dt.timestamp() if x[1].published_dt else 0,
        ),
        reverse=True,
    )

    # Keep unique by company-ish (basic heuristic: first token before dash/at)
    chosen: List[Tuple[Job, str]] = []
    seen_companies = set()
    for sc, job, key in fresh:
        # best-effort company extraction from title like "Role at Company" or "Role — Company"
        company = ""
        m = re.search(r"\s(?:at|@|—|-)\s(.+)$", job.title, flags=re.I)
        if m:
            company = clean_whitespace(m.group(1)).lower()
        if company and company in seen_companies:
            continue
        if company:
            seen_companies.add(company)
        chosen.append((job, key))
        if len(chosen) >= max_items:
            break

    if len(chosen) < min_items:
        print(f"[INFO] Only {len(chosen)} jobs (< min {min_items}). Skipping post.")
        return

    chosen_jobs = [j for j, _ in chosen]

    # Build post HTML
    post_html = build_post(chosen_jobs, cfg)
    tg_send_html(post_html)

    # Update state (store keys)
    for _, key in chosen:
        published.add(key)

    # Keep state from exploding: last 5000 keys
    published_list = list(published)
    if len(published_list) > 5000:
        published_list = published_list[-5000:]

    state["published"] = published_list
    save_state(state)


if __name__ == "__main__":
    main()

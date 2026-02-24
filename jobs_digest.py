import os
import json
import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import feedparser
import httpx

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL"]

BA_TZ = timezone(timedelta(hours=-3))


def now_ba() -> datetime:
    return datetime.now(timezone.utc).astimezone(BA_TZ)


def ru_date(dt: datetime) -> str:
    return dt.strftime("%d.%m")


def ru_time(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def normalize_url(url: str) -> str:
    if not url:
        return url
    url = url.strip().split("#", 1)[0]
    url = re.sub(r"(\?|&)(utm_[^=]+|ref|source|fbclid|gclid)=[^&]+", "", url, flags=re.I)
    url = url.replace("?&", "?").rstrip("?&")
    return url


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_config() -> Dict:
    with open("jobs_sources.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_state() -> Dict:
    if not os.path.exists("state.json"):
        return {"published": []}
    with open("state.json", "r", encoding="utf-8") as f:
        st = json.load(f)
    if "published" not in st or not isinstance(st["published"], list):
        st["published"] = []
    return st


def save_state(st: Dict) -> None:
    with open("state.json", "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


def text_contains_any(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def parse_entry_date(entry) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(entry, key, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def fetch_rss(feed_id: str, url: str):
    parsed = feedparser.parse(url)
    out = []
    for e in getattr(parsed, "entries", []) or []:
        title = re.sub(r"\s+", " ", (getattr(e, "title", "") or "").strip())
        link = normalize_url(getattr(e, "link", "") or "")
        summary = re.sub(r"\s+", " ", (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip())
        dt = parse_entry_date(e)
        if title and link:
            out.append({"title": title, "link": link, "summary": summary, "dt": dt, "source": feed_id})
    return out


def within_lookback(dt: Optional[datetime], hours: int) -> bool:
    if not dt:
        return True
    return dt >= (datetime.now(timezone.utc) - timedelta(hours=hours))


def job_key(job: Dict) -> str:
    return sha(f"{normalize_url(job['link'])}::{job['title'].lower().strip()}")


def infer_tags(job: Dict) -> List[str]:
    t = (job["title"] + " " + (job.get("summary") or "")).lower()
    tags = []
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
    for kw, tg in geo_map:
        if kw in t and tg not in tags:
            tags.append(tg)
    return tags[:5]


def score(job: Dict, filters: Dict) -> int:
    text = (job["title"] + " " + (job.get("summary") or "")).lower()
    s = 0
    if text_contains_any(text, filters.get("geo_priority_keywords", [])):
        s += 3
    if text_contains_any(text, filters.get("remote_keywords", [])):
        s += 2
    if re.search(r"(\$|usd|\b\d{2,3}\s?k\b|\b\d{4,6}\b)", text, flags=re.I):
        s += 1
    if re.search(r"\bsenior\b|\blead\b|\bstaff\b|\bprincipal\b", text, flags=re.I):
        s += 1
    return s


def market_signal(jobs: List[Dict]) -> str:
    if not jobs:
        return "в выборке мало новых ролей."
    all_text = " ".join((j["title"] + " " + (j.get("summary") or "")).lower() for j in jobs)

    salary_count = 0
    for j in jobs:
        if re.search(r"(\$|usd|\b\d{2,3}\s?k\b|\b\d{4,6}\b)", (j["title"] + " " + (j.get("summary") or "")), flags=re.I):
            salary_count += 1

    hints = []
    for kw, name in [("fintech", "fintech"), ("saas", "B2B SaaS"), ("ai", "AI"), ("ml", "ML"), ("web3", "web3")]:
        if kw in all_text:
            hints.append(name)

    msg = []
    if hints:
        msg.append(f"встречаются темы: {', '.join(hints[:3])}")
    if salary_count:
        msg.append(f"вилки/суммы указаны примерно у {salary_count} из {len(jobs)}")
    return "; ".join(msg) + "." if msg else "вакансии разнотипные, явного доминирующего тренда нет."


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
        raise RuntimeError(str(data))


def build_post(jobs: List[Dict], cfg: Dict) -> str:
    dt = now_ba()
    title = cfg.get("formatting", {}).get("title_template", "💼 Remote LATAM Jobs — {date_ru} • {time_ba} BA").format(
        date_ru=ru_date(dt),
        time_ba=ru_time(dt),
    )

    # short sources
    short_map = cfg.get("formatting", {}).get("source_short_names", {}) or {}
    srcs = sorted(set(short_map.get(j["source"], j["source"]) for j in jobs))
    srcs_short = ", ".join(srcs) if srcs else "RSS"

    out = []
    out.append(f"<b>{html_escape(title)}</b>\n")
    out.append(f"Короткая подборка свежих remote-вакансий (фокус: LATAM / USA / worldwide).\n")
    out.append(f"Отобрано: <b>{len(jobs)}</b> • Источники: <b>{html_escape(srcs_short)}</b>\n")

    for i, j in enumerate(jobs, 1):
        tags = " ".join(infer_tags(j)) or "#jobs"
        link = f'<a href="{html_escape(j["link"])}">Откликнуться</a>'
        out.append(
            f"\n<b>{i}. {html_escape(j['title'])}</b>\n"
            f"🏷 {html_escape(tags)}\n"
            f"🔗 {link}\n"
        )

    out.append(f"\n📌 <b>Сигнал рынка:</b> {html_escape(market_signal(jobs))}\n")

    footer = cfg.get("formatting", {}).get("footer_tags", ["#jobs", "#remote", "#latam"])
    out.append("\n" + " ".join(footer))

    return "".join(out).strip()


def main():
    cfg = load_config()
    st = load_state()

    meta = cfg.get("meta", {})
    feeds = cfg.get("feeds", [])
    filters = cfg.get("filters", {})
    fmt = cfg.get("formatting", {})

    max_items = int(meta.get("max_items_per_digest", fmt.get("max_per_post", 10)))
    min_items = int(meta.get("min_items_per_digest", 3))
    lookback = int(meta.get("lookback_hours", 72))

    published = set(st.get("published", []))

    collected = []
    for f in feeds:
        if not f.get("enabled", True):
            continue
        if f.get("type") != "rss":
            continue
        fid = f.get("id") or f.get("name") or "feed"
        url = f.get("url")
        if not url:
            continue
        try:
            collected.extend(fetch_rss(fid, url))
        except Exception as e:
            print(f"[WARN] feed {fid} failed: {e}")

    # filter
    fresh: List[Tuple[int, Dict, str]] = []
    for j in collected:
        if not within_lookback(j.get("dt"), lookback):
            continue

        text = (j["title"] + " " + (j.get("summary") or "")).lower()

        inc = filters.get("include_keywords", [])
        exc = filters.get("exclude_keywords", [])
        if inc and not text_contains_any(text, inc):
            continue
        if exc and text_contains_any(text, exc):
            continue

        key = job_key(j)
        if key in published:
            continue

        fresh.append((score(j, filters), j, key))

    if not fresh:
        print("[INFO] no new jobs")
        return

    # sort by score, then date
    fresh.sort(key=lambda x: (x[0], x[1]["dt"].timestamp() if x[1].get("dt") else 0), reverse=True)

    chosen = []
    for sc, j, key in fresh:
        chosen.append((j, key))
        if len(chosen) >= max_items:
            break

    if len(chosen) < min_items:
        print("[INFO] below min items, skip post")
        return

    jobs = [j for j, _ in chosen]
    post = build_post(jobs, cfg)
    tg_send_html(post)

    for _, key in chosen:
        published.add(key)

    st["published"] = list(published)[-5000:]
    save_state(st)


if __name__ == "__main__":
    main()

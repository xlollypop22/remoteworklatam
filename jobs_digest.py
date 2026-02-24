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

# LATAM countries (English mentions)
LATAM_COUNTRIES = [
    "mexico",
    "brazil",
    "argentina",
    "chile",
    "colombia",
    "peru",
    "uruguay",
    "paraguay",
    "bolivia",
    "ecuador",
    "venezuela",
    "panama",
    "costa rica",
    "guatemala",
    "honduras",
    "el salvador",
    "nicaragua",
    "dominican",
    "dominican republic",
    "puerto rico",
]


# ---------------- Time / formatting ----------------
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


def text_contains_any(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


# ---------------- IO ----------------
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


# ---------------- RSS parsing ----------------
def parse_entry_date(entry) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(entry, key, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def fetch_rss(feed_id: str, url: str) -> List[Dict]:
    parsed = feedparser.parse(url)
    out: List[Dict] = []
    for e in getattr(parsed, "entries", []) or []:
        title = re.sub(r"\s+", " ", (getattr(e, "title", "") or "").strip())
        link = normalize_url(getattr(e, "link", "") or "")
        summary = re.sub(
            r"\s+",
            " ",
            (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip(),
        )
        dt = parse_entry_date(e)
        if title and link:
            out.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "dt": dt,
                    "source": feed_id,
                }
            )
    return out


def within_lookback(dt: Optional[datetime], hours: int) -> bool:
    if not dt:
        return True
    return dt >= (datetime.now(timezone.utc) - timedelta(hours=hours))


# ---------------- Dedup key ----------------
def job_key(job: Dict) -> str:
    return sha(f"{normalize_url(job['link'])}::{job['title'].lower().strip()}")


# ---------------- Tagging (lightweight) ----------------
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
    if "devops" in t or "sre" in t or "kubernetes" in t:
        tags.append("#devops")

    # LATAM geo tags only
    geo_map = [
        ("mexico", "#mexico"),
        ("brazil", "#brazil"),
        ("argentina", "#argentina"),
        ("chile", "#chile"),
        ("colombia", "#colombia"),
        ("peru", "#peru"),
        ("uruguay", "#uruguay"),
        ("paraguay", "#paraguay"),
        ("bolivia", "#bolivia"),
        ("ecuador", "#ecuador"),
        ("venezuela", "#venezuela"),
        ("panama", "#panama"),
        ("costa rica", "#costarica"),
        ("guatemala", "#guatemala"),
        ("honduras", "#honduras"),
        ("el salvador", "#elsalvador"),
        ("nicaragua", "#nicaragua"),
        ("dominican republic", "#dominican"),
        ("puerto rico", "#puertorico"),
        ("latam", "#latam"),
        ("latin america", "#latam"),
    ]
    for kw, tg in geo_map:
        if kw in t and tg not in tags:
            tags.append(tg)

    return tags[:6]


# ---------------- Extraction for structured карточки ----------------
def extract_company(title: str) -> str:
    t = (title or "").strip()

    m = re.match(r"^([^:]{2,60}):\s+(.+)$", t)
    if m:
        company = m.group(1).strip()
        if 2 <= len(company) <= 60:
            return company

    m = re.search(r"\s(?:at|@|—|-)\s(.+)$", t, flags=re.I)
    if m:
        company = m.group(1).strip()
        if 2 <= len(company) <= 60:
            return company

    return "—"


def extract_salary(text: str) -> str:
    if not text:
        return "—"

    t = text.replace(",", "")
    m = re.search(r"(\$|usd)\s?(\d{2,6})\s?(?:-|–|to)\s?(\d{2,6})", t, flags=re.I)
    if m:
        return f"${m.group(2)}–{m.group(3)}"

    m = re.search(r"(\$|usd)\s?(\d{2,6})\s?(k)?", t, flags=re.I)
    if m:
        val = m.group(2)
        if m.group(3):
            return f"${val}k"
        return f"${val}"

    m = re.search(r"\b(\d{2,3})\s?k\s?(?:-|–|to)\s?(\d{2,3})\s?k\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}k–{m.group(2)}k"

    return "—"


def extract_latam_location(text: str) -> str:
    t = (text or "").lower()
    # LATAM region keywords
    if "latin america" in t or "latam" in t:
        return "LATAM"

    # Country-level
    mapping = [
        ("mexico", "Mexico"),
        ("brazil", "Brazil"),
        ("argentina", "Argentina"),
        ("chile", "Chile"),
        ("colombia", "Colombia"),
        ("peru", "Peru"),
        ("uruguay", "Uruguay"),
        ("paraguay", "Paraguay"),
        ("bolivia", "Bolivia"),
        ("ecuador", "Ecuador"),
        ("venezuela", "Venezuela"),
        ("panama", "Panama"),
        ("costa rica", "Costa Rica"),
        ("guatemala", "Guatemala"),
        ("honduras", "Honduras"),
        ("el salvador", "El Salvador"),
        ("nicaragua", "Nicaragua"),
        ("dominican republic", "Dominican Republic"),
        ("puerto rico", "Puerto Rico"),
    ]
    for kw, name in mapping:
        if kw in t:
            return name

    return "—"


def extract_remote_type(text: str, loc: str) -> str:
    t = (text or "").lower()
    if "hybrid" in t:
        return "Hybrid"
    if "onsite" in t or "on-site" in t:
        return "Onsite"
    # We only publish LATAM jobs, so default remote label
    if loc == "LATAM":
        return "Remote / LATAM"
    # if конкретная страна LATAM
    if loc != "—":
        return "Remote"
    # fallback
    return "Remote"


def is_latam_job(text: str) -> bool:
    t = (text or "").lower()
    if "latin america" in t or "latam" in t:
        return True
    return any(country in t for country in LATAM_COUNTRIES)


# ---------------- Scoring ----------------
def score(job: Dict, filters: Dict) -> int:
    text = (job["title"] + " " + (job.get("summary") or "")).lower()
    s = 0

    # LATAM boosted
    if "latam" in text or "latin america" in text:
        s += 4
    if any(c in text for c in LATAM_COUNTRIES):
        s += 2

    if text_contains_any(text, filters.get("remote_keywords", [])):
        s += 2
    if re.search(r"(\$|usd|\b\d{2,3}\s?k\b|\b\d{4,6}\b)", text, flags=re.I):
        s += 1
    if re.search(r"\bsenior\b|\blead\b|\bstaff\b|\bprincipal\b", text, flags=re.I):
        s += 1
    return s


# ---------------- Market signal ----------------
def market_signal(jobs: List[Dict]) -> str:
    if not jobs:
        return "в выборке мало новых ролей."

    salary_count = 0
    loc_counts: Dict[str, int] = {}

    for j in jobs:
        full_text = j["title"] + " " + (j.get("summary") or "")
        if extract_salary(full_text) != "—":
            salary_count += 1
        loc = extract_latam_location(full_text)
        if loc != "—":
            loc_counts[loc] = loc_counts.get(loc, 0) + 1

    msg = []
    if loc_counts:
        top_loc = sorted(loc_counts.items(), key=lambda x: x[1], reverse=True)[0][0]
        msg.append(f"по гео чаще встречается: {top_loc}")
    if salary_count:
        msg.append(f"вилки/суммы указаны примерно у {salary_count} из {len(jobs)}")

    return "; ".join(msg) + "." if msg else "вакансии разнотипные, явного доминирующего тренда нет."


# ---------------- Telegram ----------------
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


# ---------------- Post builder ----------------
def build_post(jobs: List[Dict], cfg: Dict) -> str:
    dt = now_ba()
    title = cfg.get("formatting", {}).get(
        "title_template",
        "💼 Remote LATAM Jobs — {date_ru} • {time_ba} BA"
    ).format(
        date_ru=ru_date(dt),
        time_ba=ru_time(dt),
    )

    out = []
    out.append(f"<b>{html_escape(title)}</b>\n")
    out.append("Подборка свежих remote-вакансий\n")
    out.append(f"Отобрано: <b>{len(jobs)}</b>\n")

    for i, j in enumerate(jobs, 1):
        full_text = (j["title"] + " " + (j.get("summary") or "")).strip()

        role = j["title"].strip()
        company = extract_company(j["title"])
        salary = extract_salary(full_text)
        loc = extract_latam_location(full_text)
        remote_type = extract_remote_type(full_text, loc)

        tags = " ".join(infer_tags(j)) or "#jobs"
        link = f'<a href="{html_escape(j["link"])}">Откликнуться</a>'

        out.append(
            f"\n<b>{i}️⃣ {html_escape(role)}</b>\n"
            f"🏢 {html_escape(company)} ({html_escape(loc)})\n"
            f"💰 {html_escape(salary)}\n"
            f"🌍 {html_escape(remote_type)}\n"
            f"🔗 {link}\n"
            f"🏷 {html_escape(tags)}\n"
        )

    out.append(f"\n📌 <b>Сигнал рынка:</b> {html_escape(market_signal(jobs))}\n")

    footer = cfg.get("formatting", {}).get("footer_tags", ["#jobs", "#remote", "#latam"])
    out.append("\n" + " ".join(footer))

    return "".join(out).strip()


# ---------------- Main ----------------
def main() -> None:
    cfg = load_config()
    st = load_state()

    meta = cfg.get("meta", {})
    feeds = cfg.get("feeds", [])
    filters = cfg.get("filters", {})
    fmt = cfg.get("formatting", {})

    # от 3 до 12 (задаётся в jobs_sources.json)
    max_items = int(meta.get("max_items_per_digest", 12))
    min_items = int(meta.get("min_items_per_digest", 3))
    lookback = int(meta.get("lookback_hours", 72))

    published = set(st.get("published", []))

    collected: List[Dict] = []
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

    # Filter + LATAM-only gate
    fresh: List[Tuple[int, Dict, str]] = []
    for j in collected:
        if not within_lookback(j.get("dt"), lookback):
            continue

        full_text = (j["title"] + " " + (j.get("summary") or "")).lower()

        inc = filters.get("include_keywords", [])
        exc = filters.get("exclude_keywords", [])
        if inc and not text_contains_any(full_text, inc):
            continue
        if exc and text_contains_any(full_text, exc):
            continue

        # LATAM-only фильтр: берём только если есть LATAM/страна в тексте
        if not is_latam_job(full_text):
            continue

        key = job_key(j)
        if key in published:
            continue

        fresh.append((score(j, filters), j, key))

    if not fresh:
        print("[INFO] no new LATAM jobs")
        return

    fresh.sort(
        key=lambda x: (
            x[0],
            x[1]["dt"].timestamp() if x[1].get("dt") else 0,
        ),
        reverse=True,
    )

    chosen: List[Tuple[Dict, str]] = []
    for _, j, key in fresh:
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

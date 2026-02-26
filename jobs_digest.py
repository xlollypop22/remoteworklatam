import os
import json
import hashlib
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import feedparser
import httpx

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL"]

BA_TZ = timezone(timedelta(hours=-3))

# LATAM countries / mentions (EN + ES/PT + abbreviations)
LATAM_COUNTRIES = [
    # English
    "mexico", "brazil", "argentina", "chile", "colombia", "peru", "uruguay", "paraguay",
    "bolivia", "ecuador", "venezuela", "panama", "costa rica", "guatemala", "honduras",
    "el salvador", "nicaragua", "dominican", "dominican republic", "puerto rico",
    # Spanish / Portuguese
    "méxico", "brasil", "argentina", "chile", "colombia", "perú", "uruguay", "paraguay",
    "bolivia", "ecuador", "venezuela", "panamá", "costa rica", "guatemala", "honduras",
    "el salvador", "nicaragua", "república dominicana", "puerto rico",
    # Abbreviations / common short forms
    "mx", "br", "ar", "cl", "co", "pe", "uy", "py", "bo", "ec", "ve", "pa", "cr", "gt",
    "hn", "sv", "ni", "do", "pr",
]


# ---------------- Time / formatting ----------------
def now_ba() -> datetime:
    return datetime.now(timezone.utc).astimezone(BA_TZ)


def ru_date(dt: datetime) -> str:
    return dt.strftime("%d.%m")


def ru_time(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def normalize_url(url: str) -> str:
    """Normalize and remove fragments and common tracking query params."""
    if not url:
        return url
    url = url.strip().split("#", 1)[0]
    # remove common tracking params (best-effort)
    url = re.sub(
        r"(\?|&)(utm_[^=]+|ref|source|fbclid|gclid)=[^&]+",
        "",
        url,
        flags=re.I,
    )
    url = url.replace("?&", "?").rstrip("?&")
    return url


def strip_query(url: str) -> str:
    """Hard strip all query params (to improve dedupe)."""
    if not url:
        return url
    return url.split("?", 1)[0]


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def text_contains_any(text: str, keywords: List[str]) -> bool:
    t = (text or "").lower()
    return any((k or "").lower() in t for k in keywords)


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
        summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip()
        summary = re.sub(r"\s+", " ", summary)

        # RSS categories/tags often contain geo; keep them in summary for filtering
        cats = []
        for t in getattr(e, "tags", []) or []:
            term = (getattr(t, "term", "") or "").strip()
            if term:
                cats.append(term)
        if cats:
            summary = (summary + " | categories: " + ", ".join(cats)).strip()

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
    """
    Strong dedupe by link without query.
    This collapses duplicates where platforms add different tracking params.
    """
    link = strip_query(normalize_url(job.get("link", "")))
    return sha(link)


# ---------------- Geo / LATAM gate ----------------
def is_latam_job(text: str) -> bool:
    t = (text or "").lower()
    if "latin america" in t or "latam" in t:
        return True
    if "south america" in t or "central america" in t or "caribbean" in t:
        return True
    return any(country in t for country in LATAM_COUNTRIES)


def extract_latam_location(text: str) -> str:
    t = (text or "").lower()
    if "latin america" in t or "latam" in t:
        return "LATAM"

    mapping = [
        ("mexico", "Mexico"),
        ("brasil", "Brazil"),
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


# ---------------- Role / company parsing ----------------
def clean_parens(s: str) -> str:
    """Remove repeated trailing parens fragments that often contain remote/geo noise."""
    s = (s or "").strip()
    # if title ends with many nested parens, keep first chunk
    # but do NOT delete important role words.
    return re.sub(r"\s+\((?:remote|work from anywhere|wfh|hybrid|on-site|onsite|.*?us.*?|.*?usa.*?|.*?united states.*?|.*?\d{4,}.*?)\)\s*$",
                  "", s, flags=re.I).strip()


def split_role_company(title: str) -> Tuple[str, str]:
    """
    Try to split: "Role at Company", "Company: Role", "Role — Company".
    Returns (role, company). Company can be "—".
    """
    t = clean_parens((title or "").strip())

    # "Role at Company"
    m = re.match(
        r"^(?P<role>.+?)\s+\bat\s+(?P<company>[^—|-]{2,80})\s*(?:[—-].*)?$",
        t,
        flags=re.I,
    )
    if m:
        role = m.group("role").strip()
        company = m.group("company").strip()
        company = re.sub(r"\s+\(.*$", "", company).strip()
        return role, company

    # "Company: Role"
    m = re.match(r"^(?P<company>[^:]{2,80}):\s*(?P<role>.+)$", t)
    if m:
        company = m.group("company").strip()
        role = m.group("role").strip()
        return role, company

    # "Role — Company" or "Role - Company"
    m = re.match(r"^(?P<role>.+?)\s+[—-]\s+(?P<company>[^()]{2,80}).*$", t)
    if m:
        role = m.group("role").strip()
        company = m.group("company").strip()
        company = re.sub(r"\s+\(.*$", "", company).strip()
        return role, company

    return t, "—"


# ---------------- Track / seniority ----------------
def infer_seniority(text: str) -> str:
    t = (text or "").lower()

    # strict word boundaries: avoid matching "coo" inside "coordinator"
    if re.search(r"\b(ceo|cto|cpo|cfo|coo)\b", t) or re.search(r"\b(vp|vice president)\b", t):
        return "C-level/VP"

    if re.search(r"\b(head|director)\b", t):
        return "Head/Director"

    if re.search(r"\b(lead|principal|staff)\b", t):
        return "Lead"

    if re.search(r"\b(senior|sr)\b", t):
        return "Senior"

    if re.search(r"\b(mid|middle|mid-level|pleno)\b", t):
        return "Middle"

    if re.search(r"\b(junior|jr|entry level|entry-level|intern|internship)\b", t):
        return "Junior"

    return "—"


def infer_track(text: str) -> str:
    t = (text or "").lower()

    # Design: only real design signals (avoid "workflow design")
    if re.search(r"\b(designer|ux|ui|product designer|ux researcher|ui designer|visual designer|graphic designer)\b", t):
        return "Design"

    # Product
    if re.search(r"\b(product manager|product owner|product lead|growth product|product ops|product operations)\b", t):
        return "Product"

    # Project / Program
    if re.search(r"\b(project manager|program manager|scrum master|pmo|delivery manager|implementation manager)\b", t):
        return "Project"

    # Data/AI
    if re.search(r"\b(data scientist|data engineer|data analyst|research analyst|analyst|analytics|business intelligence|bi)\b", t):
        return "Data/AI"
    if re.search(r"\b(machine learning|ml engineer|ai engineer|ai)\b", t):
        return "Data/AI"

    # DevOps/Sec
    if re.search(r"\b(devops|sre|site reliability|platform engineer|cloud engineer|kubernetes|secops|security engineer)\b", t):
        return "DevOps/Sec"

    # Engineering
    if re.search(r"\b(engineer|developer|software engineer|backend|frontend|fullstack|full stack|php|python|java|golang|node|react|servicenow|clojure)\b", t):
        return "Engineering"

    # Events: strict word boundaries
    if re.search(r"\b(event manager|event coordinator)\b", t) or re.search(r"\b(event|events)\b", t):
        return "Events"

    # Support/Ops
    if re.search(r"\b(customer support|support|customer success|operations|ops|contact center)\b", t):
        return "Support/Ops"

    return "Other"


def score(job: Dict, filters: Dict) -> int:
    text = (job["title"] + " " + (job.get("summary") or "")).lower()
    s = 0
    if "latam" in text or "latin america" in text:
        s += 4
    if any(c in text for c in LATAM_COUNTRIES):
        s += 2
    if text_contains_any(text, filters.get("remote_keywords", [])):
        s += 1
    if re.search(r"\b(senior|lead|staff|principal|head|director)\b", text, flags=re.I):
        s += 1
    return s


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
        "💼 Remote LATAM Jobs — {date_ru} • {time_ba} BA",
    ).format(date_ru=ru_date(dt), time_ba=ru_time(dt))

    # Group by track
    sections: Dict[str, List[Dict]] = defaultdict(list)
    for j in jobs:
        full_text = (j["title"] + " " + (j.get("summary") or "")).strip()
        sections[infer_track(full_text)].append(j)

    order = ["Design", "Product", "Project", "Events", "Engineering", "Data/AI", "DevOps/Sec", "Support/Ops", "Other"]
    track_emoji = {
        "Design": "🎨",
        "Product": "🧩",
        "Project": "🗂",
        "Events": "🎤",
        "Engineering": "🛠",
        "Data/AI": "📊",
        "DevOps/Sec": "🔐",
        "Support/Ops": "🧑‍💻",
        "Other": "📌",
    }

    out: List[str] = []
    # ONLY title in header (no time line, no "N jobs..." line)
    out.append(f"<b>{html_escape(title)}</b>\n")

    idx = 1
    for track in order:
        items = sections.get(track, [])
        if not items:
            continue

        out.append(f"\n<b>{track_emoji.get(track,'📌')} {html_escape(track)} ({len(items)})</b>\n")

        for j in items:
            full_text = (j["title"] + " " + (j.get("summary") or "")).strip()

            role, company = split_role_company(j["title"] or "")
            loc = extract_latam_location(full_text)
            grade = infer_seniority(full_text)

            meta_parts = []
            if loc != "—":
                meta_parts.append(loc)
            if grade != "—":
                meta_parts.append(grade)

            meta = " · ".join(meta_parts)
            meta_str = f" <i>[{html_escape(meta)}]</i>" if meta else ""

            link = strip_query(normalize_url(j["link"]))
            apply = f'<a href="{html_escape(link)}">Apply</a>'

            if company != "—":
                line_left = f"{idx}) <b>{html_escape(role)}</b> — {html_escape(company)}{meta_str}"
            else:
                line_left = f"{idx}) <b>{html_escape(role)}</b>{meta_str}"

            out.append(f"{line_left} · {apply}\n")
            idx += 1

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

    max_items = int(meta.get("max_items_per_digest", 8))
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

        # Strict LATAM-only
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

    # keep last N published keys
    st["published"] = list(published)[-5000:]
    save_state(st)


if __name__ == "__main__":
    main()

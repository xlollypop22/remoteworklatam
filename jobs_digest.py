import os
import json
import hashlib
from datetime import datetime, timezone
import feedparser
import httpx

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL"]

def load_sources():
    with open("jobs_sources.json", "r", encoding="utf-8") as f:
        return json.load(f)

def load_state():
    if not os.path.exists("state.json"):
        return {"published": []}
    with open("state.json", "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open("state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def job_hash(title, link):
    return hashlib.sha256((title + link).encode()).hexdigest()

def matches_filters(title, config):
    title_lower = title.lower()
    if not any(k in title_lower for k in config["include_keywords"]):
        return False
    if any(k in title_lower for k in config["exclude_keywords"]):
        return False
    return True

def fetch_jobs(config):
    jobs = []
    for feed in config["feeds"]:
        if not feed["enabled"]:
            continue
        parsed = feedparser.parse(feed["url"])
        for entry in parsed.entries:
            jobs.append({
                "title": entry.title,
                "link": entry.link
            })
    return jobs

def format_post(jobs):
    now = datetime.now(timezone.utc).strftime("%d.%m • %H:%M UTC")
    text = f"💼 Remote LATAM Jobs — {now}\n\n"
    text += f"Отобрано: {len(jobs)} вакансий\n\n"
    for i, job in enumerate(jobs, 1):
        text += f"{i}. {job['title']}\n🔗 {job['link']}\n\n"
    text += "#jobs #remote #latam"
    return text

def send_to_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    httpx.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    })

def main():
    config = load_sources()
    state = load_state()

    jobs = fetch_jobs(config)
    filtered = []
    for job in jobs:
        if matches_filters(job["title"], config):
            h = job_hash(job["title"], job["link"])
            if h not in state["published"]:
                job["hash"] = h
                filtered.append(job)

    filtered = filtered[:config["max_items"]]

    if len(filtered) < config["min_items"]:
        return

    post_text = format_post(filtered)
    send_to_telegram(post_text)

    for job in filtered:
        state["published"].append(job["hash"])

    save_state(state)

if __name__ == "__main__":
    main()

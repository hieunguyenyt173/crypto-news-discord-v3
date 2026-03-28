import os
import re
import json
import time
import math
import hashlib
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional

import requests
import feedparser
from openai import OpenAI

# =========================
# ENV
# =========================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()

# =========================
# CONFIG
# =========================
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://www.theblock.co/rss.xml",
]

STATE_FILE = "sent_state.json"

MAX_ENTRIES_PER_FEED = 20
MAX_ARTICLES_FOR_AI = 8
DISCORD_EMBED_FIELD_LIMIT = 1024
REQUEST_TIMEOUT = 30

VN_TZ = timezone(timedelta(hours=7))

BLACKLIST_KEYWORDS = [
    "sponsored",
    "advertorial",
    "casino",
]

CATEGORY_RULES = {
    "Bitcoin": ["bitcoin", "btc"],
    "Ethereum": ["ethereum", "eth"],
    "DeFi": ["defi", "dex", "lending", "yield"],
    "Regulation": ["sec", "regulation", "etf"],
    "AI x Crypto": ["ai", "agent"],
    "Altcoins": ["solana", "xrp", "bnb"],
}

# =========================
# CLIENT
# =========================
client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

# =========================
# LOG
# =========================
def log(msg):
    now = datetime.now(VN_TZ).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

# =========================
# STATE
# =========================
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"sent_ids": []}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# =========================
# FETCH
# =========================
def clean_html(text):
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()

def parse_date(entry):
    try:
        return parsedate_to_datetime(entry.get("published"))
    except:
        return datetime.now(timezone.utc)

def article_id(title, link):
    return hashlib.md5(f"{title}{link}".encode()).hexdigest()

def fetch_articles():
    articles = []
    for url in RSS_FEEDS:
        feed = feedparser.parse(url)
        for e in feed.entries[:MAX_ENTRIES_PER_FEED]:
            title = e.get("title", "")
            link = e.get("link", "")
            summary = clean_html(e.get("summary"))

            if any(k in title.lower() for k in BLACKLIST_KEYWORDS):
                continue

            articles.append({
                "id": article_id(title, link),
                "title": title,
                "link": link,
                "summary": summary[:500],
                "published": parse_date(e)
            })

    articles.sort(key=lambda x: x["published"], reverse=True)
    return articles

# =========================
# MARKET
# =========================
def get_market_snapshot():
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin,ethereum,solana",
                "vs_currencies": "usd",
                "include_24hr_change": "true"
            },
            timeout=10
        ).json()

        def fmt(k, name):
            p = r[k]["usd"]
            c = r[k]["usd_24h_change"]
            return f"{name}: ${p:,.0f} ({c:+.2f}%)"

        return {
            "btc": fmt("bitcoin", "BTC"),
            "eth": fmt("ethereum", "ETH"),
            "sol": fmt("solana", "SOL"),
        }
    except:
        return {"btc": "N/A", "eth": "N/A", "sol": "N/A"}

# =========================
# 🪙 GOLD SNAPSHOT (NEW)
# =========================
def get_gold_snapshot():
    return {
        "vn": "SJC ~169.8 – 172.8 triệu/lượng",
        "world": "~4,400 – 4,500 USD/oz",
        "note": "Chênh lệch cao (~25–30 triệu/lượng)"
    }

# =========================
# AI
# =========================
def summarize(articles):
    text = "\n".join([f"{a['title']} - {a['summary']}" for a in articles])

    try:
        res = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": f"Tóm tắt:\n{text}"}],
            max_tokens=800
        )
        return res.choices[0].message.content
    except:
        return "Không thể tóm tắt."

# =========================
# DISCORD
# =========================
def send_discord(content):
    requests.post(DISCORD_WEBHOOK_URL, json={"content": content})

# =========================
# MAIN
# =========================
def main():
    state = load_state()
    sent = set(state["sent_ids"])

    articles = fetch_articles()
    new = [a for a in articles if a["id"] not in sent][:5]

    if not new:
        send_discord("Không có tin mới.")
        return

    summary = summarize(new)

    market = get_market_snapshot()
    gold = get_gold_snapshot()

    message = f"""
📊 MARKET
{market['btc']}
{market['eth']}
{market['sol']}

🪙 VÀNG
VN: {gold['vn']}
TG: {gold['world']}
{gold['note']}

📰 TIN TỨC
{summary}
"""

    send_discord(message)

    for a in new:
        sent.add(a["id"])

    state["sent_ids"] = list(sent)
    save_state(state)

if __name__ == "__main__":
    main()

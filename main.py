import os
import json
import time
import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
import feedparser
from openai import OpenAI

# =========================
# CONFIG
# =========================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()

# Có thể đổi / thêm nguồn RSS ở đây
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://www.theblock.co/rss.xml",
]

STATE_FILE = "sent_state.json"
MAX_ARTICLES_TO_FETCH = 30
MAX_ARTICLES_TO_SUMMARIZE = 5

# =========================
# DEEPSEEK CLIENT
# =========================
client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

# =========================
# HELPERS
# =========================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"sent_ids": []}
    return {"sent_ids": []}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def make_article_id(link: str, title: str) -> str:
    raw = f"{link}|{title}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()

def parse_entry_date(entry):
    for key in ["published", "updated", "pubDate"]:
        value = entry.get(key)
        if value:
            try:
                return parsedate_to_datetime(value)
            except Exception:
                pass
    return datetime.now(timezone.utc)

def fetch_articles():
    articles = []

    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:MAX_ARTICLES_TO_FETCH]:
                title = (entry.get("title") or "").strip()
                link = (entry.get("link") or "").strip()
                summary = (entry.get("summary") or entry.get("description") or "").strip()
                published_at = parse_entry_date(entry)

                if not title or not link:
                    continue

                articles.append({
                    "id": make_article_id(link, title),
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "published_at": published_at.isoformat()
                })
        except Exception as e:
            print(f"[WARN] Failed feed {feed_url}: {e}")

    # sort mới nhất trước
    articles.sort(key=lambda x: x["published_at"], reverse=True)
    return articles

def pick_unsent_articles(articles, sent_ids):
    fresh = []
    for a in articles:
        if a["id"] not in sent_ids:
            fresh.append(a)
        if len(fresh) >= MAX_ARTICLES_TO_SUMMARIZE:
            break
    return fresh

def summarize_with_deepseek(articles):
    """
    Tạo 1 bản tin tiếng Việt ngắn gọn, dễ đọc, tránh lặp ý.
    """
    if not articles:
        return "Hiện chưa có tin mới đáng chú ý trong khung giờ này."

    source_text = []
    for i, a in enumerate(articles, 1):
        source_text.append(
            f"[{i}] {a['title']}\n"
            f"Link: {a['link']}\n"
            f"Tóm tắt gốc: {a['summary'][:1200]}\n"
        )

    prompt = f"""
Bạn là biên tập viên bản tin crypto.

Nhiệm vụ:
- Viết bản tin tiếng Việt tự nhiên, dễ đọc.
- Chỉ dùng thông tin từ các bài bên dưới.
- Không bịa thêm dữ kiện.
- Không lặp ý giữa các mục.
- Ưu tiên nêu tác động thị trường / hệ sinh thái nếu có.
- Văn phong gọn, rõ, hiện đại.
- Có tiêu đề tổng.
- Có phần "Điểm nhanh".
- Có 3-5 gạch đầu dòng.
- Cuối bản tin thêm mục "Nguồn tham khảo" và liệt kê link theo số [1], [2]...
- Tổng độ dài khoảng 400-700 từ.

Dữ liệu bài viết:
{chr(10).join(source_text)}
"""

    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "Bạn là một editor chuyên viết bản tin crypto bằng tiếng Việt."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,
        max_tokens=1800
    )

    return resp.choices[0].message.content.strip()

def send_to_discord(content: str):
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("Missing DISCORD_WEBHOOK_URL")

    # Discord giới hạn content ngắn, nên cắt thành nhiều phần nếu cần
    chunks = split_message(content, 1800)
    for chunk in chunks:
        payload = {
            "content": chunk,
            "allowed_mentions": {"parse": []}
        }
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(1)

def split_message(text: str, limit: int = 1800):
    if len(text) <= limit:
        return [text]

    parts = []
    current = ""
    for line in text.splitlines(True):
        if len(current) + len(line) > limit:
            parts.append(current)
            current = line
        else:
            current += line

    if current:
        parts.append(current)

    return parts

def main():
    if not DEEPSEEK_API_KEY:
        raise ValueError("Missing DEEPSEEK_API_KEY")
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("Missing DISCORD_WEBHOOK_URL")

    state = load_state()
    sent_ids = set(state.get("sent_ids", []))

    articles = fetch_articles()
    unsent = pick_unsent_articles(articles, sent_ids)

    if not unsent:
        send_to_discord("📰 Khung giờ này chưa có bài crypto mới nổi bật để tổng hợp.")
        return

    newsletter = summarize_with_deepseek(unsent)

    now_vn = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    final_text = f"🪙 **BẢN TIN CRYPTO**\nCập nhật: {now_vn}\n\n{newsletter}"

    send_to_discord(final_text)

    for a in unsent:
        sent_ids.add(a["id"])

    # chỉ giữ tối đa 500 id gần nhất cho gọn
    state["sent_ids"] = list(sent_ids)[-500:]
    save_state(state)

if __name__ == "__main__":
    main()

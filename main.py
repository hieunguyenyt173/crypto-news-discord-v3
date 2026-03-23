import os
import re
import json
import time
import html
import hashlib
import requests
import feedparser
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

from pydantic import BaseModel
from google import genai
from google.genai import types

# =========================
# ENV CONFIG
# =========================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "8"))
MAX_PER_CATEGORY = int(os.getenv("MAX_PER_CATEGORY", "2"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

STATE_FILE = "posted_state.json"
SOURCES_FILE = "sources.json"

# Discord embed color
EMBED_COLOR = 3447003

CATEGORY_RULES = {
    "Market": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "price", "market", "rally", "surge", "plunge", "volatility",
        "liquidation", "whale", "altcoin", "token", "bull", "bear",
        "flows", "volume", "open interest"
    ],
    "Regulation": [
        "sec", "etf", "regulation", "lawsuit", "law", "court",
        "congress", "policy", "ban", "approval", "reject", "compliance",
        "legal", "regulator"
    ],
    "DeFi": [
        "defi", "dex", "yield", "staking", "lending", "borrowing",
        "amm", "vault", "liquidity", "tvl", "farm", "stablecoin",
        "restaking", "bridge"
    ],
    "Security": [
        "hack", "exploit", "breach", "attack", "drain", "phishing",
        "security", "vulnerability", "stolen", "scam"
    ],
    "Alt & Ecosystem": [
        "airdrop", "mainnet", "testnet", "ecosystem", "partnership",
        "launch", "upgrade", "listing", "protocol", "foundation",
        "web3", "nft", "gaming", "infra", "layer 2", "l2"
    ]
}

CATEGORY_EMOJI = {
    "Market": "📈",
    "Regulation": "⚖️",
    "DeFi": "🏦",
    "Security": "🛡️",
    "Alt & Ecosystem": "🧩",
    "General": "📰"
}


# =========================
# Pydantic schemas for Gemini structured output
# =========================
class NewsSummary(BaseModel):
    title_vi: str
    summary_vi: str
    impact_vi: str
    tag_line: str


# =========================
# Utils
# =========================
def load_sources() -> List[Dict[str, str]]:
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"posted_ids": []}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def normalize_url(url: str) -> str:
    return url.split("?")[0].strip()


def make_item_id(title: str, link: str) -> str:
    raw = f"{title.strip().lower()}|{normalize_url(link)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_entry_time(entry) -> Optional[datetime]:
    if getattr(entry, "published_parsed", None):
        try:
            return datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc)
        except Exception:
            pass

    if getattr(entry, "updated_parsed", None):
        try:
            return datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=timezone.utc)
        except Exception:
            pass

    for field in ["published", "updated", "created"]:
        value = getattr(entry, field, None)
        if value:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
    return None


def format_time_bangkok(dt_utc: datetime) -> str:
    bangkok = timezone(timedelta(hours=7))
    return dt_utc.astimezone(bangkok).strftime("%d/%m %H:%M ICT")


def categorize_article(title: str, summary: str) -> str:
    text = f"{title} {summary}".lower()
    scores = {}

    for category, keywords in CATEGORY_RULES.items():
        score = 0
        for kw in keywords:
            if kw in text:
                score += 1
        scores[category] = score

    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "General"


def score_article(title: str, summary: str, category: str) -> int:
    text = f"{title} {summary}".lower()
    score = 0

    hot_words = [
        "breaking", "surge", "plunge", "approval", "reject",
        "hack", "exploit", "etf", "sec", "listing", "airdrop",
        "mainnet", "lawsuit", "liquidation", "whale"
    ]
    for word in hot_words:
        if word in text:
            score += 2

    if category != "General":
        score += 2

    if len(summary) > 120:
        score += 1

    return score


# =========================
# RSS Fetch
# =========================
def fetch_articles() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    sources = load_sources()
    all_items = []

    for source in sources:
        source_name = source["name"]
        feed_url = source["rss"]

        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            print(f"[WARN] Failed to parse {source_name}: {e}")
            continue

        for entry in feed.entries:
            title = strip_html(getattr(entry, "title", "").strip())
            link = getattr(entry, "link", "").strip()
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "")

            if not title or not link:
                continue

            published_at = parse_entry_time(entry)
            if published_at is None or published_at < cutoff:
                continue

            category = categorize_article(title, summary)
            score = score_article(title, summary, category)

            item = {
                "id": make_item_id(title, link),
                "source": source_name,
                "title": title,
                "link": normalize_url(link),
                "summary": strip_html(summary),
                "published_at": published_at,
                "category": category,
                "score": score,
            }
            all_items.append(item)

    # dedupe by link
    dedup = {}
    for item in all_items:
        if item["link"] not in dedup or item["score"] > dedup[item["link"]]["score"]:
            dedup[item["link"]] = item

    items = list(dedup.values())
    items.sort(key=lambda x: (x["score"], x["published_at"]), reverse=True)
    return items[:MAX_ITEMS]


# =========================
# Gemini Summarization
# =========================
def build_gemini_client():
    if not GEMINI_API_KEY:
        return None
    return genai.Client(api_key=GEMINI_API_KEY)


def fallback_summary_vi(item: Dict[str, Any]) -> Dict[str, str]:
    base = item["summary"] or item["title"]
    base = base[:260].strip()

    prefix_map = {
        "Market": "Tin thị trường:",
        "Regulation": "Điểm pháp lý:",
        "DeFi": "Diễn biến DeFi:",
        "Security": "Cảnh báo bảo mật:",
        "Alt & Ecosystem": "Cập nhật hệ sinh thái:",
        "General": "Tin đáng chú ý:"
    }

    prefix = prefix_map.get(item["category"], "Tin đáng chú ý:")
    summary_vi = f"{prefix} {base}"
    if not summary_vi.endswith("."):
        summary_vi += "."

    return {
        "title_vi": item["title"],
        "summary_vi": summary_vi,
        "impact_vi": "Tác động thị trường cần theo dõi thêm.",
        "tag_line": item["category"]
    }


def summarize_with_gemini(client, item: Dict[str, Any]) -> Dict[str, str]:
    text_block = (
        f"TITLE: {item['title']}\n"
        f"CATEGORY: {item['category']}\n"
        f"SOURCE: {item['source']}\n"
        f"PUBLISHED: {item['published_at'].isoformat()}\n"
        f"LINK: {item['link']}\n"
        f"ARTICLE_SNIPPET: {item['summary'][:1800]}"
    )

    prompt = f"""
Bạn là biên tập viên bản tin crypto tiếng Việt chuyên nghiệp.

Nhiệm vụ:
Chuyển toàn bộ nội dung bài báo sang tiếng Việt tự nhiên, dễ đọc như báo chí.

Yêu cầu:
- KHÔNG giữ lại tiếng Anh (trừ tên riêng: Bitcoin, Ethereum, SEC...)
- KHÔNG copy câu tiếng Anh gốc
- Viết lại hoàn toàn bằng tiếng Việt
- Văn phong giống báo tài chính / crypto

1. title_vi:
- Viết lại tiêu đề tiếng Việt tự nhiên
- Ngắn gọn, hấp dẫn
- Tối đa 110 ký tự

2. summary_vi:
- Viết 2-3 câu tiếng Việt
- Dài khoảng 60-100 từ
- Không lẫn tiếng Anh
- Viết lại nội dung, không dịch từng chữ

3. impact_vi:
- 1 câu tiếng Việt
- Giải thích vì sao tin này quan trọng

4. tag_line:
- 2-5 từ tiếng Việt
- Ví dụ: "ETF", "Hack", "Biến động giá"

Dữ liệu:
TITLE: {item['title']}
CONTENT: {item['summary']}
"""
Dữ liệu bài báo:
TITLE: {item['title']}
CATEGORY: {item['category']}
SOURCE: {item['source']}
CONTENT: {item['summary']}
"""
Nhiệm vụ: viết bản tóm tắt ngắn gọn nhưng tự nhiên, dễ đọc trong Discord.

Yêu cầu:
1. title_vi:
- Viết lại tiêu đề tiếng Việt tự nhiên.
- Không dịch máy cứng.
- Tối đa 110 ký tự.

2. summary_vi:
- Viết 2 câu tiếng Việt, tự nhiên, rõ ý.
- Dài khoảng 45 đến 85 từ.
- Chỉ dùng thông tin có trong tiêu đề và đoạn trích.
- Không bịa số liệu, không suy đoán vô căn cứ.
- Giữ giọng điệu như một bản tin ngắn.

3. impact_vi:
- 1 câu ngắn 12 đến 24 từ.
- Nói vì sao tin này đáng chú ý với người theo dõi crypto.

4. tag_line:
- 2 đến 5 từ tiếng Việt, kiểu nhãn ngắn.
- Ví dụ: "ETF", "Hack giao thức", "Biến động giá", "Cập nhật hệ sinh thái"

Nếu thông tin ít, vẫn viết ngắn gọn, sạch, không lan man.

Dữ liệu bài báo:
{text_block}
""".strip()

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.5,
                response_mime_type="application/json",
                response_schema=NewsSummary,
                max_output_tokens=500,
            ),
        )

        data = response.parsed
        if data:
            return {
                "title_vi": data.title_vi.strip(),
                "summary_vi": data.summary_vi.strip(),
                "impact_vi": data.impact_vi.strip(),
                "tag_line": data.tag_line.strip(),
            }

        return fallback_summary_vi(item)

    except Exception as e:
        print(f"[WARN] Gemini failed for {item['title'][:80]}: {e}")
        return fallback_summary_vi(item)


def enrich_articles_with_ai(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = build_gemini_client()
    enriched = []

    for item in items:
        ai = summarize_with_gemini(client, item) if client else fallback_summary_vi(item)
        item["title_vi"] = ai["title_vi"]
        item["summary_vi"] = ai["summary_vi"]
        item["impact_vi"] = ai["impact_vi"]
        item["tag_line"] = ai["tag_line"]
        enriched.append(item)

    return enriched


# =========================
# Discord Payload
# =========================
def group_items(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped = {}
    for item in items:
        grouped.setdefault(item["category"], []).append(item)

    for category in grouped:
        grouped[category].sort(key=lambda x: (x["score"], x["published_at"]), reverse=True)
        grouped[category] = grouped[category][:MAX_PER_CATEGORY]

    return grouped


def build_embed_fields(grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    ordered_categories = ["Market", "Regulation", "DeFi", "Security", "Alt & Ecosystem", "General"]
    fields = []

    for category in ordered_categories:
        if category not in grouped:
            continue

        emoji = CATEGORY_EMOJI.get(category, "📰")
        lines = []

        for item in grouped[category]:
            title_vi = item.get("title_vi", item["title"])
            if len(title_vi) > 100:
                title_vi = title_vi[:97] + "..."

            block = (
                f"**[{title_vi}]({item['link']})**\n"
                f"*{item.get('tag_line', category)}*\n"
                f"{item.get('summary_vi', item['summary'][:220])}\n"
                f"↳ {item.get('impact_vi', 'Đáng để theo dõi thêm.')}\n"
                f"`{item['source']}` • {format_time_bangkok(item['published_at'])}"
            )

            # Discord field value limit 1024 chars
            lines.append(block[:950])

        fields.append({
            "name": f"{emoji} {category}",
            "value": "\n\n".join(lines)[:1024],
            "inline": False
        })

    return fields


def chunk_list(arr: List[Any], n: int):
    for i in range(0, len(arr), n):
        yield arr[i:i+n]


def build_discord_payload(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    now_bkk = datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M ICT")

    if not items:
        return {
            "content": "📰 **Bản tin crypto 2 kỳ mỗi ngày**",
            "embeds": [
                {
                    "title": f"Crypto Daily Brief | {now_bkk}",
                    "description": f"Không có tin mới nổi bật trong {LOOKBACK_HOURS} giờ gần đây.",
                    "color": EMBED_COLOR,
                }
            ]
        }

    grouped = group_items(items)
    fields = build_embed_fields(grouped)
    embeds = []

    for idx, field_group in enumerate(chunk_list(fields, 3), start=1):
        embed = {
            "title": f"Crypto Daily Brief | {now_bkk}" if idx == 1 else f"Crypto Daily Brief | phần {idx}",
            "description": (
                f"Tổng hợp tin crypto trong {LOOKBACK_HOURS} giờ gần nhất.\n"
                f"Tóm tắt bằng tiếng Việt để bạn lướt nhanh như đọc headline trên tàu lượn nến 📉📈"
            ) if idx == 1 else "Tiếp tục bản tin.",
            "color": EMBED_COLOR,
            "fields": field_group,
            "footer": {
                "text": f"{len(items)} tin • RSS + Gemini summary • Auto digest"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        embeds.append(embed)

    return {
        "content": "📰 **Bản tin crypto 2 kỳ mỗi ngày**",
        "embeds": embeds[:3]
    }


def send_to_discord(payload: Dict[str, Any]) -> None:
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("Missing DISCORD_WEBHOOK_URL")

    res = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
    if res.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {res.status_code} | {res.text}")


# =========================
# Main
# =========================
def main():
    state = load_state()
    posted_ids = set(state.get("posted_ids", []))

    items = fetch_articles()
    new_items = [item for item in items if item["id"] not in posted_ids]

    if not new_items:
        payload = build_discord_payload([])
        send_to_discord(payload)
        print("No new items, sent empty digest.")
        return

    enriched_items = enrich_articles_with_ai(new_items[:MAX_ITEMS])
    payload = build_discord_payload(enriched_items)
    send_to_discord(payload)

    for item in new_items:
        posted_ids.add(item["id"])

    state["posted_ids"] = list(posted_ids)[-1500:]
    save_state(state)

    print(f"Posted {len(enriched_items)} new items.")


if __name__ == "__main__":
    main()

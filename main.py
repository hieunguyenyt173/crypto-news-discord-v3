import os
import re
import json
import time
import html
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import feedparser
import requests
from pydantic import BaseModel
from google import genai
from google.genai import types


# =========================
# ENV CONFIG
# =========================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "10"))
MAX_PER_CATEGORY = int(os.getenv("MAX_PER_CATEGORY", "3"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

STATE_FILE = "posted_state.json"
SOURCES_FILE = "sources.json"

EMBED_COLOR = 3447003


# =========================
# CATEGORY CONFIG
# =========================
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

CATEGORY_LABEL_VI = {
    "Market": "Thị trường",
    "Regulation": "Pháp lý",
    "DeFi": "DeFi",
    "Security": "Bảo mật",
    "Alt & Ecosystem": "Hệ sinh thái",
    "General": "Tổng hợp"
}

CATEGORY_EMOJI = {
    "Market": "📈",
    "Regulation": "⚖️",
    "DeFi": "🏦",
    "Security": "🛡️",
    "Alt & Ecosystem": "🧩",
    "General": "📰"
}

TAGLINE_MAP_VI = {
    "Market": "Biến động thị trường",
    "Regulation": "Tin pháp lý",
    "DeFi": "Cập nhật DeFi",
    "Security": "Cảnh báo bảo mật",
    "Alt & Ecosystem": "Cập nhật hệ sinh thái",
    "General": "Tin đáng chú ý"
}


# =========================
# SCHEMA
# =========================
class NewsSummary(BaseModel):
    title_vi: str
    summary_vi: str
    tag_line: str


# =========================
# FILE / STATE HELPERS
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


# =========================
# TEXT HELPERS
# =========================
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


def truncate_text(text: str, max_len: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def clean_summary(text: str) -> str:
    if not text:
        return ""

    text = text.strip()

    banned_patterns = [
        r"Đây là diễn biến đáng chú ý[^.]*\.",
        r"Nhà đầu tư nên tiếp tục theo dõi[^.]*\.",
        r"Đây là thông tin quan trọng[^.]*\.",
        r"Đáng chú ý để tiếp tục theo dõi[^.]*\.",
        r"Tin thị trường:\s*",
        r"Tin pháp lý:\s*",
        r"Diễn biến DeFi:\s*",
        r"Cảnh báo bảo mật:\s*",
        r"Cập nhật hệ sinh thái:\s*",
        r"Tin đáng chú ý:\s*",
    ]

    for pattern in banned_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_title(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip(" -–—:|")
    return truncate_text(text, 110)


def is_mostly_english(text: str) -> bool:
    if not text:
        return False

    letters = re.findall(r"[A-Za-z]", text)
    if len(letters) < 8:
        return False

    vietnamese_chars = re.findall(r"[ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]", text.lower())

    return len(vietnamese_chars) == 0


def format_time_vn(dt_utc: datetime) -> str:
    vn_tz = timezone(timedelta(hours=7))
    return dt_utc.astimezone(vn_tz).strftime("%d/%m %H:%M ICT")


# =========================
# TIME HELPERS
# =========================
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


# =========================
# CATEGORY + SCORE
# =========================
def categorize_article(title: str, summary: str) -> str:
    text = f"{title} {summary}".lower()
    scores: Dict[str, int] = {}

    for category, keywords in CATEGORY_RULES.items():
        score = 0
        for kw in keywords:
            if kw in text:
                score += 1
        scores[category] = score

    best_category = max(scores, key=scores.get)
    return best_category if scores[best_category] > 0 else "General"


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
# RSS FETCH
# =========================
def fetch_articles() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    sources = load_sources()
    all_items: List[Dict[str, Any]] = []

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

            clean_snippet = strip_html(summary)
            category = categorize_article(title, clean_snippet)
            score = score_article(title, clean_snippet, category)

            item = {
                "id": make_item_id(title, link),
                "source": source_name,
                "title": title,
                "link": normalize_url(link),
                "summary": clean_snippet,
                "published_at": published_at,
                "category": category,
                "score": score,
            }
            all_items.append(item)

    dedup_by_link: Dict[str, Dict[str, Any]] = {}
    for item in all_items:
        old = dedup_by_link.get(item["link"])
        if old is None or item["score"] > old["score"]:
            dedup_by_link[item["link"]] = item

    items = list(dedup_by_link.values())
    items.sort(key=lambda x: (x["score"], x["published_at"]), reverse=True)
    return items[:MAX_ITEMS]


# =========================
# GEMINI
# =========================
def build_gemini_client():
    if not GEMINI_API_KEY:
        return None
    return genai.Client(api_key=GEMINI_API_KEY)


def fallback_summary_vi(item: Dict[str, Any]) -> Dict[str, str]:
    title_vi = clean_title(item["title"])
    summary_vi = clean_summary(truncate_text(item["summary"] or item["title"], 220))
    tag_line = TAGLINE_MAP_VI.get(item["category"], "Tin đáng chú ý")

    if is_mostly_english(title_vi):
        title_vi = f"Tin mới: {clean_title(item['title'])}"

    return {
        "title_vi": title_vi,
        "summary_vi": summary_vi,
        "tag_line": tag_line,
    }


def translate_title_vi(client, title: str) -> str:
    clean_original = clean_title(title)
    if not client:
        return f"Tin mới: {clean_original}"

    prompt = f"""
Dịch tiêu đề tin tức crypto sau sang tiếng Việt tự nhiên.

Yêu cầu:
- Dịch sang tiếng Việt tối đa có thể.
- Không giữ tiếng Anh.
- Chỉ giữ lại tên riêng hoặc thuật ngữ bắt buộc như Bitcoin, Ethereum, SEC, ETF, Solana.
- Không thêm thông tin mới.
- Chỉ trả về đúng một dòng tiêu đề.

Tiêu đề:
{clean_original}
""".strip()

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=80,
            ),
        )

        translated = clean_title((response.text or "").strip())
        if not translated:
            return f"Tin mới: {clean_original}"

        if is_mostly_english(translated):
            return f"Tin mới: {clean_original}"

        return translated
    except Exception:
        return f"Tin mới: {clean_original}"


def summarize_with_gemini(client, item: Dict[str, Any]) -> Dict[str, str]:
    if client is None:
        return fallback_summary_vi(item)

    text_block = (
        f"TITLE: {item['title']}\n"
        f"CATEGORY: {item['category']}\n"
        f"SOURCE: {item['source']}\n"
        f"PUBLISHED: {item['published_at'].isoformat()}\n"
        f"LINK: {item['link']}\n"
        f"ARTICLE_SNIPPET: {item['summary'][:1800]}"
    )

    prompt = f"""
Bạn là biên tập viên bản tin crypto tiếng Việt.

Nhiệm vụ:
Dịch và tóm tắt nội dung bài báo thành tiếng Việt tự nhiên, ngắn gọn, dễ đọc trên Discord.

YÊU CẦU BẮT BUỘC:
- Toàn bộ đầu ra phải là tiếng Việt.
- Không giữ câu tiếng Anh.
- Chỉ giữ tên riêng hoặc thuật ngữ bắt buộc như Bitcoin, Ethereum, SEC, ETF, Solana.
- Không dùng văn mẫu.
- Không thêm câu chung chung như:
  "Đây là diễn biến đáng chú ý..."
  "Nhà đầu tư nên tiếp tục theo dõi..."
  "Đây là thông tin quan trọng..."
- Không bịa thêm dữ kiện ngoài nội dung đã cho.

Trả về JSON gồm đúng 3 trường:

1. title_vi
- DỊCH tiêu đề sang tiếng Việt.
- Không được giữ tiếng Anh, trừ tên riêng bắt buộc.
- Ngắn gọn, dễ hiểu, tối đa 110 ký tự.

2. summary_vi
- Viết đúng 2 câu tiếng Việt.
- Súc tích, nhiều thông tin.
- Không lặp lại nguyên ý của tiêu đề.
- Không dùng tiếng Anh.

3. tag_line
- Viết nhãn ngắn 2 đến 4 từ bằng tiếng Việt.
- Ví dụ: "Tin pháp lý", "Biến động giá", "Cập nhật DeFi", "Tin ETF"

Dữ liệu bài báo:
{text_block}
""".strip()

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.3,
                response_mime_type="application/json",
                response_schema=NewsSummary,
                max_output_tokens=400,
            ),
        )

        data = response.parsed
        if not data:
            return fallback_summary_vi(item)

        title_vi = clean_title(data.title_vi)
        summary_vi = clean_summary(data.summary_vi)
        tag_line = truncate_text((data.tag_line or "").strip(), 30)

        if not title_vi:
            title_vi = translate_title_vi(client, item["title"])

        if is_mostly_english(title_vi):
            title_vi = translate_title_vi(client, item["title"])

        if not summary_vi:
            return fallback_summary_vi(item)

        if not tag_line:
            tag_line = TAGLINE_MAP_VI.get(item["category"], "Tin đáng chú ý")

        return {
            "title_vi": title_vi,
            "summary_vi": summary_vi,
            "tag_line": tag_line,
        }

    except Exception as e:
        print(f"[WARN] Gemini failed for {item['title'][:80]}: {e}")
        return fallback_summary_vi(item)


def enrich_articles_with_ai(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = build_gemini_client()
    enriched: List[Dict[str, Any]] = []

    for item in items:
        ai_result = summarize_with_gemini(client, item)
        item["title_vi"] = ai_result["title_vi"]
        item["summary_vi"] = ai_result["summary_vi"]
        item["tag_line"] = ai_result["tag_line"]
        enriched.append(item)

    return enriched


# =========================
# GROUPING
# =========================
def group_items(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for item in items:
        grouped.setdefault(item["category"], []).append(item)

    for category in grouped:
        grouped[category].sort(key=lambda x: (x["score"], x["published_at"]), reverse=True)
        grouped[category] = grouped[category][:MAX_PER_CATEGORY]

    return grouped


def chunk_list(arr: List[Any], n: int):
    for i in range(0, len(arr), n):
        yield arr[i:i + n]


# =========================
# DISCORD FORMAT
# =========================
def build_embed_fields(grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    ordered_categories = ["Market", "Regulation", "DeFi", "Security", "Alt & Ecosystem", "General"]
    fields: List[Dict[str, Any]] = []

    for category in ordered_categories:
        if category not in grouped:
            continue

        emoji = CATEGORY_EMOJI.get(category, "📰")
        category_label = CATEGORY_LABEL_VI.get(category, category)
        blocks: List[str] = []

        for item in grouped[category]:
            title_vi = truncate_text(item.get("title_vi", item["title"]), 100)
            summary_vi = truncate_text(clean_summary(item.get("summary_vi", item["summary"])), 320)
            tag_line = truncate_text(item.get("tag_line", TAGLINE_MAP_VI.get(category, "Tin đáng chú ý")), 30)

            block = (
                f"**[{title_vi}]({item['link']})**\n"
                f"*{tag_line}*\n"
                f"{summary_vi}\n"
                f"`{item['source']}` • {format_time_vn(item['published_at'])}"
            )
            blocks.append(block)

        field_value = "\n\n".join(blocks)
        field_value = truncate_text(field_value, 1024)

        fields.append({
            "name": f"{emoji} {category_label}",
            "value": field_value,
            "inline": False
        })

    return fields


def build_discord_payload(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    now_vn = datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M ICT")

    if not items:
        return {
            "content": "📰 **Bản tin crypto 2 kỳ mỗi ngày**",
            "embeds": [
                {
                    "title": f"Crypto Daily Brief | {now_vn}",
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
            "title": f"Crypto Daily Brief | {now_vn}" if idx == 1 else f"Crypto Daily Brief | phần {idx}",
            "description": (
                f"Tổng hợp tin crypto trong {LOOKBACK_HOURS} giờ gần nhất.\n"
                f"Tóm tắt hoàn toàn bằng tiếng Việt, ngắn gọn và dễ đọc."
            ) if idx == 1 else "Tiếp tục bản tin.",
            "color": EMBED_COLOR,
            "fields": field_group,
            "footer": {
                "text": f"{len(items)} tin • RSS + Gemini • Auto digest"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        embeds.append(embed)

    return {
        "content": "📰 **Bản tin crypto 2 kỳ mỗi ngày**",
        "embeds": embeds[:3]
    }


# =========================
# DISCORD SEND
# =========================
def send_to_discord(payload: Dict[str, Any]) -> None:
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("Missing DISCORD_WEBHOOK_URL")

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
    if response.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {response.status_code} | {response.text}")


# =========================
# MAIN
# =========================
def main() -> None:
    state = load_state()
    posted_ids = set(state.get("posted_ids", []))

    items = fetch_articles()
    new_items = [item for item in items if item["id"] not in posted_ids]

    if not new_items:
        payload = build_discord_payload([])
        send_to_discord(payload)
        print("No new items, sent empty digest.")
        return

    selected_items = new_items[:MAX_ITEMS]
    enriched_items = enrich_articles_with_ai(selected_items)

    payload = build_discord_payload(enriched_items)
    send_to_discord(payload)

    for item in selected_items:
        posted_ids.add(item["id"])

    state["posted_ids"] = list(posted_ids)[-1500:]
    save_state(state)

    print(f"Posted {len(enriched_items)} new items.")


if __name__ == "__main__":
    main()

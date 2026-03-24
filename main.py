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
from google import genai
from google.genai import types

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# ENV CONFIG
# =========================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "8"))
MAX_PER_CATEGORY = int(os.getenv("MAX_PER_CATEGORY", "2"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
STRICT_VIETNAMESE = os.getenv("STRICT_VIETNAMESE", "true").lower() == "true"
DEBUG_LOG = os.getenv("DEBUG_LOG", "true").lower() == "true"

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


# =========================
# LOGGING
# =========================
def log(message: str) -> None:
    if DEBUG_LOG:
        print(message)


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


def cleanup_digest_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    banned_patterns = [
        r"Đây là diễn biến đáng chú ý[^.\n]*\.?",
        r"Nhà đầu tư nên tiếp tục theo dõi[^.\n]*\.?",
        r"Đây là thông tin quan trọng[^.\n]*\.?",
        r"Tin đáng chú ý[^.\n]*\.?",
        r"Dưới đây là bản tin[^.\n]*\.?",
    ]

    for pattern in banned_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def format_time_vn(dt_utc: datetime) -> str:
    vn_tz = timezone(timedelta(hours=7))
    return dt_utc.astimezone(vn_tz).strftime("%d/%m %H:%M ICT")


def now_vn_str() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M ICT")


def is_digest_mostly_vietnamese(text: str) -> bool:
    if not text:
        return False

    text = text.strip()

    ascii_words = re.findall(r"\b[a-zA-Z]{3,}\b", text)
    vietnamese_chars = re.findall(
        r"[ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]",
        text.lower()
    )

    bullet_count = text.count("•")
    source_count = text.count("Nguồn:")

    log(f"[CHECK] ascii_words={len(ascii_words)} | vietnamese_chars={len(vietnamese_chars)} | bullets={bullet_count} | sources={source_count}")

    if len(vietnamese_chars) >= 20 and bullet_count >= 1 and source_count >= 1:
        return True

    return len(ascii_words) < 25


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

    log(f"[INFO] Fetching RSS from {len(sources)} sources | cutoff={cutoff.isoformat()}")

    for source in sources:
        source_name = source["name"]
        feed_url = source["rss"]

        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            log(f"[WARN] Failed to parse {source_name}: {e}")
            continue

        entry_count = 0

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
                "summary": truncate_text(clean_snippet, 700),
                "published_at": published_at,
                "category": category,
                "score": score,
            }
            all_items.append(item)
            entry_count += 1

        log(f"[INFO] {source_name}: kept {entry_count} items")

    dedup_by_link: Dict[str, Dict[str, Any]] = {}
    for item in all_items:
        old = dedup_by_link.get(item["link"])
        if old is None or item["score"] > old["score"]:
            dedup_by_link[item["link"]] = item

    items = list(dedup_by_link.values())
    items.sort(key=lambda x: (x["score"], x["published_at"]), reverse=True)

    log(f"[INFO] Total fetched after dedup: {len(items)}")
    return items


def limit_items_balanced(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    ordered_categories = ["Market", "Regulation", "DeFi", "Security", "Alt & Ecosystem", "General"]

    for item in items:
        grouped.setdefault(item["category"], []).append(item)

    selected: List[Dict[str, Any]] = []

    for category in ordered_categories:
        if category not in grouped:
            continue

        bucket = sorted(
            grouped[category],
            key=lambda x: (x["score"], x["published_at"]),
            reverse=True
        )
        selected.extend(bucket[:MAX_PER_CATEGORY])

    selected.sort(
        key=lambda x: (x["score"], x["published_at"]),
        reverse=True
    )

    final_items: List[Dict[str, Any]] = []
    seen_title_keys = set()

    for item in selected:
        title_key = re.sub(r"[^a-z0-9]+", " ", item["title"].lower()).strip()
        title_key = " ".join(title_key.split()[:8])

        if title_key in seen_title_keys:
            continue

        seen_title_keys.add(title_key)
        final_items.append(item)

        if len(final_items) >= MAX_ITEMS:
            break

    log(f"[INFO] Selected balanced items: {len(final_items)}")
    return final_items


# =========================
# GEMINI DIGEST
# =========================
def build_gemini_client():
    if not GEMINI_API_KEY:
        raise RuntimeError("Missing GEMINI_API_KEY")

    return genai.Client(api_key=GEMINI_API_KEY)


def build_digest_prompt(items: List[Dict[str, Any]]) -> str:
    news_blocks = []

    for i, item in enumerate(items, start=1):
        news_blocks.append(
            (
                f"TIN {i}\n"
                f"Nguồn: {item['source']}\n"
                f"Chuyên mục gợi ý: {CATEGORY_LABEL_VI.get(item['category'], 'Tổng hợp')}\n"
                f"Thời gian: {format_time_vn(item['published_at'])}\n"
                f"Tiêu đề gốc: {item['title']}\n"
                f"Tóm tắt gốc: {item['summary']}\n"
                f"Link: {item['link']}"
            )
        )

    joined = "\n\n".join(news_blocks)

    return f"""
Bạn là biên tập viên bản tin crypto tiếng Việt cho Discord.

Hãy viết lại toàn bộ danh sách tin dưới đây thành MỘT bản tin hoàn chỉnh bằng tiếng Việt.

YÊU CẦU BẮT BUỘC:
- Toàn bộ nội dung phải là tiếng Việt.
- Không giữ câu tiếng Anh, trừ tên riêng bắt buộc như Bitcoin, Ethereum, SEC, ETF, Tether, Solana, Coinbase.
- Không dùng câu sáo rỗng như:
  "Đây là diễn biến đáng chú ý..."
  "Nhà đầu tư nên tiếp tục theo dõi..."
  "Đây là thông tin quan trọng..."
- Không bịa thêm dữ kiện ngoài dữ liệu gốc.
- Văn phong ngắn gọn, tự nhiên, dễ đọc trên Discord.
- Mỗi tin viết 1 đến 2 câu.
- Nhóm theo các mục nếu phù hợp:
  📈 Thị trường
  ⚖️ Pháp lý
  🏦 DeFi
  🛡️ Bảo mật
  🧩 Hệ sinh thái
  📰 Tổng hợp
- Với mỗi tin, ghi theo đúng cấu trúc sau:

• [Tiêu đề tiếng Việt]
[Tóm tắt 1 đến 2 câu]
Nguồn: [Tên nguồn] • [Thời gian]

- Không dùng markdown link.
- Không thêm lời mở đầu kiểu "Dưới đây là bản tin".
- Không thêm kết luận tổng quát ở cuối.
- Trả về đúng PHẦN NỘI DUNG bản tin, sẵn sàng để đưa vào embed Discord.

DANH SÁCH TIN:
{joined}
""".strip()


def generate_digest_text(client, items: List[Dict[str, Any]]) -> str:
    if not items:
        return f"Không có tin mới nổi bật trong {LOOKBACK_HOURS} giờ gần đây."

    prompt = build_digest_prompt(items)
    log(f"[INFO] Sending {len(items)} items to Gemini with model={GEMINI_MODEL}")

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=1800,
            ),
        )
    except Exception as e:
        raise RuntimeError(f"Gemini API call failed: {e}")

    text = cleanup_digest_text((response.text or "").strip())

    log("[DEBUG] Gemini raw output preview:")
    log(text[:1200] if text else "[EMPTY]")

    if not text:
        raise RuntimeError("Gemini returned empty digest")

    if STRICT_VIETNAMESE and not is_digest_mostly_vietnamese(text):
        raise RuntimeError("Digest is still mostly English")

    return text


# =========================
# DISCORD FORMAT
# =========================
def split_text_for_embeds(text: str, max_len: int = 3800) -> List[str]:
    text = text.strip()
    if len(text) <= max_len:
        return [text]

    parts: List[str] = []
    current = ""

    for block in text.split("\n\n"):
        block = block.strip()
        if not block:
            continue

        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                parts.append(current)

            if len(block) <= max_len:
                current = block
            else:
                for i in range(0, len(block), max_len):
                    chunk = block[i:i + max_len].strip()
                    if chunk:
                        parts.append(chunk)
                current = ""

    if current:
        parts.append(current)

    return parts[:5]


def build_discord_payload_from_digest(digest_text: str, items_count: int) -> Dict[str, Any]:
    current_time = now_vn_str()
    digest_text = cleanup_digest_text(digest_text)

    chunks = split_text_for_embeds(digest_text, max_len=3800)
    embeds = []

    for idx, chunk in enumerate(chunks, start=1):
        embed = {
            "title": f"Crypto Daily Brief | {current_time}" if idx == 1 else f"Crypto Daily Brief | phần {idx}",
            "description": chunk,
            "color": EMBED_COLOR,
            "footer": {
                "text": f"{items_count} tin • RSS + Gemini • Auto digest"
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
        raise RuntimeError("Missing DISCORD_WEBHOOK_URL")

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
    if response.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {response.status_code} | {response.text}")


# =========================
# MAIN
# =========================
def main() -> None:
    log("========== BOT START ==========")
    log(f"[ENV] Has webhook: {bool(DISCORD_WEBHOOK_URL)}")
    log(f"[ENV] Has gemini key: {bool(GEMINI_API_KEY)}")
    log(f"[ENV] Model: {GEMINI_MODEL}")
    log(f"[ENV] Test mode: {TEST_MODE}")
    log(f"[ENV] Strict Vietnamese: {STRICT_VIETNAMESE}")
    log(f"[ENV] Lookback hours: {LOOKBACK_HOURS}")
    log(f"[ENV] Max items: {MAX_ITEMS}")
    log(f"[ENV] Max per category: {MAX_PER_CATEGORY}")

    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is missing")

    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is missing")

    state = load_state()
    posted_ids = set(state.get("posted_ids", []))
    log(f"[INFO] Existing posted_ids: {len(posted_ids)}")

    all_items = fetch_articles()

    if TEST_MODE:
        candidate_items = limit_items_balanced(all_items)
    else:
        unseen_items = [item for item in all_items if item["id"] not in posted_ids]
        log(f"[INFO] Unseen items: {len(unseen_items)}")
        candidate_items = limit_items_balanced(unseen_items)

    if not candidate_items:
        log("[INFO] No candidate items")
        digest_text = f"Không có tin mới nổi bật trong {LOOKBACK_HOURS} giờ gần đây."
        payload = build_discord_payload_from_digest(digest_text, 0)
        send_to_discord(payload)
        log("[INFO] Sent empty digest")
        return

    for idx, item in enumerate(candidate_items, start=1):
        log(f"[ITEM {idx}] {item['source']} | {item['category']} | {item['title'][:120]}")

    client = build_gemini_client()
    digest_text = generate_digest_text(client, candidate_items)

    payload = build_discord_payload_from_digest(digest_text, len(candidate_items))
    send_to_discord(payload)
    log("[INFO] Discord message sent successfully")

    if not TEST_MODE:
        for item in candidate_items:
            posted_ids.add(item["id"])

        state["posted_ids"] = list(posted_ids)[-1500:]
        save_state(state)
        log(f"[INFO] State saved with {len(state['posted_ids'])} ids")
    else:
        log("[INFO] TEST_MODE enabled, state not updated")

    log("========== BOT END ==========")


if __name__ == "__main__":
    main()

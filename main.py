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
    "betting",
    "gambling",
]

CATEGORY_RULES = {
    "Bitcoin": ["bitcoin", "btc", "ordinals", "lightning"],
    "Ethereum": ["ethereum", "eth", "eip-", "layer 2", "l2", "rollup", "arbitrum", "optimism", "base"],
    "DeFi": ["defi", "dex", "lending", "yield", "liquidity", "vault", "amm", "stablecoin", "staking"],
    "Regulation": ["sec", "regulation", "regulator", "law", "compliance", "policy", "etf", "congress", "clarity act"],
    "AI x Crypto": ["ai", "agent", "agents", "inference", "compute", "depin", "gpu"],
    "Altcoins": ["solana", "xrp", "bnb", "doge", "tron", "cardano", "avax", "ton", "sui", "aptos"],
}

# =========================
# CLIENTS
# =========================
client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

# =========================
# HELPERS
# =========================
def log(msg: str) -> None:
    now = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} GMT+7] {msg}")


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"sent_ids": []}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "sent_ids" in data:
                return data
    except Exception as e:
        log(f"[WARN] load_state failed: {e}")
    return {"sent_ids": []}


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def article_id(title: str, link: str) -> str:
    raw = f"{title}|{link}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&amp;|&quot;|&#39;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_date(entry: Dict[str, Any]) -> datetime:
    for key in ["published", "updated", "pubDate"]:
        value = entry.get(key)
        if value:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
    return datetime.now(timezone.utc)


def normalize_title(title: str) -> str:
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def similarity_key(title: str) -> str:
    words = normalize_title(title).split()
    filtered = [w for w in words if len(w) > 2]
    return " ".join(filtered[:10])


def contains_blacklist(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in BLACKLIST_KEYWORDS)


def detect_mode(now_vn: datetime) -> str:
    if now_vn.hour < 14:
        return "morning"
    return "evening"


def categorize_article(title: str, summary: str) -> str:
    haystack = f"{title} {summary}".lower()
    scores = {}

    for category, keywords in CATEGORY_RULES.items():
        score = sum(1 for kw in keywords if kw in haystack)
        if score > 0:
            scores[category] = score

    if not scores:
        return "Watchlist"

    return max(scores, key=scores.get)


def fetch_articles() -> List[Dict[str, Any]]:
    articles = []

    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            entries = getattr(feed, "entries", [])[:MAX_ENTRIES_PER_FEED]

            for entry in entries:
                title = (entry.get("title") or "").strip()
                link = (entry.get("link") or "").strip()
                summary = clean_html(entry.get("summary") or entry.get("description") or "")
                published_dt = parse_date(entry)

                if not title or not link:
                    continue

                if contains_blacklist(title) or contains_blacklist(summary):
                    continue

                articles.append({
                    "id": article_id(title, link),
                    "title": title,
                    "link": link,
                    "summary": summary[:1200],
                    "published_at": published_dt.isoformat(),
                    "published_dt": published_dt,
                    "source": feed_url
                })
        except Exception as e:
            log(f"[WARN] Failed feed {feed_url}: {e}")

    articles.sort(key=lambda x: x["published_dt"], reverse=True)
    return articles


def dedupe_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped = []

    for a in articles:
        key = similarity_key(a["title"])
        if not key:
            key = normalize_title(a["title"])

        if key in seen:
            continue

        seen.add(key)
        deduped.append(a)

    return deduped


def pick_new_articles(articles: List[Dict[str, Any]], sent_ids: set, limit: int = MAX_ARTICLES_FOR_AI) -> List[Dict[str, Any]]:
    picked = []
    for a in articles:
        if a["id"] not in sent_ids:
            a["category"] = categorize_article(a["title"], a["summary"])
            picked.append(a)
        if len(picked) >= limit:
            break
    return picked


def bucket_articles(articles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    order = ["Bitcoin", "Ethereum", "DeFi", "Regulation", "AI x Crypto", "Altcoins", "Watchlist"]

    for k in order:
        buckets[k] = []

    for a in articles:
        cat = a.get("category", "Watchlist")
        buckets.setdefault(cat, []).append(a)

    return {k: v for k, v in buckets.items() if v}


def truncate(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit - 1].rstrip() + "…"


def get_market_snapshot() -> Dict[str, str]:
    result = {
        "btc": "BTC: N/A",
        "eth": "ETH: N/A",
        "sol": "SOL: N/A",
    }

    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin,ethereum,solana",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_market_cap": "true",
        }
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        def fmt_coin(symbol_key: str, label: str) -> str:
            obj = data.get(symbol_key, {})
            price = obj.get("usd")
            chg = obj.get("usd_24h_change")
            if price is None:
                return f"{label}: N/A"
            if chg is None or (isinstance(chg, float) and math.isnan(chg)):
                return f"{label}: ${price:,.0f}"
            return f"{label}: ${price:,.0f} ({chg:+.2f}%)"

        result["btc"] = fmt_coin("bitcoin", "BTC")
        result["eth"] = fmt_coin("ethereum", "ETH")
        result["sol"] = fmt_coin("solana", "SOL")

    except Exception as e:
        log(f"[WARN] market snapshot failed: {e}")

    return result


def get_gold_snapshot() -> Dict[str, str]:
    """
    Bản đơn giản, ổn định:
    - Có thể sửa tay mỗi ngày nếu muốn
    - Hoặc sau này thay bằng scraping/API riêng
    """
    return {
        "vn": "Vàng VN: SJC ~169.8 – 172.8 triệu/lượng",
        "world": "Vàng TG: ~4,400 – 4,500 USD/oz",
        "spread": "Chênh lệch: cao (~25–30 triệu/lượng)"
    }


def build_ai_input(articles: List[Dict[str, Any]]) -> str:
    blocks = []
    for i, a in enumerate(articles, 1):
        published_vn = a["published_dt"].astimezone(VN_TZ).strftime("%Y-%m-%d %H:%M")
        blocks.append(
            f"[{i}] {a['title']}\n"
            f"Category: {a.get('category', 'Watchlist')}\n"
            f"Published(GMT+7): {published_vn}\n"
            f"Link: {a['link']}\n"
            f"Summary: {a['summary']}\n"
        )
    return "\n".join(blocks)


def get_prompt(mode: str, articles: List[Dict[str, Any]]) -> str:
    source_text = build_ai_input(articles)

    if mode == "morning":
        return f"""
Bạn là biên tập viên bản tin crypto cho cộng đồng Việt Nam.

Hãy viết bản tin BUỔI SÁNG bằng tiếng Việt tự nhiên, gọn, rõ, không dịch máy.

YÊU CẦU:
- Chỉ dùng thông tin từ dữ liệu nguồn
- Không bịa thêm dữ kiện
- Tránh lặp ý
- Ưu tiên góc nhìn nhanh: hôm nay thị trường đang chú ý điều gì
- Ngắn gọn hơn bản tối
- Trả về JSON hợp lệ theo schema sau:

{{
  "title": "string",
  "quick_take": ["string", "string"],
  "sections": [
    {{
      "name": "Bitcoin | Ethereum | DeFi | Regulation | AI x Crypto | Altcoins | Watchlist",
      "items": [
        {{
          "headline": "string",
          "summary": "string",
          "impact": "string",
          "source_index": 1
        }}
      ]
    }}
  ],
  "watchlist": ["string"],
  "source_refs": ["[1] ...", "[2] ..."]
}}

QUY TẮC:
- quick_take: 2 mục
- mỗi summary: 1-2 câu
- mỗi impact: rất ngắn
- tối đa 5 tin nổi bật toàn bài
- chỉ dùng source_index có trong dữ liệu

DỮ LIỆU:
{source_text}
"""
    else:
        return f"""
Bạn là biên tập viên bản tin crypto cho cộng đồng Việt Nam.

Hãy viết bản tin BUỔI TỐI bằng tiếng Việt tự nhiên, rõ, giàu thông tin hơn bản sáng.

YÊU CẦU:
- Chỉ dùng thông tin từ dữ liệu nguồn
- Không bịa thêm dữ kiện
- Tránh lặp ý
- Tóm tắt theo cụm chủ đề
- Nêu tác động ngắn hạn nếu có
- Trả về JSON hợp lệ theo schema sau:

{{
  "title": "string",
  "quick_take": ["string", "string", "string"],
  "sections": [
    {{
      "name": "Bitcoin | Ethereum | DeFi | Regulation | AI x Crypto | Altcoins | Watchlist",
      "items": [
        {{
          "headline": "string",
          "summary": "string",
          "impact": "string",
          "source_index": 1
        }}
      ]
    }}
  ],
  "watchlist": ["string", "string"],
  "source_refs": ["[1] ...", "[2] ..."]
}}

QUY TẮC:
- quick_take: 2-3 mục
- mỗi summary: 2-3 câu
- impact: 1 câu ngắn
- tối đa 6 tin nổi bật toàn bài
- chỉ dùng source_index có trong dữ liệu

DỮ LIỆU:
{source_text}
"""


def summarize_with_deepseek(mode: str, articles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not articles:
        return None

    prompt = get_prompt(mode, articles)

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": "Bạn là editor chuyên viết bản tin crypto tiếng Việt, ngắn gọn, rõ ràng, đúng dữ kiện."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
            max_tokens=1800,
            response_format={"type": "json_object"}
        )

        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)

        if not isinstance(data, dict):
            return None

        return data

    except Exception as e:
        log(f"[WARN] DeepSeek summarization failed: {e}")
        return None


def fallback_summary(mode: str, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets = bucket_articles(articles)

    quick_take = []
    if mode == "morning":
        quick_take.append("Thị trường sáng nay xoay quanh các tin mới từ nhóm tài sản lớn và dòng vốn theo chủ đề.")
        quick_take.append("Ưu tiên theo dõi các cập nhật có thể tác động trực tiếp đến tâm lý ngắn hạn.")
    else:
        quick_take.append("Dòng tin hôm nay tập trung vào các chủ đề có khả năng ảnh hưởng tâm lý thị trường và định vị vốn ngắn hạn.")
        quick_take.append("Những câu chuyện nổi bật nhất chủ yếu xoay quanh nhóm tài sản lớn, hạ tầng và chính sách.")
        quick_take.append("Các mảng còn lại đang ở trạng thái chờ xác nhận thêm tín hiệu.")

    sections = []
    total_items = 0

    for category, items in buckets.items():
        section_items = []
        for a in items[:2]:
            if total_items >= (5 if mode == "morning" else 6):
                break

            section_items.append({
                "headline": a["title"],
                "summary": truncate(a["summary"] or "Xem nguồn để biết thêm chi tiết.", 220 if mode == "morning" else 320),
                "impact": "Theo dõi thêm để xác nhận tác động.",
                "source_index": articles.index(a) + 1
            })
            total_items += 1

        if section_items:
            sections.append({
                "name": category,
                "items": section_items
            })

    source_refs = [f"[{i}] {a['link']}" for i, a in enumerate(articles, 1)]

    return {
        "title": "Bản Tin Crypto",
        "quick_take": quick_take,
        "sections": sections,
        "watchlist": ["Theo dõi thêm phản ứng thị trường với các tin vừa xuất hiện."],
        "source_refs": source_refs
    }


def make_embed_payload(
    mode: str,
    ai_data: Dict[str, Any],
    snapshot: Dict[str, str],
    gold: Dict[str, str],
    now_vn: datetime
) -> Dict[str, Any]:
    title = ai_data.get("title") or ("Bản tin crypto sáng" if mode == "morning" else "Bản tin crypto tối")
    quick_take = ai_data.get("quick_take") or []
    sections = ai_data.get("sections") or []
    watchlist = ai_data.get("watchlist") or []
    source_refs = ai_data.get("source_refs") or []

    color = 0xF1C40F if mode == "morning" else 0x5865F2

    fields = []

    market_text = "\n".join([
        snapshot.get("btc", "BTC: N/A"),
        snapshot.get("eth", "ETH: N/A"),
        snapshot.get("sol", "SOL: N/A"),
        "",
        gold.get("vn", "Vàng VN: N/A"),
        gold.get("world", "Vàng TG: N/A"),
        gold.get("spread", "Chênh lệch: N/A"),
    ])

    fields.append({
        "name": "📊 Market Snapshot",
        "value": truncate(market_text, DISCORD_EMBED_FIELD_LIMIT),
        "inline": False
    })

    if quick_take:
        quick_text = "\n".join([f"• {x}" for x in quick_take if x])
        fields.append({
            "name": "⚡ Điểm nhanh",
            "value": truncate(quick_text, DISCORD_EMBED_FIELD_LIMIT),
            "inline": False
        })

    for sec in sections[:5]:
        sec_name = sec.get("name", "Chuyên mục")
        items = sec.get("items") or []
        lines = []

        for item in items[:2]:
            headline = item.get("headline", "").strip()
            summary = item.get("summary", "").strip()
            impact = item.get("impact", "").strip()
            source_idx = item.get("source_index")

            block = f"**{headline}**"
            if summary:
                block += f"\n{summary}"
            if impact:
                block += f"\n*Tác động nhanh:* {impact}"
            if isinstance(source_idx, int):
                block += f"\nNguồn: [{source_idx}]"

            lines.append(block)

        if lines:
            value = "\n\n".join(lines)
            fields.append({
                "name": f"🧩 {sec_name}",
                "value": truncate(value, DISCORD_EMBED_FIELD_LIMIT),
                "inline": False
            })

    if watchlist:
        watch_text = "\n".join([f"• {x}" for x in watchlist[:3]])
        fields.append({
            "name": "👀 Watchlist",
            "value": truncate(watch_text, DISCORD_EMBED_FIELD_LIMIT),
            "inline": False
        })

    if source_refs:
        source_text = "\n".join(source_refs[:8])
        fields.append({
            "name": "🔗 Nguồn tham khảo",
            "value": truncate(source_text, DISCORD_EMBED_FIELD_LIMIT),
            "inline": False
        })

    return {
        "username": "Crypto News Bot",
        "embeds": [
            {
                "title": title,
                "description": "Bản tổng hợp tự động bằng DeepSeek + RSS",
                "color": color,
                "fields": fields[:25],
                "footer": {
                    "text": f"Cập nhật: {now_vn.strftime('%Y-%m-%d %H:%M GMT+7')}"
                }
            }
        ],
        "allowed_mentions": {
            "parse": []
        }
    }


def split_embeds_if_needed(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    embeds = payload.get("embeds", [])
    if not embeds:
        return [payload]

    embed = embeds[0]
    fields = embed.get("fields", [])

    if len(fields) <= 6:
        return [payload]

    chunks = []
    for i in range(0, len(fields), 6):
        new_payload = {
            "username": payload.get("username", "Crypto News Bot"),
            "allowed_mentions": payload.get("allowed_mentions", {"parse": []}),
            "embeds": [
                {
                    "title": embed.get("title", "Bản tin crypto") if i == 0 else f"{embed.get('title', 'Bản tin crypto')} (tiếp)",
                    "description": embed.get("description", "") if i == 0 else "",
                    "color": embed.get("color", 0x5865F2),
                    "fields": fields[i:i + 6],
                    "footer": embed.get("footer", {})
                }
            ]
        }
        chunks.append(new_payload)

    return chunks


def send_to_discord_embed(payload: Dict[str, Any]) -> None:
    payloads = split_embeds_if_needed(payload)

    for idx, p in enumerate(payloads, 1):
        r = requests.post(DISCORD_WEBHOOK_URL, json=p, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        log(f"[INFO] Sent embed chunk {idx}/{len(payloads)}")
        time.sleep(1)


def send_plain_text_fallback(message: str) -> None:
    payload = {
        "content": message[:1800],
        "allowed_mentions": {"parse": []}
    }
    r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()


def main() -> None:
    if not DEEPSEEK_API_KEY:
        raise ValueError("Missing DEEPSEEK_API_KEY")
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("Missing DISCORD_WEBHOOK_URL")

    now_vn = datetime.now(VN_TZ)
    mode = detect_mode(now_vn)

    log(f"[INFO] Start bot in mode={mode}")

    state = load_state()
    sent_ids = set(state.get("sent_ids", []))

    articles = fetch_articles()
    log(f"[INFO] Fetched raw articles: {len(articles)}")

    articles = dedupe_articles(articles)
    log(f"[INFO] After dedupe: {len(articles)}")

    new_articles = pick_new_articles(articles, sent_ids)
    log(f"[INFO] New picked articles: {len(new_articles)}")

    if not new_articles:
        send_plain_text_fallback("📰 **BẢN TIN CRYPTO**\nKhung giờ này chưa có tin mới nổi bật để tổng hợp.")
        log("[INFO] No new articles. Sent fallback text.")
        return

    snapshot = get_market_snapshot()
    gold = get_gold_snapshot()
    ai_data = summarize_with_deepseek(mode, new_articles)

    if not ai_data:
        log("[WARN] Using fallback summary.")
        ai_data = fallback_summary(mode, new_articles)

    payload = make_embed_payload(mode, ai_data, snapshot, gold, now_vn)
    send_to_discord_embed(payload)

    for a in new_articles:
        sent_ids.add(a["id"])

    state["sent_ids"] = list(sent_ids)[-1000:]
    save_state(state)

    log("[INFO] Done successfully.")


if __name__ == "__main__":
    main()

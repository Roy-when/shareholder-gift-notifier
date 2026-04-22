"""
股東紀念品爬蟲 v2.2（HiStock 穩定版）
====================================
來源：HiStock（histock.tw）
說明：wantgoo 有 Cloudflare 保護無法爬取，維持 HiStock 單一穩定來源
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
import re
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path="gx.env")

HISTOCK_URL = "https://histock.tw/stock/gift.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ────────────────────────────────────────────────
# 紀念品分類關鍵字
# ────────────────────────────────────────────────
GIFT_TYPE_KEYWORDS = {
    "米":    ["米", "白米", "糙米", "蓬萊米"],
    "油":    ["油", "沙拉油", "橄欖油", "苦茶油"],
    "飲料":  ["飲料", "礦泉水", "水", "茶", "咖啡", "果汁"],
    "提貨券": ["提貨券", "兌換券", "禮券", "購物金", "折扣券",
               "商品卡", "商品券", "禮物卡", "抵用券",
               "7-11", "7-ELEVEN", "711", "統一超商",
               "全家", "FamilyMart", "全聯", "萊爾富", "OK超商",
               "超商", "便利商店", "便利店"],
    "食品":  ["餅乾", "罐頭", "泡麵", "醬油", "鹽", "糖", "醋",
               "麵條", "米粉", "肉鬆", "雞肉", "益生菌", "膠囊",
               "飲品", "保健", "燕窩", "膠原"],
    "日用品": ["衛生紙", "洗碗精", "洗衣精", "沐浴乳", "牙膏",
               "香皂", "皂", "洗手", "清潔", "洗衣", "濕紙巾",
               "口罩", "酒精"],
}

def classify_gift(gift_str: str) -> list:
    matched = [t for t, kws in GIFT_TYPE_KEYWORDS.items() if any(kw in gift_str for kw in kws)]
    return matched if matched else ["其他"]

def parse_date(date_str: str):
    """將 M/D 格式轉為 YYYY-MM-DD，不推算明年（已截止維持負數）"""
    date_str = date_str.strip()
    if not date_str or "/" not in date_str:
        return None
    try:
        parts = date_str.split("/")
        m, d = int(parts[0]), int(parts[1])
        return datetime(datetime.now().year, m, d).strftime("%Y-%m-%d")
    except Exception:
        return None

def get_deadline_status(buy_date_str: str) -> str:
    parsed = parse_date(buy_date_str)
    if not parsed:
        return "不明"
    days_left = (datetime.strptime(parsed, "%Y-%m-%d").date() - date.today()).days
    if days_left > 5:  return "充裕"
    if days_left >= 4: return "注意"
    if days_left >= 1: return "緊急"
    return "已截止"

def get_days_left(buy_date_str: str):
    parsed = parse_date(buy_date_str)
    if not parsed:
        return None
    return (datetime.strptime(parsed, "%Y-%m-%d").date() - date.today()).days

def parse_price(price_str: str):
    cleaned = re.sub(r"[^\d.]", "", str(price_str).strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ────────────────────────────────────────────────
# 爬取 HiStock
# ────────────────────────────────────────────────
def crawl_histock() -> list:
    print("📡 正在爬取 HiStock 資料...")
    try:
        resp = requests.get(HISTOCK_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ 請求失敗：{e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = []
    seen_codes = set()  # 避免重複

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            # 同時處理 td（資料行）和跳過 th（標題行）
            cols = row.find_all("td")
            if len(cols) < 8:
                continue
            try:
                code = cols[0].text.strip()
                # 過濾空白、非數字開頭（排除廣告列等雜訊）
                if not code or not re.match(r"^\d", code):
                    continue
                # 避免重複（三個區段可能有相同代號）
                if code in seen_codes:
                    continue
                seen_codes.add(code)

                # 紀念品欄位去除「參考圖」等雜訊文字
                gift_raw = cols[7].text.strip()
                gift = re.sub(r"\s*參考圖.*$", "", gift_raw).strip()

                rows.append({
                    "代號":      code,
                    "名稱":      cols[1].text.strip(),
                    "股價":      cols[2].text.strip(),
                    "最後買進日": cols[3].text.strip(),
                    "股東會日期": cols[4].text.strip(),
                    "性質":      cols[5].text.strip(),
                    "開會地點":  cols[6].text.strip(),
                    "紀念品":    gift,
                })
            except Exception:
                continue

    print(f"✅ 爬取完成，共 {len(rows)} 筆（含已截止）")
    return rows


# ────────────────────────────────────────────────
# 建立 JSON
# ────────────────────────────────────────────────
def build_json(rows: list) -> dict:
    records = []
    for row in rows:
        buy_raw  = row.get("最後買進日", "").strip()
        meet_raw = row.get("股東會日期", "").strip()
        gift     = row.get("紀念品", "").strip()

        records.append({
            "code":        row.get("代號", ""),
            "name":        row.get("名稱", ""),
            "price":       parse_price(row.get("股價", "")),
            "buyDate":     parse_date(buy_raw),
            "buyDateRaw":  buy_raw,
            "meetingDate": parse_date(meet_raw),
            "type":        row.get("性質", ""),
            "location":    row.get("開會地點", ""),
            "gift":        gift,
            "giftTypes":   classify_gift(gift),
            "status":      get_deadline_status(buy_raw),
            "daysLeft":    get_days_left(buy_raw),
        })

    # 依截止天數排序：負數（已截止）→ 0 → 正數 → None 最後
    records.sort(key=lambda x: (
        x["daysLeft"] is None,
        x["daysLeft"] if x["daysLeft"] is not None else 9999
    ))

    return {
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total":     len(records),
        "records":   records,
    }


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("─" * 60)
    print("🚀 股東紀念品爬蟲 v2.2")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    rows = crawl_histock()

    if len(rows) < 50:
        print(f"❌ 爬取筆數異常（{len(rows)} 筆），可能網站改版，請檢查爬蟲！")
        exit(1)

    data = build_json(rows)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ 已輸出 data.json（{data['total']} 筆）")
    print("─" * 60)

"""
股東紀念品爬蟲 v2.1（雙來源合併版）
====================================
來源策略：
  - 主要來源：玩股網 wantgoo（資料較完整）
  - 補充來源：HiStock（補充 wantgoo 沒有的股票）
  - 合併規則：以股票代號為 key，wantgoo 優先，histock 補缺漏
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import json
import re
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path="gx.env")

WANTGOO_URL = "https://www.wantgoo.com/stock/calendar/shareholders-meeting-souvenirs?year=2026"
HISTOCK_URL = "https://histock.tw/stock/gift.aspx"

HEADERS_WANTGOO = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.wantgoo.com/stock/calendar/shareholders-meeting",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

HEADERS_HISTOCK = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ────────────────────────────────────────────────
# 分類關鍵字
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
    """將 M/D 格式轉為 YYYY-MM-DD，不推算明年"""
    date_str = date_str.strip()
    if not date_str or "/" not in date_str:
        return None
    try:
        parts = date_str.split("/")
        m, d = int(parts[0]), int(parts[1])
        year = datetime.now().year
        return datetime(year, m, d).strftime("%Y-%m-%d")
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
# 1. 爬取 wantgoo（主要來源）
# ────────────────────────────────────────────────
def crawl_wantgoo() -> dict:
    """回傳 {代號: row_dict}"""
    print("📡 正在爬取 wantgoo 資料...")
    result = {}
    try:
        # 用 Session 模擬真實瀏覽器行為，先訪問首頁取得 cookie
        session = requests.Session()
        session.get("https://www.wantgoo.com/", headers=HEADERS_WANTGOO, timeout=15)
        time.sleep(1.5)
        resp = session.get(WANTGOO_URL, headers=HEADERS_WANTGOO, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"⚠️  wantgoo 請求失敗：{e}")
        return result

    soup = BeautifulSoup(resp.text, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue
            try:
                # 第一欄：代號+名稱 合在一個 td，格式「4564元翎」
                code_name = cols[0].text.strip()
                # 用正規式拆出代號（數字開頭）和名稱
                m = re.match(r"(\d+[A-Z\-]*)\s*(.*)", code_name)
                if not m:
                    continue
                code = m.group(1).strip()
                name = m.group(2).strip()
                if not code:
                    continue

                result[code] = {
                    "代號":      code,
                    "名稱":      name,
                    "股價":      cols[1].text.strip(),
                    "紀念品":    cols[2].text.strip(),
                    "最後買進日": cols[3].text.strip(),
                    "股東會日期": cols[4].text.strip(),
                    "性質":      cols[5].text.strip(),
                    "開會地點":  cols[6].text.strip(),
                }
            except Exception:
                continue

    print(f"✅ wantgoo 爬取完成，共 {len(result)} 筆")
    return result


# ────────────────────────────────────────────────
# 2. 爬取 HiStock（補充來源）
# ────────────────────────────────────────────────
def crawl_histock() -> dict:
    """回傳 {代號: row_dict}"""
    print("📡 正在爬取 HiStock 補充資料...")
    result = {}
    try:
        resp = requests.get(HISTOCK_URL, headers=HEADERS_HISTOCK, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"⚠️  HiStock 請求失敗：{e}")
        return result

    soup = BeautifulSoup(resp.text, "html.parser")
    for table in soup.find_all("table"):
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue
            try:
                code = cols[0].text.strip()
                if not code:
                    continue
                result[code] = {
                    "代號":      code,
                    "名稱":      cols[1].text.strip(),
                    "股價":      cols[2].text.strip(),
                    "最後買進日": cols[3].text.strip(),
                    "股東會日期": cols[4].text.strip(),
                    "性質":      cols[5].text.strip(),
                    "開會地點":  cols[6].text.strip(),
                    "紀念品":    cols[7].text.strip(),
                }
            except Exception:
                continue

    print(f"✅ HiStock 爬取完成，共 {len(result)} 筆")
    return result


# ────────────────────────────────────────────────
# 3. 合併兩個來源
# ────────────────────────────────────────────────
def merge_sources(wantgoo: dict, histock: dict) -> list:
    """wantgoo 為主，histock 補充沒有的代號"""
    merged = {}

    # 先放入 wantgoo 全部
    for code, row in wantgoo.items():
        merged[code] = row

    # histock 只補充 wantgoo 沒有的
    added = 0
    for code, row in histock.items():
        if code not in merged:
            merged[code] = row
            added += 1

    print(f"📊 合併結果：wantgoo {len(wantgoo)} 筆 + histock 補充 {added} 筆 = 共 {len(merged)} 筆")
    return list(merged.values())


# ────────────────────────────────────────────────
# 4. 過濾無效資料（不發放 / 未決定）
# ────────────────────────────────────────────────
def filter_valid(rows: list) -> list:
    skip_keywords = ["不發放", "未決定"]
    valid = [r for r in rows if not any(kw in r.get("紀念品", "") for kw in skip_keywords)]
    skip_count = len(rows) - len(valid)
    if skip_count > 0:
        print(f"🗑️  過濾「不發放/未決定」{skip_count} 筆，剩 {len(valid)} 筆")
    return valid


# ────────────────────────────────────────────────
# 5. 建立 JSON
# ────────────────────────────────────────────────
def build_json(rows: list) -> dict:
    records = []
    for row in rows:
        buy_raw = row.get("最後買進日", "").strip()
        meeting_raw = row.get("股東會日期", "").strip()
        gift = row.get("紀念品", "").strip()

        records.append({
            "code":        row.get("代號", ""),
            "name":        row.get("名稱", ""),
            "price":       parse_price(row.get("股價", "")),
            "buyDate":     parse_date(buy_raw),
            "buyDateRaw":  buy_raw,
            "meetingDate": parse_date(meeting_raw),
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
        "total": len(records),
        "records": records,
    }


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("─" * 60)
    print("🚀 股東紀念品爬蟲 v2.1（雙來源合併版）")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    # Step 1：爬取兩個來源
    wantgoo_data = crawl_wantgoo()
    time.sleep(1)  # 避免短時間連續請求
    histock_data = crawl_histock()

    # Step 2：合併
    merged = merge_sources(wantgoo_data, histock_data)

    # Step 3：過濾無效
    valid = filter_valid(merged)

    # Step 4：品質監控
    if len(valid) < 50:
        print(f"❌ 爬取筆數異常（{len(valid)} 筆），可能網站改版，請檢查爬蟲！")
        exit(1)

    # Step 5：輸出 JSON
    data = build_json(valid)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 已輸出 data.json（{data['total']} 筆）")
    print("─" * 60)

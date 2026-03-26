"""
股東紀念品爬蟲 v2.0（Web App 版）
====================================
改動：
  - 輸出 data.json 給前端 Web App 讀取
  - 移除 Notion 相關邏輯
  - 加入爬蟲品質監控：筆數 < 50 筆時主動報錯讓 GitHub Actions 失敗
  - 截止狀態由 Python 計算後寫入 JSON
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import json
import os
import re
from dotenv import load_dotenv

load_dotenv(dotenv_path="gx.env")

HISTOCK_URL = "https://histock.tw/stock/gift.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

GIFT_TYPE_KEYWORDS = {
    "米":    ["米", "白米", "糙米", "蓬萊米"],
    "油":    ["油", "沙拉油", "橄欖油", "苦茶油"],
    "飲料":  ["飲料", "礦泉水", "水", "茶", "咖啡", "果汁"],
    "提貨券": ["提貨券", "兌換券", "禮券", "購物金", "折扣券"],
    "食品":  ["餅乾", "罐頭", "泡麵", "醬油", "鹽", "糖", "醋"],
    "日用品": ["衛生紙", "洗碗精", "洗衣精", "沐浴乳", "牙膏"],
}

def classify_gift(gift_str: str) -> list:
    matched = [t for t, kws in GIFT_TYPE_KEYWORDS.items() if any(kw in gift_str for kw in kws)]
    return matched if matched else ["其他"]

def parse_date(date_str: str):
    date_str = date_str.strip()
    if not date_str or "/" not in date_str:
        return None
    try:
        parts = date_str.split("/")
        m, d = int(parts[0]), int(parts[1])
        year = datetime.now().year
        target = datetime(year, m, d)
        if target.date() < datetime.now().date():
            target = datetime(year + 1, m, d)
        return target.strftime("%Y-%m-%d")
    except Exception:
        return None

def get_deadline_status(buy_date_str: str) -> str:
    parsed = parse_date(buy_date_str)
    if not parsed:
        return "不明"
    days_left = (datetime.strptime(parsed, "%Y-%m-%d").date() - date.today()).days
    if days_left > 5:   return "充裕"
    if days_left >= 4:  return "注意"
    if days_left >= 1:  return "緊急"
    return "已截止"

def get_days_left(buy_date_str: str):
    parsed = parse_date(buy_date_str)
    if not parsed:
        return None
    return (datetime.strptime(parsed, "%Y-%m-%d").date() - date.today()).days

def parse_price(price_str: str):
    cleaned = re.sub(r"[^\d.]", "", price_str.strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

def crawl_gifts() -> pd.DataFrame:
    print("📡 正在爬取 HiStock 資料...")
    try:
        resp = requests.get(HISTOCK_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ 請求失敗：{e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    data = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue
            try:
                data.append({
                    "代號":      cols[0].text.strip(),
                    "名稱":      cols[1].text.strip(),
                    "股價":      cols[2].text.strip(),
                    "最後買進日": cols[3].text.strip(),
                    "股東會日期": cols[4].text.strip(),
                    "性質":      cols[5].text.strip(),
                    "開會地點":  cols[6].text.strip(),
                    "紀念品":    cols[7].text.strip(),
                })
            except Exception:
                continue

    df = pd.DataFrame(data)
    df = df[df["代號"].str.strip() != ""].reset_index(drop=True)
    print(f"✅ 爬取完成，共 {len(df)} 筆資料")
    return df

def build_json(df: pd.DataFrame) -> dict:
    records = []
    for _, row in df.iterrows():
        buy_date_iso = parse_date(row["最後買進日"])
        meeting_date_iso = parse_date(row["股東會日期"])
        records.append({
            "code":         row["代號"],
            "name":         row["名稱"],
            "price":        parse_price(row["股價"]),
            "buyDate":      buy_date_iso,
            "buyDateRaw":   row["最後買進日"],
            "meetingDate":  meeting_date_iso,
            "type":         row["性質"],
            "location":     row["開會地點"],
            "gift":         row["紀念品"],
            "giftTypes":    classify_gift(row["紀念品"]),
            "status":       get_deadline_status(row["最後買進日"]),
            "daysLeft":     get_days_left(row["最後買進日"]),
        })

    # 依截止天數排序（None 排最後）
    records.sort(key=lambda x: (x["daysLeft"] is None, x["daysLeft"] if x["daysLeft"] is not None else 9999))

    return {
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": len(records),
        "records": records,
    }

if __name__ == "__main__":
    print("─" * 60)
    print(f"🚀 股東紀念品爬蟲 v2.0")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    df = crawl_gifts()

    # Quinn：筆數監控，< 50 筆視為爬蟲異常，主動讓 CI 失敗
    if len(df) < 50:
        print(f"❌ 爬取筆數異常（{len(df)} 筆），可能網站改版，請檢查爬蟲！")
        exit(1)

    data = build_json(df)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ 已輸出 data.json（{data['total']} 筆）")
    print("─" * 60)

"""
股東紀念品 → Notion 同步腳本 v1.2（GitHub Actions 專用版）
==========================================================
v1.2 改動：
  - 移除互動式 input()，改從環境變數直接讀取（相容 GitHub Actions Secrets）
  - 移除 gx.env 寫入邏輯（CI 環境不需要）
  - NOTION_DATABASE_ID 未設定時直接報錯提示，不卡住等待輸入

本機執行：在 gx.env 設定以下兩個變數
  NOTION_TOKEN=secret_xxxxxxxxxx
  NOTION_DATABASE_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

GitHub Actions：在 Secrets 設定同名變數即可
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
import re
from dotenv import load_dotenv
from notion_client import Client

# ────────────────────────────────────────────────
# 載入環境變數（本機讀 gx.env，GitHub Actions 讀 Secrets）
# ────────────────────────────────────────────────
load_dotenv(dotenv_path="gx.env")

HISTOCK_URL = "https://histock.tw/stock/gift.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

if not NOTION_TOKEN:
    raise ValueError(
        "缺少 NOTION_TOKEN！\n"
        "本機：請在 gx.env 加入 NOTION_TOKEN=secret_xxx\n"
        "GitHub Actions：請在 Secrets 加入 NOTION_TOKEN"
    )

if not NOTION_DATABASE_ID:
    raise ValueError(
        "缺少 NOTION_DATABASE_ID！\n"
        "請先在本機執行一次 v1.1 版建立資料庫，\n"
        "再將產生的 Database ID 填入：\n"
        "  本機：gx.env 的 NOTION_DATABASE_ID=xxx\n"
        "  GitHub Actions：Settings → Secrets → NOTION_DATABASE_ID"
    )

notion = Client(auth=NOTION_TOKEN)

# ────────────────────────────────────────────────
# 紀念品類型關鍵字對照表
# ────────────────────────────────────────────────
GIFT_TYPE_KEYWORDS = {
    "米":    ["米", "白米", "糙米", "蓬萊米"],
    "油":    ["油", "沙拉油", "橄欖油", "苦茶油"],
    "飲料":  ["飲料", "礦泉水", "水", "茶", "咖啡", "果汁"],
    "提貨券": ["提貨券", "兌換券", "禮券", "購物金", "折扣券"],
    "食品":  ["餅乾", "罐頭", "泡麵", "醬油", "鹽", "糖", "醋"],
    "日用品": ["衛生紙", "洗碗精", "洗衣精", "沐浴乳", "牙膏"],
}

def classify_gift(gift_str: str) -> list:
    matched = []
    for gift_type, keywords in GIFT_TYPE_KEYWORDS.items():
        if any(kw in gift_str for kw in keywords):
            matched.append(gift_type)
    return matched if matched else ["其他"]


# ────────────────────────────────────────────────
# 1. 爬取 HiStock 資料
# ────────────────────────────────────────────────
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


# ────────────────────────────────────────────────
# 2. 輔助函式：日期 & 股價解析
# ────────────────────────────────────────────────
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


def parse_price(price_str: str):
    cleaned = re.sub(r"[^\d.]", "", price_str.strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ────────────────────────────────────────────────
# 3. 查詢現有資料庫中的股票代號（避免重複新增）
# ────────────────────────────────────────────────
def get_existing_codes(database_id: str) -> dict:
    existing = {}
    cursor = None
    while True:
        params = {"database_id": database_id, "page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        result = notion.databases.query(**params)
        for page in result["results"]:
            rich_text = page["properties"].get("代號", {}).get("rich_text", [])
            if rich_text:
                existing[rich_text[0]["text"]["content"]] = page["id"]
        if not result.get("has_more"):
            break
        cursor = result["next_cursor"]
    return existing


# ────────────────────────────────────────────────
# 4. 建立單筆 Notion 屬性
# ────────────────────────────────────────────────
def build_properties(row: pd.Series) -> dict:
    today_iso = datetime.now().strftime("%Y-%m-%d")
    gift_text = row["紀念品"]
    gift_types = classify_gift(gift_text)

    props = {
        "名稱（股票）": {
            "title": [{"text": {"content": f"{row['名稱']}（{row['代號']}）"}}]
        },
        "代號":     {"rich_text": [{"text": {"content": row["代號"]}}]},
        "開會地點": {"rich_text": [{"text": {"content": row["開會地點"]}}]},
        "紀念品":   {"rich_text": [{"text": {"content": gift_text}}]},
        "紀念品類型": {
            "multi_select": [{"name": t} for t in gift_types]
        },
        "性質":       {"select": {"name": row["性質"] if row["性質"] else "其他"}},
        "資料更新日": {"date": {"start": today_iso}},
    }

    price = parse_price(row["股價"])
    if price is not None:
        props["股價"] = {"number": price}

    buy_date = parse_date(row["最後買進日"])
    if buy_date:
        props["最後買進日"] = {"date": {"start": buy_date}}

    meeting_date = parse_date(row["股東會日期"])
    if meeting_date:
        props["股東會日期"] = {"date": {"start": meeting_date}}

    return props


# ────────────────────────────────────────────────
# 5. 同步資料到 Notion
# ────────────────────────────────────────────────
def sync_to_notion(df: pd.DataFrame, database_id: str):
    if df.empty:
        print("⚠️  無資料可同步")
        return

    print(f"🔄 正在同步 {len(df)} 筆資料到 Notion...")
    existing = get_existing_codes(database_id)
    created, updated, failed = 0, 0, 0
    parse_failed_count = 0

    for _, row in df.iterrows():
        code = row["代號"]

        if parse_date(row["最後買進日"]) is None and row["最後買進日"].strip():
            parse_failed_count += 1

        props = build_properties(row)
        try:
            if code in existing:
                page_id = existing[code]
                current = notion.pages.retrieve(page_id)
                is_purchased = current["properties"]["已購買"]["checkbox"]
                props["已購買"] = {"checkbox": is_purchased}  # 保留手動勾選狀態
                notion.pages.update(page_id=page_id, properties=props)
                updated += 1
            else:
                props["已購買"] = {"checkbox": False}
                notion.pages.create(
                    parent={"database_id": database_id},
                    properties=props,
                )
                created += 1

            time.sleep(0.7)  # retrieve+update 連發，0.7s 安全間隔

        except Exception as e:
            print(f"  ❌ {code} {row['名稱']} 失敗：{e}")
            failed += 1

    print(f"\n📊 同步結果：新增 {created} 筆 ｜ 更新 {updated} 筆 ｜ 失敗 {failed} 筆")

    if parse_failed_count > 0:
        print(f"⚠️  有 {parse_failed_count} 筆「最後買進日」格式無法解析，日期欄位已略過（其他資料仍正常同步）")


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("─" * 60)
    print("🚀 股東紀念品 Notion 同步腳本 v1.2（GitHub Actions 版）")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    df = crawl_gifts()
    if df.empty:
        print("❌ 爬取失敗，程式結束")
        exit(1)

    sync_to_notion(df, NOTION_DATABASE_ID)

    print("\n" + "─" * 60)
    print("✅ 執行完成！請前往 Notion 查看資料")
    print("─" * 60)

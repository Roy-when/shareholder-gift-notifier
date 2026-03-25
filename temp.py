"""
股東紀念品 → Notion 同步腳本 v1.4（合併建庫+驗證版）
==========================================================
v1.4 改動（工程師團隊修正）：
  [Casey]  建庫 + 同步邏輯合併為一支檔案，不再需要 notion_stock_gift_sync.py
  [Riley]  加入 validate_database_id()，執行前自動驗證 ID 是資料庫還是頁面
  [Quinn]  ID 是頁面時直接報清楚錯誤，引導使用者重新建庫

使用方式：
  本機第一次執行：gx.env 只需要 NOTION_TOKEN，不設 NOTION_DATABASE_ID
                  程式會請你輸入 Parent Page ID 並自動建庫
  本機之後執行：gx.env 有 NOTION_DATABASE_ID 就直接同步
  GitHub Actions：Secrets 設定 NOTION_TOKEN + NOTION_DATABASE_ID
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
# 載入環境變數
# ────────────────────────────────────────────────
load_dotenv(dotenv_path="gx.env")

HISTOCK_URL = "https://histock.tw/stock/gift.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()

if not NOTION_TOKEN:
    raise ValueError(
        "缺少 NOTION_TOKEN！\n"
        "本機：請在 gx.env 加入 NOTION_TOKEN=secret_xxx\n"
        "GitHub Actions：請在 Secrets 加入 NOTION_TOKEN"
    )

notion = Client(auth=NOTION_TOKEN, timeout_ms=30_000)

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
# 2. Riley：驗證 ID 是資料庫還是頁面
# ────────────────────────────────────────────────
def validate_database_id(database_id: str) -> bool:
    """
    呼叫 Notion API 確認 ID 是否為資料庫。
    回傳 True = 是資料庫，False = 是頁面或不存在。
    """
    try:
        notion.databases.retrieve(database_id=database_id)
        return True
    except Exception as e:
        err = str(e)
        if "is a page" in err:
            print(f"\n❌ ID 驗證失敗：{database_id} 是一個「頁面」，不是資料庫！")
            print("   請清除 gx.env 裡的 NOTION_DATABASE_ID，重新執行讓程式建立資料庫。")
        elif "404" in err or "Could not find" in err:
            print(f"\n❌ ID 驗證失敗：找不到 {database_id}，請確認 ID 正確且 Integration 已授權。")
        else:
            print(f"\n❌ ID 驗證失敗：{e}")
        return False


# ────────────────────────────────────────────────
# 3. 建立 Notion 資料庫（首次執行）
# ────────────────────────────────────────────────
def create_notion_database(parent_page_id: str) -> str:
    print("🏗️  正在建立 Notion 資料庫...")
    db = notion.databases.create(
        parent={"type": "page_id", "page_id": parent_page_id},
        title=[{"type": "text", "text": {"content": "📋 股東紀念品追蹤"}}],
        properties={
            "名稱（股票）": {"title": {}},
            "代號":         {"rich_text": {}},
            "開會地點":     {"rich_text": {}},
            "紀念品":       {"rich_text": {}},
            "紀念品類型":   {"multi_select": {}},
            "股價":         {"number": {"format": "number"}},
            "最後買進日":   {"date": {}},
            "股東會日期":   {"date": {}},
            "資料更新日":   {"date": {}},
            "性質": {
                "select": {
                    "options": [
                        {"name": "股東常會",   "color": "blue"},
                        {"name": "股東臨時會", "color": "yellow"},
                        {"name": "其他",       "color": "gray"},
                    ]
                }
            },
            "已購買": {"checkbox": {}},
            "距截止天數": {
                "formula": {
                    "expression": 'dateBetween(prop("最後買進日"), now(), "days")'
                }
            },
        },
    )
    database_id = db["id"]
    print(f"✅ 資料庫建立完成！")
    print(f"   Database ID：{database_id}")
    return database_id


# ────────────────────────────────────────────────
# 4. 安全寫入 gx.env
# ────────────────────────────────────────────────
def upsert_env_var(key: str, value: str, env_path: str = "gx.env"):
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            found = True
        else:
            new_lines.append(line)
    if not found:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines.append("\n")
        new_lines.append(f"{key}={value}\n")
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    print(f"✅ 已將 {key} 寫入 gx.env")


# ────────────────────────────────────────────────
# 5. 輔助函式：日期 & 股價解析
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
# 6. 查詢現有資料庫中的股票代號
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
# 7. 建立單筆 Notion 屬性
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
        "紀念品類型": {"multi_select": [{"name": t} for t in gift_types]},
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
# 8. 單筆同步（含 retry）
# ────────────────────────────────────────────────
def sync_one(row: pd.Series, existing: dict, database_id: str, max_retry: int = 2) -> str:
    code = row["代號"]
    props = build_properties(row)
    for attempt in range(max_retry + 1):
        try:
            if code in existing:
                page_id = existing[code]
                current = notion.pages.retrieve(page_id)
                is_purchased = current["properties"]["已購買"]["checkbox"]
                props["已購買"] = {"checkbox": is_purchased}
                notion.pages.update(page_id=page_id, properties=props)
                return "updated"
            else:
                props["已購買"] = {"checkbox": False}
                notion.pages.create(
                    parent={"database_id": database_id},
                    properties=props,
                )
                return "created"
        except Exception as e:
            if attempt < max_retry:
                wait = 2 ** attempt
                print(f"  ⚠️  {code} 第 {attempt+1} 次失敗，{wait}s 後重試：{e}")
                time.sleep(wait)
            else:
                print(f"  ❌ {code} {row['名稱']} 最終失敗：{e}")
                return "failed"


# ────────────────────────────────────────────────
# 9. 同步資料到 Notion
# ────────────────────────────────────────────────
def sync_to_notion(df: pd.DataFrame, database_id: str):
    if df.empty:
        print("⚠️  無資料可同步")
        return
    total = len(df)
    print(f"🔄 正在同步 {total} 筆資料到 Notion...")
    existing = get_existing_codes(database_id)
    print(f"   資料庫現有 {len(existing)} 筆，開始逐筆同步...\n")
    created, updated, failed, parse_failed = 0, 0, 0, 0
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        if parse_date(row["最後買進日"]) is None and row["最後買進日"].strip():
            parse_failed += 1
        result = sync_one(row, existing, database_id)
        if result == "created":   created += 1
        elif result == "updated": updated += 1
        else:                     failed += 1
        if i % 20 == 0 or i == total:
            print(f"   進度：{i}/{total}  ✅新增 {created}  🔄更新 {updated}  ❌失敗 {failed}")
        time.sleep(0.7)
    print(f"\n📊 同步完成：新增 {created} 筆 ｜ 更新 {updated} 筆 ｜ 失敗 {failed} 筆")
    if parse_failed > 0:
        print(f"⚠️  有 {parse_failed} 筆「最後買進日」格式無法解析，日期欄位已略過")


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("─" * 60)
    print("🚀 股東紀念品 Notion 同步腳本 v1.4")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    # Step 1：爬取資料
    df = crawl_gifts()
    if df.empty:
        print("❌ 爬取失敗，程式結束")
        exit(1)

    # Step 2：確認 Database ID
    database_id = NOTION_DATABASE_ID

    if not database_id:
        # 沒有 ID → 互動建庫（本機首次執行）
        print("\n⚠️  未設定 NOTION_DATABASE_ID，進入建庫流程")
        print("請輸入你想放資料庫的 Notion 頁面 ID")
        print("（網址最後 32 碼，例：notion.so/yourname/【這段】?v=...）")
        parent_page_id = input("Parent Page ID：").strip().replace("-", "")
        if not parent_page_id:
            print("❌ 未輸入，程式結束")
            exit(1)
        database_id = create_notion_database(parent_page_id)
        upsert_env_var("NOTION_DATABASE_ID", database_id)
        print("   已寫入 gx.env，下次執行不需再輸入\n")
    else:
        # 有 ID → 先驗證是否為資料庫（Riley 建議）
        print(f"🔍 驗證 Database ID：{database_id}")
        if not validate_database_id(database_id):
            print("\n💡 請執行以下步驟：")
            print("   1. 打開 gx.env，把 NOTION_DATABASE_ID 那行刪除或清空")
            print("   2. 重新執行此腳本，會自動進入建庫流程")
            exit(1)
        print("✅ ID 驗證通過，開始同步\n")

    # Step 3：同步資料
    sync_to_notion(df, database_id)

    print("\n" + "─" * 60)
    print("✅ 執行完成！請前往 Notion 查看資料")
    print("─" * 60)

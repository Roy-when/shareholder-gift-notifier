"""
股東紀念品 → Notion 同步腳本 v1.1（工程師修正版）
=================================================
修正項目（by 工程師團隊）：
  [Casey]  sleep 從 0.35 → 0.7，避免 retrieve+update 連發觸發 Notion rate limit
  [Casey]  加入 parse_failed_count，日期解析失敗時統一回報，不再靜默跳過
  [Quinn]  gx.env 寫入改為「讀取後替換」，不覆蓋既有的 TOKEN 設定
  [Riley]  額外新增「紀念品類型」Multi-select，自動關鍵字分類（米/油/飲料/提貨券/其他）

使用前準備：
  pip install requests beautifulsoup4 pandas notion-client python-dotenv openpyxl

gx.env 需要加入（NOTION_DATABASE_ID 首次執行後自動寫入）：
  NOTION_TOKEN=secret_xxxxxxxxxx
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
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # 首次可留空，會自動建立

if not NOTION_TOKEN:
    raise ValueError(
        "gx.env 缺少 NOTION_TOKEN！\n"
        "請前往 https://www.notion.so/my-integrations 建立 Integration 後填入。"
    )

notion = Client(auth=NOTION_TOKEN)

# ────────────────────────────────────────────────
# 紀念品類型關鍵字對照表（Riley 建議）
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
    """根據關鍵字自動分類紀念品類型，回傳 list（可複選）"""
    matched = []
    for gift_type, keywords in GIFT_TYPE_KEYWORDS.items():
        if any(kw in gift_str for kw in keywords):
            matched.append(gift_type)
    return matched if matched else ["其他"]


# ────────────────────────────────────────────────
# 1. 爬取 HiStock 資料
# ────────────────────────────────────────────────
def crawl_gifts() -> pd.DataFrame:
    """爬取 HiStock 股東紀念品頁面"""
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
# 2. 自動建立 Notion 資料庫（首次執行）
# ────────────────────────────────────────────────
def create_notion_database(parent_page_id: str) -> str:
    """在指定頁面建立資料庫，回傳 database_id"""
    print("🏗️  正在建立 Notion 資料庫...")

    db = notion.databases.create(
        parent={"type": "page_id", "page_id": parent_page_id},
        title=[{"type": "text", "text": {"content": "📋 股東紀念品追蹤"}}],
        properties={
            "名稱（股票）": {"title": {}},
            "代號":         {"rich_text": {}},
            "開會地點":     {"rich_text": {}},
            "紀念品":       {"rich_text": {}},
            "紀念品類型":   {"multi_select": {}},   # Riley 建議：方便篩選
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
    print(f"✅ 資料庫建立完成！Database ID：{database_id}")
    return database_id


# ────────────────────────────────────────────────
# 3. 輔助函式：日期 & 股價解析
# ────────────────────────────────────────────────
def parse_date(date_str: str):
    """將 M/D 格式轉為 YYYY-MM-DD，自動推算年份"""
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
        return None  # 失敗回傳 None，由呼叫端計數


def parse_price(price_str: str):
    """將股價字串轉為浮點數"""
    cleaned = re.sub(r"[^\d.]", "", price_str.strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ────────────────────────────────────────────────
# 4. 安全寫入 gx.env（Quinn 修正：不覆蓋既有設定）
# ────────────────────────────────────────────────
def upsert_env_var(key: str, value: str, env_path: str = "gx.env"):
    """
    若 key 已存在於 env 檔則替換該行，否則 append。
    避免重複寫入或意外覆蓋 NOTION_TOKEN 等既有設定。
    """
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

    print(f"✅ 已將 {key} 寫入 {env_path}")


# ────────────────────────────────────────────────
# 5. 查詢現有資料庫中的股票代號（避免重複新增）
# ────────────────────────────────────────────────
def get_existing_codes(database_id: str) -> dict:
    """回傳 {代號: page_id} 字典"""
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
# 6. 建立單筆 Notion 屬性
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
# 7. 同步資料到 Notion
# ────────────────────────────────────────────────
def sync_to_notion(df: pd.DataFrame, database_id: str):
    if df.empty:
        print("⚠️  無資料可同步")
        return

    print(f"🔄 正在同步 {len(df)} 筆資料到 Notion...")
    existing = get_existing_codes(database_id)
    created, updated, failed = 0, 0, 0
    parse_failed_count = 0  # Casey：日期解析失敗計數器

    for _, row in df.iterrows():
        code = row["代號"]

        # Casey：檢查日期解析，失敗就計數但繼續執行不中斷
        if parse_date(row["最後買進日"]) is None and row["最後買進日"].strip():
            parse_failed_count += 1

        props = build_properties(row)
        try:
            if code in existing:
                # 更新：保留「已購買」手動勾選狀態
                page_id = existing[code]
                current = notion.pages.retrieve(page_id)
                is_purchased = current["properties"]["已購買"]["checkbox"]
                props["已購買"] = {"checkbox": is_purchased}
                notion.pages.update(page_id=page_id, properties=props)
                updated += 1
            else:
                # 新增
                props["已購買"] = {"checkbox": False}
                notion.pages.create(
                    parent={"database_id": database_id},
                    properties=props,
                )
                created += 1

            # Casey + Quinn 修正：retrieve+update 連發，0.7s 才能安全避開 rate limit
            time.sleep(0.7)

        except Exception as e:
            print(f"  ❌ {code} {row['名稱']} 失敗：{e}")
            failed += 1

    print(f"\n📊 同步結果：新增 {created} 筆 ｜ 更新 {updated} 筆 ｜ 失敗 {failed} 筆")

    # Casey：統一回報日期解析失敗（不再靜默跳過）
    if parse_failed_count > 0:
        print(f"⚠️  有 {parse_failed_count} 筆「最後買進日」格式無法解析，日期欄位已略過（其他資料仍正常同步）")


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("─" * 60)
    print("🚀 股東紀念品 Notion 同步腳本 v1.1")
    print(f"   執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60 + "\n")

    # Step 1：爬取資料
    df = crawl_gifts()
    if df.empty:
        print("❌ 爬取失敗，程式結束")
        exit(1)

    # Step 2：確認資料庫 ID（沒有就互動建立）
    database_id = NOTION_DATABASE_ID

    if not database_id:
        print("\n⚠️  尚未設定 NOTION_DATABASE_ID，將自動建立資料庫")
        print("請輸入你想放資料庫的 Notion 頁面 ID")
        print("（從頁面網址複製最後 32 碼，例：notion.so/yourname/【這段】?v=...）")
        parent_page_id = input("Parent Page ID：").strip().replace("-", "")
        if not parent_page_id:
            print("❌ 未輸入，程式結束")
            exit(1)

        database_id = create_notion_database(parent_page_id)

        # Quinn 修正：安全寫回，不覆蓋 NOTION_TOKEN
        upsert_env_var("NOTION_DATABASE_ID", database_id)
        print("   下次執行不需再輸入 Page ID\n")

    # Step 3：同步資料
    sync_to_notion(df, database_id)

    print("\n" + "─" * 60)
    print("✅ 執行完成！請前往 Notion 查看資料")
    print("─" * 60)

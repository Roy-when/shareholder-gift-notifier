import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# ────────────────────────────────────────────────
# 載入 gx.env（您的自訂環境變數檔案）
# ────────────────────────────────────────────────
load_dotenv(dotenv_path="gx.env")

URL = "https://histock.tw/stock/gift.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 從 gx.env 讀取所有變數
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")          # ← 群組推播目標
USER_ID = os.getenv("USER_ID")            # 若未來想同時推播個人，可保留使用

if not LINE_CHANNEL_ACCESS_TOKEN or not GROUP_ID:
    raise ValueError(
        "gx.env 缺少必要變數！請確認檔案內容包含：\n"
        "LINE_CHANNEL_ACCESS_TOKEN=您的長期 Token\n"
        "GROUP_ID=C您的群組ID\n"
        "(USER_ID 可選，若要同時推播個人可加入)"
    )


def crawl_gifts():
    """爬取 HiStock 並儲存 Excel"""
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"請求失敗：{e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    data = []

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue
            try:
                data.append({
                    "代號": cols[0].text.strip(),
                    "名稱": cols[1].text.strip(),
                    "股價": cols[2].text.strip(),
                    "最後買進日": cols[3].text.strip(),
                    "股東會日期": cols[4].text.strip(),
                    "性質": cols[5].text.strip(),
                    "開會地點": cols[6].text.strip(),
                    "紀念品": cols[7].text.strip(),
                })
            except:
                continue

    df = pd.DataFrame(data)
    df = df[df["代號"].str.strip() != ""]

    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"股東紀念品_{today_str}.xlsx"
    df.to_excel(filename, index=False, engine="openpyxl")
    print(f"已儲存：{filename} （{len(df)} 筆）")

    return df


def send_line_push(message: str):
    """推播到群組（使用 GROUP_ID）"""
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    payload = {
        "to": GROUP_ID,                    # ← 群組推播
        "messages": [{"type": "text", "text": message}]
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            print("✅ 群組通知發送成功")
        else:
            print(f"❌ 推播失敗（HTTP {r.status_code}）：{r.text}")
    except Exception as e:
        print(f"❌ 發送異常：{e}")


def check_and_notify(df: pd.DataFrame):
    """檢查最後買進日並通知（提前 5 天開始提醒）"""
    if df.empty:
        print("無資料可檢查")
        return

    today = datetime.now().date()
    notified = 0

    for _, row in df.iterrows():
        date_str = row["最後買進日"].strip()
        if not date_str or "/" not in date_str:
            continue

        try:
            m, d = map(int, date_str.split("/"))
            buy_date = datetime(today.year, m, d).date()
            days_left = (buy_date - today).days

            # 修改處：從 <=3 改為 <=5
            if 0 < days_left <= 5:
                msg = (
                    f"⚠️ 股東紀念品提醒（提前 5 天通知）\n"
                    f"股票：{row['名稱']} ({row['代號']})\n"
                    f"最後買進日：{date_str}（剩 {days_left} 天）\n"
                    f"紀念品：{row['紀念品']}\n"
                    f"股價參考：{row['股價']}\n"
                    f"詳情：https://histock.tw/stock/gift.aspx\n"
                    f"※ 提醒：最後買進日為 T+0，需考慮 T+2 交割，請提早準備。"
                )
                send_line_push(msg)
                notified += 1
                time.sleep(1.2)  # 避免短時間大量發送
        except:
            continue

    if notified == 0:
        print("今日無需通知項目（剩餘 5 天內）")
    else:
        print(f"已發送 {notified} 則群組通知")


# ────────────────────────────────────────────────
# 主程式
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"開始執行 ── {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60)

    df = crawl_gifts()
    check_and_notify(df)

    print("─" * 60)
    print("執行完成")

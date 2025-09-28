import os
import time
import json
from datetime import datetime
from typing import Dict

import httpx
from bs4 import BeautifulSoup

URL = "https://minsk.gov.by/ru/freepage/other/arendnoe_zhiljo/"

BOT_TOKEN = os.getenv("BOT_TOKEN", "7660220912:AAEcwSBMJM88jyJkeNLScLi6LV2_-stzADM")
CHAT_ID = os.getenv("CHAT_ID", "-1003097916199")
DATA_FILE = "state.json"
CHECK_INTERVAL = 120  # 2 минуты

def send_telegram(text: str) -> None:
    """Отправка сообщения в Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        print("[WARN] BOT_TOKEN/CHAT_ID не заданы")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = httpx.post(url, data=payload, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Telegram: {e}")

def fetch_html() -> str:
    r = httpx.get(URL, timeout=30)
    r.raise_for_status()
    return r.text

def parse_flats() -> Dict[str, str]:
    """Собираем список строк (каждая квартира как строка)."""
    html = fetch_html()
    soup = BeautifulSoup(html, "html.parser")
    flats: Dict[str, str] = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cols = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cols) < 2:
                continue
            row_text = " | ".join(cols)
            flats[row_text] = row_text
    return flats

def load_state() -> Dict[str, str]:
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_state(state: Dict[str, str]) -> None:
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def tick_once(first_run=False):
    old = load_state()
    new = parse_flats()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if first_run:
        count = len(new)
        msg = f"📊 На сайте сейчас доступно {count} квартир"
        print(f"[{ts}] Первичный запуск: {msg}")
        send_telegram(msg)
    else:
        if old != new:
            diff = len(new) - len(old)
            if diff > 0:
                msg = f"🔔📢 Добавлено {diff} новых квартир!"
            elif diff < 0:
                msg = f"🔔❌ Убрано {abs(diff)} квартир!"
            else:
                msg = "🔔✏️ Изменения в списке квартир!"
            print(f"[{ts}] {msg}")
            send_telegram(msg)
        else:
            print(f"[{ts}] Изменений нет")

    save_state(new)

def main_loop():
    print(f"Watcher запущен. Интервал: {CHECK_INTERVAL} сек")
    tick_once(first_run=True)
    while True:
        time.sleep(CHECK_INTERVAL)
        tick_once()

if __name__ == "__main__":
    main_loop()

import os
import time
import json
from datetime import datetime
from typing import Dict

import httpx
from bs4 import BeautifulSoup

URL = "https://minsk.gov.by/ru/freepage/other/arendnoe_zhiljo/"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
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
    """Загрузка HTML с сайта с защитой от ошибок."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.1 Safari/537.36"
        )
    }
    try:
        r = httpx.get(URL, headers=headers, timeout=30)
        r.raise_for_status()
        return r.text
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] HTTP ошибка {e.response.status_code}: {e}")
    except httpx.RequestError as e:
        print(f"[ERROR] Ошибка сети: {e}")
    except Exception as e:
        print(f"[ERROR] Неизвестная ошибка: {e}")
    return ""

def parse_flats() -> Dict[str, str]:
    """Собираем список строк (каждая квартира как строка)."""
    html = fetch_html()
    if not html:
        return {}
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

    if not new:
        print(f"[{ts}] Сайт недоступен, пробую позже")
        return

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

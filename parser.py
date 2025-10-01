import os
import time
import json
import re
import hashlib
import logging
import tempfile
from datetime import datetime
from typing import Dict, Optional
from collections import defaultdict

import httpx
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo   # для часового пояса

# --- Конфигурация ---
URL = "https://minsk.gov.by/ru/freepage/other/arendnoe_zhiljo/"
BOT_TOKEN = os.getenv("7660220912:AAEcwSBMJM88jyJkeNLScLi6LV2_-stzADM")
CHAT_ID = os.getenv("-1003097916199")
DATA_FILE = os.getenv("DATA_FILE", "/mnt/data/state.json")  # Railway Volume
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "240"))    # 4 минуты
WORK_HOURS = (8, 20)  # локальное время Минска
TIMEZONE = os.getenv("TZ", "Europe/Minsk")  # можно переопределить
MAX_MSG_LEN = 4000

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# --- HTTP заголовки ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MinskWatcher/1.0)",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://minsk.gov.by/"
}

# --- Время с учётом таймзоны ---
def now_local():
    return datetime.now(ZoneInfo(TIMEZONE))

# --- Telegram ---
def send_telegram(text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        logging.warning("BOT_TOKEN/CHAT_ID не заданы")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID}

    chunks = [text[i:i+MAX_MSG_LEN] for i in range(0, len(text), MAX_MSG_LEN)]
    for chunk in chunks:
        for attempt in range(3):
            try:
                r = httpx.post(url, data={**payload, "text": chunk}, timeout=20)
                r.raise_for_status()
                break
            except Exception as e:
                logging.error(f"Telegram attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)

# --- Загрузка HTML ---
def fetch_html() -> str:
    for attempt in range(3):
        try:
            r = httpx.get(URL, headers=HEADERS, timeout=(10, 20))
            r.raise_for_status()
            return r.text
        except Exception as e:
            logging.error(f"fetch_html attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return ""

# --- Вспомогательные функции ---
def extract_last_number(text: str) -> Optional[str]:
    if not text:
        return None
    nums = re.findall(r"\d+[.,]?\d*", text)
    return nums[-1] if nums else text

def flat_hash(flat: Dict[str, str]) -> str:
    return hashlib.md5(json.dumps(flat, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

# --- Парсинг ---
def parse_flats() -> Dict[str, Dict[str, str]]:
    html = fetch_html()
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    flats: Dict[str, Dict[str, str]] = {}

    for block in soup.select("div.inside"):
        district_tag = block.find_previous("a", class_="ofside")
        district = district_tag.get_text(strip=True) if district_tag else None

        last_update = None
        upd = block.find_next("div", class_="org-status")
        if upd:
            m = re.search(r"Дата обновления информации:\s*(\d{2}\.\d{2}\.\d{4})", upd.get_text())
            if m:
                last_update = m.group(1)

        article = None
        for p in block.find_all("p"):
            text = p.get_text()
            if "Жилищного кодекса" in text:
                m = re.search(r"стат(ья|ьи)\s+(\d+)", text)
                if m:
                    article = f"ст. {m.group(2)} ЖК РБ"
                    break

        for table in block.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) <= 1:
                continue
            headers = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["td", "th"])]

            for idx, tr in enumerate(rows[1:], start=1):
                cols = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(cols) < 2:
                    continue

                address = next((c for h, c in zip(headers, cols) if "адрес" in h), None)
                if not address:
                    continue

                rooms = next((c for h, c in zip(headers, cols) if "комн" in h), None)
                area_total = None
                area_living = None
                rent = next((c for h, c in zip(headers, cols) if "плата" in h), None)
                repair = next((c for h, c in zip(headers, cols) if "ремонт" in h), None)
                appl = next((c for h, c in zip(headers, cols) if "приём" in h or "заявлен" in h), None)

                for h, c in zip(headers, cols):
                    if "общая" in h:
                        area_total = c
                    elif "жилая" in h:
                        area_living = c

                rent = extract_last_number(rent)
                repair = extract_last_number(repair)

                if appl:
                    dates = re.findall(r"\d{2}\.\d{2}\.\d{4}", appl)
                    if len(dates) >= 2:
                        appl = f"{dates[0]} – {dates[1]}"
                    elif len(dates) == 1:
                        appl = dates[0]

                # ключ уникализируем индексом строки
                key = f"{(district or '').strip()}:{address.strip()}:{idx}"
                flat = {
                    "district": district,
                    "address": address,
                    "rooms": rooms,
                    "area_total": area_total,
                    "area_living": area_living,
                    "rent": rent,
                    "repair_cost": repair,
                    "application_period": appl,
                    "last_update": last_update,
                    "article": article,
                }
                flat["hash"] = flat_hash(flat)
                flats[key] = flat

    return flats

# --- Состояние ---
def load_state() -> Dict[str, Dict[str, str]]:
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, Dict[str, str]]) -> None:
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            json.dump(state, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_name = tmp.name
        os.replace(tmp_name, DATA_FILE)
    except Exception as e:
        logging.error(f"save_state: {e}")

# --- Основная логика ---
def tick_once(first_run=False):
    try:
        old = load_state()
        new = parse_flats()

        if not new:
            logging.warning("Не удалось получить данные, пропускаем итерацию")
            return

        if first_run:
            msg_lines = [f"📊 На сайте сейчас доступно {len(new)} квартир:"]
            grouped = defaultdict(list)
            for flat in new.values():
                grouped[flat.get("district") or "Неизвестный район"].append(flat)

            for district, flats in grouped.items():
                msg_lines.append(f"\n🏢 {district}")
                for flat in flats:
                    msg_lines.append(
                        f"🏠 {flat.get('address')}\n"
                        f"   {flat.get('rooms') or ''}, {flat.get('area_total') or ''} м²\n"
                        f"   Плата: {flat.get('rent') or '-'}\n"
                        f"   Ремонт: {flat.get('repair_cost') or '-'}\n"
                        f"   Приём заявлений: {flat.get('application_period') or '-'}\n"
                        f"   📖 {flat.get('article') or 'статья не указана'}\n"
                    )

            send_telegram("\n".join(msg_lines))
            logging.info("Первичный запуск: сообщение отправлено")

                else:
            added, removed, changed = [], [], []

            for k, v in new.items():
                if k not in old:
                    added.append(v)
                elif v["hash"] != old[k].get("hash"):
                    changed.append(v)

            for k, v in old.items():
                if k not in new:
                    removed.append(v)

            if added or removed or changed:
                msg_lines = []

                if added:
                    msg_lines.append(f"🔔📢 Добавлено {len(added)} квартир:")
                    for flat in added:
                        msg_lines.append(
                            f"🏠 {flat.get('address')} "
                            f"({flat.get('rooms') or ''}, {flat.get('area_total') or ''} м²)"
                        )

                if removed:
                    msg_lines.append(f"\n🔔❌ Убрано {len(removed)} квартир:")
                    for flat in removed:
                        msg_lines.append(
                            f"🏠 {flat.get('address')} "
                            f"({flat.get('rooms') or ''}, {flat.get('area_total') or ''} м²)"
                        )

                if changed:
                    msg_lines.append(f"\n🔔✏️ Изменено {len(changed)} квартир:")
                    for flat in changed:
                        msg_lines.append(
                            f"🏠 {flat.get('address')} "
                            f"({flat.get('rooms') or ''}, {flat.get('area_total') or ''} м²)"
                        )

                send_telegram("\n".join(msg_lines))
                logging.info(
                    f"Изменения: добавлено {len(added)}, "
                    f"убрано {len(removed)}, изменено {len(changed)}"
                )

        # сохраняем новое состояние
        save_state(new)

    except Exception as e:
        logging.exception(f"tick_once: неперехваченное исключение: {e}")


# --- Основной цикл ---
def main():
    first = True
    while True:
        now = now_local()
        if WORK_HOURS[0] <= now.hour < WORK_HOURS[1]:
            tick_once(first_run=first)
            first = False
        else:
            logging.info("Вне рабочего времени, проверка пропущена")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()


import asyncio
import os
import time
from datetime import datetime
from contextlib import asynccontextmanager
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
import uvicorn
import firebase_admin
from firebase_admin import credentials, messaging

# ==========================================
# НАСТРОЙКИ СЕРВЕРА
# ==========================================
ENABLE_TEST_PANEL = True
CHECK_INTERVAL_SECONDS = 4
FIREBASE_KEY_PATH = "firebase_credentials.json"

# По 1 надежному каналу на каждый регион (без дублирования и ошибки 302)
REGIONS = {
    "sevastopol": {
        "name": "Севастополь",
        "topic": "sevastopol_alerts",
        "channels": ["https://t.me/s/razvozhaev"],
        "is_alarm": False,
        "is_test": False,
        "status": "Ожидание...",
        "error": None,
        "last_check": "—",
    },
    "crimea": {
        "name": "Республика Крым",
        "topic": "crimea_alerts",
        "channels": ["https://t.me/s/Aksenov82"],
        "is_alarm": False,
        "is_test": False,
        "status": "Ожидание...",
        "error": None,
        "last_check": "—",
    },
    "kerch": {
        "name": "Керчь и Керченский мост",
        "topic": "kerch_alerts",
        "channels": [
            "https://t.me/s/most_official"
        ],
        "is_alarm": False,
        "is_test": False,
        "status": "Ожидание...",
        "error": None,
        "last_check": "—",
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8"
}

last_checked_msg_ids = {}

if not firebase_admin._apps:
    if os.path.exists(FIREBASE_KEY_PATH):
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
        firebase_admin.initialize_app(cred)
        print("[Firebase] Успешно инициализирован.")
    else:
        print(f"[ВНИМАНИЕ] Файл {FIREBASE_KEY_PATH} не найден! Push-уведомления только в консоли.")


def build_all_states_payload() -> dict:
    """Формирует полную картину всех 3 регионов для мгновенного переключения на телефоне."""
    return {
        "sev_alarm": "1" if REGIONS["sevastopol"]["is_alarm"] else "0",
        "sev_test": "1" if REGIONS["sevastopol"]["is_test"] else "0",
        "cri_alarm": "1" if REGIONS["crimea"]["is_alarm"] else "0",
        "cri_test": "1" if REGIONS["crimea"]["is_test"] else "0",
        "ker_alarm": "1" if REGIONS["kerch"]["is_alarm"] else "0",
        "ker_test": "1" if REGIONS["kerch"]["is_test"] else "0",
        "server_ts": str(int(time.time()))
    }


def send_push_notification(region_key: str, title: str, body: str, alert_type: str, is_test: bool = False):
    if region_key not in REGIONS:
        return

    region = REGIONS[region_key]
    if alert_type == "ALARM":
        region["is_alarm"] = True
        region["is_test"] = is_test
    else:
        region["is_alarm"] = False
        region["is_test"] = False

    topic = region["topic"]

    print(f"\n==========================================")
    print(f"[PUSH -> {region['name']}] Заголовок: {title} | Тип: {alert_type} | Тест: {is_test}")
    print(f"[PUSH] Текст: {body}")
    print(f"==========================================\n")

    if not firebase_admin._apps:
        return

    try:
        data_payload = {
            "title": title,
            "body": body,
            "type": alert_type,  # "ALARM" или "CANCEL"
            "region": region_key,
            "is_test": "1" if is_test else "0",
        }
        data_payload.update(build_all_states_payload())

        message = messaging.Message(
            data=data_payload,
            topic=topic,
            android=messaging.AndroidConfig(
                priority="high",
                ttl=0,
            ),
        )
        response = messaging.send(message)
        print(f"[Firebase] Доставлено в {topic}: {response}")
    except Exception as e:
        print(f"[Firebase] Ошибка отправки: {e}")


def send_heartbeat_sync():
    """Тихий фоновый сигнал всем клиентам, что сервер онлайн и какие регионы в тревоге."""
    if not firebase_admin._apps:
        return
    try:
        data_payload = {"type": "SYNC"}
        data_payload.update(build_all_states_payload())
        message = messaging.Message(
            data=data_payload,
            topic="sevastopol_alerts",
            android=messaging.AndroidConfig(priority="high", ttl=15),
        )
        messaging.send(message)
    except Exception:
        pass


def parse_alert_status(text: str, region_key: str):
    t = text.lower()

    if region_key == "kerch":
        if ("временно перекрыто" in t or "воздушная тревога" in t or "ракетная опасность" in t) and "восстановлено" not in t and "отбой" not in t:
            return "ALARM"
        elif ("движение" in t and "восстановлено" in t) or "отбой" in t:
            return "CANCEL"

    alarm_triggers = [
        "воздушная тревога",
        "ракетная опасность",
        "авиационная опасность",
        "угроза атаки бпла",
        "опасность по бпла"
    ]
    if any(k in t for k in alarm_triggers) and "отбой" not in t:
        return "ALARM"
    elif "отбой" in t and any(w in t for w in ["тревог", "воздушн", "опасност", "бпла"]):
        return "CANCEL"
    return None


async def check_single_channel(client: httpx.AsyncClient, region_key: str, channel_url: str):
    region = REGIONS[region_key]
    now_str = datetime.now().strftime("%H:%M:%S")
    channel_short = channel_url.split("/")[-1]

    try:
        # Если в ссылке нет /s/, пытаемся сначала через /s/, а при редиректе читаем карточку канала
        response = await client.get(channel_url, headers=HEADERS, timeout=10.0)
        if response.status_code != 200:
            region["status"] = "Ошибка"
            region["error"] = f"HTTP {response.status_code} ({channel_short})"
            region["last_check"] = now_str
            return

        soup = BeautifulSoup(response.text, "lxml")
        messages = soup.select(".tgme_widget_message_wrap")

        # Если у канала отключено веб-превью (/s/ редиректит на обычную страницу t.me/channel),
        # сервер все равно проверяет доступность канала без падения в ошибку 302
        if not messages:
            page_title = soup.select_one(".tgme_page_title")
            if page_title:
                region["status"] = "Активно"
                region["error"] = None
                region["last_check"] = now_str
                return
            else:
                region["status"] = "Ошибка"
                region["error"] = f"Канал недоступен ({channel_short})"
                region["last_check"] = now_str
                return

        region["status"] = "Активно"
        region["error"] = None
        region["last_check"] = now_str

        latest_wrap = messages[-1]
        msg_div = latest_wrap.select_one(".tgme_widget_message")
        if not msg_div:
            return

        post_data = msg_div.get("data-post", "")
        current_id = post_data.split("/")[-1] if "/" in post_data else None

        text_block = latest_wrap.select_one(".tgme_widget_message_text")
        text = text_block.get_text(separator="\n").strip() if text_block else ""

        if channel_url not in last_checked_msg_ids:
            last_checked_msg_ids[channel_url] = current_id
            print(f"[TG Парсер] Подключен {channel_url} (Последний ID: {current_id})")
            return

        if current_id and current_id != last_checked_msg_ids[channel_url]:
            last_checked_msg_ids[channel_url] = current_id
            region_name = region["name"]
            print(f"\n[TG Новый пост в {region_name} #{current_id}]:\n{text}")

            status = parse_alert_status(text, region_key)
            if status == "ALARM":
                send_push_notification(
                    region_key,
                    "ВНИМАНИЕ! ВОЗДУШНАЯ ТРЕВОГА!",
                    f"Немедленно пройдите в укрытие! ({region_name})",
                    "ALARM",
                    is_test=False
                )
            elif status == "CANCEL":
                send_push_notification(
                    region_key,
                    "Отбой воздушной тревоги",
                    f"Опасность миновала ({region_name}).",
                    "CANCEL",
                    is_test=False
                )

    except Exception as e:
        region["status"] = "Ошибка"
        region["error"] = f"Сбой сети ({channel_short})"
        region["last_check"] = now_str
        print(f"[TG Парсер] Ошибка соединения ({channel_url}): {e}")


async def tg_watcher_task():
    counter = 0
    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            tasks = []
            for region_key, region_data in REGIONS.items():
                for url in region_data["channels"]:
                    tasks.append(check_single_channel(client, region_key, url))
            await asyncio.gather(*tasks)

            counter += 1
            if counter % 3 == 0:
                await asyncio.to_thread(send_heartbeat_sync)

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(tg_watcher_task())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/api/status")
async def get_server_status(region: str = Query("sevastopol")):
    reg_data = REGIONS.get(region, REGIONS["sevastopol"])
    return {
        "server_online": True,
        "region": region,
        "is_alarm": reg_data["is_alarm"],
        "is_test": reg_data["is_test"],
        "all_regions": {
            k: {"is_alarm": v["is_alarm"], "is_test": v["is_test"]}
            for k, v in REGIONS.items()
        }
    }


@app.get("/api/all_status")
async def get_all_status():
    return {"server_online": True, "regions": REGIONS}


@app.get("/", response_class=HTMLResponse)
async def test_panel():
    if not ENABLE_TEST_PANEL:
        raise HTTPException(status_code=404, detail="Not Found")

    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>XAlert — Центр мониторинга</title>
        <style>
            * { box-sizing: border-box; }
            body {
                font-family: 'Segoe UI', sans-serif;
                margin: 0;
                padding: 32px;
                background-color: #0e0e10;
                color: #fff;
                display: flex;
                gap: 28px;
                align-items: flex-start;
                min-height: 100vh;
            }
            .left-panel {
                width: 350px;
                flex-shrink: 0;
                background-color: #18181c;
                padding: 24px;
                border-radius: 14px;
                border: 1px solid #2b2b33;
                box-shadow: 0 8px 30px rgba(0,0,0,0.5);
            }
            .right-panel {
                flex-grow: 1;
                display: flex;
                flex-direction: column;
                gap: 18px;
            }
            h2 { margin-top: 0; font-size: 20px; color: #ececf1; }
            label { display: block; font-size: 13px; color: #9e9ea8; margin-bottom: 6px; }
            select {
                width: 100%;
                padding: 12px;
                margin-bottom: 16px;
                border-radius: 8px;
                background: #25252c;
                color: #fff;
                border: 1px solid #3a3a44;
                font-size: 15px;
            }
            button {
                width: 100%;
                padding: 13px;
                margin: 6px 0;
                border: none;
                border-radius: 8px;
                font-size: 14px;
                font-weight: bold;
                cursor: pointer;
                transition: 0.2s;
            }
            .btn-alarm { background-color: #d32f2f; color: white; }
            .btn-alarm:hover { background-color: #b71c1c; }
            .btn-cancel { background-color: #388e3c; color: white; }
            .btn-cancel:hover { background-color: #2e7d32; }
            #action-status {
                margin-top: 14px;
                font-size: 13px;
                min-height: 20px;
                color: #90caf9;
                text-align: center;
            }
            .region-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                gap: 18px;
            }
            .region-card {
                background-color: #18181c;
                border: 1px solid #2b2b33;
                border-radius: 14px;
                padding: 20px;
                transition: 0.2s;
            }
            .region-card.alarm-border {
                border: 2px solid #ff1744;
                box-shadow: 0 0 25px rgba(255, 23, 68, 0.25);
            }
            .region-title {
                font-size: 18px;
                font-weight: bold;
                margin-bottom: 14px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 8px;
            }
            .row {
                display: flex;
                justify-content: space-between;
                margin: 8px 0;
                font-size: 14px;
                color: #b0b0b8;
            }
            .badge {
                padding: 4px 10px;
                border-radius: 6px;
                font-size: 12px;
                font-weight: bold;
                white-space: nowrap;
            }
            .badge-ok { background: rgba(76, 175, 80, 0.18); color: #66bb6a; }
            .badge-err { background: rgba(244, 67, 54, 0.2); color: #ef5350; }
            .badge-alarm { background: #ff1744; color: #fff; }
            .badge-calm { background: #2a2a32; color: #9e9ea8; }
            .channels-list {
                font-size: 12px;
                color: #787882;
                margin-top: 10px;
                word-break: break-all;
            }
            .error-box {
                margin-top: 10px;
                padding: 8px;
                border-radius: 6px;
                background: rgba(244, 67, 54, 0.12);
                color: #ff8a80;
                font-size: 12px;
            }
        </style>
    </head>
    <body>
        <div class="left-panel">
            <h2>Тестовая панель</h2>
            <label for="regionSelect">Выберите регион для теста:</label>
            <select id="regionSelect">
                <option value="sevastopol">Севастополь</option>
                <option value="crimea">Республика Крым</option>
                <option value="kerch">Керчь и Керченский мост</option>
            </select>
            <button class="btn-alarm" onclick="trigger('/api/test/alarm')">ВКЛЮЧИТЬ ТРЕВОГУ</button>
            <button class="btn-cancel" onclick="trigger('/api/test/cancel')">ОТБОЙ ТРЕВОГИ</button>
            <div id="action-status">Сервер работает штатно</div>
        </div>

        <div class="right-panel">
            <h2>Мониторинг каналов и состояния регионов</h2>
            <div class="region-grid" id="regionsContainer"></div>
        </div>

        <script>
            async function trigger(endpoint) {
                const region = document.getElementById('regionSelect').value;
                const status = document.getElementById('action-status');
                status.innerText = 'Отправка сигнала...';
                try {
                    const res = await fetch(endpoint + '?region=' + encodeURIComponent(region), { method: 'POST' });
                    const data = await res.json();
                    status.innerText = data.message;
                    loadStatus();
                } catch (e) {
                    status.innerText = 'Ошибка соединения с сервером';
                }
            }

            async function loadStatus() {
                try {
                    const res = await fetch('/api/all_status');
                    const data = await res.json();
                    const container = document.getElementById('regionsContainer');
                    container.innerHTML = '';

                    for (const [key, reg] of Object.entries(data.regions)) {
                        const isOnline = reg.status === 'Активно';
                        const hasError = reg.error !== null;
                        const isAlarm = reg.is_alarm;
                        const isTest = reg.is_test;

                        let alarmBadgeText = 'Спокойно';
                        if (isAlarm) {
                            alarmBadgeText = isTest ? 'ТРЕВОГА - ТЕСТ' : 'ТРЕВОГА';
                        }

                        const card = document.createElement('div');
                        card.className = 'region-card' + (isAlarm ? ' alarm-border' : '');

                        card.innerHTML = `
                            <div class="region-title">
                                <span>${reg.name}</span>
                                <span class="badge ${isAlarm ? 'badge-alarm' : 'badge-calm'}">
                                    ${alarmBadgeText}
                                </span>
                            </div>
                            <div class="row">
                                <span>Статус канала:</span>
                                <span class="badge ${isOnline ? 'badge-ok' : 'badge-err'}">${reg.status}</span>
                            </div>
                            <div class="row">
                                <span>Ошибки:</span>
                                <span>${hasError ? 'Есть ошибка' : 'Нет ошибок'}</span>
                            </div>
                            <div class="row">
                                <span>Последняя проверка:</span>
                                <span>${reg.last_check}</span>
                            </div>
                            <div class="channels-list">
                                Источник: ${reg.channels.join(', ')}
                            </div>
                            ${hasError ? `<div class="error-box">${reg.error}</div>` : ''}
                        `;
                        container.appendChild(card);
                    }
                } catch (e) {
                    console.error('Ошибка обновления статуса', e);
                }
            }

            loadStatus();
            setInterval(loadStatus, 2000);
        </script>
    </body>
    </html>
    """


@app.post("/api/test/alarm")
async def trigger_alarm(region: str = Query("sevastopol")):
    if not ENABLE_TEST_PANEL:
        raise HTTPException(status_code=403, detail="Test mode disabled")
    if region not in REGIONS:
        raise HTTPException(status_code=400, detail="Unknown region")
    reg_name = REGIONS[region]["name"]
    send_push_notification(
        region,
        "ВНИМАНИЕ! ВОЗДУШНАЯ ТРЕВОГА! (ТЕСТ)",
        f"Тестовая проверка системы оповещения ({reg_name}).",
        "ALARM",
        is_test=True
    )
    return {"status": "ok", "message": f"Тестовая ТРЕВОГА ({reg_name}) включена!"}


@app.post("/api/test/cancel")
async def trigger_cancel(region: str = Query("sevastopol")):
    if not ENABLE_TEST_PANEL:
        raise HTTPException(status_code=403, detail="Test mode disabled")
    if region not in REGIONS:
        raise HTTPException(status_code=400, detail="Unknown region")
    reg_name = REGIONS[region]["name"]
    send_push_notification(
        region,
        "Отбой воздушной тревоги (ТЕСТ)",
        f"Тестовая тревога завершена ({reg_name}).",
        "CANCEL",
        is_test=False
    )
    return {"status": "ok", "message": f"Сигнал ОТБОЯ ({reg_name}) отправлен!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
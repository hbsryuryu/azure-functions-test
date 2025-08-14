import logging
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import azure.functions as func
from dotenv import load_dotenv

load_dotenv(override=True)  # ローカル用。Azure ではアプリ設定を参照
TARGET_WEBAPP_URL = os.getenv("TARGET_WEBAPP_URL", "")

def main(myTimer: func.TimerRequest) -> None:  # ← function.json の name と一致させる
    try:
        if myTimer.past_due:
            logging.warning("Timer is past due!")

        # JST の実行時刻
        now = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")

        url = TARGET_WEBAPP_URL
        if not url:
            logging.error("TARGET_WEBAPP_URL is empty. Skip calling WebApp.")
            return

        payload = {"request_time": now}
        headers = {"Content-Type": "application/json"}

        # 10秒上限（接続5秒、応答10秒）
        res = requests.post(url, json=payload, headers=headers, timeout=(5, 10))
        res.raise_for_status()  # HTTPエラーを例外に
        logging.info("Called WebApp at %s status=%s body=%s", now, res.status_code, res.text[:300])

    except requests.exceptions.Timeout:
        logging.exception("Timeout while calling WebApp (<=10s).")
    except requests.exceptions.RequestException:
        logging.exception("HTTP error while calling WebApp.")
    except Exception as e:
        logging.exception("TimerCallWebApp failed: %s", e)
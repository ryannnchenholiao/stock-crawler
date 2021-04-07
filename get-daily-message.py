import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.rrule import rrule, DAILY
import time

from pymongo import MongoClient

from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

url = "https://mops.twse.com.tw/mops/web/ajax_t05st02"


def get_response(date):

    year = date.year - 1911
    month = date.month
    day = date.day
    print(f"date: {year}/{month}/{day}")

    data = {
        "encodeURIComponent": 1,
        "step": 1,
        "step00": 0,
        "firstin": 1,
        "off": 1,
        "TYPEK": "all",
        "year": year,
        "month": month,
        "day": day,
    }

    retry_time = 0
    while retry_time < 10:
        session = requests.Session()
        retry = Retry(total=10, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=5)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        res = session.post(url, data=data)

        if "overrun" not in res.text.lower():
            return res

        print("response text contains 'overrun', sleep 10 s")
        time.sleep(10)
        retry_time += 1

    raise Exception("failed after crawler retry 10 time")


def get_company_messages(date):
    res = get_response(date)

    soup = BeautifulSoup(res.text, "html.parser")

    company_msg = []
    for tr in soup.find("form", {"action": "/mops/web/ajax_t05st02"}).find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            row = [td.text.strip().replace("\r\n", "") for td in tds]

            date_str, time, code, name, title, _ = row

            date = datetime.strptime(
                str(int(date_str[:3]) + 1911) + date_str[3:], "%Y/%m/%d"
            )

            company_msg.append(
                {
                    "date": date,
                    "time": time,
                    "company_code": code,
                    "company_name": name,
                    "title": title,
                }
            )

    return company_msg


latest_msg = db.company_daily_messages.find_one({}, sort=[("date", -1)])
latest_date = latest_msg["date"]

today = datetime.now()

dates = [
    dt
    for dt in rrule(
        DAILY,
        dtstart=latest_date,
        until=today,
    )
]

for date in dates:
    company_msg = get_company_messages(date)

    for msg in company_msg:
        db.company_daily_messages.update_one(
            {
                "date": msg["date"],
                "time": msg["time"],
                "company_code": msg["company_code"],
            },
            {"$setOnInsert": msg},
            upsert=True,
        )

    time.sleep(1)

mongo_client.close()

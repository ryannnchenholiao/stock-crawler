import os
import requests
import json
import time
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, WEEKLY
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

if SINCE_DATE:
    since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")
else:
    latest_msg = db.company_daily_messages.find_one({}, sort=[("date", -1)])
    if latest_msg:
        since_date = latest_msg["date"]
    else:
        since_date = datetime.now()

if UNTIL_DATE:
    until_date = datetime.strptime(UNTIL_DATE, "%Y-%m-%d")
else:
    until_date = datetime.now()

if until_date > datetime.now():
    until_date = datetime.now()

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, 'WANTGOO_MEMBER_TOKEN is missing')

print(f"since_date: {since_date}")
print(f"until_date: {until_date}")


cookie = f"member_token={WANTGOO_MEMBER_TOKEN}"


def crawl_stock_date_chips(company_code, since_date, until_date):
    since = since_date.strftime("%Y/%m/%d")
    until = until_date.strftime("%Y/%m/%d")

    url = (
        f"https://www.wantgoo.com/stock/{company_code}/major-investors/branch-buysell-data?"
        "isOverBuy=true&"
        f"endDate={until}&"
        f"beginDate={since}"
    )

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=5)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    res = session.get(
        url,
        headers={
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            "(KHTML, likeGecko) Chrome/90.0.1234.56 Safari/537.36",
            "cookie": cookie,
        },
    )

    res = json.loads(res.text)

    return {
        "sinceDate": since_date,
        "untilDate": until_date,
        "data": res["data"],
    }


def get_stock_chips(company_code, since_date, until_date):
    mondays = [
        dt
        for dt in rrule(
            WEEKLY,
            dtstart=since_date,
            until=until_date,
        )
    ]

    for since in mondays:
        print(f"date: {since}")
        until = since + timedelta(days=5)

        if until > datetime.now():
            print(f'until date {until} is greater than today, skip')
            break

        chips = crawl_stock_date_chips(company_code, since, until)

        db[company_code].update_one(
            {
                "sinceDate": chips["sinceDate"],
                "untilDate": chips["untilDate"],
            },
            {"$setOnInsert": chips},
            upsert=True,
        )

        time.sleep(3)


company_code = "4930"

company_msg = get_stock_chips(company_code, since_date, until_date)

mongo_client.close()

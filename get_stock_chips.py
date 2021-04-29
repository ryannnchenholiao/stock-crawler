import os
import requests
import json
import time
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, WEEKLY, MO
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")
COMPANY_CODES = os.environ.get("COMPANY_CODES")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()
since_date = None

if SINCE_DATE:
    since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")

if UNTIL_DATE:
    until_date = datetime.strptime(UNTIL_DATE, "%Y-%m-%d")
    if until_date > datetime.now():
        until_date = datetime.now()
else:
    until_date = datetime.now()

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, "WANTGOO_MEMBER_TOKEN is missing")

cookie = f"member_token={WANTGOO_MEMBER_TOKEN}"

if not COMPANY_CODES:
    raise ValueError(COMPANY_CODES, "COMPANY_CODES is missing")

company_codes = COMPANY_CODES.split(",")


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
            byweekday=[MO],
        )
    ]

    for since in mondays:
        until = since + timedelta(days=4)
        print(f"date range: {since} ~ {until}")

        if until.date() >= datetime.now().date():
            print(f"until date {until} is gte today, skip")
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


for company_code in company_codes:
    if not since_date:
        latest_data = db[company_code].find_one({}, sort=[("sinceDate", -1)])

        if latest_data:
            since_date = latest_data["sinceDate"]
        else:
            raise ValueError(since_date, "can not get SINCE_DATE")

    print(f"since_date: {since_date}")
    print(f"until_date: {until_date}")
    print(f"company_code: {company_code}")

    get_stock_chips(company_code, since_date, until_date)

mongo_client.close()

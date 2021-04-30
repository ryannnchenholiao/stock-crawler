import os
import requests
import json
import time
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
MONGO_DB = os.environ.get("MONGO_DB")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")

# mongo settings
mongo_client = MongoClient(MONGO_URL)
if not MONGO_DB:
    db = mongo_client.get_default_database()
else:
    db = mongo_client['MONGO_DB']

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, 'WANTGOO_MEMBER_TOKEN is missing')

# session keep alive and retry automatically
session = requests.Session()
adapter = HTTPAdapter(max_retries=5)
session.mount("http://", adapter)
session.mount("https://", adapter)
cookie = f"member_token={WANTGOO_MEMBER_TOKEN}"


def get_date_range():

    SINCE_DATE = os.environ.get("SINCE_DATE")
    UNTIL_DATE = os.environ.get("UNTIL_DATE")

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")
    else:
        latest_msg = db.company_daily_messages.find_one({}, sort=[("date", -1)])
        if latest_msg:
            since_date = latest_msg["date"]
        else:  # from the first weekday of this week
            today = datetime.datetime.now()
            since_date = today + datetime.timedelta(days=-today.weekday())

    if UNTIL_DATE:
        until_date = datetime.strptime(UNTIL_DATE, "%Y-%m-%d")
    else:
        until_date = datetime.now()

    if until_date > datetime.now():
        until_date = datetime.now()

    return since_date, until_date


def crawl_stock_date_chips(company_code, since_date, until_date):

    since = since_date.strftime("%Y/%m/%d")
    until = until_date.strftime("%Y/%m/%d")

    url = (
        f"https://www.wantgoo.com/stock/{company_code}/major-investors/branch-buysell-data?"
        "isOverBuy=true&"
        f"endDate={until}&"
        f"beginDate={since}"
    )

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


def get_stock_chips(company_code, since_date, until_date, date_interval=5):

    dates = [dt for dt in rrule(DAILY, dtstart=since_date, until=until_date)]
    dates = dates[0:: date_interval + 1]

    for date in dates:

        since = date
        until = date + timedelta(days=date_interval)

        if since == until and since.weekday() in [5, 6]:
            print(f'since date and until date {since} are weekend, skip')
            continue
        if until > datetime.now():
            print(f'until date {until} is greater than today, skip')
            break
        elif until >= until_date:
            until = until_date

        print(f"Now processing date: {since} to {until}")

        chips = crawl_stock_date_chips(company_code, since, until)

        db[company_code].update_one(
            {
                "sinceDate": chips["sinceDate"],
                "untilDate": chips["untilDate"],
            },
            {
                "$setOnInsert": chips
            },
            upsert=True,
        )
        time.sleep(3)

    return


def main():

    company_code = "4930"

    since_date, until_date = get_date_range()

    get_stock_chips(company_code, since_date, until_date)


if __name__ == '__main__':
    main()


from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv
import json
import os
import pandas as pd
from pymongo import MongoClient
import random
import requests
import time

load_dotenv()
SAVE_MONGO = os.environ.get('SAVE_MONGO') == 'true'

# mongo settings
if SAVE_MONGO:
    MONGO_URL = os.environ.get('MONGO_URL')
    MONGO_DB = os.environ.get('MONGO_DB')

    mongo_client = MongoClient(MONGO_URL)
    if not MONGO_DB:
        db = mongo_client.get_default_database()
    else:
        db = mongo_client['MONGO_DB']

WANTGOO_MEMBER_TOKEN = os.environ.get('WANTGOO_MEMBER_TOKEN')
if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, 'WANTGOO_MEMBER_TOKEN is missing')

# session keep alive and retry automatically
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=5)
session.mount('http://', adapter)
session.mount('https://', adapter)
cookie = f'member_token={WANTGOO_MEMBER_TOKEN}'


def random_num():
    return random.randint(0, 2) + random.random()


def get_date_range():

    SINCE_DATE = os.environ.get('SINCE_DATE')
    UNTIL_DATE = os.environ.get('UNTIL_DATE')
    DATE_INTERVAL = os.environ.get('DATE_INTERVAL')

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, '%Y-%m-%d')
    else:
        latest_msg = db.company_daily_messages.find_one({}, sort=[('date', -1)])
        if latest_msg:
            since_date = latest_msg['date']
        else:  # from the first weekday of this week
            today = datetime.datetime.now()
            since_date = today + datetime.timedelta(days=-today.weekday())

    if UNTIL_DATE:
        until_date = datetime.strptime(UNTIL_DATE, '%Y-%m-%d')
    else:
        until_date = datetime.now()

    # check invalid
    if until_date > datetime.now():
        until_date = datetime.now()

    if not DATE_INTERVAL:
        date_interval = 7
    else:
        date_interval = int(DATE_INTERVAL)
        if date_interval < 1:
            raise ValueError(date_interval, 'Invalid date_interval')

    return since_date, until_date, date_interval


def crawl_stock_date_chips(company_code, since_date, until_date):

    since = since_date.strftime('%Y/%m/%d')
    until = until_date.strftime('%Y/%m/%d')

    url = (
        f'https://www.wantgoo.com/stock/{company_code}/major-investors/branch-buysell-data?'
        'isOverBuy=true&'
        f'endDate={until}&'
        f'beginDate={since}'
    )

    res = session.get(
        url,
        headers={
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            '(KHTML, likeGecko) Chrome/90.0.1234.56 Safari/537.36',
            'cookie': cookie,
        },
    )

    res = json.loads(res.text)

    return {
        'sinceDate': since_date,
        'untilDate': until_date,
        'data': res['data'],
    }


def get_stock_chips(company_code, since_date, until_date, date_interval):

    dates = [dt for dt in rrule(DAILY, dtstart=since_date, until=until_date)]
    dates = dates[0:: date_interval]

    if not SAVE_MONGO:
        all_chips = []

    for date in dates:

        since = date
        until = date + timedelta(days=date_interval - 1)

        if since == until and since.weekday() in [5, 6]:
            print(f'since date and until date {since} are weekend, skip')
            continue
        if until > datetime.now():
            print(f'until date {until} is greater than today, skip')
            break
        elif until >= until_date:
            until = until_date

        print(f'Now processing date: {since} to {until}')

        chips = crawl_stock_date_chips(company_code, since, until)

        if SAVE_MONGO:
            db[company_code].update_one(
                {
                    'sinceDate': chips['sinceDate'],
                    'untilDate': chips['untilDate'],
                },
                {
                    '$setOnInsert': chips
                },
                upsert=True,
            )
        else:
            all_chips.append(chips)

        time.sleep(random_num())

    if not SAVE_MONGO:

        all_data = defaultdict(list)
        for data_info in all_chips:
            tmp_since_date = data_info['sinceDate'].strftime('%Y-%m-%d')
            tmp_until_date = data_info['untilDate'].strftime('%Y-%m-%d')
            for datum in data_info['data']:
                all_data['agentId'].append(datum['agentId'])
                all_data['agentName'].append(datum['agentName'])
                all_data['buyQuantities'].append(datum['buyQuantities'])
                all_data['sellQuantities'].append(datum['sellQuantities'])
                all_data['buyPriceAvg'].append(datum['buyPriceAvg'])
                all_data['sellPriceAvg'].append(datum['sellPriceAvg'])
                all_data['sinceDate'].append(tmp_since_date)
                all_data['untilDate'].append(tmp_until_date)

        df = pd.DataFrame.from_dict(all_data)
        output_file_name = './data/crawled-chips/{0}_{1}_{2}_chips.csv'.format(
            company_code,
            since_date.strftime('%Y%m%d'),
            until_date.strftime('%Y%m%d')
        )
        df.to_csv(output_file_name)

    return


def main():

    company_code = '4930'

    since_date, until_date, date_interval = get_date_range()
    print(f'Init since date: {since_date}')
    print(f'Init until_date: {until_date}')

    get_stock_chips(company_code, since_date, until_date, date_interval)


if __name__ == '__main__':
    main()

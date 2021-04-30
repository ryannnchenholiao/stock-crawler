import os
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
COMPANY_CODE = os.environ.get("COMPANY_CODE")
WEEKLY_THRESHOLD = os.environ.get("WEEKLY_THRESHOLD", "10")
TOTAL_THRESHOLD = os.environ.get("TOTAL_THRESHOLD", "100")

if not COMPANY_CODE:
    raise ValueError(COMPANY_CODE, "COMPANY_CODE is missing")

company_code = COMPANY_CODE
weekly_big_trader_threshold = int(WEEKLY_THRESHOLD)
total_big_trader_threshold = int(TOTAL_THRESHOLD)

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

weekly_trades = list(db[company_code].find({}))

if not weekly_trades:
    raise ValueError(
        weekly_trades, f"company code {company_code} may not be crawled yet"
    )

weekly_trades_stats = []

for weekly_trade in weekly_trades:
    count_weekly_traders = map(
        lambda trade: {
            **trade,
            "quantitiesCount": trade["buyQuantities"] - trade["sellQuantities"],
        },
        weekly_trade["data"],
    )
    big_traders = list(
        filter(
            lambda trade: abs(trade["quantitiesCount"]) >= weekly_big_trader_threshold,
            count_weekly_traders,
        )
    )

    weekly_trades_stats.append(
        {trader["agentName"]: trader["quantitiesCount"] for trader in big_traders}
    )

weekly_trades_df = pd.DataFrame(
    weekly_trades_stats, index=map(lambda trade: trade["sinceDate"], weekly_trades)
)

# fill NaN to 0 and do cumsum
cusum_trades_df = weekly_trades_df.fillna(0).cumsum()

# filter columns according to last row value
big_trader_filter = cusum_trades_df[-1:].squeeze() > total_big_trader_threshold

big_trades_df = cusum_trades_df.loc[:, big_trader_filter]


# sort columns according to last row value
big_trades_df = big_trades_df.sort_values(
    big_trades_df.last_valid_index(), axis="columns", ascending=False
)

print(big_trades_df.to_markdown())

# stock-crawler #
Taiwan stock crawler

## Usage

* Crawl chips of specific company within given a date span
    1. Modify/Create `.env` as the sample (`sample.env`)
        * If you don't have `WANTGOO_MEMBER_TOKEN`, here are somethings you need to do
            1. Sign up and sign in to the https://www.wantgoo.com/ 
            2. Turn on your browser console and go to: https://www.wantgoo.com/stock/2002/major-investors/broker-buysell
            3. Find a html-type-request whose document name is `broker-buysell`
            4. Find your `WANTGOO_MEMBER_TOKEN` in the header/cookie
    2. Exec `get_stock_chips.py`


## Supported Data
1. `/data/台灣證交所證券辨識號碼一覽表.xlsx`
    - Last updated at: 2021.5.1
    - Src: https://www.twse.com.tw/zh/page/products/stock-code2.html

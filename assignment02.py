from yahoo import read_symbols, YAHOO_HTMLS
from urllib import request, parse
from tqdm import tqdm
import sys

def scrape_descriptions_sync():
    """DZ Scrape companies descriptions. sync"""
    # TODO: Second DZ с помощь urllib
    # прочитать Symbols, for symbol in tqdm(symbols)
    # исользовать urllib get запросы на yahoo и полученное записывать в файл с помощью
    # добавить tqdm(symbols)

    myheader = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
    }

    symbols = read_symbols()
    YAHOO_HTMLS.mkdir(parents=True, exist_ok=True)


    for symbol in tqdm(symbols):
        #Examplt myurl = "https://finance.yahoo.com/quote/AAPL/profile?p=AAPL"
        myurl = f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}'

        try:
            req = request.Request(myurl, headers=myheader)
            response = request.urlopen(req)
            text = response.readlines()

        except Exception:
            print("Error occuried during web request!!")
            print(sys.exc_info()[1])

        f = open(YAHOO_HTMLS / f'{symbol}.html', 'wb')
        for line in text:
            f.write(line)

def main():
    scrape_descriptions_sync()

if __name__ == '__main__':
    main()

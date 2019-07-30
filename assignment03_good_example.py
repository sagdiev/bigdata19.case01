"""
Assignment 03
=============
The goal of this assignment is to start working on individual project.
You need to find data source, and scrape it to Parquet file.
It is recommended to scrape data asynchronously, in batches.
Run this code with
    > fab run "assignment03:scrape_data()"
"""

import aiohttp
import asyncio
from contextlib import closing
import lxml.html
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import config as cfg
from yahoo import read_symbols, YAHOO_HTMLS


DATA_FILE = cfg.BUILDDIR / 'data.parquet'


def scrape_data(dst=DATA_FILE, compression='BROTLI'):
    """Scrape custom data."""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
        }
    symbols = read_symbols()
    columns = ('symbol', 'sector', 'industry', 'employees', 'description')
    schema = pa.schema([(col, pa.string()) for col in columns])

    def parse(text):
        tree = lxml.html.fromstring(text)
        row = {}
        row['description'] = '\n'.join(tree.xpath('//section[h2//*[text()="Description"]]/p/text()'))
        info = (tree.xpath('//div[@class="asset-profile-container"]//p[span[text()="Sector"]]') or [None])[0]
        if info is not None:
            row['sector'] = (info.xpath('./span[text()="Sector"]/following-sibling::span[1]/text()') or [''])[0]
            row['industry'] = (info.xpath('./span[text()="Industry"]/following-sibling::span[1]/text()') or [''])[0]
            row['employees'] = (info.xpath('./span[text()="Full Time Employees"]/following-sibling::span[1]/span/text()') or [''])[0].replace(',', '')
        return row

    async def fetch(symbol, session, progress):
        async with session.get(f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}') as response:
            text = await response.read()
            row = {'symbol': symbol}
            row.update(parse(text))
            progress.update()
            return row

    async def run(symbols, writer, progress, batch_size=1000):
        async with aiohttp.ClientSession(headers=headers) as session:
            start, stop = 0, batch_size
            while start < len(symbols):
                tasks = (asyncio.ensure_future(fetch(symbol, session, progress)) for symbol in symbols[start:stop])
                rows = await asyncio.gather(*tasks)
                table = pa.Table.from_arrays([pa.array(row.get(col, '') for row in rows) for col in columns], schema=schema)
                writer.write_table(table)
                start, stop = stop, stop + batch_size

    with tqdm(total=len(symbols)) as progress:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
        with closing(pq.ParquetWriter(dst, schema, use_dictionary=False, compression=compression, flavor={'spark'})) as writer:
            loop.run_until_complete(asyncio.ensure_future(run(symbols, writer, progress)))

import aiofiles
from aiohttp import ClientSession
import asyncio
from collections import defaultdict
from contextlib import closing
import csv
import io
import lxml.html
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import re
import sys
import tarfile
from tqdm import tqdm

import config as cfg


PROJECT_ARCH = cfg.BUILDDIR / 'project_main_html.tbz2'
PROJECT_DATA = cfg.BUILDDIR / 'project_main.csv'
PROJECT_HTMLS = cfg.BUILDDIR / 'project_main_html'
PROJECT_PARQUET = cfg.BUILDDIR / 'project_main.parquet'

PROJECT_LIST_FILES = (
    cfg.DATADIR / 'project_main' / 'forum_list.csv',
    )


def read_symbols():
    """Read symbols from NASDAQ dataset"""

    symbols = set()

    for filename in PROJECT_LIST_FILES:
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbols.add(row['Symbol'].upper().strip())

    return list(sorted(symbols))


def scrape_descriptions_async():
    """Scrape companies descriptions asynchronously."""

    symbols = read_symbols()
    progress = tqdm(total=len(symbols), file=sys.stdout, disable=False)
    PROJECT_HTMLS.mkdir(parents=True, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        }

    async def fetch(symbol, session):
        async with session.get(f'https://forum.bits.media/index.php?/topic/{symbol}/') as response:
            text = await response.read()
            async with aiofiles.open(PROJECT_HTMLS / f'{symbol}.html', 'wb') as f:
                await f.write(text)
            progress.update(1)

    async def run(symbols):
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(fetch(symbol, session)) for symbol in symbols)
            await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
    loop.run_until_complete(asyncio.ensure_future(run(symbols)))
    progress.close()


def compress_descriptions(encoding='utf-8', batch_size=1000, compression='BROTLI'):
    """Convert tarfile to parquet"""

    names = ('symbol', 'html')

    def read_incremental():
        """Incremental generator of batches"""
        with tarfile.open(PROJECT_ARCH) as archive:
            batch = defaultdict(list)
            for member in tqdm(archive):
                if member.isfile() and member.name.endswith('.html'):
                    batch['symbol'].append(Path(member.name).stem)
                    batch['html'].append(archive.extractfile(member).read().decode(encoding))
                    if len(batch['symbol']) >= batch_size:
                        yield pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)
                        batch = defaultdict(list)
            if batch:
                yield pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)  # last partial batch

    writer = None
    for batch in read_incremental():
        if writer is None:
            writer = pq.ParquetWriter(PROJECT_PARQUET, batch.schema, use_dictionary=False, compression=compression, flavor={'spark'})
        writer.write_table(batch)
    writer.close()


def decompress_descriptions(encoding='utf-8'):
    """Convert parquet to tarfile"""

    pf = pq.ParquetFile(PROJECT_PARQUET)

    progress = tqdm(file=sys.stdout, disable=False)

    with tarfile.open(PROJECT_ARCH, 'w:bz2') as archive:
        for i in range(pf.metadata.num_row_groups):
            table = pf.read_row_group(i)
            columns = table.to_pydict()
            for symbol, html in zip(columns['symbol'], columns['html']):
                bytes = html.encode(encoding)
                s = io.BytesIO(bytes)
                tarinfo = tarfile.TarInfo(name=f'yahoo/{symbol}.html')
                tarinfo.size = len(bytes)
                archive.addfile(tarinfo=tarinfo, fileobj=s)
                progress.update(1)

    progress.close()


def scrape_data(dst=PROJECT_PARQUET, compression='BROTLI'):
    """Scrape custom data."""
    #TODO Написать для моего проекта

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
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
        async with session.get(f'https://forum.bits.media/index.php?/topic/{symbol}/') as response:
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


def parse_descriptions(src=PROJECT_PARQUET, dst=PROJECT_DATA):
    """Parse scraped pages."""

    reader = pq.ParquetFile(src)

    with tqdm(total=reader.metadata.num_rows) as progress:

        with open(dst, "w") as f:
            writer = csv.DictWriter(f, fieldnames=['symbol', 'page_number', 'comment_id', 'comment_date', 'comment_text'])
            writer.writeheader()
            for g in range(reader.metadata.num_row_groups):
                table = reader.read_row_group(g).to_pydict()
                for symbol, html in zip(table['symbol'],table['html']):

                    tree = lxml.html.fromstring(html)
                    row = {'symbol':symbol.strip()}
                    row['page_number'] = 1      #todo add page_number functional

                    infos = (tree.xpath('//article') or [None])

                    for info in infos:
                        if info is not None:
                            row['comment_id'] = (info.xpath('./@id') or [''])[0]
                            row['comment_date'] = (info.xpath('.//time/@datetime') or [''])[0]
                            row['comment_text'] = text((info.xpath('.//div[@data-role="commentContent"]') or [''])[0])
                            writer.writerow(row)


                    #row['text'] = '\n'.join(tree.xpath('//section[h2//*[text()="Description"]]/p/text()'))
                    #info = (tree.xpath('//div[@class="asset-profile-container"]//p[span[text()="Sector"]]') or [None])[0]
                    #if info is not None:
                    #   row['sector'] = (info.xpath('./span[text()="Sector"]/following-sibling::span[1]/text()') or [''])[0]
                    #    row['industry'] = (info.xpath('./span[text()="Industry"]/following-sibling::span[1]/text()') or [''])[0]
                    #    row['employees'] = (info.xpath('./span[text()="Full Time Employees"]/following-sibling::span[1]/span/text()') or [''])[0].replace(',', '')

                    progress.update()

def text(node, separator=' '):
    """Convert node to text"""
    if node is None:
        return ''
    elif isinstance(node, str):
        return node.strip()
    elif isinstance(node, list):
        return separator.join(text(t) for t in node)
    else:
        return re.sub('\s+', ' ',
            separator.join(
                [text(getattr(node, 'text', ''))]
                + [text(c) for c in node.getchildren()]
                + [text(getattr(node, 'tail', ''))]
                )).strip()

def main():
    #parse_descriptions()
    #scrape_descriptions_async()
    #compress_descriptions()
    parse_descriptions()


if __name__ == '__main__':
    main()

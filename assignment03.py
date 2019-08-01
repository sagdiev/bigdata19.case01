"""
Assignment 03
=============

The goal of this assignment is to start working on individual project.
You need to find data source, and scrape it to Parquet file.
It is recommended to scrape data asynchronously, in batches.

Run this code with

    > fab run assignment03:scrape_data()

import config as cfg

DATA_FILE = cfg.BUILDDIR / 'data.parquet'


def scrape_data():
    "Scrape custom data."

"""

import aiofiles
from aiohttp import ClientSession
import asyncio
from collections import defaultdict
import csv
import io
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import tarfile
from tqdm import tqdm
import config as cfg

PROJECT_ARCH = cfg.BUILDDIR / 'project02.tbz2'
PROJECT_HTMLS = cfg.BUILDDIR / 'project02_html'
PROJECT_PARQUET = cfg.BUILDDIR / 'data.parquet'
PROJECT_PARQUET_FILE = cfg.BUILDDIR / 'project02.parquet'

PROJECT_LIST_FILES = (
    cfg.DATADIR / 'project02' / 'forum_list.csv',
    )


def scrape_data():
    """Scrape custom data."""
    encoding = 'utf-8'
    compression = 'BROTLI'
    names = ('symbol', 'html')

    symbols = read_symbols()
    progress = tqdm(total=len(symbols), file=sys.stdout, disable=False)
    PROJECT_HTMLS.mkdir(parents=True, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        }

    def read_batch():
        batch = defaultdict(list)

        async def fetch(symbol, session, batch):
            async with session.get(f'https://forum.bits.media/index.php?/topic/{symbol}/') as response:
                text = await response.read()
                batch['symbol'].append(symbol)
                batch['html'].append(text.decode(encoding))
                progress.update(1)


        async def run(symbols):

            async with ClientSession(headers=headers) as session:
                tasks = (asyncio.ensure_future(fetch(symbol, session, batch)) for symbol in symbols)
                await asyncio.gather(*tasks)


        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
        loop.run_until_complete(asyncio.ensure_future(run(symbols)))
        progress.close()

        yield pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)


    writer = None
    for batch in read_batch():
        if writer is None:
            writer = pq.ParquetWriter(PROJECT_PARQUET, batch.schema, use_dictionary=False, compression=compression,
                                      flavor={'spark'})
        writer.write_table(batch)
    writer.close()


def read_symbols():
    """Read symbols from FORUM Lists dataset"""

    symbols = set()

    for filename in PROJECT_LIST_FILES:
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbols.add(row['topic_id'].upper().strip())

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

    pf = pq.ParquetFile(PROJECT_PARQUET_FILE)

    progress = tqdm(file=sys.stdout, disable=False)

    with tarfile.open(PROJECT_ARCH, 'w:bz2') as archive:
        for i in range(pf.metadata.num_row_groups):
            table = pf.read_row_group(i)
            columns = table.to_pydict()
            for symbol, html in zip(columns['symbol'], columns['html']):
                bytes = html.encode(encoding)
                s = io.BytesIO(bytes)
                tarinfo = tarfile.TarInfo(name=f'topic/{symbol}.html')
                tarinfo.size = len(bytes)
                archive.addfile(tarinfo=tarinfo, fileobj=s)
                progress.update(1)

    progress.close()


def main():
    scrape_data()


if __name__ == '__main__':
    main()

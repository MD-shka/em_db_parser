from datetime import datetime
import asyncio
import aiohttp
from init_db import create_tables
from saver_db import save_to_db
from downloader import download_file
from utils import get_all_days
from file_parser import parse_data


async def process_reports(all_days: list[str]) -> None:
    async with aiohttp.ClientSession() as session:
        tasks = [single_process_report(date, session) for date in all_days]
        await asyncio.gather(*tasks, return_exceptions=True)


async def single_process_report(date: str, session: aiohttp.ClientSession) -> None:
    file_data = await download_file(date, session)
    if file_data:
        df = parse_data(file_data)  # type: ignore
        await save_to_db(df)


async def main(date_start: datetime, date_end: datetime) -> None:
    await create_tables()
    await process_reports(get_all_days(date_start, date_end))


if __name__ == "__main__":
    asyncio.run(main(datetime(2023, 1, 9), datetime.now()))

from typing import Optional
import aiohttp


async def download_file(date: str,
                        session: aiohttp.ClientSession) -> Optional[bytes]:
    url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{date}162000.xls"
    try:
        async with session.get(url) as response:
            return await response.read()
    except aiohttp.ClientError as e:
        print(f"Ошибка при скачивании отчета за {date}: {e}")
        return None
    
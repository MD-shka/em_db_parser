import asyncio
from io import BytesIO
from datetime import datetime, timedelta
import aiohttp
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import Base, SpimexTradingResult # type: ignore
from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(bind=engine, class_=AsyncSession, # type: ignore
                             expire_on_commit=False)  # type: ignore


async def create_tables() -> None:
    async with engine.begin() as session:
        await session.run_sync(Base.metadata.drop_all)
        await session.run_sync(Base.metadata.create_all)


def get_all_days(date_start: datetime, date_end: datetime) -> list[str]:
    return [(date_start + timedelta(days=i)).strftime('%Y%m%d') for i in
            range((date_end - date_start).days + 1)]  # type: ignore


async def download_file(date: str,
                        session: aiohttp.ClientSession) -> bytes | None:
    url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{date}162000.xls"
    try:
        async with session.get(url) as response:
            return await response.read()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Ошибка при скачивании отчета за {date}: {e}")
        return None


def format_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "Код Инструмента": "exchange_product_id",
        "Наименование Инструмента": "exchange_product_name",
        "Базис поставки": "delivery_basis_name",
        "Объем Договоров в единицах измерения": "volume",
        "Обьем Договоров, руб.": "total",
        "Количество Договоров, шт.": "count",
    })
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
    for col in ["volume", "total", "count"]:
        df[col] = df[col].fillna(0).astype(int)
    df["oil_id"] = df["exchange_product_id"].apply(lambda x: x[:4])
    df["delivery_basis_id"] = df["exchange_product_id"].apply(lambda x: x[4:7])
    df["delivery_type_id"] = df["exchange_product_id"].apply(lambda x: x[-1])
    return df


async def parse_data(file_data: bytes) -> pd.DataFrame:
    df = await asyncio.to_thread(pd.read_excel,
                                 BytesIO(file_data))  # type: ignore
    date = str(df.iloc[2, 1]).rsplit(" ", maxsplit=1)[-1]
    index_of_header = \
        df[df.iloc[:, 1] == "Единица измерения: Метрическая тонна"].index[0]
    df = df.iloc[index_of_header + 1:, [1, 2, 3, 4, 5, -1]].reset_index(
        drop=True)
    df.columns = pd.Index(df.iloc[0].str.replace("\n", " "))
    df = df.drop([0, 1])
    df = df[df.iloc[:, -1] != "-"].dropna().reset_index(drop=True)
    df["date"] = date
    return format_df_for_db(df)


async def save_to_db(df: pd.DataFrame) -> None:
    async with async_session() as session:
        async with session.begin():
            await session.execute(SpimexTradingResult.__table__.insert(),
                                  df.to_dict(orient="records"))


async def process_reports(all_days: list[str]) -> None:
    async with aiohttp.ClientSession() as session:
        tasks = [single_process_report(date, session) for date in all_days]
        await asyncio.gather(*tasks, return_exceptions=True)


async def single_process_report(date: str,
                                session: aiohttp.ClientSession) -> None:
    file_data = await download_file(date, session)
    if file_data:
        df = await parse_data(file_data)  # type: ignore
        await save_to_db(df)


async def main(date_start: datetime, date_end: datetime) -> None:
    await create_tables()
    await process_reports(get_all_days(date_start, date_end))


if __name__ == "__main__":
    asyncio.run(main(datetime(2023, 1, 9), datetime.today()))

import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import SpimexTradingResult # type: ignore
from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(bind=engine, class_=AsyncSession, # type: ignore
                             expire_on_commit=False)


async def save_to_db(df: pd.DataFrame) -> None:
    async with async_session() as session:
        async with session.begin():
            await session.execute(SpimexTradingResult.__table__.insert(),
                                  df.to_dict(orient="records"))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import Base  # type: ignore
from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,  # type: ignore
    expire_on_commit=False,
)


async def create_tables() -> None:
    async with engine.begin() as session:
        await session.run_sync(Base.metadata.drop_all)
        await session.run_sync(Base.metadata.create_all)

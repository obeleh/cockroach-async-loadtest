import asyncio
import os
import ssl

from sqlalchemy import Column, FetchedValue
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


class Flow(Base):
    id = Column(UUID, primary_key=True, name='id', server_default=FetchedValue())
    data = Column(JSONB, name='data')
    __tablename__ = "flow"


async def async_main():
    engine_kwargs = {}
    if "CA_CERT" in os.environ:
        sslctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=os.environ["CA_CERT"]
        )
        sslctx.check_hostname = False
        engine_kwargs["connect_args"] = {
            "ssl": sslctx
        }
    engine = create_async_engine(
        os.environ["DATABASE_URL"],
        **engine_kwargs
    )
    async_session: AsyncSession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        async with session.begin():
            flow = Flow(data={"status": "new", "metadata": {"bankId": "123"}})
            session.add(flow)
            print("Hi")


asyncio.run(async_main())

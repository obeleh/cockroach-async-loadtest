import argparse
import os
import random
import ssl
import string
import sys

from sqlalchemy import MetaData, Column, FetchedValue
from sqlalchemy.dialects.postgresql import JSONB, UUID
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

metadata = MetaData()
Base = declarative_base()


class Flow(Base):
    id = Column(UUID, primary_key=True, name='id', server_default=FetchedValue())
    data = Column(JSONB, name='data')
    __tablename__ = "flow"


ERR_COUNT = 0


async def gen_flows(engine, count, p_nr):
    async_session: AsyncSession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    letters = string.digits
    async with async_session() as session:
        for nr in range(count):
            if nr % 100 == 0:
                print(nr, p_nr)
            bank_id = ''.join(random.choice(letters) for _ in range(10))
            flow = Flow(data={"status": "new", "metadata": {"bankId": bank_id}})
            try:
                async with session.begin():
                    session.add(flow)
                async with session.begin():
                    result = await session.get(Flow, flow.id)
                    try:
                        assert result.id == flow.id
                    except Exception as assertion_err:
                        print(assertion_err, file=sys.stderr)
                        sys.exit(1)
            except Exception as ex2:
                print(ex2)
                global ERR_COUNT
                ERR_COUNT += 1


async def async_main(count: int, parallel: int):
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
    all_tasks = asyncio.gather(*[gen_flows(engine, count, p) for p in range(parallel)])
    await all_tasks
    await engine.dispose()
    print("ERR_COUNT", ERR_COUNT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', type=int, default=1000)
    parser.add_argument('-p', '--parallel', type=int, default=10)
    args = parser.parse_args()
    asyncio.run(async_main(count=args.count, parallel=args.parallel))

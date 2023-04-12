import asyncio
from aiopg.connection import Connection
import psycopg2
import time
from listen.settings import settings
from listen.utils import get_connection


async def listen(conn: Connection, event):
    print("começou a", event)
    async with conn.cursor() as cur:
        await cur.execute(f"LISTEN {event}")
        while True:
            try:
                msg = await conn.notifies.get()
            except psycopg2.Error as ex:
                print("ERROR: ", ex)
                return
            if msg.payload == "finish":
                return
            else:
                print("Receive <-", msg.payload)


async def main(tables: list[str], operations: list[str]):
    ctx = get_connection(settings.DATABASE_URL_PG)
    events = list(f"event_{t}_{o}" for t in tables for o in operations)
    # events = [events[2]]
    print(list(events))
    async with ctx as pool:
        async with pool.acquire() as conn:
            await asyncio.gather(*[listen(conn, event=e) for e in events])
    print("finalizando ...")

import asyncio
import os
import motor.motor_asyncio
import httpx

from ChinaXivXiv.defines import DEFAULT_HEADERS
from ChinaXivXiv.workers.IA_uploader import IA_upload_worker


async def main():
    transport = httpx.AsyncHTTPTransport(retries=3)
    h_client = httpx.AsyncClient(timeout=60, transport=transport)
    h_client.headers.update(DEFAULT_HEADERS)
    m_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGODB_URI"))

    db = m_client["chinaxiv"]
    global_chinaxiv_collection = db["global_chinaxiv"]

    cors = [
        IA_upload_worker(
            client=h_client,
            collection=global_chinaxiv_collection,
        ) for _ in range(5)]
    await asyncio.gather(*cors)


if __name__ == '__main__':
    asyncio.run(main())

import asyncio
from datetime import datetime
import os
import time
import motor.motor_asyncio
import httpx

from ChinaXivXiv.defines import DEBUG, DEFAULT_HEADERS, Status
from ChinaXivXiv.mongo_ops import create_fileids_queue_index, find_max_id, init_queue
from ChinaXivXiv.util import arg_parser
from ChinaXivXiv.workers.IA_uploader import IA_upload_worker
from ChinaXivXiv.workers.file_downloader import file_downloader_worker
from ChinaXivXiv.workers.fileid_finder import fileid_finder_worker
from ChinaXivXiv.workers.metadata_scraper import metadata_scraper_worker
from ChinaXivXiv.workers.status_mover import status_mover_worker
from ChinaXivXiv.workers.task_provider import task_provider_worker


async def main():
    args = arg_parser()
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
            args=args
        ) for _ in range(5)]
    await asyncio.gather(*cors)


if __name__ == '__main__':
    asyncio.run(main())

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
    m_client = motor.motor_asyncio.AsyncIOMotorClient(args.mongo)

    db = m_client["chinaxiv"]
    fileids_queue_collection = db["fileids_queue"]

    MOVER = 0
    if MOVER:
        cors = [
            status_mover_worker(
                c_queue=fileids_queue_collection,
                FROM = Status.UPLOADTOIA_FAIL,
                TO = Status.METADATA_DONE,
        ) for _ in range(1 if DEBUG else 50)]
        return await asyncio.gather(*cors)

    if args.task_provider:
        await task_provider_worker(
            fileids_queue_collection=fileids_queue_collection,
            args=args
        )
    elif args.fileid_finder:
        cors = [
            fileid_finder_worker(
            c_queue=fileids_queue_collection,
            client=h_client
        )for _ in range(1 if DEBUG else 2)
    ]
        await asyncio.gather(*cors)

    elif args.file_downloader:
        return await file_downloader_worker(
            c_queue=fileids_queue_collection,
            client=h_client,
            qos=args.qos
        )
    elif args.metadata_scraper:
        cors = [
            metadata_scraper_worker(
                c_queue=fileids_queue_collection,
                client=h_client
        ) for _ in range(1 if DEBUG else 3)]
        return await asyncio.gather(*cors)
    elif args.ia_uploader:
        cors = [
            IA_upload_worker(
                client=h_client,
                c_queue=fileids_queue_collection,
                args=args
        ) for _ in range(1 if DEBUG else int(args.qos))]
        return await asyncio.gather(*cors)
    else:
        print("no worker specified")

if __name__ == '__main__':
    asyncio.run(main())

import asyncio
import random
import motor.motor_asyncio
import httpx
import time
import os
from ChinaXivXiv.defines import Status
from ChinaXivXiv.exceptions import EmptyContent

from ChinaXivXiv.mongo_ops import claim_task, update_task


async def file_downloader_worker(c_queue: motor.motor_asyncio.AsyncIOMotorCollection, client: httpx.AsyncClient, qos: int|float):
    check_time = time.time()
    downloads_in_10s = 0
    while not os.path.exists("stop"):
        # reset
        # while sdadsad     !!!!await claim_task(c_queue, status_from=Status.DOWNLOAD_DONE, status_to=Status.DONE):
        #     pass
        # while TASK := await claim_task(c_queue, status_from=Status.DOWNLOAD_FAIL, status_to=Status.DONE):
        #     print(TASK.id)
        # while await claim_task(c_queue, status_from=Status.DOWNLOAD_PROCESSING, status_to=Status.DONE):
        #     pass
        # while await claim_task(c_queue, status_from=Status.DOWNLOAD_TODO, status_to=Status.DONE):
        #     pass
        # return
        # 

        # 1. claim a task
        TASK = await claim_task(c_queue, status_from=Status.DONE, status_to=Status.DOWNLOAD_PROCESSING)
        if not TASK:
            print("no task to claim, waiting...")
            await asyncio.sleep(random.randint(3, 10))
            continue

        downloads_in_10s += 1
        if downloads_in_10s >= qos * 10:
            t = 10 - (time.time() - check_time)
            if t > 0:
                print(f"DOWNLOAD_RATE_LIMITED, waiting {t}s")
                await asyncio.sleep(t if t > 0 else 0)
            check_time = time.time()
            downloads_in_10s = 0

        # 2. process task
        print(f"DOWNLOAD_PROCESSING id: {TASK.id}")
        try:
            r = await client.get(f"https://chinaxiv.org/user/download.htm?id={TASK.id}", follow_redirects=True)
            if r.status_code == 404:
                raise EmptyContent(f"status_code: {r.status_code}")
            assert r.status_code == 200, f"status_code: {r.status_code}"
        except EmptyContent as e:
            print("Empty", TASK.id)
            await update_task(c_queue, TASK, Status.DOWNLOAD_EMPTY)
            continue
        except Exception as e:
            print(repr(e))
            await update_task(c_queue, TASK, Status.DOWNLOAD_FAIL)
            continue
        os.makedirs(f"files/{TASK.id}/", exist_ok=True)
        with open(f"files/{TASK.id}/{TASK.content_disposition_filename}", "wb") as f:
            f.write(r.content)

        # 3. update task
        print(f"DOWNLOAD_DONE id: {TASK.id}")
        await update_task(c_queue, TASK, Status.DOWNLOAD_DONE)
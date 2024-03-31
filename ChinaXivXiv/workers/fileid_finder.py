import asyncio
import os
import random

import motor.motor_asyncio
import httpx

from ChinaXivXiv.defines import Status
from ChinaXivXiv.exceptions import EmptyContent
from ChinaXivXiv.mongo_ops import claim_task, update_task


async def fileid_finder_worker(c_queue: motor.motor_asyncio.AsyncIOMotorCollection, client: httpx.AsyncClient):
    while not os.path.exists("stop"):
        # 1. claim a task
        TASK = await claim_task(c_queue, status_from=Status.TODO, status_to=Status.PROCESSING)
        if not TASK:
            print("no task to claim, waiting...")
            await asyncio.sleep(random.randint(3, 10))
            continue

        # 2. process task
        print(f"PROCESSING id: {TASK.id}")
        try:
            r = await look_fileid(client, TASK.id)
        except EmptyContent as e:
            print("Empty", TASK.id)
            await update_task(c_queue, TASK, Status.EMPTY)
            continue
        except Exception as e:
            print(repr(e))
            await update_task(c_queue, TASK, Status.FAIL)
            continue

        # 3. update task
        print(f"DONE id: {TASK.id}, {r.headers}")
        await update_task(c_queue, TASK, Status.DONE, r.headers)


async def look_fileid(client: httpx.AsyncClient, fileid: str|int):
    headers = {
        'Connection': 'close', # 对面服务器有点奇葩，HEAD 不会关闭连接……
    }
    headers = None
    url = f"https://chinaxiv.org/user/download.htm?id={fileid}"
    resp = await client.head(url, follow_redirects=True, headers=headers)
    if resp.status_code == 200:
        # Content-Disposition: attachment; filename="202308.00522v1.pdf"
        return resp
    elif resp.status_code == 404:
        raise EmptyContent(f"status_code: {resp.status_code}")
    else:
        if resp.status_code == 502:
            await asyncio.sleep(3)
            print(f"502, retrying {url}")
            return await look_fileid(client, fileid)
        raise Exception(f"status_code: {resp.status_code}")
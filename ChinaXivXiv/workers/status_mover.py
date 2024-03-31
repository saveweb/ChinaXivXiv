import asyncio
import os
import random
import httpx

import motor.motor_asyncio
from ChinaXivXiv.defines import Status
from ChinaXivXiv.mongo_ops import claim_task


async def status_mover_worker(c_queue: motor.motor_asyncio.AsyncIOMotorCollection, FROM, TO):
    while not os.path.exists("stop"):
        # 1. claim a task
        TASK = await claim_task(c_queue, status_from=FROM, status_to=TO)
        if not TASK:
            print("no task to claim, waiting...")
            return
        print(f'{TASK.id} {TASK.status} -> {TO}')
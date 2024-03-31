import asyncio
from datetime import datetime
import os
import time

import motor.motor_asyncio
from ChinaXivXiv.defines import Status
from ChinaXivXiv.mongo_ops import create_fileids_queue_index, find_max_id, init_queue
from ChinaXivXiv.util import Args


async def task_provider_worker(fileids_queue_collection: motor.motor_asyncio.AsyncIOMotorCollection, args: Args):
    if await find_max_id(fileids_queue_collection) == 0:
        await create_fileids_queue_index(fileids_queue_collection)
    last_op = time.time()
    while not os.path.exists("stop"):
        max_id = await find_max_id(fileids_queue_collection)
        if max_id >= args.end_fileid:
            print(f"max_id >= args.end_id: {max_id} >= {args.end_fileid}")
            break
        t = 1 - (time.time() - last_op)
        await asyncio.sleep(t if t > 0 else 0)
        last_op = time.time()

        count_tasks_todo = await fileids_queue_collection.count_documents(filter={"status": Status.TODO})
        if count_tasks_todo > (args.qos * 3):
            print(f"{datetime.now()} | too many TODO tasks: {count_tasks_todo}, waiting...")
            await asyncio.sleep(10)
            continue

        print(f"{datetime.now()} | will created {args.qos} TODO tasks: {max_id}->{max_id+1+args.qos} |"
                "Now at %.2f%%" % (max_id / args.end_fileid * 100), end="\r")

        await init_queue(fileids_queue_collection, start_id=max_id+1, end_id=max_id+1+int(args.qos))
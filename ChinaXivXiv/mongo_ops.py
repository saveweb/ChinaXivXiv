from datetime import datetime
import time
from typing import Optional
import httpx
import motor.motor_asyncio

from ChinaXivXiv.defines import Status, Task


async def init_queue(queue_col: motor.motor_asyncio.AsyncIOMotorCollection, start_id: int, end_id: int, status: str = Status.TODO):
    """
    start_id: 1, end_id: 5
    will create id: 1, 2, 3, 4

    doc: {"id": int,"status": str}
    """
    assert queue_col.name == "fileids_queue"
    assert status in Status.__dict__.values()
    assert start_id > 0
    assert start_id <= end_id
    if start_id == end_id:
        print(f"start_id == end_id: {start_id}")
        return
    docs = []
    for i in range(start_id, end_id):
        docs.append({
            "id": i,
            "status": status,
        })
        if len(docs) == 100000:
            s_time = time.time()
            await queue_col.insert_many(docs, ordered=False)
            e_time = time.time()
            docs = []
            print(f"inserted c_queue={i} | {e_time - s_time}", end="\r")
    if docs:
        await queue_col.insert_many(docs)
    print(f"inserted c_queue={end_id}", end="\r")


async def claim_task(queue: motor.motor_asyncio.AsyncIOMotorCollection,
                     status_from: str = Status.TODO,
                     status_to: str=Status.PROCESSING) -> Optional[Task]:
    assert status_from in Status.__dict__.values()
    assert status_to in Status.__dict__.values()

    TASK = await queue.find_one_and_update(
        filter={"status": status_from},
        update={"$set": {
            "status": status_to,
            }},
        sort=[("_id", -1)],
    )
    return Task(**TASK) if TASK else None

async def update_task(queue: motor.motor_asyncio.AsyncIOMotorCollection, TASK: Task, status: str):
    assert status in Status.__dict__.values()
    update = {"$set": {
            "status": status,
        }}

    await queue.update_one(
        filter={"_id": TASK._id},
        update=update
    )


async def create_fileids_queue_index(collection: motor.motor_asyncio.AsyncIOMotorCollection):
    await collection.create_index("status")
    await collection.create_index("id", unique=True)

async def find_max_id(collection: motor.motor_asyncio.AsyncIOMotorCollection):
    doc = await collection.find_one(sort=[("id", -1)])
    if doc:
        return doc["id"]
    else:
        return 0
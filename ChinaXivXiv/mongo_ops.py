from typing import Optional
import motor.motor_asyncio

from ChinaXivXiv.defines import Status, Task



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

async def update_task(queue: motor.motor_asyncio.AsyncIOMotorCollection, TASK: Task, status: str|int):
    # assert status in Status.__dict__.values()
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
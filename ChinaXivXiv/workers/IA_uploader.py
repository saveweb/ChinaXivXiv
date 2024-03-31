import asyncio
from datetime import datetime
import os
from pprint import pprint
import random
import time
import io
import traceback
from typing import Dict, Optional
import httpx

import internetarchive

import motor.motor_asyncio
from ChinaXivXiv.defines import Status, Task
from ChinaXivXiv.mongo_ops import claim_task, create_fileids_queue_index, find_max_id, init_queue, update_task
from ChinaXivXiv.util import Args
from ChinaXivXiv.workers.metadata_scraper import parse_keywords, parse_subjects

NOTES = """\
- 元数据由脚本提取，仅供参考，以 ChinaXiv.org 官网为准。（如元数据识别有误/需要更新，请留言）
- 截至 2023-11-28，ChinaXiv 背后的 istic.ac.cn 维护的 DOI 前缀 https://doi.org/10.12074 大量新论文的 DOI 链接无法正确重定向，因此使用者请谨慎使用 external-identifier 中的 DOI 链接。（使用前看下 DOI 跳转是否正常。如正常，则永久有效）
- 部分论文并没有使用 DOI 10.12074 前缀（例如 https://chinaxiv.org/abs/202205.00023 ），它们的元数据可能异常。
- “版本历史”的“下载全文”按钮链接到的是 ChinaXiv.org 的原始链接，未来可能会失效。
"""

async def IA_upload_worker(client: httpx.AsyncClient, c_queue: motor.motor_asyncio.AsyncIOMotorCollection, args: Args):
    while not os.path.exists("stop"):
        # 1. claim a task
        TASK = await claim_task(c_queue, status_from=Status.METADATA_DONE, status_to=Status.UPLOADTOIA_PROCESSING)

        # dry run
        # TASK = await claim_task(c_queue, status_from=Status.METADATA_DONE, status_to=Status.METADATA_DONE)
        # async def update_task(c_queue, task, status):  # noqa: F811
        #     print(f"update_task: {task.id} {status}")

        if not TASK:
            print("no task to claim, waiting...")
            await asyncio.sleep(random.randint(3, 10))
            continue

        # 2. process task
        print(f"PROCESSING id: {TASK.id}")
        try:
            await async_upload(client, TASK)
        except Exception as e:
            traceback.print_exc()
            print(repr(e))
            await update_task(c_queue, TASK, Status.UPLOADTOIA_FAIL)
            print("waiting 5 minutes...")
            await asyncio.sleep(300)
            continue

        # 3. update task
        print(f"DONE id: {TASK.id}")
        await update_task(c_queue, TASK, Status.UPLOADTOIA_DONE)


def load_ia_keys():
    """ key_acc, key_sec """
    with open(".ia_keys", "r") as f:
        keys = f.read().splitlines()
    if len(keys) > 2:
        print("load_ia_keys:", keys[2])
    return (keys[0], keys[1])

"""
{
    _id: ObjectId("655c5adf2d57d22c4d587116"),
    id: 15083,
    status: 'METADATA_DONE',
    claim_at: ISODate("2023-11-28T03:52:29.313Z"),
    update_at: ISODate("2023-11-28T03:52:30.641Z"),
    content_disposition: 'attachment; filename="201712.01994v1.pdf"',
    content_disposition_filename: '201712.01994v1.pdf',
    content_length: 865828,
    content_type: 'application/octet-stream;charset=UTF-8',
    metadata: {
      title: '中间锦鸡儿CibHLH027基因的克隆和功能研究',
      authors: [ '杨天瑞', '李娜', '张秀娟', '杨杞', '王瑞刚', '李国婧' ],
      journal: '中国生物工程杂志',
      pubyear: 2017,
      version: 1,
      csoaid: '201712.01994',
      copyQuotation: '杨天瑞,李娜,张秀娟,杨杞,王瑞刚,李国婧.(2017).中间锦鸡儿CibHLH027基因的克隆和功能研究.中国生物工程
  志.[ChinaXiv:201712.01994]'
    }
}
"""

async def async_upload(client: httpx.AsyncClient, TASK: Task):
    assert TASK.metadata, "metadata is None"
    assert TASK.content_disposition_filename
    assert TASK.content_disposition_filename.startswith(TASK.metadata["csoaid"])
    assert f'{TASK.metadata["csoaid"]}v{TASK.metadata["version"]}.pdf' == TASK.content_disposition_filename
    
    chinaxiv_permanent_with_version_url = f'https://chinaxiv.org/abs/{TASK.metadata["csoaid"]}v{TASK.metadata["version"]}'

    with open(f'core_html/{TASK.id}.html', 'r') as f:
        core_html = f.read()

    external_identifier = [
        f'urn:chinaxiv:{TASK.metadata["csoaid"]}',
        f'urn:chinaxiv:{TASK.metadata["csoaid"]}V{TASK.metadata["version"]}',

        f'urn:doi:10.12074/{TASK.metadata["csoaid"]}',

        # https://registry.identifiers.org/registry/cstr
        # e.x.: CSTR:32003.36.ChinaXiv.201604.00018.V2
        f'urn:cstr:32003.36.ChinaXiv.{TASK.metadata["csoaid"]}.V{TASK.metadata["version"]}',
    ]
    subjects = parse_subjects(core_html.encode("utf-8"))
    keywords = parse_keywords(core_html.encode("utf-8"))

    YYYY = TASK.metadata["pubyear"]
    assert 1900 <= YYYY <= 2100
    MM = TASK.metadata["csoaid"][4:6]
    assert len(MM) == 2 and (1 <= int(MM) <= 12)

    metadata = {
        "title": TASK.metadata["title"],
        "creator": TASK.metadata["authors"], # List[str]
        "date": f'{YYYY}-{MM}',
        "subject": ["ChinaXiv", TASK.metadata["journal"]] + subjects + keywords, # TODO: may overflow 255 chars
        "description": core_html,
        "source": chinaxiv_permanent_with_version_url,
        "external-identifier": external_identifier,

        "notes": NOTES,
        "mediatype": "texts",
        "collection": "opensource",
        # "collection": "test_collection",
        "scanner": "ChinaXivXiv v0.1.0", # TODO: update this
        "rights": "https://chinaxiv.org/user/license.htm",
        # ^^^^ IA native metadata ^^^^

        # vvvv custom metadata field vvvv
        "journal": TASK.metadata["journal"],

        "chinaxiv": TASK.metadata["csoaid"], # == chinaxiv_csoaid, so we can just search "chinaxiv:yyyymm.nnnnnn" to get the item on IA
        "chinaxiv_id": TASK.id, # each version has a unique id, even if they have the same csoaid
        "chinaxiv_copyQuotation": TASK.metadata["copyQuotation"], # 推荐引用格式 | suggested citation format
    }
    identifier = f"ChinaXiv-{TASK.metadata['csoaid']}V{TASK.metadata['version']}"
    # identifier = f"TEST-ChinaXiv-{TASK.metadata['csoaid']}V{TASK.metadata['version']}"
    core_html_filename = f"{TASK.metadata['csoaid']}v{TASK.metadata['version']}-abs.html"
    core_html_filename = None # TODO: DISABLE
    file_name = TASK.content_disposition_filename
    
    # http://65.109.48.39:41830/chfs/shared/files/12335/201711.00804v1.pdf
    if os.path.exists(f'files/{TASK.id}/{TASK.content_disposition_filename}'):
        with open(f'files/{TASK.id}/{TASK.content_disposition_filename}', 'rb') as f:
            file_content = f.read()
    else:
        direct_url_prefix = "http://65.109.48.39:41830/chfs/shared/files/"
        print(f"downloading {direct_url_prefix}{TASK.id}/{TASK.content_disposition_filename} ... {TASK.content_length} bytes")
        r = await client.get(f'{direct_url_prefix}{TASK.id}/{TASK.content_disposition_filename}')
        assert r.status_code == 200
        file_content = r.content
    # pprint(metadata)
    await do_upload(identifier, metadata,
                    core_html, core_html_filename,
                    file_content, file_name)
    await wait_until_ia_item_is_ready(identifier)
    return identifier

async def do_upload(identifier: str, metadata: Dict,
                    core_html: Optional[str], core_html_filename: Optional[str],
                    file_content: bytes, file_name: str):
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _do_upload,
                                identifier, metadata,
                                core_html, core_html_filename,
                                file_content, file_name)
    await task
    return task.result()
                    


def _do_upload(identifier: str, metadata: Dict,
               core_html: Optional[str], core_html_filename: Optional[str],
               file_content: bytes, file_name: str):
    ia = internetarchive.get_session()
    ia.access_key, ia.secret_key = load_ia_keys()
    item = ia.get_item(identifier)
    files = {
        file_name: io.BytesIO(file_content),
    }
    if core_html_filename:
        assert core_html is not None
        files[core_html_filename] = io.BytesIO(core_html.encode("utf-8"))
    resps = item.upload(files, metadata=metadata, verbose=True)
    print(resps)
    return resps

async def wait_until_ia_item_is_ready(identifier: str):
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _wait_until_ia_item_is_ready, identifier)
    await task
    return task.result()

def _wait_until_ia_item_is_ready(identifier: str):

    # for testing
    if identifier.startswith("//RETURN_TRUE/"):
        return True
    if identifier.startswith("//RETURN_FALSE/"):
        return False
    if identifier.startswith("//RAISE_EXCEPTION/"):
        raise NotImplementedError("test exception")
    
    
    ia = internetarchive.get_session()
    item = ia.get_item(identifier) # refresh item
    tries = 400
    for tries_left in range(tries, 0, -1):
        if item.exists:
            break

        print(f"Waiting for item to be created ({tries_left} tries left)  ...", end='\r')
        if tries < 395:
            print(f"IA overloaded, still waiting for item to be created ({tries_left} tries left)  ...", end='\r')
        time.sleep(30)
        item = ia.get_item(identifier)

    if not item.exists:
        raise TimeoutError(f"IA overloaded, item still not created after {400 * 30} seconds")

    print(f"item {identifier} is ready")
    return True

if __name__ == "__main__":
    async def tese_wait_until_ia_item_is_ready():
        assert await wait_until_ia_item_is_ready("//RETURN_TRUE/STWP") is True
        assert await wait_until_ia_item_is_ready("//RETURN_FALSE/STWP") is False
        try:
            await wait_until_ia_item_is_ready("//RAISE_EXCEPTION/STWP")
        except NotImplementedError:
            pass
        else:
            raise AssertionError("test failed")

    asyncio.run(tese_wait_until_ia_item_is_ready())
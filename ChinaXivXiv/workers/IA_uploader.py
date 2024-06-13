import asyncio
from datetime import datetime
import os
from pprint import pprint
import random
import time
import io
from dataclasses import dataclass
import traceback
from typing import Dict, List, Optional
import httpx

import internetarchive

import motor.motor_asyncio
from ChinaXivXiv.defines import ChinaXivGlobalMetadata, ChinaXivHtmlMetadata, Status, Task
from ChinaXivXiv.mongo_ops import claim_task, create_fileids_queue_index, find_max_id, init_queue, update_task
from ChinaXivXiv.util import Args
from ChinaXivXiv.workers.metadata_scraper import get_chinaxivhtmlmetadata_from_html, get_core_html, parse_keywords, parse_subjects

NOTES = """\
- 元数据由脚本提取，仅供参考，以 ChinaXiv.org 官网为准。（如元数据识别有误/需要更新，请留言）
- external-identifier 中的 DOI 链接由脚本提取，极有可能不准。使用前看下 DOI 跳转是否正常。如正常，则永久有效；如遇坏 identifier 可以尝试手动在 DOI 后加上V{版本号}。
- 部分论文并没有使用 DOI 10.12074 前缀（例如 https://chinaxiv.org/abs/202205.00023 ），它们的元数据可能异常。
- “版本历史”的“下载全文”按钮链接到的是 ChinaXiv.org 的原始链接，未来可能会失效。
"""

async def IA_upload_worker(client: httpx.AsyncClient, collection: motor.motor_asyncio.AsyncIOMotorCollection, args: Args):
    while not os.path.exists("stop"):
        # 1. claim a task
        TASK = await claim_task(collection, status_from=Status.TODO, status_to=Status.UPLOADTOIA_PROCESSING)
        if not TASK:
            print("no task to claim, waiting...")
            await asyncio.sleep(random.randint(3, 10))
            continue
        # 2. process task
        print(f"PROCESSING id: {TASK.identifier}")

        # curl 'https://global.chinaxiv.org/api/get_browse_db' -X POST -H 'Accept: application/json, text/plain, */*' -H 'Content-Type: application/json; charset=UTF-8' --data-raw '{"domains":[{"value":"chinaxiv_48172","select_value":"id"}],"dbs":["chinaxiv"]}' | jq

        r_global_chinaxiv_metadata_from_get_browse_db = await client.post("https://global.chinaxiv.org/api/get_browse_db", json={
            "domains": [{"value": TASK.identifier.removeprefix("localIdentifier:"), "select_value": "id"}], # chinaxiv_34185
            "dbs": ["chinaxiv"]
        })
        metadata_from_browse_db_dblist = r_global_chinaxiv_metadata_from_get_browse_db.json()["dbList"]
        assert len(metadata_from_browse_db_dblist) == 1
        metadata_from_browse_db = metadata_from_browse_db_dblist[0]
        assert metadata_from_browse_db["id"] == TASK.identifier.removeprefix("localIdentifier:")
        version: str = metadata_from_browse_db["version"]
        assert isinstance(version, str) and version.isdigit()

        print(metadata_from_browse_db)

        chinaxiv_global_metadata = ChinaXivGlobalMetadata(
            title=TASK.metadata["title"],
            author=TASK.metadata["author"] if "author" in TASK.metadata else None,
            keyword=TASK.metadata["keyword"] if "keyword" in TASK.metadata else None,
            article_id=TASK.metadata["article-id"],
        )
        print(chinaxiv_global_metadata.article_id)
        versions = await collection.count_documents({"metadata.article-id": chinaxiv_global_metadata.article_id})
        print(f"{TASK.identifier}, {TASK.metadata['article-id'][0]} has {versions} versions, this is version {version}")


        chinaxiv_permanent_with_version_url = f"https://chinaxiv.org/abs/{TASK.metadata['article-id'][0]}v{version}"
        print("CURL", chinaxiv_permanent_with_version_url)
        headers = {
            'Connection': 'close', # 对面服务器有点奇葩，HEAD 不会关闭连接……
        }
        r_html = await client.get(chinaxiv_permanent_with_version_url, headers=headers, follow_redirects=False)
        assert r_html.status_code == 200

        html_metadata = get_chinaxivhtmlmetadata_from_html(html=r_html.content, url=chinaxiv_permanent_with_version_url)
        core_html = get_core_html(html=r_html.content, url=chinaxiv_permanent_with_version_url)


        ia_identifier = await async_upload(client, html_metadata, core_html)
        print(f"uploaded to IA: {ia_identifier}")

        await update_task(collection, TASK, status=Status.UPLOADTOIA_DONE)

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


async def async_upload(client: httpx.AsyncClient, html_metadata: ChinaXivHtmlMetadata, core_html: str):
    assert html_metadata, "metadata is None"
    assert f'{html_metadata.csoaid}v{html_metadata.version}.pdf'
    
    chinaxiv_permanent_with_version_url = f'https://chinaxiv.org/abs/{html_metadata.csoaid}v{html_metadata.version}'

    external_identifier = [
        f'urn:chinaxiv:{html_metadata.csoaid}',
        f'urn:chinaxiv:{html_metadata.csoaid}V{html_metadata.version}',

        f'urn:doi:10.12074/{html_metadata.csoaid}V{html_metadata.version}',

        # https://registry.identifiers.org/registry/cstr
        # e.x.: CSTR:32003.36.ChinaXiv.201604.00018.V2
        f'urn:cstr:32003.36.ChinaXiv.{html_metadata.csoaid}.V{html_metadata.version}',
    ]

    YYYY = html_metadata.pubyear
    assert 1900 <= YYYY <= 2100
    MM = html_metadata.csoaid[4:6]
    assert len(MM) == 2 and (1 <= int(MM) <= 12)

    metadata = {
        "title": html_metadata.title,
        "creator": html_metadata.authors, # List[str]
        "date": f'{YYYY}-{MM}',
        "subject": ["ChinaXiv", html_metadata.journal] + html_metadata.subjects + html_metadata.keywords, # TODO: may overflow 255 chars
        "description": core_html,
        "source": chinaxiv_permanent_with_version_url,
        "external-identifier": external_identifier,

        "notes": NOTES,
        "mediatype": "texts",
        "collection": "opensource",
        # "collection": "test_collection",
        "scanner": "ChinaXivXiv v0.2.0", # TODO: update this
        "rights": "https://chinaxiv.org/user/license.htm",
        # ^^^^ IA native metadata ^^^^

        # vvvv custom metadata field vvvv
        "journal": html_metadata.journal,

        "chinaxiv": html_metadata.csoaid, # == chinaxiv_csoaid, so we can just search "chinaxiv:yyyymm.nnnnnn" to get the item on IA
        "chinaxiv_id": html_metadata.chinaxiv_id, # each version has a unique id, even if they have the same csoaid
        "chinaxiv_copyQuotation": html_metadata.copyQuotation, # 推荐引用格式 | suggested citation format
    }
    identifier = f"ChinaXiv-{html_metadata.csoaid}V{html_metadata.version}"
    # identifier = f"TEST-ChinaXiv-{metadata.csoaid}V{metadata.version}"
    core_html_filename = f"{html_metadata.csoaid}v{html_metadata.version}-abs.html"
    core_html_filename = None # TODO: DISABLE

    file_name = f'{html_metadata.csoaid}v{html_metadata.version}.pdf'
    # https://chinaxiv.org/businessFile/201601/201601.00051v1/201601.00051v1.pdf
    # https://chinaxiv.org/businessFile/202406/202406.00122v1/202406.00122v1.pdf
    url = f"https://chinaxiv.org/businessFile/{html_metadata.csoaid.split('.')[0]}/{html_metadata.csoaid}v{html_metadata.version}/{html_metadata.csoaid}v{html_metadata.version}.pdf"
    print(f"downloading {url}")
    r = await client.get(url)
    print(f"downloaded {url}, status_code: {r.status_code}, content_length: {len(r.content)}")
    assert r.status_code == 200
    file_content = r.content
    # pprint(metadata)
    # asset it's pdf
    assert file_content[:4] == b'%PDF'

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
import asyncio
import os
import random
import traceback
from bs4 import BeautifulSoup, element

import motor.motor_asyncio
import httpx

from ChinaXivXiv.defines import ChinaXivHtmlMetadata, Status, Task
from ChinaXivXiv.exceptions import EmptyContent
from ChinaXivXiv.mongo_ops import claim_task, update_task


async def metadata_scraper_worker(c_queue: motor.motor_asyncio.AsyncIOMotorCollection, client: httpx.AsyncClient):
    REPRASE = 0
    while not os.path.exists("stop"):
        # 1. claim a task
        if REPRASE:
            TASK = await claim_task(c_queue, status_from=Status.METADATA_DONE, status_to=Status.METADATA_PROCESSING)
            if not TASK:
                print("no task to claim, exiting...")
                return
            print(f"REPRASE id: {TASK.id}")
            assert TASK.metadata

            copyQuotation = TASK.metadata["copyQuotation"]
            authors, pubyear, _, journal, _prefer_identifier = parse_authors_from_copyQuotation(copyQuotation)
            if authors != TASK.metadata["authors"]:
                print("pubyear | journal | _prefer_identifier | authors")
                print(" | ".join([str(pubyear), journal, _prefer_identifier, ",".join(authors)]))
            new_metadata  = TASK.metadata
            new_metadata["authors"] = authors
            new_metadata["pubyear"] = pubyear
            new_metadata["journal"] = journal
            await update_task(c_queue, TASK, Status.METADATA_FAIL, metadata=new_metadata)
            continue

        TASK = await claim_task(c_queue, status_from=Status.DOWNLOAD_DONE, status_to=Status.METADATA_PROCESSING)
        if not TASK:
            print("no task to claim, waiting...")
            await asyncio.sleep(random.randint(3, 10))
            continue

        # 2. process task
        print(f"PROCESSING id: {TASK.id}")
        try:
            await asyncio.sleep(random.randint(0, 1))
            r = await get_page_response(client, TASK)
            if r.status_code == 404:
                raise EmptyContent(f"status_code: {r.status_code}")
            if r.status_code == 403:
                print("403, sleep 15s")
                await asyncio.sleep(15)
            assert r.status_code == 200, f"status_code: {r.status_code}"
        except EmptyContent as e:
            print(TASK.id, 'EMPTY')
            await update_task(c_queue, TASK, Status.METADATA_EMPTY)
            continue
        except Exception as e:
            print(TASK.id, f'Error: {repr(e)}')
            traceback.print_exc()
            await update_task(c_queue, TASK, Status.METADATA_FAIL)
            continue

        # 3. parse HTML
        print(f"Parsing HTML id: {TASK.id}")
        try:
            fileid, title, version, csoaid = parse_info_from_html(r.content)
            
            assert str(fileid) == str(TASK.id)
            assert TASK.content_disposition_filename and \
                   TASK.content_disposition_filename.startswith(csoaid)
            assert f'v{version}' in TASK.content_disposition_filename.lower()
            copyQuotation = get_copyQuotation(r.content)
            authors, pubyear, _, journal, _prefer_identifier = parse_authors_from_copyQuotation(copyQuotation)
            print("title | authors | journal | pubyear | version | csoaid | _prefer_identifier")
            print(authors, journal, str(pubyear), str(version), csoaid, _prefer_identifier)

            core_html = get_core_html(r.content, str(r.url))
            os.makedirs("core_html", exist_ok=True)
            with open(f"core_html/{TASK.id}.html", "w") as f:
                f.write(core_html)

            # 4. update task
            print(f"DONE id: {TASK.id}")
            print(copyQuotation)
            # metadata: Task.metadata
            await update_task(c_queue, TASK, Status.METADATA_DONE, metadata={
                "title": title,
                "authors": authors,
                "journal": journal,
                "pubyear": pubyear,
                "version": version,
                "csoaid": csoaid,

                "copyQuotation": copyQuotation,
            })


        except Exception as e:
            print(TASK.id, str(r.url), f'Error: {repr(e)}')
            traceback.print_exc()
            await update_task(c_queue, TASK, Status.METADATA_FAIL)
            continue




async def get_page_response(client: httpx.AsyncClient, TASK: Task):
    headers = {
        'Connection': 'close', # 对面服务器有点奇葩，HEAD 不会关闭连接……
    }
        # content_disposition_filename: '201604.00018v2.pdf',

    assert TASK.content_disposition_filename is not None
    assert TASK.content_disposition_filename.endswith(".pdf")

    str_parts = TASK.content_disposition_filename.split(".")
    assert len(str_parts) == 3
    chinaxiv_id_with_version = ".".join(str_parts[:len(str_parts)-1])
    assert len(chinaxiv_id_with_version.split(".")) == 2

    # 201604.00018v2
    chinaxiv_permanent_with_version_url = f"https://chinaxiv.org/abs/{chinaxiv_id_with_version}"
    print("CURL", chinaxiv_permanent_with_version_url)
    return await client.get(chinaxiv_permanent_with_version_url, headers=headers, follow_redirects=False)



"""
<div id="zzviewmode" style="display:none;position:fixed;width:880px;height:450px;top:0;left:0;right:0;bottom:0;margin:auto;z-index:1000;border:1px solid white;border-radius:3px;background-color:#FFFFFF;">
    <div class="content" style="height:360px;padding-top:20px;">
        <form id="form1" action="" method="post">
			<input type="hidden" id="id" name="id" value="77780" />
			<input type="hidden" id="email" name="email" value="" />
			<input type="hidden" id="title" name="title" value="CSNS EPICS PV信息平台的设计与实现" />
			<input type="hidden" id="version" name="version" value="1" />
			<input type="hidden" id="csoaid" name="csoaid" value="202311.00062" />
			<input type="hidden" id="starID1" name="starID1" />
"""
def parse_info_from_html(html: bytes):
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"id": "form1"})
    assert form is not None
    fileid = form.find("input", {"id": "id"}).get("value") # type: ignore
    title = form.find("input", {"id": "title"}).get("value") # type: ignore
    version = form.find("input", {"id": "version"}).get("value") # type: ignore
    csoaid = form.find("input", {"id": "csoaid"}).get("value") # type: ignore

    assert fileid is not None and isinstance(fileid, str) and fileid.isdigit()
    assert title is not None and isinstance(title, str)
    assert version is not None and isinstance(version, str) and version.isdigit()
    assert csoaid is not None and isinstance(csoaid, str) and "." in csoaid and csoaid.split(".")[0].isdigit() and csoaid.split(".")[1].isdigit()

    version = int(version)
    
    return fileid, title, version, csoaid


def get_copyQuotation(html: bytes):
    soup = BeautifulSoup(html, "html.parser")
    copyQuotation = soup.find("span", {"id": "copyQuotation"})
    assert copyQuotation is not None
    return copyQuotation.text.strip()

# <span id="copyQuotation">薛康佳,张玉亮,王林,吴煊,李明涛,何泳成,朱鹏.(2023).CSNS EPICS PV信息平台的设计与实现.原子核物理评论.doi:10.12074/202311.00062V1 </span>
# 何厚军,韩运成,王晓彧,刘玉敏,张佳辰,任雷,郑明杰.(2023).基于载流子演化的3D P+PNN+多沟槽结构提升Betavoltaic核电池性能.中国科学院科技论文预发布平台.doi:10.12074/202310.00022V1
# Debbie F. Crawford,Michael H. O'Connor,Tom Jovanovic,Alexander Herr,Robert John Raison,Deborah A. O'Connell,Tim Baynes.(2016).A spatial assessment of potential biomass for bioenergy in Australia in 2010, and possible expansion by 2030 and 2050.GCB Bioenergy.[ChinaXiv:201605.00524]

# authors.({pubyear:int}).title.journal.{identifier}
def parse_authors_from_copyQuotation(copyQuotation: str):

    authors_text = copyQuotation.split(".(")[0]
    authors = []

    author = ""
    for idx, char in enumerate(authors_text):
        if idx == len(authors_text)-1 and char == ",": # 最后一个字符是逗号
            continue # 忽略

        if char == "," and (authors_text[idx+1] != " "): # 逗号后面无空格
            authors.append(author)
            author = ""
        else: # 不是逗号/逗号后面有空格（人名的一部分）
            author += char
    if author:
        authors.append(author)

    assert authors


    pubyear = "".join(
        [char for char in copyQuotation.split(".(")[1].split(").")[0] if char.isdigit()]
    )
    assert pubyear
    pubyear = int(pubyear)

    text_after_pubyear = copyQuotation.split(".(")[1:]
    text_after_pubyear = ".(".join(text_after_pubyear)
    text_after_pubyear = text_after_pubyear.split(").")[1:]
    text_after_pubyear = ").".join(text_after_pubyear)

    title = text_after_pubyear.split(".")[0]
    assert title

    journal = text_after_pubyear.split(".")[1]


    prefer_identifier = ".".join(text_after_pubyear.split(".")[2:])

    return authors, pubyear, title, journal, prefer_identifier
    

def get_core_html(html: bytes, url: str):
    from urllib.parse import urljoin
    soup = BeautifulSoup(html, "html.parser")
    # .paper > .flex_item content > .hd
    core_html = soup.find("div", {"class": "paper"}).find("div", {"class": "flex_item content"}).find("div", {"class": "hd"}) # type: ignore
    # 删带有 "相关论文推荐" 字样的 ft
    assert isinstance(core_html, element.Tag)
    for ft in core_html.find_all("div", {"class": "ft"}):
        if "相关论文推荐" in ft.text:
            ft.decompose()
    # 删“点击复制”span copyBtn
    for copyBtn in core_html.find_all("span", {"id": "copyBtn"}):
        copyBtn.decompose()

    # 删除 div id="journalSelect"
    for journalSelect in core_html.find_all("div", {"id": "journalSelect"}):
        journalSelect.decompose()

    # print(core_html.prettify())
    # 将相对链接转换为绝对链接
    for a in core_html.find_all("a"):
        if a.get("href", "").startswith("/"):
            a["href"] = urljoin(url, a["href"])
        with open("core.html", "w") as f:
            # 最小化输出
            f.write(core_html.prettify(formatter="minimal"))
            
    return core_html.prettify(formatter="minimal")


def parse_subjects(html: bytes):
    subjects = []
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        if a.get("href", "") and (
            "field=subject" in a.get("href", "")
            or 
            "field=domain" in a.get("href", "")
            ):
            subjects.append(a.text.strip())
    return subjects

def parse_keywords(html: bytes):
    keywords = []
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        if a.get("href", "") and (
            "field=keywords" in a.get("href", "")
            ):
            keywords.append(a.text.strip())
    return keywords            

def get_chinaxivhtmlmetadata_from_html(html: bytes, url: str):
    fileid, title, version, csoaid = parse_info_from_html(html)
    copyQuotation = get_copyQuotation(html)
    authors, pubyear, title, journal, prefer_identifier = parse_authors_from_copyQuotation(copyQuotation)
    core_html = get_core_html(html, url)
    metadata = ChinaXivHtmlMetadata(
        chinaxiv_id=int(fileid),
        title=title,
        authors=authors,
        journal=journal,
        pubyear=pubyear,
        version=version,
        csoaid=csoaid,
        copyQuotation=copyQuotation,
        subjects=parse_subjects(html),
        keywords=parse_keywords(html),
        prefer_identifier=prefer_identifier
    )
    return metadata

if __name__ == '__main__':
    def test_parse_info_from_html():
        client = httpx.Client()
        from ChinaXivXiv.defines import DEFAULT_HEADERS
        client.headers.update(DEFAULT_HEADERS)
        r = client.get("https://chinaxiv.org/abs/202311.00077v1")
        assert r.status_code == 200
        metadata = get_chinaxivhtmlmetadata_from_html(r.content, str(r.url))
        print(metadata)

    test_parse_info_from_html()
from bs4 import BeautifulSoup, element

import httpx

from ChinaXivXiv.defines import ChinaXivHtmlMetadata



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
# .(2024).快速射电暴观测数据干扰缓解方法研究.天文学报.doi:10.15940/j.cnki.0001-5245.2024.02.010
# authors.({pubyear:int}).title.journal.{identifier}
def parse_authors_from_copyQuotation(copyQuotation: str):

    authors_text = copyQuotation.split(".(")[0]
    authors = []

    try:
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

        if authors_text:
            assert authors, f"{authors_text} | {authors}"
    except Exception as e:
        authors = None


    try:
        pubyear = "".join(
            [char for char in copyQuotation.split(".(")[1].split(").")[0] if char.isdigit()]
        )
        assert pubyear
        pubyear = int(pubyear)
    except Exception as e:
        pubyear = None

    try:
        text_after_pubyear = copyQuotation.split(".(")[1:]
        text_after_pubyear = ".(".join(text_after_pubyear)
        text_after_pubyear = text_after_pubyear.split(").")[1:]
        text_after_pubyear = ").".join(text_after_pubyear)
    except Exception as e:
        text_after_pubyear = None

    try:
        title = text_after_pubyear.split(".")[0]
        assert title
    except Exception as e:
        title = None

    try:
        journal = text_after_pubyear.split(".")[1]
        assert journal
    except Exception as e:
        journal = None

    try:
        prefer_identifier = ".".join(text_after_pubyear.split(".")[2:])
        assert prefer_identifier
    except Exception as e:
        prefer_identifier = None

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
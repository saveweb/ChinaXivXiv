from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from bson import ObjectId

END_FILEID = 78000
DEFAULT_HEADERS = {
    "User-Agent": "ChinaXiv Archive Mirror Project/0.1.0 (STW; SaveTheWeb; +github.com/saveweb; saveweb@saveweb.org) (qos-rate-limit: 3q/s)",
}
DEBUG = 1

class Status:
    TODO = "TODO"
    """ 任务刚创建，等待领取 """
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    EMPTY = "EMPTY"
    """ 无数据，可能是不存在/被删除(?) """
    FAIL = "FAIL"
    # FEZZ = "FEZZ"
    # """ 特殊: 任务冻结 """

    # DOWNLOAD_TODO = "DOWNLOAD_TODO"
    DOWNLOAD_PROCESSING = "DOWNLOAD_PROCESSING"
    DOWNLOAD_DONE = "DOWNLOAD_DONE"
    DOWNLOAD_EMPTY = "DOWNLOAD_EMPTY"
    DOWNLOAD_FAIL = "DOWNLOAD_FAIL"

    # METADATA_TODO = "METADATA_TODO"
    METADATA_PROCESSING = "METADATA_PROCESSING"
    METADATA_DONE = "METADATA_DONE"
    METADATA_EMPTY = "METADATA_EMPTY"
    METADATA_FAIL = "METADATA_FAIL"

    # UPLOADTOIA_TODO = "UPLOADTOIA_TODO"
    UPLOADTOIA_PROCESSING = "UPLOADTOIA_PROCESSING"
    UPLOADTOIA_DONE = "UPLOADTOIA_DONE"
    UPLOADTOIA_FAIL = "UPLOADTOIA_FAIL"


@dataclass
class Task:
    _id: ObjectId
    identifier: str
    """ localIdentifier:chinaxiv_1041 """
    status: Status
    datestamp: str
    metadata: Dict

    def __post_init__(self):
        assert self.status in Status.__dict__.values()


@dataclass
class ChinaXivHtmlMetadata:
    chinaxiv_id: int
    """ 又名 fileid """
    title: str
    authors: List[str]
    journal: str
    pubyear: int
    version: int
    csoaid: str
    """ 又名 article-id """
    copyQuotation: str

    subjects: List[str]
    keywords: List[str]

    prefer_identifier: Optional[str]
    """ DOI or csoaid """

@dataclass
class ChinaXivGlobalMetadata:
    title: List[str]
    article_id: List[str]
    """ csoaid """
    author: Optional[List[str]] = None
    keyword: Optional[List[str]] = None
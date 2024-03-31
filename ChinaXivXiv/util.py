import argparse
from dataclasses import dataclass

from ChinaXivXiv.defines import END_FILEID

@dataclass
class Args:
    mongo: str = "mongodb://localhost:27017"
    """ mongodb://xxx:yy@zzz:1111 """
    task_provider: bool = False
    """ 定义为任务提供者，全局只能有一个 """
    end_fileid: int = END_FILEID
    """ 任务队列结束的*大概 id (任务提供者) 精度为 +- qos """
    qos: float = 3.0
    """ 每秒生成任务数 (任务提供者) """
    fileid_finder: bool = False
    """ 文件id嗅探 """
    file_downloader: bool = False
    """ 定义为文件下载者 """
    metadata_scraper: bool = False
    """ 定义为元数据获取者 """
    ia_uploader: bool = False
    """ 上传文件到 IA """


def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo",          type=str,   default=Args.mongo,         help=Args.mongo)
    parser.add_argument("--task_provider",  action="store_true",  default=False,    help=str(Args.task_provider))
    parser.add_argument("--end_fileid",     type=int,   default=Args.end_fileid,    help=str(Args.end_fileid))
    parser.add_argument("--qos",            type=float,   default=Args.qos,           help=str(Args.qos))
    parser.add_argument("--fileid_finder",  action="store_true",  default=False,    help=str(Args.fileid_finder))
    parser.add_argument("--file_downloader",action="store_true",  default=False,    help=str(Args.file_downloader))
    parser.add_argument("--metadata_scraper",action="store_true",  default=False,    help=str(Args.metadata_scraper))
    parser.add_argument("--ia_uploader",    action="store_true",  default=False,    help=str(Args.ia_uploader))
    return Args(**vars(parser.parse_args()))
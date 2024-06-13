import datetime
import os

import oaipmh_scythe.exceptions
import pymongo.errors
from tqdm import tqdm

import motor.motor_asyncio

from oaipmh_scythe import Scythe
import oaipmh_scythe.client
from oaipmh_scythe.models import Record
from oaipmh_scythe.utils import xml_to_dict

oaipmh_scythe.client.USER_AGENT = "ChinaXiv Archive Mirror Project/0.1.0 (STW; SaveTheWeb; +github.com/saveweb; saveweb@saveweb.org) (qos-rate-limit: 3q/s)"

class ChinaXivRecord(Record):
    def get_metadata(self):
        return xml_to_dict(
            self.xml.find('.//' + self._oai_namespace + 'metadata'),
            strip_ns=self._strip_ns
        )

# 生成每天的日期
def generate_dates(start_date: datetime.datetime, m: int):
    delta = datetime.timedelta(days=31)
    for _ in range(m):
        yield start_date.strftime("%Y-%m-%dT00:00:00Z"), (start_date + delta).strftime("%Y-%m-%dT00:00:00Z")
        start_date += delta


async def main():
    m_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGODB_URI"))
    db = m_client["chinaxiv"]
    collection = db["global_chinaxiv"]


    scythe = Scythe('https://global.chinaxiv.org/oaiapi/getdata')
    scythe.class_mapping["ListRecords"] = ChinaXivRecord

    for start, end in generate_dates(datetime.datetime(2024, 1, 1), 12):
        print(start, end)
        records = scythe.list_records(metadata_prefix='oai_dc', source="chinaxiv", startTime=start ,endTime=end)
        
        docs = []
        try:
            for record in records:
                print(record.header.identifier, record.header.datestamp, record.header.setSpecs)
                # print(record.metadata)

                docs.append({
                    "identifier": record.header.identifier,
                    "datestamp": record.header.datestamp,
                    "metadata": record.metadata
                })
        except oaipmh_scythe.exceptions.NoRecordsMatch:
            print("NoRecordsMatch")
            continue
        if not docs:
            continue
        print("inserting...")
        try:
            await collection.insert_many(docs, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            print(e.details)
            print("inserted")
        print("inserted")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
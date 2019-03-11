# This reads in the Binlog events from Master RDS Instance and puts into Kinesis stream
# This reads in specifically the courier-acquisition-replica endpoint and writes to Kinesis Stream
# Install boto3 and  mysql-replication on the EMR

import json
import boto3

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)

# This is a simple binlog reader using the mysql-replication library we are able to read in different events
# And put them into Kinesis Stream as Json events
# Example Binlog row that we write into Kinesis stream looks like below
# {"row": {"values": {"applicant_id": "eafcc8e9-950f-4b34-af6d-3ac85685126d", "contact_time": "LUNCH"}},
#  "schema": "applicants",
#  "table": "applicants_contact_time",
#  "type": "WriteRowsEvent"}


def main():
    kinesis = boto3.client("kinesis")

    stream = BinLogStreamReader(
        connection_settings={
            "host": "courier-acquisition-replica-bi-prod.cdgenoikdoe5.us-west-2.rds.amazonaws.com",
            "port": 3306,
            "user": "produser",
            "passwd": "***"},
        server_id=100,
        blocking=True,
        resume_stream=True,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    for binlogevent in stream:
        for row in binlogevent.rows:
            event = {
                "schema": binlogevent.schema,
                "table": binlogevent.table,
                "type": type(binlogevent).__name__,
                "row": row
            }

            kinesis.put_record(StreamName="DataTest", Data=json.dumps(event, sort_keys=True, default=str), PartitionKey="default")
            #print json.dumps(event, event, indent=4, sort_keys=True, default=str)


if __name__ == "__main__":
    main()

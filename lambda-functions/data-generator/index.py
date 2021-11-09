import boto3
          import os
          import uuid
          import random
          import json
          import time

          client = boto3.client("kinesis")
          stream_name = os.environ["KinesisStreamName"]

          def publish_records():
              records = []

              # Data source 1 transaction data version 1
              tx_old = {"version": 1, "txid":str(random.randrange(1,101)), "amount": random.random() * 100}
              records.append({'Data':bytes(json.dumps(tx_old),'utf-8'), 'PartitionKey': uuid.uuid4().hex})

              # Data source 1 transaction data version 2: txid renamed to transactionId & new source field added
              tx_new = {"version": 2, "transactionId":str(random.randrange(1,101)), "amount": random.random() * 100, "source": random.choice(['Web', 'Mobile', 'Store'])}
              records.append({'Data':bytes(json.dumps(tx_new),'utf-8'), 'PartitionKey': uuid.uuid4().hex})

              # Data source 2: Change data capture (CDC) use case. Pushing multiple table data to same stream
              table_1 = {"version": 1, "table":"Customer", "data":{"id": random.randrange(1,5),"name": random.choice(["John","Carlos","Mary","Nikki","Richard"]), "country": random.choice(["US","UK"])}}
              records.append({'Data':bytes(json.dumps(table_1),'utf-8'), 'PartitionKey': uuid.uuid4().hex})

              table_2 = {"version": 1, "table":"Order", "data":{"id": random.randrange(1,501), "customerId":random.randrange(1,5), "qty":random.randrange(1,5), "product":{"name":"Book "+str(random.randrange(1,500)), "price": random.uniform(10.0, 50.0)}  }}
              records.append({'Data':bytes(json.dumps(table_2),'utf-8'), 'PartitionKey': uuid.uuid4().hex})

              print(tx_old)
              print(tx_new)
              print(table_1)
              print(table_2)
              client.put_records(Records=records, StreamName=stream_name)

          def lambda_handler(event, context):
              for i in range(10):
                  publish_records()
                  time.sleep(3)

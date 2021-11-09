from __future__ import print_function
          import base64
          import json
          import datetime

          # Signature for all Lambda functions that user must implement
          def lambda_handler(firehose_records_input, context):

              # Create return value.
              firehose_records_output = {'records': []}


              for firehose_record_input in firehose_records_input['records']:
                  # Get user payload
                  payload = base64.b64decode(firehose_record_input['data'])
                  json_value = json.loads(payload)


                  # Create output Firehose record and add modified payload and record ID to it.
                  firehose_record_output = {}

                  table = "Transaction"
                  if "table" in json_value:
                      table = json_value["table"]

                  now = datetime.datetime.now()
                  partition_keys = {"table": table, "year": str(now.year), "month": str(now.month), "day": str(now.day)}

                  # Create output Firehose record and add modified payload and record ID to it.
                  firehose_record_output = {'recordId': firehose_record_input['recordId'],
                                            'data': firehose_record_input['data'],
                                            'result': 'Ok',
                                            'metadata': { 'partitionKeys': partition_keys }}

                  # Must set proper record ID
                  # Add the record to the list of output records.

                  firehose_records_output['records'].append(firehose_record_output)

              # At the end return processed records
              return firehose_records_output

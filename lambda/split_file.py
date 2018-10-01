import boto3
import importlib
import json
import pivot
import util


def create_payload(bucket, key, offsets, token, prefix, file_id=None, more=None):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket
        },
        "object": {
          "key": key,
        },
        "offsets": offsets,
        "extra_params": {
          "token": token,
          "prefix": prefix
        }
      }
    }]
  }
  if file_id is not None:
    payload["Records"][0]["s3"]["object"]["file_id"] = file_id
  if more is not None:
    payload["Records"][0]["s3"]["object"]["more"] = more

  return payload


def invoke(client, name, payload):
  response = client.invoke(
    FunctionName=name,
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def split_file(bucket_name, key, input_format, output_format, offsets, params):
  split_size = params["split_size"]

  s3 = params["s3"] if "s3" in params else boto3.resource("s3")
  client = params["client"] if "client" in params else boto3.client("lambda")

  if util.is_set(params, "ranges"):
    [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key)
  else:
    input_bucket = bucket_name
    input_key = key

  obj = s3.Object(input_bucket, input_key)

  more = True
  file_id = params["file_id"] - 1 if "file_id" in params else 0
  if not util.is_set(params, "adjust"):
    format_lib = importlib.import_module(params["format"])
    iterator_class = getattr(format_lib, "Iterator")
    iterator = iterator_class(obj, params["chunk_size"], offsets)

  while more:
    file_id += 1
    if util.is_set(params, "adjust"):
      offsets = {
        "offsets": [(file_id - 1) * split_size, min(obj.content_length, (file_id) * split_size) - 1],
        "adjust": True,
      }
      more = (offsets["offsets"][-1] != (obj.content_length - 1))
    else:
      offsets, more = iterator.nextOffsets()

    payload = create_payload(input_bucket, input_key, offsets, params["token"], output_format["prefix"], file_id, more)

    s3_params = payload["Records"][0]["s3"]

    if util.is_set(params, "ranges"):
      s3_params["extra_params"]["pivots"] = ranges

    if params["context"].get_remaining_time_in_millis() < 20*1000:
      s3_params["extra_params"]["prefix"] = input_format["prefix"]
      s3_params["object"]["key"] = key
      s3_params["extra_params"]["file_id"] = file_id
      s3_params["extra_params"]["id"] = file_id
      params["bucket_format"]["last"] = False

      util.invoke(client, params["name"], params, payload)
      return
    else:
      util.invoke(client, params["output_function"], params, payload)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  params["context"] = context
  m = util.run(bucket_name, key, params, split_file)
  del params["context"]
  util.show_duration(context, m, params)

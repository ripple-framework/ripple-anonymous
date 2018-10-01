import boto3
import json
import util


def run(key, params, input_format, output_format, offsets):
  util.print_read(input_format, key, params)
  s3 = boto3.resource("s3")
  input_format["prefix"] = params["match_prefix"]
  prefix = util.key_prefix(util.file_name(input_format))
  objects = util.get_objects(params["bucket"], prefix=prefix)
  assert(len(objects) == 1)
  species_key = objects[0].key
  object_key = key.replace("/tmp/", "")

  obj = s3.Object(params["bucket"], species_key)
  match = util.read(obj, 0, obj.content_length)

  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["bucket"],
        },
        "object": {
          "key": object_key,
        },
        "extra_params": {
          "token": params["token"],
          "prefix": output_format["prefix"],
          "species": util.parse_file_name(match)["suffix"]
        }
      }
    }]
  }

  client = boto3.client("lambda")
  response = client.invoke(
    FunctionName=params["output_function"],
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  return []

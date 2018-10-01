import boto3
import json
import pivot
import util


def map_file(bucket_name, key, input_format, output_format, offsets, params):
  client = boto3.client("lambda")
  util.print_read(input_format, key, params)

  if util.is_set(params, "ranges"):
    [bucket_name, key, ranges] = pivot.get_pivot_ranges(bucket_name, key)
    prefix = util.key_prefix(key)
    objects = util.get_objects(bucket_name, prefix=prefix)
    objects = list(set(map(lambda o: o.key, objects)))
  else:
    if "map_bucket_key_prefix" in params:
      objects = util.get_objects(params["map_bucket"], prefix=prefix)
      objects = list(set(map(lambda o: o.key, objects)))
    else:
      objects = util.get_objects(params["map_bucket"])
      if params["directories"]:
        objects = list(filter(lambda o: "/" in o.key, objects))
        objects = list(set(map(lambda o: o.key.split("/")[0], objects)))
      else:
        objects = list(set(map(lambda o: o.key, objects)))

  file_id = 0

  for i in range(len(objects)):
    obj = objects[i]
    file_id += 1
    target_file = obj

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name,
          },
          "object": {
            "file_id": file_id,
            "more": (i + 1) != len(objects)
          },
          "extra_params": {
            "token": params["token"],
            "target_bucket": params["map_bucket"],
            "target_file": target_file,
            "prefix": output_format["prefix"],
          }
        }
      }]
    }

    if params["input_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = key
      payload["Records"][0]["s3"]["extra_params"][params["bucket_key_value"]] = target_file
    elif params["bucket_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = target_file
      payload["Records"][0]["s3"]["extra_params"][params["input_key_value"]] = key
    else:
      raise Exception("Need to specify field for map key")

    if util.is_set(params, "ranges"):
      payload["Records"][0]["s3"]["extra_params"]["pivots"] = ranges
      payload["Records"][0]["s3"]["pivots"] = ranges

    util.invoke(client, params["output_function"], params, payload)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, map_file)
  util.show_duration(context, m, params)

import boto3
import importlib
import util


def find_match(bucket_name, key, input_format, output_format, offsets, params):
  util.print_read(input_format, key, params)
  [combine, keys] = util.combine_instance(bucket_name, key)
  if combine:
    print("Finding match")
    best_match = None
    match_score = 0
    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    s3 = boto3.resource("s3")

    keys.sort()
    with open(util.LOG_NAME, "a+") as f:
      for key in keys:
        obj = s3.Object(bucket_name, key)
        it = iterator(obj, params["chunk_size"])
        if params["find"] == "max sum":
          score = it.sum(params["identifier"])
        else:
          raise Exception("Not implemented", params["find"])

        print("key {0:s} score {1:d}".format(key, score))
        f.write("key {0:s} score {1:d}\n".format(key, score))
        if score > match_score:
          best_match = key
          match_score = score

    if best_match is None:
      best_match = keys[0]

    output_format["ext"] = "match"
    output_format["suffix"] = "match"
    file_name = util.file_name(output_format)
    util.write(input_format, bucket_name, file_name, str.encode(best_match), params)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, find_match)
  util.show_duration(context, m, params)

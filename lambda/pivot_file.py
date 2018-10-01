import boto3
import importlib
import util


def create_pivots(s3, sorted_input, params):
  max_identifier = int(sorted_input[-1][0] + 1)
  pivots = list(map(lambda p: p[0], sorted_input))
  num_bins = 2 * params["num_bins"]
  increment = int((len(sorted_input) + num_bins - 1) / num_bins)
  pivots = pivots[0::increment]
  if pivots[-1] == max_identifier - 1:
    pivots[-1] = max_identifier
  else:
    pivots.append(max_identifier)
  return pivots


def handle_pivots(bucket_name, key, input_format, output_format, offsets, params):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  if len(offsets) == 0:
    sorted_input = iterator.get(obj, 0, obj.content_length, params["identifier"])
  else:
    sorted_input = iterator.get(obj, offsets["offsets"][0], offsets["offsets"][-1], params["identifier"])
  sorted_input = sorted(sorted_input, key=lambda k: k[0])
  pivots = create_pivots(s3, sorted_input, params)

  output_format["ext"] = "pivot"
  pivot_key = util.file_name(output_format)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  util.write(input_format, params["bucket"], pivot_key, content, params)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, handle_pivots)
  util.show_duration(context, m, params)

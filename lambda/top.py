import boto3
import heapq
import importlib
import util


class Element:
  def __init__(self, identifier, value):
    self.value = value
    self.identifier = identifier

  def __lt__(self, other):
    return self.identifier < other.identifier


def find_top(bucket_name, key, input_format, output_format, offsets, params):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  if len(offsets) == 0:
    util.print_read(input_format, key, params)
  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  it = iterator(obj, offsets, params["chunk_size"])

  top = []
  more = True
  identifier = params["identifier"] if "identifier" in params else None
  while more:
    [values, more] = it.next(identifier=identifier)

    for value in values:
      heapq.heappush(top, Element(value[0], value[1]))
      if len(top) > params["number"]:
        heapq.heappop(top)

  values = list(map(lambda t: t.value, top))
  content = iterator.fromArray(obj, values, offsets)

  file_name = util.file_name(output_format)
  util.write(output_format, bucket_name, file_name, str.encode(content), params)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, find_top)
  util.show_duration(context, m, params)

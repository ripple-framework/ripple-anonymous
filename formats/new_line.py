import boto3
import iterator
import util


class Iterator(iterator.Iterator):
  IDENTIFIER = "\n"

  def __init__(self, obj, chunk_size, offsets={}):
    self.identifier = Iterator.IDENTIFIER
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    iterator.Iterator.__setup__(self, offsets)

  def fromArray(items, includeHeader=False):
    assert(not includeHeader)
    items = list(map(lambda item: item.strip(), items))
    content = Iterator.IDENTIFIER.join(items)
    return content

  def get(obj, start_byte, end_byte, identifier=""):
    assert(identifier == "")
    content = util.read(obj, start_byte, end_byte)
    items = list(content.split(Iterator.IDENTIFIER))
    return items

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    assert(not params["sort"])
    if "s3" in params:
      s3 = params["s3"]
    else:
      s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    with open(temp_name, "ab+") as f:
      for i in range(len(keys)):
        key = keys[i]
        bucket.download_fileobj(key, f)

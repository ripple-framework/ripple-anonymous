import boto3
import heapq
import iterator
import new_line
import util


def mapItems(items):
  return " ".join(list(map(lambda item: str(item), items)))


def mapLine(line):
  return ",".join(list(map(lambda items: mapItems(items), line)))


def getItem(items):
  return list(map(lambda item: float(item), items.split(" ")))


def getItems(line):
  i = line.split(",")
  items = list(map(lambda items: getItem(items), i))
  items[0] = i[0]
  return items


def getLines(content):
  lines = content.split(new_line.Iterator.IDENTIFIER)
  lines = list(filter(lambda line: len(line) > 0, lines))
  return list(map(lambda line: getItems(line), lines))


class Iterator(new_line.Iterator):
  def __init__(self, obj, chunk_size, offsets={}):
    self.identifier = new_line.Iterator.IDENTIFIER
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    iterator.Iterator.__setup__(self, offsets)

  def fromArray(obj, lines, offsets):
    assert(len(offsets["offsets"]) == 0)
    return new_line.Iterator.IDENTIFIER.join(list(map(lambda line: mapLine(line), lines)))

  def get(obj, start_byte, end_byte, identifier):
    content = util.read(obj, start_byte, end_byte)
    return getLines(content)

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    assert(not util.is_set(params, "sort"))
    if "s3" in params:
      s3 = params["s3"]
    else:
      s3 = boto3.resource("s3")

    top_scores = {}
    for i in range(len(keys)):
      key = keys[i]
      lines = getLines(s3.Object(bucket_name, key).get()["Body"].read().decode("utf-8"))
      for line in lines:
        s = line[0]
        if s not in top_scores:
          top_scores[s] = list(map(lambda x: [-1*x[0], x[1]], line[2:]))
          heapq.heapify(top_scores[s])
        else:
          for [score, c] in line[1:]:
            if len(top_scores[s]) < params["n"] or -1*score > top_scores[s][0][0]:
              heapq.heappush(top_scores[s], [-1*score, c])
            if len(top_scores[s]) > params["n"]:
              heapq.heappop(top_scores[s])

    with open(temp_name, "w+") as f:
      for s in top_scores.keys():
        line = s
        for [score, c] in top_scores[s]:
          line += ",{d} {c}".format(d=-1*score, c=c)
        f.write("{0:s}\n".format(line))

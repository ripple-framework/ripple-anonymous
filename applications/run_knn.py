import boto3
import classification
import heapq
import math
import util


def distance(pixel, clz):
  [px, py, pr, pg, pb] = pixel
  [cx, cy, cr, cg, cb, cc] = clz
  return math.sqrt((cr - pr) ** 2 + (cg - pg) ** 2 + (cb - pb) ** 2)


def run(file, params, input_format, output_format, offsets):
  s3 = boto3.resource("s3")
  train_obj = s3.Object(params["train_bucket"], params["train"])
  it = classification.Iterator(train_obj, 100000, offsets)

  lines = open(file).readlines()
  pixels = list(map(lambda line: list(map(lambda i: float(i), line.split(" "))), lines))

  top_scores = {}
  more = True
  while more:
    [clz, more] = it.next()
    for pixel in pixels:
      [px, py, pr, pg, pb] = pixel
      s = "{x} {y} {r} {g} {b}".format(x=px, y=py, r=pr, g=pg, b=pb)
      top_scores[s] = []
      for c in clz:
        if len(c) == 0:
          continue
        score = distance(pixel, c)
        if len(top_scores[s]) < params["n"] or -1*score > top_scores[s][0][0]:
          heapq.heappush(top_scores[s], [-1*score, c])
          if len(top_scores[s]) > params["n"]:
            heapq.heappop(top_scores[s])

  output_format["ext"] = "knn"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  with open(output_file, "w+") as f:
    for s in top_scores.keys():
      line = s
      for [score, c] in top_scores[s]:
        [cx, cy, cr, cg, cb, cc] = c
        line += ",{d} {c}".format(d=-1*score, c=cc)
      f.write("{0:s}\n".format(line))
  return [output_file]

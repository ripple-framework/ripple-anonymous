import inspect
import json
import os
import sys
import unittest
from tutils import S3, Bucket, Object, Context, Client

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/lambda")
import split_file
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\nG H I\nJ K L\nM N O\nP Q R\n"
object1 = Object("0/123.400000-13/1/1-0-suffix.new_line", content)
bucket1 = Bucket("bucket1", [object1])
log = Bucket("log", [])
input_format = util.parse_file_name(object1.key)
output_format = dict(input_format)
output_format["prefix"] = 1

params = {
  "test": True,
  "s3": S3([bucket1, log]),
  "ranges": False,
  "batch_size": 3,
  "chunk_size": 20,
  "token": 45,
  "format": "new_line",
  "output_function": "an-output-function",
  "bucket_format": dict(input_format),
  "file": "split_file",
  "log": "log",
  "split_size": 20,
  "adjust": True,
}


def get_payload(bucket_name, key, token, prefix, offsets=None):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket_name
        },
        "object": {
          "key": key,
        },
        "extra_params": {
          "token": token,
          "prefix": prefix,
        },
      }
    }]
  }

  if offsets is not None:
    payload["Records"][0]["s3"]["offsets"] = {
      "offsets": offsets,
      "adjust": True,
    }
  return payload


def get_invoke(name, bucket_name, key, token, prefix, offsets, file_id, more, bin=None):
  payload = get_payload(bucket_name, key, token, prefix, offsets)
  if bin is None:
    payload["Records"][0]["s3"]["object"]["file_id"] = file_id
    payload["Records"][0]["s3"]["object"]["more"] = more

  return {
    "name": name,
    "type": "Event",
    "payload": json.JSONEncoder().encode(payload),
  }


class SplitFunction(unittest.TestCase):
  def check_payload_equality(self, expected_invokes, actual_invokes):
    self.assertEqual(len(expected_invokes), len(actual_invokes))
    for i in range(len(expected_invokes)):
      expected_invokes[i]["payload"] = json.JSONDecoder().decode(expected_invokes[i]["payload"])
      actual_invokes[i]["payload"] = json.JSONDecoder().decode(actual_invokes[i]["payload"])
      self.assertDictEqual(expected_invokes[i], actual_invokes[i])

  def test_basic(self):
    p = dict(params)
    p["client"] = Client()
    p["context"] = Context(50*1000)
    client = p["client"]

    split_file.split_file(bucket1.name, object1.key, input_format, output_format, {}, p)
    self.assertEqual(len(client.invokes), 2)

    invoke1 = get_invoke("an-output-function", bucket1.name, object1.key, token=45, prefix=1, offsets=[0, 19], file_id=1, more=True)
    invoke2 = get_invoke("an-output-function", bucket1.name, object1.key, token=45, prefix=1, offsets=[20, 35], file_id=2, more=False)

    expected_invokes = [invoke1, invoke2]
    self.check_payload_equality(expected_invokes, client.invokes)

  def test_next_invoke_trigger(self):
    p = dict(params)
    p["client"] = Client()
    p["context"] = Context(1000)
    p["name"] = "split-file"
    client = p["client"]

    input_format = util.parse_file_name(object1.key)
    output_format = dict(input_format)
    output_format["prefix"] = 1
    split_file.split_file(bucket1.name, object1.key, input_format, output_format, {}, p)
    self.assertEqual(len(client.invokes), 1)

    payload = get_payload(bucket1.name, object1.key, token=45, prefix=0, offsets=[0,19])
    payload["Records"][0]["s3"]["object"]["file_id"] = 1
    payload["Records"][0]["s3"]["object"]["more"] = True
    payload["Records"][0]["s3"]["extra_params"]["file_id"] = 1
    payload["Records"][0]["s3"]["extra_params"]["id"] = 1

    invoke = {
      "name": "split-file",
      "type": "Event",
      "payload": json.JSONEncoder().encode(payload),
    }

    expected_invokes = [invoke]
    self.check_payload_equality(expected_invokes, client.invokes)

  def test_next_invoke(self):
    p = dict(params)
    p["client"] = Client()
    p["context"] = Context(50 * 1000)
    client = p["client"]
    p["prefix"] = 0
    p["id"] = 2
    p["object"] = {"more": True, "file_id": 2}

    util.run(bucket1.name, object1.key,  p, split_file.split_file)
    payload = get_payload(bucket1.name, object1.key, token=45, prefix=1, offsets=[20, 35])
    payload["Records"][0]["s3"]["object"]["file_id"] = 2
    payload["Records"][0]["s3"]["object"]["more"] = False
    invoke = {
      "name": "an-output-function",
      "type": "Event",
      "payload": json.JSONEncoder().encode(payload),
    }

    expected_invokes = [invoke]
    self.check_payload_equality(expected_invokes, client.invokes)


if __name__ == "__main__":
  unittest.main()

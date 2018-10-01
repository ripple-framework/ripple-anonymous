import inspect
import os
import sys
import unittest
import tutils
from unittest.mock import MagicMock
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util

object1 = Object("0/123.400000-13/1/1-0-suffix.txt")
object2 = Object("1/123.400000-13/1/1-0-suffix.txt")
object3 = Object("0/123.400000-13/1/2-1-suffix.txt")
object4 = Object("1/123.400000-13/1/2-1-suffix.txt")
object5 = Object("0/123.400000-13/1/3-1-suffix.txt")
object6 = Object("1/123.400000-13/1/3-1-suffix.txt")
bucket1 = Bucket("bucket1", [object1, object2, object3, object4])
bucket2 = Bucket("bucket2", [object5, object6])
log = Bucket("log", [object5, object6])
s3 = S3([bucket1, bucket2, log])
params = {
  "log": "log",
  "s3": s3,
  "test": True,
}


class FileNameMethods(unittest.TestCase):
  def test_file_name_parser(self):
    m = {
      "prefix": 0,
      "timestamp": 123.4,
      "nonce": 42,
      "bin": 12,
      "file_id": 3,
      "last": False,
      "suffix": "hello",
      "ext": "txt"
    }
    self.assertDictEqual(m, util.parse_file_name(util.file_name(m)))
    self.assertEqual("0/123.400000-13/1/1-0-suffix.txt", util.file_name(util.parse_file_name("0/123.400000-13/1/1-0-suffix.txt")))


class ObjectsMethods(unittest.TestCase):
  def test_get_objects(self):
    objects = util.get_objects("bucket1", prefix=None, params=params)
    self.assertEqual(len(objects), 4)
    self.assertTrue(tutils.equal_lists(objects, [object1, object2, object3, object4]))

    objects = util.get_objects("bucket1", prefix="0", params=params)
    self.assertEqual(len(objects), 2)
    self.assertTrue(tutils.equal_lists(objects, [object1, object3]))


class ExecutionMethods(unittest.TestCase):
  def test_run(self):
    params["prefix"] = 0
    params["request_id"] = "987abc"
    params["token"] = 1357
    params["object"] = {}
    params["file"] = "test_function"
    params["s3"] = s3

    # Call on object that doesn't have a log entry
    func = MagicMock()
    util.run("bucket1", "0/123.4-13/1/1-0-suffix.txt", params, func)
    self.assertTrue(func.called)

    # Call on object that does have a log entry
    func = MagicMock()
    util.run("bucket2", "0/123.4-13/1/3-1-suffix.txt", params, func)
    self.assertFalse(func.called)


if __name__ == "__main__":
  unittest.main()

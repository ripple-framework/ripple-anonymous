import inspect
import os
import sys
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import pivot

class PivotMethods(unittest.TestCase):
  def test_get_pivot_rangess(self):
    content = "bucket_name\nfile_name\n10\t15\t23\t37\t40"
    object1 = Object("pivot.pivot", content)
    bucket1 = Bucket("bucket1", [object1])
    s3 = S3([bucket1])
    params = {
      "test": True,
      "s3": s3,
    }

    [file_bucket, file_key, ranges] = pivot.get_pivot_ranges(bucket1.name, object1.key, params)
    self.assertEqual(file_bucket, "bucket_name")
    self.assertEqual(file_key, "file_name")
    expected_ranges = [{
      "range": [10, 15],
      "bin": 1,
    }, {
      "range": [15, 23],
      "bin": 2,
    }, {
      "range": [23, 37],
      "bin": 3,
    }, {
      "range": [37, 40],
      "bin": 4,
    }]
    self.assertEqual(ranges, expected_ranges)

  def test_combine(self):
    object1 = Object("test1.pivot", "bucket1\nkey1\n20\t25\t60\t61\t80")
    object2 = Object("test2.pivot", "bucket1\nkey2\n1\t40\t50\t63\t81")
    object3 = Object("test3.pivot", "bucket1\nkey3\n10\t12\t40\t41\t42")

    objects = [object1, object2, object3]
    bucket1 = Bucket("bucket1", objects)
    s3 = S3([bucket1])
    params = {
      "s3": s3,
      "batch_size": 1,
      "chunk_size": 10,
      "identifier": "",
      "tests": True,
      "sort": False,
    }
    keys = list(map(lambda o: o.key, objects))
    temp_name = "/tmp/ripple_test"
    # 1 10 12 20 25 40 40 41 42 50 60 61 63 80 81
    # *             *              *           *
    params["num_bins"] = 3
    pivot.Iterator.combine("bucket1", keys, temp_name, params)
    with open(temp_name) as f:
      self.assertEqual(f.read(), "bucket1\nkey3\n1.0\t40.0\t60.0\t81.0")

    # 1 10 12 20 25 40 40 41 42 50 60 61 63 80 81
    # *                                        *
    params["num_bins"] = 1
    pivot.Iterator.combine("bucket1", keys, temp_name, params)
    with open(temp_name) as f:
      self.assertEqual(f.read(), "bucket1\nkey3\n1.0\t81.0")
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

import inspect
import os
import sys
import unittest
from tutils import S3, Bucket, Object, Context, Client

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/lambda")
import combine_files
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\n"
object1 = Object("0/123.400000-13/1/1-0-suffix.new", content, 2)
content = "G H I\nJ K L\n"
object2 = Object("0/123.400000-13/1/2-1-suffix.new", content, 1)
bucket1 = Bucket("bucket1", [object1, object2])

content = "A B C\nD E F\n"
object4 = Object("0/123.400000-13/1/1-0-suffix.new", content, 0)
content = "G H I\nJ K L\n"
object5 = Object("0/123.400000-13/1/2-0-suffix.new", content, 2)
content = "M N O\bP Q R\n"
object6 = Object("0/123.400000-13/1/3-1-suffix.new", content, 1)
bucket2 = Bucket("bucket2", [object4, object5, object6])

log = Bucket("log", [])
input_format = util.parse_file_name(object1.key)
output_format = dict(input_format)
output_format["prefix"] = 1

params = {
  "test": True,
  "s3": S3([bucket1, bucket2, log]),
  "bucket": bucket1.name,
  "write_count": 0,
  "storage_class": "test",
  "ranges": False,
  "batch_size": 2,
  "chunk_size": 20,
  "token": 45,
  "format": "new_line",
  "sort": False,
  "bucket_format": dict(input_format),
  "file": "split_file",
  "log": "log",
}


class CombineFunction(unittest.TestCase):
  def test_basic(self):
    p = dict(params)
    p["client"] = Client()
    p["context"] = Context(50*1000)
    combine_files.combine(bucket1.name, object1.key, input_format, output_format, {}, p)
    self.assertEqual(len(bucket1.objects.objects), 3)
    combined_obj = bucket1.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1/1-1-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")

  def test_batches(self):
    p = dict(params)
    p["bucket"] = bucket2.name
    p["client"] = Client()
    p["context"] = Context(50*1000)
    input_format["file_id"] = 3
    combine_files.combine(bucket2.name, object6.key, input_format, output_format, {}, p)
    self.assertEqual(len(bucket2.objects.objects), 4)
    combined_obj = bucket2.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1/2-1-suffix.new")
    self.assertEqual(combined_obj.content, "M N O\bP Q R\n")

    input_format["file_id"] = 2
    combine_files.combine(bucket2.name, object5.key, input_format, output_format, {}, p)
    self.assertEqual(len(bucket2.objects.objects), 5)
    combined_obj = bucket2.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1/1-0-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")


if __name__ == "__main__":
  unittest.main()

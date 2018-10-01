import inspect
import os
import sys
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import tsv


class IteratorMethods(unittest.TestCase):
  def test_next_offsets(self):
    obj = Object("test.tsv", "A\tB\tC\na\tb\tc\n1\t2\t3\n")

    # Read everything in one pass
    it = tsv.Iterator(obj, 20)
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 6)
    self.assertEqual(offsets["offsets"][1], 17)

    # Requires multiple passes
    it = tsv.Iterator(obj, 10)
    [offsets, more] = it.nextOffsets()
    self.assertTrue(more)
    self.assertEqual(offsets["offsets"][0], 6)
    self.assertEqual(offsets["offsets"][1], 11)

    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 12)
    self.assertEqual(offsets["offsets"][1], 17)

  def test_next(self):
    obj = Object("test.tsv", "A\tB\tC\na\tb\tc\n1\t2\t3\n")

    # Requires multiple passes
    it = tsv.Iterator(obj, 10)
    [o, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(o, ["a\tb\tc", ""])

    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["1\t2\t3", ""])

    # Read everything in one pass
    it = tsv.Iterator(obj, 20)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["a\tb\tc", "1\t2\t3", ""])

  def test_combine(self):
    object1 = Object("test1.tsv", "A\tB\tC\na\tb\tc\n1\t2\t3\n")
    object2 = Object("test2.tsv", "D\tE\tF\nd\te\tf\n4\t5\t6\n")
    object3 = Object("test3.tsv", "G\tH\tI\ng\th\ti\n7\t8\t9\n")
    object4 = Object("test4.tsv", "J\tK\tL\nj\tk\tl\n10\t11\t12\n")
    objects = [object1, object2, object3, object4]
    bucket1 = Bucket("bucket1", objects)
    s3 = S3([bucket1])
    params = {
      "s3": s3,
      "chunk_size": 10,
      "identifier": "",
      "sort": False,
    }

    keys = list(map(lambda o: o.key, objects))
    temp_name = "/tmp/ripple_test"
    tsv.Iterator.combine("bucket1", keys, temp_name, params)
    with open(temp_name) as f:
      self.assertEqual(f.read(), "A\tB\tC\na\tb\tc\n1\t2\t3\nd\te\tf\n4\t5\t6\ng\th\ti\n7\t8\t9\nj\tk\tl\n10\t11\t12\n")
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

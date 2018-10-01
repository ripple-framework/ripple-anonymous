import inspect
import os
import sys
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import new_line


class IteratorMethods(unittest.TestCase):
  def test_next_offsets(self):
    obj = Object("test.new_line", "A B C\na b c\n1 2 3\n")

    # Read everything in one pass
    it = new_line.Iterator(obj, 20)
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 17)

    # Requires multiple passes
    it = new_line.Iterator(obj, 10)
    [offsets, more] = it.nextOffsets()
    self.assertTrue(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 5)

    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 6)
    self.assertEqual(offsets["offsets"][1], 17)

    # Specify offsets
    it = new_line.Iterator(obj, 10, {"offsets": [6, 11]})
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 6)
    self.assertEqual(offsets["offsets"][1], 11)

  def test_adjust(self):
    obj = Object("test.new_line", "A B C\na b c\n1 2 3\nD E F\nd e f\n")
    offsets = {
      "offsets": [8, 13],
      "adjust": True
    }

    it = new_line.Iterator(obj, 10, offsets)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["a b c"])

    # No adjustment needed
    offsets = {
      "offsets": [6, 11],
      "adjust": True
    }
    it = new_line.Iterator(obj, 10, offsets)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["a b c"])

    # Beginning of content
    offsets = {
      "offsets": [0, 7],
      "adjust": True
    }
    it = new_line.Iterator(obj, 10, offsets)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["A B C"])

    # Beginning of content
    offsets = {
      "offsets": [26, obj.content_length - 1],
      "adjust": True
    }
    it = new_line.Iterator(obj, 10, offsets)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["d e f", ""])

  def test_next(self):
    obj = Object("test.new_line", "A B C\na b c\n1 2 3\n")

    # Requires multiple passes
    it = new_line.Iterator(obj, 10)
    [o, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(o, ["A B C", ""])

    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["a b c", "1 2 3", ""])

    # Read everything in one pass
    it = new_line.Iterator(obj, 20)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["A B C", "a b c", "1 2 3", ""])

  def test_combine(self):
    object1 = Object("test1.new_line", "A B C\na b c\n1 2 3\n")
    object2 = Object("test2.new_line", "D E F\nd e f\n4 5 6\n")
    object3 = Object("test3.new_line", "G H I\ng h i\n7 8 9\n")
    object4 = Object("test4.new_line", "J K L\nj k l\n10 11 12\n")
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
    new_line.Iterator.combine("bucket1", keys, temp_name, params)
    with open(temp_name) as f:
      self.assertEqual(f.read(), object1.content + object2.content + object3.content + object4.content)
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

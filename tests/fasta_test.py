import inspect
import os
import sys
import unittest
from tutils import Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import fasta


class IteratorMethods(unittest.TestCase):
  def test_next_offsets(self):
    obj = Object("test.fasta", ">A\tB\tC\n>a\tb\tc\n>1\t2\t3\n")

    # Read everything in one pass
    it = fasta.Iterator(obj, 30)
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 20)

    # Requires multiple passes
    it = fasta.Iterator(obj, 8)
    [offsets, more] = it.nextOffsets()
    self.assertTrue(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 6)

    [offsets, more] = it.nextOffsets()
    self.assertTrue(more)
    self.assertEqual(offsets["offsets"][0], 7)
    self.assertEqual(offsets["offsets"][1], 13)

    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 14)
    self.assertEqual(offsets["offsets"][1], 20)

  def test_next(self):
    obj = Object("test.fasta", ">A\tB\tC\n>a\tb\tc\n>1\t2\t3\n")

    # Requires multiple passes
    it = fasta.Iterator(obj, 8)
    [o, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(o, [">A\tB\tC\n"])

    [o, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(o, [">a\tb\tc\n"])

    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, [">1\t2\t3\n"])

    # Read everything in one pass
    it = fasta.Iterator(obj, 30)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, [">A\tB\tC\n", ">a\tb\tc\n", ">1\t2\t3\n"])


if __name__ == "__main__":
  unittest.main()

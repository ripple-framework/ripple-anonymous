import iterator
import new_line
import util


class Iterator(new_line.Iterator):
  def __init__(self, obj, chunk_size, offsets={}):
    self.identifier = new_line.Iterator.IDENTIFIER
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    iterator.Iterator.__setup__(self, offsets)

  def fromArray(obj, items, offsets):
    assert(len(offsets["offsets"]) == 0)
    return new_line.Iterator.IDENTIFIER.join(list(map(lambda i: " ".join(i), items)))

  def get(obj, start_byte, end_byte, identifier):
    content = util.read(obj, start_byte, end_byte)
    lines = content.split(new_line.Iterator.IDENTIFIER)
    lines = list(filter(lambda line: len(line) > 0, lines))
    classifications = list(map(lambda line: list(map(lambda x: float(x), line.split(" "))), lines))
    return classifications

import iterator
import util


class Iterator(iterator.Iterator):
  IDENTIFIER = ">"

  def __init__(self, obj, chunk_size, offsets={}):
    self.identifier = Iterator.IDENTIFIER
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    self.indicator_at_beginning = True
    iterator.Iterator.__setup__(self, offsets)

  def fromArray(items, includeHeader=False):
    assert(not includeHeader)
    items = list(map(lambda item: item.strip(), items))
    content = Iterator.IDENTIFIER.join(items)
    return Iterator.IDENTIFIER + content

  def get(obj, start_byte, end_byte, identifier=""):
    assert(identifier == "")
    content = util.read(obj, start_byte, end_byte)
    items = content.split(Iterator.IDENTIFIER)
    items = filter(lambda item: len(item) > 0, items)
    items = list(map(lambda item: Iterator.IDENTIFIER + item, items))
    return items

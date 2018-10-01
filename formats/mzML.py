import boto3
import hashlib
import iterator
import re
import util
import xml.etree.ElementTree as ET


class Iterator(iterator.Iterator):
  INDEX_LIST_OFFSET_REGEX = re.compile("[\s\S]*<indexListOffset>(\d+)</indexListOffset>")
  OFFSET_REGEX = re.compile("<offset[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_COUNT_REGEX = re.compile('<spectrumList [\s\S]*count="(\d+)"')
  SPECTRUM_LIST_CLOSE_TAG = "</spectrumList>"
  INDEX_CHUNK_SIZE = 1000
  SPECTRUM_CLOSE_TAG = "</spectrum>"
  ID_REGEX = re.compile(".*scan=([0-9]+).*")
  XML_NAMESPACE = "http://psi.hupo.org/ms/mzml"

  def __init__(self, obj, chunk_size, offsets={}):
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    ET.register_namespace("", Iterator.XML_NAMESPACE)
    self.footer_offset = 235
    self.batch_size = int(chunk_size / 2500)
    self.remainder = ""
    if "header" in offsets:
      self.header_length = offsets["header"]["end"]
      self.offsets = offsets["offsets"]
      self.end_byte = self.offsets[-1]
      self.offsets = self.offsets[:-1]
      self.content_length = self.end_byte
      self.current_offset = self.content_length
      self.total_count = len(self.offsets)
    else:
      self.start_byte = None
      self.findOffsets()

  def findOffsets(self):
    end_byte = self.content_length
    start_byte = end_byte - self.footer_offset
    stream = util.read(self.obj, start_byte, end_byte)
    m = self.INDEX_LIST_OFFSET_REGEX.match(stream)
    assert(m is not None)
    self.spectra_list_offset = int(m.group(1))
    self.header_length = self.spectra_list_offset
    self.total_count = 0

    start_byte = self.spectra_list_offset
    end_byte = start_byte + self.INDEX_CHUNK_SIZE
    stream = util.read(self.obj, start_byte, end_byte)
    index = stream.find("<offset")
    if index != -1:
      self.spectra_list_offset += stream.find("<offset")
    self.current_offset = self.spectra_list_offset

    start_byte = self.spectra_list_offset - self.INDEX_CHUNK_SIZE
    end_byte = self.spectra_list_offset
    stream = util.read(self.obj, start_byte, end_byte)

    index = stream.rfind(Iterator.SPECTRUM_CLOSE_TAG)
    self.end_byte = start_byte + index + len(Iterator.SPECTRUM_CLOSE_TAG) - 1

    self.updateOffsets()
    if len(self.offsets) != 0:
      end_byte = self.offsets[0]
      self.header_length = end_byte - 1
      stream = util.read(self.obj, 0, end_byte)
      m = self.SPECTRUM_LIST_COUNT_REGEX.search(stream)
      assert(m is not None)
      self.total_count = int(m.group(1))

  def getCount(self):
    return self.total_count

  def more(self):
    return self.current_offset < self.content_length

  def nextOffsets(self):
    if self.content_length == 0 or (len(self.offsets) == 0 and not self.more()):
      return ({"offsets": []}, False)

    # Plus one is so we get end byte of value
    while len(self.offsets) < (self.batch_size + 1) and self.more():
      self.updateOffsets()

    offsets = self.offsets[:self.batch_size]
    self.offsets = self.offsets[self.batch_size:]

    if len(self.offsets) > 0:
      end = min(self.offsets[0], self.spectra_list_offset)
    else:
      end = self.spectra_list_offset
    o = {"offsets": [offsets[0], end - 1]}
    o["header"] = {"start": 0, "end": self.header_length}

    return (o, len(self.offsets) > 0 or self.more())

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(self.content_length, start_byte + self.chunk_size)
    stream = util.read(self.obj, start_byte, end_byte)
    stream = self.remainder + stream
    offset_regex = list(self.OFFSET_REGEX.finditer(stream))
    self.offsets += list(map(lambda r: int(r.group(1)), offset_regex))
    if len(offset_regex) > 0:
      regex_offset = offset_regex[-1].span(0)[1]
      stream = stream[regex_offset:]
      self.remainder = stream
    else:
      self.remainder = ""
    self.current_offset = end_byte + 1

  def getIdentifier(spectrum, identifier):
    if identifier == "mass":
      return Iterator.getMass(spectrum)
    elif identifier == "tic":
      return Iterator.getTIC(spectrum)
    else:
      raise Exception("Unknown identifier", identifier)

  def cvParam(spectrum, name):
    for cvParam in spectrum.iter("cvParam"):
      if cvParam.get("name") == name:
        return float(cvParam.get("value"))

  def getMass(spectrum):
    return Iterator.cvParam(spectrum, "base peak m/z")

  def getTIC(spectrum):
    return Iterator.cvParam(spectrum, "total ion current")

  def get(obj, start_byte, end_byte, identifier):
    content = util.read(obj, start_byte, end_byte)
    index = content.rfind(Iterator.SPECTRUM_LIST_CLOSE_TAG)
    if index != -1:
      content = content[:index]

    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = root.iter("spectrum")

    # Filter out MS1
    spectra = list(filter(lambda s: Iterator.cvParam(s, "ms level") == 2.0, spectra))

    if identifier:
      spectra = list(map(lambda s: (Iterator.getIdentifier(s, identifier), s), spectra))
    else:
      spectra = list(spectra)
    return spectra

  def header(obj, start, end, count):
    content = util.read(obj, start, end)
    m = Iterator.SPECTRUM_LIST_COUNT_REGEX.search(content)
    original = m.group(0)
    replacement = original.replace(m.group(1), str(count))
    content = content.replace(original, replacement)
    return content

  def fromArray(obj, spectra, offsets):
    content = Iterator.header(obj, offsets["header"]["start"], offsets["header"]["end"], len(spectra))
    offset = len(content)
    offsets = []

    for i in range(len(spectra)):
      xml = spectra[i]
      xml.set("index", str(i))
      m = Iterator.ID_REGEX.match(xml.get("id"))
      offsets.append((m.group(1), offset))
      spectrum = ET.tostring(xml).decode()
      offset += len(spectrum)
      content += spectrum

    content += "</spectrumList></run></mzML>\n"
    list_offset = len(content)
    content += '<indexList count="2">\n'
    content += '<index name="spectrum">\n'
    for offset in offsets:
      content += '<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(offset[0], offset[1])
    content += "</index>\n"
    content += "</indexList>\n"
    content += "<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset)
    content += "<fileChecksum>"

    content += str(hashlib.sha1(content.encode("utf-8")).hexdigest())
    content += "</fileChecksum>\n</indexedmzML>"
    return content

  def createContent(content):
    index = content.rindex(Iterator.SPECTRUM_CLOSE_TAG)
    content = content[:index + len(Iterator.SPECTRUM_CLOSE_TAG)]
    root = ET.fromstring("<data>" + content + "</data>")
    return str.encode(Iterator.fromArray(list(root.iter("spectrum"))))

  def endByte(self):
    return self.end_byte

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    iterators = []
    count = 0
    s3 = boto3.resource("s3")

    for key in keys:
      obj = s3.Object(bucket_name, key)
      iterator = Iterator(obj, params["chunk_size"], {})
      iterators.append(iterator)
      count += iterator.getCount()

    header = Iterator.header(obj, 0, iterator.header_length, count)

    with open(temp_name, "w+") as f:
      content = header
      f.write(content)
      offset = len(content)
      offsets = []
      index = 0

      for iterator in iterators:
        more = True
        while more:
          [spectra, more] = iterator.next(identifier=False)
          content = ""
          for i in range(len(spectra)):
            xml = spectra[i]
            xml.set("index", str(index))
            offsets.append((xml.get("id"), offset))
            spectrum = ET.tostring(xml).decode()
            offset += len(spectrum)
            content += spectrum
            index += 1
          f.write(content)

      content = "</spectrumList></run></mzML>\n"
      list_offset = len(content) + offset
      content += '<indexList count="2">\n'
      content += '<index name="spectrum">\n'
      for offset in offsets:
        content += '<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(offset[0], offset[1])
      content += "</index>\n"
      content += "</indexList>\n"
      content += "<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset)
      content += "<fileChecksum>"
      content += "</fileChecksum>\n</indexedmzML>"
      f.write(content)

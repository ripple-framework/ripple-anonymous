from PIL import Image
import os
import util


def setup_file(bin_id, output_format, output_files):
  output_format["bin"] = bin_id
  temp_file = "/tmp/{0:s}".format(util.file_name(output_format))
  dir_name = os.path.dirname(temp_file)
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)
  output_files.append(temp_file)
  f = open(temp_file, "w+")
  return f


def run(file, params, input_format, output_format, offsets):
  im = Image.open(file)
  px = im.load()
  width, height = im.size
  output_format["ext"] = "pixel"

  bin_id = output_format["bin"]
  count = 0
  output_files = []
  f = setup_file(bin_id, output_format, output_files)

  for y in range(height):
    for x in range(width):
      pixel = px[x, y]
      f.write("{x} {y} {r} {g} {b}\n".format(x=x, y=y, r=pixel[0], g=pixel[1], b=pixel[2]))
      count += 1
      if count == params["batch_size"]:
        f.close()
        bin_id += 1
        f = setup_file(bin_id, output_format, output_files)
        count = 0

  f.close()
  return output_files

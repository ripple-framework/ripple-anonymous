import os
import subprocess
import util


def run(file, params, input_format, output_format, offsets):
  if params["action"] == "decompress":
    assert("ArInt" in file)
    for f in ["outfileChrom", "outfileName"]:
      input_format["suffix"] = f
      file_path = util.download(params["bucket"], util.file_name(input_format))
      os.rename(file_path, "/tmp/test_{0:s}-0".format(f))
    input_name = "/tmp/test_outfileArInt-0"
  else:
    input_name = "/tmp/input"

  os.rename(file, input_name)

  util.download(params["program_bucket"], "output")
  subprocess.call("chmod 755 /tmp/output", shell=True)
  output_dir = "/tmp/methyl-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    params["action"],
    input_name,
    output_dir
  ]

  command = "cd /tmp; ./output {0:s}".format(" ".join(arguments))
  util.check_output(command)

  output_files = []
  if params["action"] == "decompress":
    result_dir = output_dir
  else:
    result_dir = "{0:s}/compressed_input".format(output_dir)

  for subdir, dirs, files in os.walk(result_dir):
    if params["action"] == "decompressed":
      assert(len(files) == 1)

    for f in files:
      if params["action"] == "compress":
        output_format["suffix"] = f.split("_")[-1].split("-")[0]
      else:
        output_format["suffix"] = "decompressed"

      output_file = "/tmp/{0:s}".format(util.file_name(output_format))
      if params["action"] == "compress":
        os.rename("{0:s}/compressed_input/{1:s}".format(output_dir, f), output_file)
      else:
        os.rename("{0:s}/{1:s}".format(output_dir, f), output_file)
      output_files.append(output_file)

  return output_files

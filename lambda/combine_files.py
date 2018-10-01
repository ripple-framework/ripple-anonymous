import importlib
import os
import util


def combine(bucket_name, key, input_format, output_format, offsets, params):
  util.print_read(input_format, key, params)

  batch_size = params["batch_size"] if "batch_size" in params else 1
  file_id = int((input_format["file_id"] + batch_size - 1) / batch_size)

  # TODO: Fix.
  if "batch_size" in params:
    output_format["file_id"] = file_id
  else:
    output_format["file_id"] = input_format["bin"]
  output_format["bin"] = 1
  util.make_folder(output_format)

  [combine, keys, last] = util.combine_instance(bucket_name, key, params)
  if combine:
    if "batch_size" in params:
      output_format["last"] = last
    else:
      output_format["last"] = (output_format["file_id"] == params["num_bins"])
    msg = "Combining TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d}"
    msg = msg.format(input_format["timestamp"], input_format["nonce"], input_format["bin"], input_format["file_id"])
    print(msg)

    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    file_name = util.file_name(output_format)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    iterator.combine(bucket_name, keys, temp_name, params)
    f = open(temp_name, "rb")
    util.write(input_format, params["bucket"], file_name, f, params)
    f.close()
    os.remove(temp_name)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, combine)
  util.show_duration(context, m, params)

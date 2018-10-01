import boto3
import importlib
import util


def run_application(bucket_name, key, input_format, output_format, offsets, params):
  s3 = boto3.resource('s3')

  if len(offsets) == 0:
    temp_file = util.download(bucket_name, key)
  else:
    obj = s3.Object(bucket_name, key)
    format_lib = importlib.import_module(params["format"])
    iterator_class = getattr(format_lib, "Iterator")
    iterator = iterator_class(obj, 0, offsets)
    temp_file = "/tmp/{0:s}".format(key)
    with open(temp_file, "w") as f:
      f.write(util.read(obj, iterator.current_offset, iterator.content_length))

  application_lib = importlib.import_module(params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(temp_file, params, input_format, output_format, offsets)

  for output_file in output_files:
    p = util.parse_file_name(output_file.replace("/tmp/", ""))
    if p is None:
      index = output_file.rfind(".")
      ext = output_file[index+1:]
      output_format["ext"] = ext
      new_key = util.file_name(output_format)
    else:
      new_key = util.file_name(p)

    util.write(input_format, params["bucket"], new_key, open(output_file, "rb"), params)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, run_application)
  util.show_duration(context, m, params)

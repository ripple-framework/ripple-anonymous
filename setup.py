import argparse
import botocore
import json
import os
import shutil
import subprocess
import util


def upload_function_code(client, zip_file, name, p, create):
  zipped_code = open(zip_file, "rb").read()
  fparams = p["functions"][name]
  if create:
    response = client.create_function(
      FunctionName=name,
      Runtime="python3.6",
      Role="arn:aws:iam::{0:d}:role/{1:s}".format(p["account"], p["role"]),
      Handler="{0:s}.handler".format(fparams["file"]),
      Code={
        "ZipFile": zipped_code
      },
      Tags={
        "Application": p["tag"],
      },
      Timeout=p["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 201)

  else:
    response = client.update_function_code(
      FunctionName=name,
      ZipFile=zipped_code
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    response = client.tag_resource(
      Resource="arn:aws:lambda:{0:s}:{1:d}:function:{2:s}".format(p["region"], p["account"], name),
      Tags={
        "Application": p["tag"],
      }
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 204)
    response = client.update_function_configuration(
      FunctionName=name,
      Timeout=p["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    try:
      client.remove_permission(
        FunctionName=name,
        StatementId=name + "-" + p["bucket"]
      )
    except Exception as e:
      print("Cannot remove permissions for", name, p["bucket"])


def create_parameter_files(zip_directory, function_name, params):
  files = []
  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    if pparams["name"] == function_name:
      p = {**pparams, **params["functions"][function_name]}
      for value in ["timeout", "num_bins", "bucket", "storage_class", "log", "scheduler"]:
        if value in params:
          p[value] = params[value]
      name = "{0:d}.json".format(i)
      json_path = "{0:s}/{1:s}".format(zip_directory, name)
      f = open(json_path, "w")
      f.write(json.dumps(p))
      f.close()
      files.append(name)
  return files


def copy_file(directory, file_path):
  index = file_path.rfind("/")
  file_name = file_path[index + 1:]
  shutil.copyfile(file_path, "{0:s}/{1:s}".format(directory, file_name))
  return file_name


def upload_functions(client, params):
  zip_directory = "lambda_dependencies"
  zip_file = "lambda.zip"

  response = client.list_functions()
  functions = set(list(map(lambda f: f["FunctionName"], response["Functions"])))

  if os.path.isdir(zip_directory):
    shutil.rmtree(zip_directory)

  for name in params["functions"]:
    fparams = params["functions"][name]
    files = []
    os.makedirs(zip_directory)
    if "knn" in params["folder"]:
      for folder, subdir, ff in os.walk("libraries"):
        if folder.count("/") == 0:
          for d in subdir:
            shutil.copytree("libraries/" + d, zip_directory + "/" + d)
            files.append(d)
    file = "{0:s}.py".format(fparams["file"])
    shutil.copyfile("lambda/{0:s}".format(file), "{0:s}/{1:s}".format(zip_directory, file))
    files.append(file)
    for file in ["formats/iterator.py", "formats/pivot.py", "util.py"]:
      files.append(copy_file(zip_directory, file))

    if "application" in fparams:
      files.append(copy_file(zip_directory, "applications/{0:s}.py".format(fparams["application"])))

    if "format" in fparams:
      form = fparams["format"]
      if "dependencies" in params and form in params["dependencies"]["formats"]:
        for file in params["dependencies"]["formats"][form]:
          files.append(copy_file(zip_directory, file))
      files.append(copy_file(zip_directory, "formats/{0:s}.py".format(form)))

    files += create_parameter_files(zip_directory, name, params)
    os.chdir(zip_directory)
    subprocess.call("zip -r ../{0:s} {1:s}".format(zip_file, " ".join(files)), shell=True)
    os.chdir("..")

    upload_function_code(client, zip_file, name, params, name not in functions)
    os.remove(zip_file)
    shutil.rmtree(zip_directory)


def setup_notifications(client, bucket, config):
  response = client.put_bucket_notification_configuration(
      Bucket=bucket,
      NotificationConfiguration=config
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)


def clear_triggers(client, bucket, params):
  setup_notifications(client, bucket, {})


def create_bucket(client, bucket_name, params):
  try:
    client.create_bucket(
      ACL="public-read-write",
      Bucket=bucket_name,
      CreateBucketConfiguration={
        "LocationConstraint": params["region"],
      }
    )
  except botocore.exceptions.ClientError as ex:
    if "BucketAlreadyOwnedByYou" not in str(ex):
      raise ex

  client.put_bucket_tagging(
    Bucket=bucket_name,
    Tagging={
      "TagSet": [{
        "Key": "Application",
        "Value": params["tag"],
      }]
    }
  )


def function_arns(params):
  name_to_arn = {}
  client = util.lambda_client(params)
  for function in client.list_functions()["Functions"]:
    name_to_arn[function["FunctionName"]] = function["FunctionArn"]
  return name_to_arn


def setup_triggers(params):
  name_to_arn = function_arns(params)

  prefixes = {}
  for name in params["functions"]:
    prefixes[name] = []

  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    if i == 0 or "output_function" not in params["pipeline"][i - 1]:
      prefixes[pparams["name"]].append(i)

  client = util.setup_client("s3", params)
  lambda_client = util.lambda_client(params)
  configurations = []
  for name in params["functions"]:
    args = {
      "FunctionName": name,
      "StatementId": name + "-" + params["bucket"],
      "Action": "lambda:InvokeFunction",
      "Principal": "s3.amazonaws.com",
      "SourceAccount": str(params["account"]),
      "SourceArn": "arn:aws:s3:::{0:s}".format(params["bucket"]),
    }
    lambda_client.add_permission(**args)

    for prefix in prefixes[name]:
      configurations.append({
        "LambdaFunctionArn": name_to_arn[name],
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{"Name": "prefix", "Value": "{0:d}/".format(prefix)}]
          }
        }
      })

      if "suffix" in params["functions"][name]:
        configurations[-1]["Filter"]["Key"]["FilterRules"].append(
          {"Name": "suffix", "Value": params["functions"][name]["suffix"]}
        )

  config = {
    "LambdaFunctionConfigurations": configurations,
  }
  setup_notifications(client, params["bucket"], config)


def setup(params):
  s3 = util.setup_client("s3", params)
  create_bucket(s3, params["bucket"], params)
  create_bucket(s3, params["log"], params)
  clear_triggers(s3, params["bucket"], params)

  client = util.lambda_client(params)
  upload_functions(client, params)
  if not util.is_set(params, "scheduler"):
    setup_triggers(params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  setup(params)


if __name__ == "__main__":
  main()

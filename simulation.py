import argparse
import benchmark
import boto3
import os
from dateutil import parser
import json
import numpy as np
import queue
import random
from scipy import special
import setup
import scheduler
import threading
import time
import util


DATE_INDEX = 6
BINS = 60 * 60


lock = threading.Lock()


class Request(threading.Thread):
  def __init__(self, thread_id, time, request_queue, params, client, scheduler_queue, task, wait=False):
    super(Request, self).__init__()
    self.time = time
    self.params = dict(params)
    self.thread_id = thread_id
    self.duration = 0
    self.upload_duration = 0
    self.failed_attempts = 0
    self.alive = True
    self.params["input_bucket"] = self.params["bucket"]
    self.client = client
    self.wait = wait
    self.request_queue = request_queue
    self.scheduler_queue = scheduler_queue
    self.task = task

  def run(self):
    print("Thread", self.thread_id, "Sleeping for", self.time)
    time.sleep(self.time)
    if self.params["model"] == "ec2":
      benchmark.process_params(self.params)
      benchmark.process_iteration_params(self.params, 1)
      benchmark.upload_input(self.params)[1]
      # stats = benchmark.run_ec2_script(self.client, self.params)
    else:
      while not self.request_queue.empty():
        try:
          item = self.request_queue.get(timeout=0)
          if self.task == "clear" or self.task == "parse":
            parts = item.split("-")
            benchmark.process_params(self.params)
            benchmark.process_iteration_params(self.params, 1)
            self.params["now"] = float(parts[0])
            self.params["nonce"] = int(parts[1])
            if self.task == "clear":
              print("Thread {0:d}: Clearing logs".format(self.thread_id))
              benchmark.clear_buckets(self.params)
            else:
              print("Thread {0:d}: Parsing logs".format(self.thread_id))
              stats = benchmark.parse_logs(self.params, self.params["now"], 0, 0)
              dir_path = "results/{0:s}/{1:s}/{2:f}-{3:d}".format(self.params["folder"], self.params["input_name"], self.params["now"], self.params["nonce"])
              os.makedirs(dir_path)
              with open("{0:s}/stats".format(dir_path), "w+") as f:
                f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))
          else:
            self.params["input_name"] = item
            self.params["input_bucket"] = self.params["bucket"]
            self.params["stats"] = False
            if self.wait:
              benchmark.run(self.params, self.thread_id)
            else:
              benchmark.process_params(self.params)
              benchmark.process_iteration_params(self.params, 1)
              print("Thread {0:d}: Processing file {1:s}".format(self.thread_id, self.params["input_name"]))
              [upload_timestamp, upload_duration] = benchmark.upload_input(self.params, self.thread_id)
              payload = scheduler.payload(self.params["bucket"], self.params["key"])
              self.scheduler_queue.put(scheduler.Item("fifo", upload_timestamp, 0, self.thread_id, payload))
        except queue.Empty as e:
          pass
      # payload = scheduler.payload(self.params["bucket"], self.params["key"])
      # self.queue.put(scheduler.Item(self.scheduler, util.parse_file_name(self.params["key"])["timestamp"], 0, self.thread_id, payload))
      # [upload_duration, duration, failed_attempts] = benchmark.run(self.params, self.thread_id)
      # self.upload_duration = upload_duration
      # self.duration = duration
      # self.failed_attempts = failed_attempts
      # print("Thread {0:d}: Done in {1:f}".format(self.thread_id, duration))
      # msg = "Thread {0:d}: Upload Duration {1:f}. Duration {2:f}. Failed Attempts {3:f}"
      # msg = msg.format(self.thread_id, self.upload_duration, self.duration, self.failed_attempts)
      # print(msg)
      # stats = benchmark.parse_logs(self.params, self.params["now"] * 1000, self.upload_duration, self.duration)

      # dir_path = "results/{0:s}/{1:f}-{2:d}".format(self.params["folder"], self.params["now"], self.params["nonce"])
      # os.makedirs(dir_path)
      # with open("{0:s}/stats".format(dir_path), "w+") as f:
      #   f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))
      # benchmark.clear_buckets(self.params)

      # self.alive = False


def parse_csv(file_name, num_requests):
  datetimes = {}
  f = open(file_name)
  lines = f.readlines()[1:]
  for line in lines:
    dt = line.split(",")[DATE_INDEX]
    dt = dt[1:-2]
    parts = dt.split(" ")
    if len(parts) != 2:
      continue
    date = parser.parse(dt)

    if parts[0] not in datetimes:
      datetimes[parts[0]] = []
    datetimes[parts[0]].append(date.hour * 60 * 60 + date.minute * 60 + date.second)

  for date in datetimes:
    if len(datetimes[date]) == num_requests:
      return datetimes[date]


def create_request_distribution(distribution, args):
  random.seed(0)
  max_concurrency = 200
  time_range = 60 * 60  # 1 hour
  requests = []
  if distribution in ["uniform", "zipfian"]:
    interval = 60  # Request every 60 seconds
    #  num_intervals = 10
  else:
    interval = 7 * 60
    #  num_intervals = int((time_range + interval) / interval)

  if distribution == "uniform":
    num_requests = 1
    for i in range(0, time_range + 1, interval):
      requests += [i] * num_requests
  elif distribution == "zipfian":
    # https://stackoverflowcom/questions/43601074/zipf-distribution-how-do-i-measure-zipf-distribution-using-python-numpy.
    x = np.arange(1, 61)
    a = 2.
    y = x**(-a) / special.zetac(a) * max_concurrency
    y = list(map(lambda i: int(i), list(y)))
    random.shuffle(y)
    print(y)
    for i in range(len(y)):
      requests += [i * interval] * y[i]
  elif distribution == "bursty":
    interval = 1200  # Every 20 minutes
    for i in range(interval, time_range + interval, interval):
      requests += [i] * 10
      #requests += [i] * 100#max_concurrency
  elif distribution == "concurrent":
    for i in range(args.concurrency):
      requests.append(args.delay * i)

  return requests


class Second(threading.Thread):
  def __init__(self, s, params):
    super(Second, self).__init__()
    self.s = s
    self.params = params

  def run(self):
    self.s.wait(self.params["num_output"])


def deadline(params):
  folder = params["folder"]
  with open("results/{0:s}/params".format(folder), "w+") as f:
    f.write(json.dumps(params, indent=4, sort_keys=True))

  setup.setup(params)
  benchmark.process_params(params)
  benchmark.process_iteration_params(params, 1)

  threads = []
  client = None

  params["scheduler"] = True
  deadline_params = dict(params)
  deadline_params["stats"] = False
  m = {
    "prefix": "0",
    "timestamp": deadline_params["now"],
    "nonce": deadline_params["nonce"],
    "bin": 1,
    "file_id": 1,
    "continue": False,
    "suffix": "tide",
    "last": True,
    "ext": deadline_params["ext"]
  }
  deadline_params["key"] = util.file_name(m)
  [upload_timestamp, upload_duration] = benchmark.upload_input(deadline_params, 0)
  sleep = 30

  q = queue.Queue()
  q.put(params["input_name"])
  threads.append(Request(1, sleep, q, dict(params), client, wait=True))
  threads[0].start()
  s = scheduler.Scheduler("fifo", deadline_params, run=False)
  q = s.setup()
  payload = scheduler.payload(deadline_params["bucket"], deadline_params["key"])
  payload["continue"] = True
  q.put(scheduler.Item("fifo", upload_duration, 0, 0, payload))
  second = Second(s, deadline_params)
  second.start()
  threads[-1].join()
  print("Restarting first job")
  s.run = True
  second.join()
#  s.wait(deadline_params["num_output"])


def run(args, params):
  session = boto3.Session(
           aws_access_key_id=params["access_key"],
           aws_secret_access_key=params["secret_key"],
           region_name=params["region"]
  )
  s3 = session.resource("s3")
  if "sample_bucket" in params:
    file_names = list(map(lambda o: o.key, s3.Bucket(params["sample_bucket"]).objects.all()))
  else:
    file_names = [params["input_name"]]

  folder = params["folder"]
  with open("results/{0:s}/params".format(folder), "w+") as f:
    f.write(json.dumps(params, indent=4, sort_keys=True))

  task = "run"
  if params["model"] == "lambda" and task == "run":
    setup.setup(params)

  requests = create_request_distribution(args.distribution, args)
  client = None
  if params["model"] == "ec2":
    benchmark.process_params(params)
    benchmark.process_iteration_params(params, 1)
    create_stats = benchmark.create_instance(params)
    instance = create_stats["instance"]
    ec2 = create_stats["ec2"]
    initiate_stats = benchmark.initiate_instance(ec2, instance, params)
    client = initiate_stats["client"]
    benchmark.setup_instance(client, params)

  i = 0
  threads = []
  schedule = "fifo"
  s = scheduler.Scheduler(schedule, params)
  scheduler_queue = s.setup()
  request_queue = queue.Queue()
  num_requests = len(requests)

  if task == "clear" or task == "parse":
    for obj in util.s3(params).Bucket(params["bucket"]).objects.filter(Prefix="0/"):
      request_queue.put(obj.key.split("/")[1])
  else:
    for i in range(len(requests)):
      request_queue.put(file_names[i % len(file_names)])

  num_threads = 300
  for i in range(num_threads):
    threads.append(Request(i, 0, request_queue, params, client, scheduler_queue, task))
    threads[-1].start()

  s.wait(num_requests)
  for thread in threads:
    thread.join()
  #  threads = []
#    if len(requests) > 0:
#      temp_time = current_time
#      current_time = requests[0]
#      time.sleep(current_time - temp_time)

  #  stats = benchmark.parse_logs(thread.params, 0, 0, 0)
  #  dir_path = "results/{0:s}/{1:f}-{2:d}".format(thread.params["folder"], thread.params["now"], thread.params["nonce"])
  #  os.makedirs(dir_path)
  #  with open("{0:s}/stats".format(dir_path), "w+") as f:
  #    f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))
  #  benchmark.clear_buckets(thread.params)

  print("Processed {0:d} requests".format(num_requests))
  #if params["model"] == "ec2":
  #  benchmark.terminate_instance(instance, client, params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--distribution", type=str, help="Distribution to use")
  parser.add_argument("--concurrency", type=int, help="Number of concurrent instances to run")
  parser.add_argument('--folder', type=str, help="Folder to store results in")
  parser.add_argument('--delay', type=int, default=0, help="Default delay for concurrent execution")
  parser.add_argument('--deadline', action="store_true", default=False, help="Simulate deadline")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["setup"] = False
  params["iterations"] = 1
  if len(args.folder) > 0:
    params["folder"] = args.folder

  if args.deadline:
    deadline(params)
  else:
    run(args, params)


if __name__ == "__main__":
  main()

import subprocess
import util


def run(file, params, input_format, output_format, offsets):
  util.download(params["program_bucket"], "ssw_test")
  subprocess.call("chmod 755 /tmp/ssw_test", shell=True)

  target_fasta = params["target"]
  output_format["ext"] = "blast"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))

  util.download(params["target_bucket"], target_fasta)

  command = "cd /tmp; ./ssw_test -p {0:s} {1:s} > {2:s}".format(target_fasta, file, output_file)
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  return [output_file]

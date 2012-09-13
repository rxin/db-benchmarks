#!/usr/bin/python

# Used to run dbgen in parallel and store the output to HDFS.
# This should be used together with a parallel ssh tool or Spark or Hadoop.
#
# Author: Reynold Xin
# Email: rxin [at] cs.berkeley.edu
#

"""
To run this distributed in Spark:

/root/spark/spark-shell

val num_tasks = 10
import scala.sys.process._
sc.parallelize(1 to num_tasks, num_tasks).map { partition =>
  val cmd = "/root/db-benchmarks/parallel-dbgen.py -s 10 -l /mnt/tpch10g -h /tpch10g -n %d -p %d".format(
    num_tasks, partition)
  cmd !!
}.collect()

"""

import getopt
import os
import time
import socket
import subprocess
import sys

SLAVE_FILE = "/root/ephemeral-hdfs/conf/slaves"
HADOOP_EXECUTABLE = "/root/ephemeral-hdfs/bin/hadoop"


def usage():
  print __file__


def generate_data_to_hdfs(
  local_output, hdfs_output, partition, scale_factor, num_parts):
  """Generate data using dbgen and save it to HDFS."""

  dbgen_path = os.path.dirname(os.path.abspath(__file__)) + "/tpch-dbgen"

  # Generate the data to local_output
  try:
    os.makedirs(local_output)
    print "created directory: %s" % local_output
  except:
    pass

  print "chdir: %s" % local_output
  os.chdir(local_output)  # dbgen only outputs to current directory
  cmd = "%s/dbgen -b %s/dists.dss -v -f -s %.1f -S %d -C %d" % (
    dbgen_path, dbgen_path, scale_factor, partition, num_parts)
  execute(cmd)

  # Copy the data from local_output to hdfs_output
  if partition == 1:
    # master: copy the two non-partitioned tables.
    copy_local_file_to_hdfs("nation.tbl", "%s/nation/nation.tbl" % hdfs_output)
    copy_local_file_to_hdfs("region.tbl", "%s/region/region.tbl" % hdfs_output)

  # master & slave: copy all partitioned tables.
  copy_partitioned_table(hdfs_output, "supplier", partition)
  copy_partitioned_table(hdfs_output, "part", partition)
  copy_partitioned_table(hdfs_output, "partsupp", partition)
  copy_partitioned_table(hdfs_output, "orders", partition)
  copy_partitioned_table(hdfs_output, "lineitem", partition)
  copy_partitioned_table(hdfs_output, "customer", partition)


def execute(cmd):
  print cmd
  subprocess.call(cmd, shell=True)


def copy_partitioned_table(hdfs_output, table_name, partition):
  local_file_name = "%s.tbl.%d" % (table_name, partition)
  hdfs_file_name = "%s/%s/%s" % (hdfs_output, table_name, local_file_name)
  copy_local_file_to_hdfs(local_file_name, hdfs_file_name)


def copy_local_file_to_hdfs(local_path, hdfs_path):
  execute("%s fs -copyFromLocal %s %s" % (
    HADOOP_EXECUTABLE, local_path, hdfs_path))


def main():

  try:
    opts, args = getopt.getopt(sys.argv[1:], "s:p:n:l:h:",
      ["scale=", "part=", "num-parts=", "local-output=", "hdfs-output="])
  except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit()

  local_output = None
  hdfs_output = None
  num_parts = None
  scale_factor = None
  partition = None

  for o, a in opts:
    if o in ("-l", "--local-output"):
      local_output = a
    elif o in ("-h", "--hdfs-output"):
      hdfs_output = a
    elif o in ("-s", "--scale"):
      try:
        scale_factor = float(a)
      except:
        print "-s or --scale expects a number."
    elif o in ("-p", "--part"):
      partition = a
    elif o in ("-n", "--num-parts"):
      try:
        num_parts = int(a)
      except:
        print "-n or --num-parts expects an integer."

  print "local output: %s" % local_output
  print "hdfs_output: %s" % hdfs_output
  print "partition: ",
  print partition
  print "num_parts: %d" % num_parts
  print "scale_factor: %.1f" % scale_factor

  if (local_output is None or hdfs_output is None or partition is None or
      num_parts is None or scale_factor is None):
    usage()
    sys.exit()

  try:
    partition = int(partition)
  except:
    # partition is not specified as an integer.
    # Determine the partition using the slave file. Assign the dbgen partition
    # id based on the position of the current node in the slave list.
    slaves = open(SLAVE_FILE).readlines()
    slaves_ips = map(lambda s: socket.gethostbyname(s.strip()), slaves)
    current_node_ip = socket.gethostbyname(socket.gethostname())

    partition = 1 # for master
    num_tasks = len(slaves_ips) + 1
    
    if current_node_ip in slaves_ips:
      # for slave
      partition = slaves_ips.index(current_node_ip) + 2
    print "Automatically determine the partition index: %d" % partition

  generate_data_to_hdfs(
    local_output, hdfs_output, partition, scale_factor, num_parts)


if __name__ == "__main__":
  main()
 

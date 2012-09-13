#!/usr/bin/python

# Used to run dbgen in parallel.
# This should be used in combination with a parallel ssh tool.

import os
import time
import socket
import subprocess
import sys

if len(sys.argv) != 2:
  print "Specify scale factor as the first argument."
  sys.exit(1)

scale_factor = float(sys.argv[1])

# dbgen must be run from its own directory.
os.chdir("/mnt/db-benchmarks/tpch-dbgen")

# paths
SLAVE_FILE = "/root/ephemeral-hdfs/conf/slaves"
TARGET_PATH_BASE = "/tpch"


def execute(cmd):
  print cmd
  subprocess.call(cmd, shell=True)

def copy_partitioned_table(table_name, task_index):
  copy_local_file_to_hdfs(table_name + ".tbl." + str(task_index), table_name)

def copy_local_file_to_hdfs(filename, target):
  target_dir = TARGET_PATH_BASE + "/" + target + "/"
  #execute("/root/ephemeral-hdfs/bin/hadoop fs -mkdir %s" % target_dir)
  execute(
    "/root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal %s %s" % (
      filename, target_dir))


# we assign the dbgen chunk id based on the position of the current node in
# the slave list.
slaves = open(SLAVE_FILE).readlines()
slaves_ips = map(lambda s: socket.gethostbyname(s.strip()), slaves)
current_node_ip = socket.gethostbyname(socket.gethostname())

task_index = 1 # for master
num_tasks = len(slaves_ips) + 1

if current_node_ip in slaves_ips:
  # for slave
  task_index = slaves_ips.index(current_node_ip) + 2

cmd = "./dbgen -v -f -s %.1f -S %d -C %d" % (scale_factor, task_index, num_tasks)  
print "generating data chunk %d / %d" % (task_index, num_tasks)
print cmd
subprocess.call(cmd, shell=True)

if task_index == 1:
  # master: copy the two non-partitioned tables.
  copy_local_file_to_hdfs("nation.tbl", "nation")
  copy_local_file_to_hdfs("region.tbl", "region")

# master & slave: copy all partitioned tables.
copy_partitioned_table("supplier", task_index)
copy_partitioned_table("part", task_index)
copy_partitioned_table("partsupp", task_index)
copy_partitioned_table("orders", task_index)
copy_partitioned_table("lineitem", task_index)
copy_partitioned_table("customer", task_index)


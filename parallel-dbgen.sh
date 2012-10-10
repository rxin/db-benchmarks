#!/bin/bash

usage()
{
cat << EOF
usage: $0 options

OPTIONS:
   -s   scale
   -n   number of tasks
   -h   HDFS output
   -l   local ouptut
EOF
}

# Get directory of parallel-dbgen.py
dbBenchmarkDir="$(cd `dirname $0`; pwd)"
logFile="dbgen.log"

if [ ! -e "$dbBenchmarkDir/tpch-dbgen/dbgen" ]; then
  echo "Compile dbgen first."
  exit 1
fi

if [ "x$SPARK_HOME" == "x" ]; then
  SPARK_HOME="/root/spark"
fi

if [ ! -e $SPARK_HOME/spark-shell ] ; then
  echo "$SPARK_HOME/spark-shell not found"
  exit 1
fi


scale=1
localOutput=/mnt/tpch
hdfsOutput=/tpch
numTasks=2
while getopts “s:l:h:n:” OPTION
do
  case $OPTION in
    s)
      scale=$OPTARG
      ;;
    h)
      hdfsOutput=$OPTARG
      ;;
    n)
      numTasks=$OPTARG
      ;;
    l)
      localOutput=$OPTARG
      ;;
    ?)
      usage
      exit
      ;;
  esac
done

echo " 
  val scale = $scale 
  val num_tasks = $numTasks 
  val localOutput = \"$localOutput\"
  val hdfsOutput = \"$hdfsOutput\" 

  import scala.sys.process._
  sc.parallelize(1 to num_tasks, num_tasks).map { partition =>
    val cmd = \"$dbBenchmarkDir/parallel-dbgen.py -s %f -l %s -h %s -n %d -p %d\".format(
      scale.toFloat, localOutput, hdfsOutput, num_tasks.toInt, partition.toInt)
    cmd !!
  }.collect()
  
  exit
" | $SPARK_HOME/spark-shell 2>&1 | tee $logFile 

# Scala interpreter messes up terminal
reset
echo "Check $logFile for execution output."

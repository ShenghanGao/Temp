#!/bin/bash
################################################################################
#  A simple Python based example for Spark
#  Designed to run on SDSC's Comet resource.
#  T. Yang. Modified from Mahidhar Tatineni's scripe for scala
################################################################################
#SBATCH --job-name="sparkpython-demo"
#SBATCH --output="sparkwc.%j.%N.out"
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --export=ALL
#SBATCH -t 00:30:00

### Environment setup for Hadoop and Spark
module load spark
export PATH=/opt/hadoop/2.6.0/sbin:$PATH
export HADOOP_CONF_DIR=$HOME/mycluster.conf
export WORKDIR=`pwd`

myhadoop-configure.sh

### Start HDFS.  Starting YARN isn't necessary since Spark will be running in
### standalone mode on our cluster.
start-dfs.sh

### Load in the necessary Spark environment variables
source $HADOOP_CONF_DIR/spark/spark-env.sh

### Start the Spark masters and workers.  Do NOT use the start-all.sh provided 
### by Spark, as they do not correctly honor $SPARK_CONF_DIR
myspark start

### Copy the data into HDFS
hdfs dfs -mkdir -p /user/$USER
#hdfs dfs -put $WORKDIR/facebook_combined.txt /user/$USER/
#hdfs dfs -put $WORKDIR/data/simple1 /user/$USER/simple1
hdfs dfs -put $WORKDIR/data/convertedOut/$1.seq  /user/$USER/input.seq

spark-submit user_artist_matrix.py /user/$USER/input.seq output

#copy out 
rm -f out/ua.txt >/dev/null || true
mkdir -p out
#hadoop dfs -copyToLocal output/part* $WORKDIR
hadoop dfs -copyToLocal output/part* out/ua.txt


### Shut down Spark and HDFS
myspark stop
stop-dfs.sh

### Clean up
myhadoop-cleanup.sh

#!/bin/bash
#SBATCH --job-name="importer"
#SBATCH --output="importer.%j.%N.out"
#SBATCH --partition=compute
#SBATCH --nodes=24
#SBATCH --ntasks-per-node=24
#SBATCH -t 00:55:00

#This subcript executes a Hadoop-based converter to produce a seq file for .txt file
#the output file is under i-output/convertedOut 
 
export HADOOP_CONF_DIR=/home/$USER/cometcluster
export WORKDIR=`pwd`
module load hadoop/1.2.1
#module load hadoop/2.6.0
myhadoop-configure.sh
start-all.sh
hadoop dfs -mkdir input

#copy data
#hadoop dfs -copyFromLocal $WORKDIR/SINGLE.TXT input/
#hadoop jar $WORKDIR/AnagramJob.jar input/SINGLE.TXT output

#hadoop dfs -copyFromLocal $WORKDIR/data/billOfRights.txt input/a


#process billofRights.txt from linux directory
#output billofRights.txt.seq to Hadoop directory $WORKDIR/convertedOut
echo $0
hadoop jar $WORKDIR/wc.jar  Importer data/$1
#hadoop jar $WORKDIR/wc.jar  Importer data/oscar.txt 

echo "Copy outout from hadoop"
hadoop dfs -copyToLocal $WORKDIR/convertedOut data


#hadoop dfs -get /user/$USER/output/part* $WORKDIR/
#hadoop dfs -copyToLocal output/ WC-output
#hadoop dfs -copyToLocal output/part* $WORKDIR

stop-all.sh
myhadoop-cleanup.sh

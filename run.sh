#!/bin/bash

IN_DIR=InputFolder
OUT_DIR=OutputFolder
REPO_DIR=/home/cloudera/git/vtad4f/SimplePageRank
CLASS_NAME=PageRank

# to remove "Input/OutputFolder" directory and all its files
hadoop fs -rm -r $IN_DIR
hadoop fs -rm -r $OUT_DIR

# to create a new input folder
# to copy a file from local directory to hadoop environment
# to see the files inside "InputFolder"
hadoop fs -mkdir $IN_DIR
hadoop fs -copyFromLocal $REPO_DIR/file/*.txt $IN_DIR 
hadoop fs -ls $IN_DIR

# running mapreduce operation
hadoop jar $REPO_DIR/jar/$CLASS_NAME.jar $CLASS_NAME $IN_DIR $OUT_DIR

# to see the files inside "OutputFolder"
# to see the content inside "OutputFolder/part-r-00000" file
hadoop fs -ls $OUT_DIR
hadoop fs -cat $OUT_DIR/part-r-00000 


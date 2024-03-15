#!/bin/bash

# Path to the scripts and Hadoop jar
ZCAT_PARSER_SCRIPT="./zcat_parser.sh"
HDFS_UPLOADER_SCRIPT="./hdfs_uploader.sh"
HADOOP_JAR_PATH="./ChunkProcessor.jar"
HADOOP_CLASS="ChunkProcessor"

# Function to clean up and exit
cleanup() {
    echo "Terminating all subprocesses..."
    kill -SIGTERM $ZCAT_PID $UPLOADER_PID $HADOOP_PID 2>/dev/null
    wait $ZCAT_PID $UPLOADER_PID $HADOOP_PID
    echo "All subprocesses terminated."
    exit
}

# Trap SIGINT and SIGTERM signals and call cleanup function
trap cleanup SIGINT SIGTERM

# Make sure the scripts are executable
chmod +x "$ZCAT_PARSER_SCRIPT"
chmod +x "$HDFS_UPLOADER_SCRIPT"

echo "Starting zcat parser..."
"$ZCAT_PARSER_SCRIPT" &
ZCAT_PID=$!

echo "Starting HDFS uploader..."
"$HDFS_UPLOADER_SCRIPT" &
UPLOADER_PID=$!

echo "Starting Hadoop job..."
hadoop jar "$HADOOP_JAR_PATH" "$HADOOP_CLASS" &
HADOOP_PID=$!

# Wait for all processes to complete
wait $ZCAT_PID
wait $UPLOADER_PID
wait $HADOOP_PID

echo "All processes have completed."

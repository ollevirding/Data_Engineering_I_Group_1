#!/bin/bash

LOCAL_DIR="yasp-chunks_test"
HDFS_DIR="yasp_chunk_10000"
MAX_CHUNKS=10
HDFS_COMPLETE_FILE="job_status/COMPLETE"

# Function to count the number of chunks in the HDFS directory
count_hdfs_chunks() {
    hdfs dfs -count "$HDFS_DIR" | awk '{print $2}'
}

# Function to check if the COMPLETE file exists
is_complete() {
    [[ -f "$COMPLETE" ]]
}

# Main loop
while true; do
    for chunk in "$LOCAL_DIR"/*; do
        if [[ -f "$chunk" ]]; then
            HDFS_CHUNK_COUNT=$(count_hdfs_chunks)

            # Wait if the number of chunks in HDFS is at or exceeds the limit
            while [[ "$HDFS_CHUNK_COUNT" -ge $MAX_CHUNKS ]]; do
                echo "Maximum number of chunks in HDFS reached. Waiting..."
                sleep 1
                HDFS_CHUNK_COUNT=$(count_hdfs_chunks)
            done

            # Upload the chunk to HDFS and remove it locally
            echo "Uploading $chunk to HDFS..."
            hdfs dfs -put "$chunk" "$HDFS_DIR"
            echo "Removing local chunk $chunk..."
            rm "$chunk"
        fi
    done

    # Check for the COMPLETE file and exit if it exists and no local chunks are left
    if is_complete && [[ $(ls -1 "$LOCAL_DIR"/* 2>/dev/null | wc -l) -eq 0 ]]; then
        echo "COMPLETE file found and no more local chunks. Uploading COMPLETE file to HDFS and exiting..."
        touch "$COMPLETE"
        hdfs dfs -put "$COMPLETE" "$HDFS_COMPLETE_FILE"
        break
    fi
done
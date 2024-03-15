#!/bin/bash

TARGET_DIR="yasp-chunks_test"
MAX_CHUNKS=10
CHUNK_PREFIX="${TARGET_DIR}/yasp-chunk-10000-"
SPLIT_LINES=10000
SOURCE_FILE="yasp-dump-2015-12-18.json.gz"

# Function to count the number of chunks in the target directory
count_chunks() {
    find "$TARGET_DIR" -type f | wc -l
}

# Create target directory if it doesn't exist
mkdir -p "$TARGET_DIR"

# Start processing
zcat "$SOURCE_FILE" | split -l $SPLIT_LINES - "$CHUNK_PREFIX" &
SPLIT_PID=$!

while true; do
    CHUNK_COUNT=$(count_chunks)
    echo "Number of chunks: $CHUNK_COUNT"

    if [[ $CHUNK_COUNT -ge $MAX_CHUNKS ]]; then
        echo "Maximum number of chunks reached. Pausing..."
        # Pause the split process
        kill -STOP $SPLIT_PID
        while [[ $(count_chunks) -ge $MAX_CHUNKS ]]; do
            # Wait for some chunks to be processed
            sleep 10
        done
        # Resume the split process
        echo "Resuming splitting..."
        kill -CONT $SPLIT_PID
    fi

    # If the split process has finished, create the COMPLETE file and exit the loop
    if ! ps -p $SPLIT_PID > /dev/null; then
        echo "Splitting complete. Creating COMPLETE file..."
        touch "${TARGET_DIR}/COMPLETE"
        break
    fi

    sleep 5  # Check every 5 seconds
done

wait $SPLIT_PID

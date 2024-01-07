#!/bin/sh

# Step 3: Clean up - stop the query if the script is terminated
cleanup() {
    echo "Stopping the query..."
    curl -s -X DELETE "http://api-test/v1/nes/query/stop-query?queryId=${QUERY_ID}"
    echo "Query stopped. Exiting."
    exit 0
}

FILENAME="/query/query.json"

while true; do
    QUERY_ID=$(curl -s -d@"$FILENAME" http://api-test/v1/nes/query/execute-query | jq -r '.queryId')

    if [ -n "$QUERY_ID" ] || [ "$QUERY_ID" = "null" ]; then
        break
    else
        echo "Error: Unable to extract queryId. Retrying..."
        sleep 5
    fi
done

# Trap script termination signals to perform cleanup
trap cleanup EXIT INT TERM
echo "Query started with queryId: $QUERY_ID"

# Step 2: Continuously check query status
while true; do
    STATUS=$(curl -s "http://api-test/v1/nes/query/query-status?queryId=${QUERY_ID}" | jq -r '.status')

    if [ "$STATUS" == "COMPLETED" ]; then
        echo "Query completed successfully."
        break
    elif [ "$STATUS" == "FAILED" ]; then
        echo "Error: Query failed. Exiting."
        exit 1
    fi

    # Sleep for a short interval before checking again
    sleep 5
done


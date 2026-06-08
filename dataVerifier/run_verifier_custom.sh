#!/bin/bash

# Script to run statisticalDataVerifier.py multiple times
# Usage: ./run_verifier_custom.sh [iterations] [database] [collection] [max_docs]
# Example: ./run_verifier_custom.sh 5 samquote tasks 3000

# Default values
ITERATIONS=${1:-10}
DATABASE=${2:-samquote}
COLLECTION=${3:-quotes}
MAX_DOCS=${4:-5000}

echo "Starting $ITERATIONS iterations of statistical data verification..."
echo "Command: python statisticalDataVerifier.py $DATABASE $COLLECTION $MAX_DOCS"
echo "======================================================="

for i in $(seq 1 $ITERATIONS)
do
    echo ""
    echo "--- Iteration $i/$ITERATIONS ---"
    echo "Starting at: $(date)"
    
    # Run the verification script and capture output
    output=$(python statisticalDataVerifier.py $DATABASE $COLLECTION $MAX_DOCS 2>&1)
    exit_code=$?
    
    # Display full output
    echo "$output"
    
    # Check for data discrepancies using grep
    discrepancies=$(echo "$output" | grep -E "(does not match for document|does not exist in document|does not exist in the destination collection)")
    
    if [ ! -z "$discrepancies" ]; then
        echo ""
        echo "*** DATA DISCREPANCIES FOUND IN ITERATION $i ***"
        echo "$discrepancies"
        echo "*** END OF DISCREPANCIES ***"
        echo ""
        echo "STOPPING EXECUTION DUE TO DATA DISCREPANCIES!"
        echo "Discrepancies found at iteration $i of $ITERATIONS"
        echo "Stopped at: $(date)"
        exit 1
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo "Iteration $i completed successfully"
    else
        echo "Iteration $i failed with exit code: $exit_code"
        echo "STOPPING EXECUTION DUE TO SCRIPT FAILURE!"
        echo "Failed at iteration $i of $ITERATIONS"
        echo "Stopped at: $(date)"
        exit $exit_code
    fi
    
    echo "Finished at: $(date)"
    echo "======================================================="
done

echo ""
echo "All $ITERATIONS iterations completed!"
echo "Finished at: $(date)"
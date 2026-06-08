# How to 

## What is it

- Capture all output from the Python script (including stderr)
- Grep for the three specific error patterns:
  - "does not match for document" (field value mismatch)
  - "does not exist in document" (missing field)
  - "does not exist in the destination collection" (missing document)
- Stop on discrepancies: As soon as any data discrepancy is detected, the script will:
  - Display the discrepancies found
     - Show a clear "STOPPING EXECUTION" message
     - Display which iteration the discrepancy was found in
     - Exit with code 1
  - Stop on script failures: If the Python script itself fails (non-zero exit code), the script will:
     - Show the failure message
     - Display which iteration failed
     - Exit with the same exit code as the Python script
- Sample behavior:
  - If discrepancies are found:

```log
  --- Iteration 3/100 ---
...normal output...

*** DATA DISCREPANCIES FOUND IN ITERATION 3 ***
Field 'status' does not match for document with _id '507f1f77bcf86cd799439011'
*** END OF DISCREPANCIES ***

STOPPING EXECUTION DUE TO DATA DISCREPANCIES!
Discrepancies found at iteration 3 of 100
Stopped at: Mon Oct 14 10:45:32 UTC 2025
``` 

  - If all iterations pass without discrepancies:

```log
...all iterations complete...
All 100 iterations completed!
Finished at: Mon Oct 14 11:30:15 UTC 2025
```

## Usage

```bash
# Run 10 times with default parameters
./run_verifier_custom.sh

# Run 5 times with custom parameters
./run_verifier_custom.sh 5 samquote tasks 3000

# Run 15 times with different collection
./run_verifier_custom.sh 15 samquote quotes 5000
```
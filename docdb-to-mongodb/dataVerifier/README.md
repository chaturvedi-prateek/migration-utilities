# Statistical Data Verifier - Setup Requirements

## Python Requirements

### Python Version
- Python 3.6 or higher

### Dependencies
Install the required Python packages:

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install pymongo>=4.0.0
```

## System Requirements

### For DocumentDB Connection
- **TLS Certificate**: Download the DocumentDB CA certificate bundle
  ```bash
  wget https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem -O /path/to/global-bundle.pem
  ```
  
- **Network Access**: Ensure your system can connect to:
  - DocumentDB cluster on port 27017
  - MongoDB Atlas cluster on port 27017 (or 27015/27016 for SRV)

### For MongoDB Atlas Connection
- **Internet Access**: Required for SRV DNS resolution and connection
- **Authentication**: Valid username/password credentials

## Configuration

### Update Connection Strings
Before running, update the connection strings in `statisticalDataVerifier.py`:

1. **DocumentDB Connection** (line ~63):
   ```python
   client1 = pymongo.MongoClient(
       "mongodb://username:password@your-docdb-cluster:27017/?directConnection=true&retryWrites=false",
       tls=True,
       tlsCAFile="/path/to/your/global-bundle.pem",
       tlsAllowInvalidHostnames=False
   )
   ```

2. **MongoDB Atlas Connection** (line ~70):
   ```python
   client2 = pymongo.MongoClient("mongodb+srv://username:password@your-atlas-cluster/?retryWrites=true&w=majority")
   ```

## Usage

### Command Line Arguments
```bash
python statisticalDataVerifier.py <database_name> <collection_name> <max_number_docs>
```

### Examples
```bash
# Compare 1000 documents from samquote.quotes
python statisticalDataVerifier.py samquote quotes 1000

# Use default 5% sampling for samquote.tasks  
python statisticalDataVerifier.py samquote tasks 0

# Compare 5000 documents from mydb.mycollection
python statisticalDataVerifier.py mydb mycollection 5000
```

### Examples using runVerifierCustom.sh

```bash
# Compare 5000 documents from mydb.mycollection in a loop of 200 iterations
./runVerifierCustom.sh 200 samquote quotes 5000
```

## Performance Considerations

### Memory Requirements
- The script loads sampled documents into memory
- Memory usage depends on document size and sample count
- Recommended: 2GB+ RAM for large collections

### Threading
- Uses 10 threads for parallel document comparison
- CPU usage scales with thread count
- Recommended: Multi-core CPU for better performance

## Security Notes

### Credentials
- Never commit connection strings with credentials to version control
- Consider using environment variables for sensitive data:
  ```python
  import os
  username = os.getenv('DB_USERNAME')
  password = os.getenv('DB_PASSWORD')
  ```

### TLS Certificates
- Keep TLS certificate files secure and up-to-date
- Verify certificate paths are correct for your environment
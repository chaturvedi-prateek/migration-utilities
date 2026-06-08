import pymongo
from pymongo import MongoClient
# from comparisonStrategies.compareFieldByField import compare_documents
import time
import threading
import sys


# Define a function to compare documents field by field
def compare_documents(documents, destination_documents):
    for source_doc in documents:
        destination_doc = next((doc for doc in destination_documents if doc["_id"] == source_doc["_id"]), None)
        if destination_doc:
            #print(f"Comparing document with _id '{source_doc['_id']}'... with destination document with _id '{destination_doc['_id']}'")
            for field in source_doc:
                if field in destination_doc:
                    if source_doc[field] != destination_doc[field]:
                        print(f"Field '{field}' does not match for document with _id '{source_doc['_id']}'")
                else:
                    print(f"Field '{field}' does not exist in document with _id '{source_doc['_id']}'")
        else:
            print(f"Document with _id '{source_doc['_id']}' does not exist in the destination collection")

def get_sample_document_count(source_collection, max_number_docs):
    # Get the total count of documents
    total_count = source_collection.estimated_document_count({})
    sample_doc_count = min(int(total_count * 0.05), int(max_number_docs))
    print(f"Total count of documents in the source collection: {total_count}")
    print(f"Total count of documents sampled (5%): {sample_doc_count}")
    return sample_doc_count

def get_sample_documents(source_collection, sample_doc_count):
    # Run aggregation with $sample option and retrieve all documents to memory
    print("Sampling documents from the source collection...")
    start_time = time.time()
    sample_documents = list(source_collection.aggregate([{"$sample": {"size": sample_doc_count}}]))
    end_time = time.time()
    sampling_time = end_time - start_time
    print(f"Sampling completed in {sampling_time} seconds")
    return sample_documents

def query_destination_collection(destination_collection, sample_documents):
    # Query client2 with all the documents by _id
    print("Querying documents from the destination collection...")
    start_time = time.time()
    result = list(destination_collection.find({"_id": {"$in": [doc["_id"] for doc in sample_documents]}}))
    end_time = time.time()
    querying_time = end_time - start_time
    print(f"Querying completed in {querying_time} seconds")
    return result

def main():
    # Check if command line arguments are provided
    if len(sys.argv) != 4:
        print("Usage: python statisticalDataVerifier.py <database_name> <collection_name> <max_number_docs>")
        print("Example: python statisticalDataVerifier.py samquote quotes 1000")
        print("Note: Use 0 for max_number_docs to use default 5% sampling")
        sys.exit(1)
    
    # Get arguments from command line
    database_name = sys.argv[1]
    collection_name = sys.argv[2]
    max_number_docs = sys.argv[3]

    # DocumentDB connection with proper TLS configuration
    client1 = pymongo.MongoClient(
        "mongodb://whos:who@fqdn.docdb.amazonaws.com:27017/?directConnection=true&retryWrites=false",
        tls=True,
        tlsCAFile="/PATH/TO/global-bundle.pem",
        tlsAllowInvalidHostnames=False
    )

    # MongoDB Atlas connection 
    client2 = pymongo.MongoClient("mongodb+srv://whos:who@fqdn.mongodb.net/?retryWrites=true&w=majority")


    print(f"Using database: {database_name}")
    print(f"Using collection: {collection_name}")
    print(f"Max number of documents: {max_number_docs}")
    print("-" * 50)

    # Connect to the source MongoDB cluster
    # client1 = MongoClient(source_connection_string)
    source_database = client1[database_name]
    source_collection = source_database[collection_name]

    # Connect to the destination MongoDB cluster
    # client2 = MongoClient(destination_connection_string)
    destination_database = client2[database_name]
    destination_collection = destination_database[collection_name]

    # Call the function in the main function
    sample_doc_count = get_sample_document_count(source_collection, max_number_docs)

    # Call the function in the main function
    sample_documents = get_sample_documents(source_collection, sample_doc_count)

    # Call the function in the main function
    destination_documents = query_destination_collection(destination_collection, sample_documents)

    print(f"Sampled doc count : {sample_doc_count}")
    print(f"Sampled Documents length: {len(sample_documents)}")

    # Split the sample_documents array into chunks
    chunk_size = len(sample_documents) // 10
    print(f"Chunk size: {chunk_size}")
    chunks = [sample_documents[i:i+chunk_size] for i in range(0, len(sample_documents), chunk_size)]

    # TODO: Can I improve the performance by using multiple processes?.
    # Create and start 10 threads
    start_time = time.time()
    threads = []
    for chunk in chunks:
        thread = threading.Thread(target=compare_documents, args=(chunk, destination_documents))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    end_time = time.time()
    comparison_time = end_time - start_time
    print(f"Comparison completed in {comparison_time} seconds")

main()
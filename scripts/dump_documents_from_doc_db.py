"""
This program extracts a collection from a MongoDB instance and formats in a way the
pipeline works.
"""

from document import write_document_from_main as main
import json
import pprint
import sys

if __name__ == "__main__":
    query_to_run_json = sys.argv[1]

    with open(query_to_run_json, "r") as f:
        query_to_run = json.load(f)

    pprint.pprint(query_to_run)

    base_directory = sys.argv[2]
    base_name = sys.argv[3]
    runtime_config_json = sys.argv[4]

    if len(sys.argv) > 5:
        collection_name = sys.argv[5]

    with open(runtime_config_json, "r") as f:
        runtime_config = json.load(f)

    if "batch_size" in runtime_config["source_db_config"]:
        batch_size = runtime_config["source_db_config"]["batch_size"]
    else:
        batch_size = 5000

    main(query_to_run, base_directory, base_name, runtime_config["mongo_db_config"],
         size_of_batches=batch_size, overwritten_collection_name=collection_name)
"""
This program extracts a collection from a MongoDB instance and formats in a way the
pipeline works.
"""

from prediction_matrix_generate.document import write_document_from_main
import json
import pprint
import argparse

if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-q", "--query_json_filename", dest="query_json_filename")
    argparse_obj.add_argument("-n", "--batch_file_size", dest="batch_file_size", default=5000, type=int)
    argparse_obj.add_argument("-d", "--directory", dest="base_directory", default="./")
    argparse_obj.add_argument("-r", "--runtime_config_json", dest="run_time_config_json",
                              help="A JSON file which includes runtime environment details")
    argparse_obj.add_argument("-c", "--collection_name", dest="collection_name",
                              help="MongoDB collection name to load into")
    argparse_obj.add_argument("-a", "--base_filename_prefix", dest="base_filename",
                              help="Prefix to add to the filename")

    arg_obj = argparse_obj.parse_args()

    base_directory = arg_obj.base_directory
    base_name = arg_obj.base_directory
    runtime_config_json = arg_obj.run_time_config_json
    collection_name = arg_obj.collection_name

    with open(runtime_config_json, "r") as f:
        runtime_config = json.load(f)

    batch_size = arg_obj.batch_file_size

    query_to_run = json.load(f)
    pprint.pprint(query_to_run)
    write_document_from_main(query_to_run, base_directory, base_name, runtime_config["mongo_db_config"],
         size_of_batches=batch_size, overwritten_collection_name=collection_name)
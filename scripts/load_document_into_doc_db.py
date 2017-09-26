from document import load_document_main
import argparse
import sys

"""
A simple program for loading JSON dicts into a MongoDB collection
"""


if __name__ == "__main__":
    # TODO: Add support for GZIP json

    argparse_obj = argparse.ArgumentParser(description="Generate a JSON file that specifies which fields to filter from an HDF5 file container.")
    argparse_obj.add_argument("-b", "--batch-json-filename", dest="batch_json_filename", help="")
    argparse_obj.add_argument("-r", "--runtime-config-json", dest="run_time_config_json",
                              help="A JSON file which includes runtime environment details")
    argparse_obj.add_argument("-c", "--collection-name", dest="collection_name", default=None,
                              help="Optional: MongoDB collection name to load into")

    arg_obj = argparse_obj.parse_args()
    load_document_main(arg_obj.batch_json_filename, arg_obj.run_time_config_json, arg_obj.collection_name)


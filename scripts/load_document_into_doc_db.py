from document import load_document_main
import argparse
import sys

"""
A simple program for loading JSON dicts into a MongoDB collection
"""


if __name__ == "__main__":

    # TODO: Add support for GZIP json

    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-b", "--batch_json_filename", dest="batch_json_filename", help="")
    argparse_obj.add_argument("-r", "--runtime_config_json", dest="run_time_config_json",
                              help="A JSON file which includes runtime environment details")
    argparse_obj.add_argument("-c", "--collection_name", dest="collection_name", default=None,
                              help="Optional: MongoDB collection name to load into")

    load_document_main()

    arg_obj = argparse_obj.parse_args()
    load_document_main(arg_obj.batch_json_filename, arg_obj.run_time_config_json, arg_obj.collection_name)


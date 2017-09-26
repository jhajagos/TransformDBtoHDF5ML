import argparse
import sys
import os

try:
    from prediction_matrix_generate.db_document import main_json
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    from prediction_matrix_generate.db_document import main_json


if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser(description="A tool to map relational database tables to JSON documents")

    argparse_obj.add_argument("-c", "--db-map-json", dest="db_map_json_filename",
                              nargs="+",
                              help="JSON files containing the mappings of the DB to a document")
    argparse_obj.add_argument("-r", "--runtime-config-json", dest="run_time_config_json",
                              help="A JSON file which includes runtime environment details")

    arg_obj = argparse_obj.parse_args()
    main_json(arg_obj.db_map_json_filename, arg_obj.run_time_config_json)
import sys
import os
import argparse

try:
    from prediction_matrix_generate.document_hdf5 import main
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    from prediction_matrix_generate.document_hdf5 import main

if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser(help="A tool for mapping JSON documents to matrices in a HDF5 container")
    argparse_obj.add_argument("-a", "--base-filename-prefix", dest="base_filename",
                              help="Prefix for generated filenames")
    argparse_obj.add_argument("-c", "--data-template-json-filename", dest="data_template_json_filenames",
                              help="Either a single or multiple JSON files which contain mapping details", nargs="+")
    argparse_obj.add_argument("-b", "--batch-json-filename", dest="batch_json_filename",
                              help="JSON file which has")

    arg_obj = argparse_obj.parse_args()
    main(arg_obj.base_filename, arg_obj.batch_json_filename, arg_obj.data_template_json_filenames)
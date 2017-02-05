import sys
from prediction_matrix_generate.document_hdf5 import main
import argparse

if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-a", "--base_filename", dest="base_filename", help="Prefix to add to the filename")
    argparse_obj.add_argument("-t", "--data_template_json_filename", dest="data_template_json_filename",
                              help="")
    argparse_obj.add_argument("-b", "--batch_json_filename", dest="batch_json_filename", help="")

    arg_obj = argparse_obj.parse_args()
    main(arg_obj.base_filename, arg_obj.batch_json_filename, arg_obj.data_template_json_filename)
import argparse
import json
from prediction_matrix_generate.post_process_hdf5 import main

if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-f", "--hdf5_filename", "HDF5 file name for post processing", dest="hdf5_filename")
    argparse_obj.add_argument("-r", "--rules_json_filename", "JSON files with rules", dest="rules_json_filename")

    arg_obj = argparse_obj.parse_args()
    rules = json.load(arg_obj.rules_json_filename)

    main(arg_obj.hdf5_filename, rules)

import argparse
import h5py
import json
import sys
import os

try:
    from prediction_matrix_generate.utility_functions import query_rows_hdf5
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    from prediction_matrix_generate.utility_functions import query_rows_hdf5


def main(hdf5_filename, query_json_filename, output_json_filename):

    f5p = h5py.File(hdf5_filename, "r")

    with open(query_json_filename, "r") as f:
        row_selection_struct = json.load(f)

    query_result = query_rows_hdf5(f5p, row_selection_struct)

    query_array_result = query_result[0]

    query_result_list = query_array_result.tolist()

    number_of_results = len(query_result_list)



    print("Number of rows selected %s" % number_of_results)

    print("Writing '%s'" % output_json_filename)
    with open(output_json_filename, "w") as fw:

        json.dump(query_result_list, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    argparse_obj = argparse.ArgumentParser(description="Generate a JSON file that specifies which to filter from an HDF5 file container")

    argparse_obj.add_argument("-f", "--hdf5-filename", help="HDF5 file name for post processing", dest="hdf5_filename")

    argparse_obj.add_argument("-q", "--query-json-filename", help="A JSON file which lists the fields to select",
                              dest="query_json_filename")

    argparse_obj.add_argument("-o", "--output-json-filename", help="File to write row selection critera to",
                              default="row_selection.json")

    arg_obj = argparse_obj.parse_args()

    main(arg_obj.hdf5_filename, arg_obj.query_json_filename, arg_obj.output_json_filename)





    # def find_multiple_column_indices_hdf5(h5p, items_with_path):
#     """
#      [("/discharge/demographic", ("age"), ("gender", "m")]
#
#      Returns a dictionary {"/discharge/demographic": ([3, 6], [['gender', 'age'], ['m', ''], ['','']])}
#     """
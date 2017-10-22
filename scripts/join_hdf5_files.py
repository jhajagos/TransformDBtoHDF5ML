import argparse
import os
import h5py
import csv
import sys

try:
    from prediction_matrix_generate import merge_hdf5
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    from prediction_matrix_generate import merge_hdf5


def main(left_hdf5_file_name, right_hdf5_file_name, out_hdf5_file_name,
         csv_linking_file_name, identifier_path_1, identifier_position_1,
         identifier_path_2, identifier_position_2):

    with h5py.File(left_hdf5_file_name, "r") as lf5:
        with h5py.File(right_hdf5_file_name, "r") as rf5:
            with h5py.File(out_hdf5_file_name, "w") as wf5:
                linking_result = read_csv_filename_to_list(csv_linking_file_name)
                merge_hdf5.merge_f_pointer_hdf5(lf5, rf5, wf5, identifier_path_1, identifier_position_1,
                                                identifier_path_2, identifier_position_2, linking_result
                                                )


def read_csv_filename_to_list(csv_file_name):

    with open(csv_file_name, "rb") as f:
        csv_reader = csv.reader(f)

        i = 0
        result_list = []
        for row in csv_reader:

            if i > 0:
                result_list += [[int(row[0]), int(row[1])]]

            i += 1

        return result_list


if __name__ == "__main__":
    arg_parser_obj = argparse.ArgumentParser()

    arg_parser_obj.add_argument("-l", "--left-filename-to-join", dest="left_filename_to_join")
    arg_parser_obj.add_argument("-r", "--right-filename-to-join", dest="right_filename_to_join")
    arg_parser_obj.add_argument("-o", "--hdf5-filename-to-write", dest="hdf5_filename_to_write")
    arg_parser_obj.add_argument("--identifier-path-left", dest="identifier_path_left")
    arg_parser_obj.add_argument("--identifier-pos-left", dest="identifier_pos_left", default=0)
    arg_parser_obj.add_argument("--identifier-path-right", dest="identifier_path_right")
    arg_parser_obj.add_argument("--identifier-pos-right", dest="identifier_pos_right", default=0)
    arg_parser_obj.add_argument("-c", "--csv-filename", default="csv_filename",
                                help="First column is the left identifier and second column is the right identifier")

    arg_obj = arg_parser_obj.parse_args()

    main(arg_obj.left_filename_to_join, arg_obj.right_filename_to_join, arg_obj.hdf5_filename_to_write,
         arg_obj.csv_filename, arg_obj.identifier_path_left, int(arg_obj.identifier_pos_left),
         arg_obj.identifier_path_right, int(arg_obj.identifier_pos_right))
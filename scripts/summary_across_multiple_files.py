import os
import prediction_matrix_generate.utility_functions as upx
import csv
import h5py
import numpy as np
import argparse

"""
Generates a CSV file which lists basic statistics on multiple HDF5 files enclosed in a starting directory
"""


def main(starting_directory):

    file_summary_csv = os.path.join(starting_directory, "hdf5_files_summary.csv")

    header = ["full_directory", "directory", "file_name", "hdf5_path", "number_of_rows", "number_of_columns", "number_of_cells", "non_zero_entries", "fraction_non_zero"]
    with open(file_summary_csv, "wb") as fw:
        csv_writer = csv.writer(fw)
        csv_writer.writerow(header)
        for dir_name, subdir_list, file_list in os.walk(starting_directory):

            for file_name in file_list:
                base_name, ext  = os.path.splitext(file_name)
                if ext == ".hdf5":
                    hdf5_file_name = os.path.join(dir_name, file_name)

                    h5 = h5py.File(hdf5_file_name)
                    group_paths = upx.get_all_paths(h5["/"])

                    if group_paths is not None:
                        for group_path in group_paths:
                            if group_path.split("/")[-1] == "core_array":
                                numeric_array = h5[group_path]
                                non_zero = np.where(numeric_array[...] > 0)
                                n_rows, n_columns = numeric_array.shape
                                n_cells = n_rows * n_columns
                                n_non_zero = len(non_zero[0])
                                if n_cells is None or n_cells == 0:
                                    fraction_non_zero = None
                                else:
                                    fraction_non_zero = 1.0 * n_non_zero / n_cells

                                row_to_write = [dir_name, os.path.split(dir_name)[-1], file_name, group_path, n_rows, n_columns, n_cells, n_non_zero, fraction_non_zero]
                                print(row_to_write)
                                csv_writer.writerow(row_to_write)

if __name__ == "__main__":
    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-d", "--directory", dest="directory",
                              help="Starting directory to recurse through and compute basic statistics on.")

    arg_obj = argparse_obj.parse_args()
    main(arg_obj.directory)
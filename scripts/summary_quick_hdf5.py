"""Exports as CSV file for each field and path which list the number of
non zero elements, the fraction that are non-zero
"""

from prediction_matrix_generate.utility_functions import get_all_paths
import csv
import h5py
import numpy as np
import argparse

#TODO: Add path list


def main(hdf5_filename, csv_file_name=None, threshold_value_to_include=0.01):
    fp5 = h5py.File(hdf5_filename)

    paths = get_all_paths(fp5["/"])

    core_array_paths = [p for p in paths if p.split("/")[-1] == "core_array"]

    stripped_split_paths = [p.split("/")[:-1] for p in core_array_paths]
    stripped_paths = ["/".join(sp) for sp in stripped_split_paths]

    if csv_file_name is None:
        csv_file_name = hdf5_filename + ".summary.csv"

        with open(csv_file_name, "wb") as fw:
            csv_writer = csv.writer(fw)
            header = ["path", "c1", "c2", "c3", "non-zero", "to_include", "fraction non-zero"]
            csv_writer.writerow(header)
            for stripped_path in stripped_paths:
                print(stripped_path)
                column_annotation_path = stripped_path + "/column_annotations"
                core_array_path = stripped_path + "/core_array"
                column_annotations = fp5[column_annotation_path][...]

                for j in range(column_annotations.shape[1]):
                    slice_of_interest = fp5[core_array_path][:, j]
                    number_of_rows = slice_of_interest.shape[0]
                    non_zero_values = np.where(slice_of_interest > 0)
                    n_non_zero_values = len(non_zero_values[0])
                    column_name_to_write = column_annotations[:,j]
                    column_names = column_name_to_write.transpose().tolist()

                    fraction_non_zero = (1.0 * n_non_zero_values) / number_of_rows
                    if fraction_non_zero >= threshold_value_to_include:
                        to_include = "1"
                    else:
                        to_include = ""

                    row_to_write = [stripped_path] + column_names + [n_non_zero_values, to_include, fraction_non_zero]

                    csv_writer.writerow(row_to_write)


if __name__ == "__main__":
    argparse_obj = argparse.ArgumentParser()

    argparse_obj.add_argument("-f", "--hdf5-filename", dest="hdf5_filename", help="HDF5 file to summarize")
    argparse_obj.add_argument("-m", "--mark-frequency_by_fraction", dest="threshold_value_to_include", default=0.01, type=float,
                              help="Mark records that occur at a fraction of at least between 0 and 1"
                             )

    arg_obj = argparse_obj.parse_args()
    main(hdf5_filename=arg_obj.hdf5_filename, threshold_value_to_include=arg_obj.threshold_value_to_include)
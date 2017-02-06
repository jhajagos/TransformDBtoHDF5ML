"""
    Subset vertically and horizontally an HDF5 file.
"""

from prediction_matrix_generate.utility_functions import main_subset
import argparse
import json


def main(hdf5_file_to_read, hdf5_file_to_write, json_population_selection, json_field_selection=None):
    if json_field_selection is not None:
        with open(json_field_selection, "r") as f:
            field_selection = json.load(f)
    else:
        field_selection = None

    if json_population_selection is not None:

        with open(json_population_selection, "r") as f:
            population_selection = json.load(f)
    else:
        population_selection = None

    print(population_selection)

    main_subset(hdf5_file_to_read, hdf5_file_to_write, population_selection, field_selection)


if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser()
    argparse_obj.add_argument("-f", "--in-hdf5-filename",  help="HDF5 file to read and make compact",
                              dest="in_hdf5_filename")
    argparse_obj.add_argument("-o", "--out-hdf5-filename", help="HDF5 file to read and make compact",
                              dest="out_hdf5_filename")
    argparse_obj.add_argument("-c", "--column-selection-json-filename",
                              help="A JSON file which lists which columns to select in the matrices",
                              dest="column_selection_json_filename", default=None
                              )
    argparse_obj.add_argument("-r", "--row-selection-json-filename",
                              help="A JSON file which lists which rows to select in the matrices",
                              dest="row_selection_json_filename", default=None
                              )

    arg_obj = argparse_obj.parse_args()
    main(arg_obj.in_hdf5_filename, arg_obj.out_hdf5_filename, arg_obj.row_selection_json_filename, arg_obj.column_selection_json_filename)


"""
    Subset vertically and horizontally an HDF5 file.
"""

from prediction_matrix_generate.utility_functions import main_subset
import sys
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

    if len(sys.argv) == 1:
        print("Usage: python compact_subset_hdf5.py hdf5_file_to_read.hdf5 hdf5_file_to_write.hdf5 [row_selection.json|None] [column_selection.json]")
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        if sys.argv[3] == "None":
            main(sys.argv[1], sys.argv[2], None, sys.argv[4])
        else:
            main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

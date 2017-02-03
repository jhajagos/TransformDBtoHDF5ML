"""
    Subset vertically and horizontally an HDF5 file.
"""

from utility_prediction import *
import h5py
import sys
import json


def main_subset(hdf5_file_name, hdf5_file_name_to_write_to, queries_to_select_list=None, columns_to_include_list=None,
                compact_columns=None):

    fp5 = h5py.File(hdf5_file_name, "r")

    if columns_to_include_list:
        column_path_dict = find_multiple_column_indices_hdf5(fp5, columns_to_include_list)
    else:
        column_path_dict = None

    if queries_to_select_list is not None:
        row_array = query_rows_hdf5(fp5, queries_to_select_list)
        print(row_array)
    else:
        for path in column_path_dict:
            n_rows = fp5[path + "/core_array/"][:, 0].shape[0]
            break

        row_array = (np.arange(0, n_rows),)

    wfp5 = h5py.File(hdf5_file_name_to_write_to, "w")

    if column_path_dict is not None:

        for path in column_path_dict:

            core_array_path = "/".join(path.split("/") + ["core_array"])
            column_annotations_path = "/".join(path.split("/") + ["column_annotations"])

            indices, annotations = column_path_dict[path]
            column_annotations_ds = wfp5.create_dataset(column_annotations_path, shape=annotations.shape,
                                                        dtype=annotations.dtype, compression="gzip")
            column_annotations_ds[...] = annotations
            n_columns = annotations.shape[1]
            n_rows = row_array[0].shape[0]

            source_data_set = fp5[core_array_path]
            core_array_ds = wfp5.create_dataset(core_array_path, dtype=source_data_set.dtype,
                                                shape=(n_rows, n_columns), compression="gzip")

            full_column_core_array = fp5[core_array_path][:, indices]
            print(path)
            if len(full_column_core_array.shape) > 1:
                subset_full_column_core_array = full_column_core_array[row_array[0], :]
                core_array_ds[...] = subset_full_column_core_array
            else:
                subset_full_column_core_array = full_column_core_array[row_array[0]]
                core_array_ds[...] = np.array([subset_full_column_core_array]).transpose()

    else:
        all_paths = get_all_paths(fp5["/"])
        for path in all_paths:
            if "core_array" in path.split("/"):
                core_array_ds = create_dataset_with_new_number_of_rows(fp5, wfp5, path, row_array[0].shape[0])
                core_array_ds[...] = fp5[path][row_array[0], :]
            else:
                copy_data_set(fp5, wfp5, path, compression="gzip")


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

"""
    Utility functions for working with HDF5 files created by the library
    'build_hdf5_matrix_from_document.py'
"""

import numpy as np
import gzip
import json
import h5py


def data_dict_load(data_dict_json_file_name):
    """Load a JSON dict even if the JSON dict is compressed"""
    if data_dict_json_file_name[-2:] == "gz":
        with gzip.open(data_dict_json_file_name, "rb") as f:
            data_dict = json.loads(f.read().decode("ascii"))
    else:
        with open(data_dict_json_file_name, "rb") as fj:
            data_dict = json.load(fj)

    return data_dict


def get_all_paths(h5py_group):
    """Recurse and get all non-groups"""
    non_groups = []
    for group_name in h5py_group:
        if not h5py_group[group_name].__class__ == h5py_group.__class__:
            non_groups += [h5py_group[group_name].name]
        else:
            non_groups.extend(get_all_paths(h5py_group[group_name]))

    if len(non_groups):
        return non_groups


def copy_data_set(h5p1, h5p2, path, compression="gzip"):
    """Copy a data_set from one path to another"""
    ds1 = h5p1[path]
    source_shape = ds1.shape
    source_dtype = ds1.dtype

    ds2 = h5p2.create_dataset(path, shape=source_shape, dtype=source_dtype, compression=compression)
    ds2[...] = ds1[...]


def create_data_set_with_new_number_of_rows(h5p1, h5p2, path, new_number_rows, compression="gzip"):
    ds1 = h5p1[path]
    source_dtype = ds1.dtype

    updated_shape = (new_number_rows, ds1.shape[1])
    ds2 = h5p2.create_dataset(path, shape=updated_shape, dtype=source_dtype, compression=compression)
    return ds2


def copy_into_data_set_starting_at(ds1, h5p2, path, starting_position):
    ds2 = h5p2[path]
    ds2_shape = ds2.shape
    ds2_rows = ds2_shape[0]
    ending_position = starting_position + ds2_rows

    ds1[starting_position : ending_position] = ds2[...]
    return ending_position


def query_rows_hdf5(h5p, queries):
    """
        hp5 is a pointer to a file opened by h5py library
        returns an array of indices to subset the matrix

   A simplistic query:

    [
     ("/discharge/demographic", ["gender","m"], 1),
     ("/discharge/demographic", "age", [65, 66, 67]),
     ("/lab/values", "attribute": "a1c", 8, ">=")
    ]

    The query is select all males who are between the ages of 65 and 67 and who have
    an a1c test greater than or equal to 8.
    """

    rows_that_match = None

    for query in queries:
        path = query[0]
        field = query[1]
        query_part = query[2:]

        column_indices, _ = find_column_indices_hdf5(h5p, path, field)

        if len(column_indices) == 1:
            pass
        else:
            if len(column_indices):
                raise RuntimeError, "The field '%s' should match only a single column" % field
            else:
                raise RuntimeError, "The field '%s' did not match any column" % field

        core_array_path = "/".join(path.split("/") + ["core_array"])

        core_array = h5p[core_array_path][:, column_indices]

        if len(query_part) == 1:
            query_value = query_part[0]
            if query_value.__class__ in ([].__class__, ().__class__): # Multiple values in a list to match
                local_rows_that_match = None
                for query_value_item in query_value: # This is a union
                    real_local_rows_that_match = np.where(core_array == query_value_item)
                    if local_rows_that_match is None:
                        local_rows_that_match = real_local_rows_that_match
                    else:
                        local_rows_that_match = np.union1d(real_local_rows_that_match, local_rows_that_match)
            else: # Single value equality
                local_rows_that_match = np.where(core_array == query_value)

            if rows_that_match is not None:
                rows_that_match = np.intersect1d(rows_that_match, local_rows_that_match)
            else:
                rows_that_match = local_rows_that_match
        else: # Range queries
            if len(query_part) == 2:
                query_value, query_value_qualifier = query_part
                if query_value_qualifier == ">":
                    local_rows_that_match = np.where(core_array > query_value)
                elif query_value_qualifier == ">=":
                    local_rows_that_match = np.where(core_array >= query_value)
                elif query_value_qualifier == "<":
                    local_rows_that_match = np.where(core_array < query_value)
                elif query_value_qualifier == "<=":
                    local_rows_that_match = np.where(core_array <= query_value)

                if rows_that_match is not None:
                    rows_that_match = np.intersect1d(rows_that_match, local_rows_that_match)
                else:
                    rows_that_match = local_rows_that_match
    return rows_that_match


def find_multiple_column_indices_hdf5(h5p, items_with_path):
    """
     [("/discharge/demographic", ("age"), ("gender", "m")]

     Returns a dictionary {"/discharge/demographic": ([3, 6], [['gender', 'age'], ['m', ''], ['','']])}

    """

    indices_dict = {}
    for path_set in items_with_path:
        path = path_set[0]
        items_to_find = path_set[1:]

        for item_to_find in items_to_find:
            c_indices, c_annotations = find_column_indices_hdf5(h5p, path, item_to_find)

            if c_indices.shape[0]:
                if path in indices_dict:
                    p_indices, p_annotations = indices_dict[path]
                    pc_indices = np.concatenate((p_indices, c_indices))
                    pc_annotations = np.concatenate((p_annotations, c_annotations), 1)

                    indices_dict[path] = (pc_indices, pc_annotations)

                else:
                    indices_dict[path] = (c_indices, c_annotations)

    return indices_dict


def find_column_indices_hdf5(h5p, path, items_to_find):
    """Given a path find slices for the array that match a pattern. Items to find can either be a single
        item, a list, or a tuple. The function matches items vertically."""

    split_path = path.split("/")
    if "column_annotations" == path[-1]:
        pass
    else:
        split_path += ["column_annotations"]
        path = "/".join(split_path)

    column_annotations = h5p[path][...]
    return find_column_indices(column_annotations, items_to_find)


def find_column_indices(column_annotations, items_to_find):
    if items_to_find.__class__ != ().__class__:
        if items_to_find.__class__ == [].__class__:
            items_to_find = tuple(items_to_find)
        else:
            items_to_find = tuple([items_to_find])

    filter_indexes = []
    for i in range(len(items_to_find)):

        if i == 0:
            restrictive_indices = np.arange(column_annotations.shape[1])
        else:
            restrictive_indices = filter_indexes[i-1]
        value_to_find = items_to_find[i]

        filter_index = np.where(column_annotations[i, restrictive_indices] == value_to_find)
        filter_indexes += [restrictive_indices[filter_index[0]]]

    return filter_indexes[-1], column_annotations[:, filter_indexes[-1]]


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
                core_array_ds = create_data_set_with_new_number_of_rows(fp5, wfp5, path, row_array[0].shape[0])
                core_array_ds[...] = fp5[path][row_array[0], :]
            else:
                copy_data_set(fp5, wfp5, path, compression="gzip")
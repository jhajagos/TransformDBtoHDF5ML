"""
Practical post processing for example creating normalized values
"""


# Question: Figure out how to overwrite a data set
# Create a new dataset but open the file with the 'r+'

import h5py
import numpy as np


[
    {
        "path": "/classes/independent/dx",
        "write_path": "/classes/independent/dx_presence",
        "rule": "zero_or_one"
    },
]


def generate_row_slices(rows, chunks):
    full_iterations = rows // chunks

    list_of_slices = []
    for i in range(full_iterations):
        list_of_slices += [(i * chunks, (i + 1) * chunks)]

    if rows % chunks != 0:
        list_of_slices += [(full_iterations * chunks, rows)]

    return list_of_slices


def main(hdf5_file, rules, chunks=5000, compression="gzip"):
    f5 = h5py.File(hdf5_file, 'r+')

    for rule in rules:
        print("Executing '%(rule)s' on path '%(path)s'" % rule)

        rule_name = rule["rule"]
        path = rule["path"]
        core_array_path = path + "/core_array"
        ds = f5[core_array_path]

        source_shape = ds.shape
        source_dtype = ds.dtype
        rows, columns = source_shape

        write_path = rule["write_path"]
        write_path_core_array = write_path + "/core_array"

        rule_processed = 0

        source_column_annotation_path = path + "/column_annotations"
        destination_column_annotation_path = write_path + "/column_annotations"

        if rule_name == "zero_or_one":

            zero_one_ds = f5.create_dataset(write_path_core_array, shape=source_shape, dtype=source_dtype,
                                            compression=compression)
            row_slices = generate_row_slices(rows, chunks)

            for row_slice in row_slices:
                start_index, end_index = row_slice
                number_of_rows = end_index - start_index
                sliced_array = ds[start_index:end_index,:]
                sliced_zero_one_array = np.zeros(shape=(number_of_rows, columns))
                sliced_zero_one_array[np.nonzero(sliced_array)] = 1
                zero_one_ds[start_index:end_index, :] = sliced_zero_one_array

            column_annotations = f5[source_column_annotation_path][...]
            rule_processed = 1

        elif rule_name == "normalize_category_count":

            rule_processed = 1
            column_annotations = f5[source_column_annotation_path][...]
            categories = column_annotations[0, :]

            index_list = []
            start_i = 0
            current_categories = categories[0]
            for i in range(len(categories)):
                if categories[i] != current_categories:
                    index_list += [(start_i, i)]
                    current_categories = categories[i]
                    start_i = i
            index_list += [(start_i, i+1)]

            normalized_ds = f5.create_dataset(write_path_core_array, shape=source_shape, dtype=source_dtype,
                                            compression=compression)

            row_slices = generate_row_slices(rows, chunks)

            for row_slice in row_slices:
                start_row_index, end_row_index = row_slice

                for index in index_list:
                    start_column_index, end_column_index = index
                    column_category = ds[start_row_index:end_row_index, start_column_index:end_column_index]

                    sum_category = np.reshape(np.sum(column_category, axis=1), (column_category.shape[0], 1))

                    normalized_counts = column_category / sum_category
                    normalized_counts[np.isnan(normalized_counts)] = 0

                    normalized_ds[start_row_index:end_row_index, start_column_index:end_column_index] = normalized_counts

        else:
            print("Rule '%s' not supported" % rule_name)

        if rule_processed:
            ds_column_annotations = f5.create_dataset(destination_column_annotation_path, shape=column_annotations.shape, dtype=column_annotations.dtype, compression=compression)
            ds_column_annotations[...] = column_annotations

        print("Writing to '%(write_path)s'" % rule)


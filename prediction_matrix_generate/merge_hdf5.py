from utility_functions import get_all_paths
import h5py
import numpy as np


def merge_f_pointer_hdf5(f5_p1, f5_p2, f5_w, identifier_path_1, identifier_path_2, identifier_position_1,
                         identifier_position_2, linking_list, join_outer=False, path_name_prefix_1="", path_name_prefix_2="",
                         chunk_size=1000):

    identifier_1 = f5_p1[identifier_path_1][:, identifier_position_1]
    identifier_2 = f5_p2[identifier_path_2][:, identifier_position_2]

    # Build dictionaries
    position_dict_1 = {}
    position_dict_2 = {}

    for i in range(identifier_1.shape[0]):
        position_dict_1[identifier_1[i]] = i

    for i in range(identifier_2.shape[0]):
        position_dict_2[identifier_2[i]] = i

    # print(linking_list)
    # print(identifier_1)
    # print(identifier_2)
    #
    # print(position_dict_1)
    # print(position_dict_2)

    # Sort linking list to correspond to order in matrix
    linking_list_to_sort = []

    for link_tuple in linking_list:
        id_1, id_2 = link_tuple

        if id_1 in position_dict_1:
            id_1_pos = position_dict_1[id_1]
        else:
            id_1_pos = None

        if id_2 in position_dict_2:
            id_2_pos = position_dict_2[id_2]
        else:
            id_2_pos = None

        if id_1_pos is not None:
            linking_list_to_sort += [[id_1_pos, id_1, id_2_pos, id_2]]

    linking_list_to_sort.sort(key=lambda x: x[0])

    #print(linking_list_to_sort)
    i = 0
    new_linking_list_to_sort = []
    for link_tuple in linking_list_to_sort:
        new_linking_list_to_sort += [[i] + link_tuple]
        i += 1

    new_number_of_rows  = len(new_linking_list_to_sort)

    left_row_copy_array = np.zeros((new_number_of_rows, 2), dtype= np.dtype(int))
    for i in range(new_number_of_rows):
        left_row_copy_array[i, :] = np.array([new_linking_list_to_sort[i][1], new_linking_list_to_sort[i][0]])

    #print(left_row_copy_array)
    n_nones = 0
    for i in range(new_number_of_rows):
        if new_linking_list_to_sort[i][3] is None:
            n_nones += 1

    right_row_copy_array = np.zeros((new_number_of_rows-n_nones, 2), dtype=np.dtype(int))

    right_i = 0
    for i in range(new_number_of_rows):
        #print(new_linking_list_to_sort[i])

        right_id_pos = new_linking_list_to_sort[i][3]
        left_id_pos = new_linking_list_to_sort[i][0]

        if right_id_pos is not None:
            right_row_copy_array[right_i, :] = np.array([right_id_pos, left_id_pos])
            right_i += 1

    p1_paths = get_all_paths(f5_p1["/"])
    for p1_path in p1_paths:
        path_to_write_1 = path_name_prefix_1 + p1_path
        #print(path_to_write_1, p1_path)
        ds_c = f5_p1[p1_path]
        if p1_path[-1 * len("core_array"):] == "core_array":
           copy_numeric_rows_from_path(f5_p1, p1_path, f5_w, path_to_write_1, left_row_copy_array)
        else:
            cds = f5_w.create_dataset(path_to_write_1, shape=ds_c.shape, dtype=ds_c.dtype)
            cds[...] = ds_c[...]

    p2_paths = get_all_paths(f5_p2["/"])
    for p2_path in p2_paths:
        path_to_write_2 = path_name_prefix_2 + p2_path
        ds_c = f5_p2[p2_path]
        if p2_path[-1 * len("core_array"):] == "core_array":
            copy_numeric_rows_from_path(f5_p2, p2_path, f5_w, path_to_write_2, right_row_copy_array, number_of_rows=left_row_copy_array.shape[0])
        else:
            cds = f5_w.create_dataset(path_to_write_2, shape=ds_c.shape, dtype=ds_c.dtype)
            cds[...] = ds_c[...]


# Start with the left side
def copy_numeric_rows_from_path(f5, path_1, wf5, path_2, copy_array, number_of_rows=None):

    f5a = f5[path_1]
    f5a_shape = f5a.shape
    f5a_dtype = f5a.dtype

    if number_of_rows is None:
        number_of_rows = copy_array.shape[0]

    copied_array = np.zeros((number_of_rows, f5a_shape[1]), dtype=f5a_dtype)

    for i in range(copy_array.shape[0]):
        copied_array[copy_array[i,1], :] = f5a[copy_array[i,0], :]

    d_s_w = wf5.create_dataset(path_2, shape=(number_of_rows, f5a_shape[1]), dtype=f5a_dtype, compression="gzip")
    d_s_w[...] = copied_array[...]

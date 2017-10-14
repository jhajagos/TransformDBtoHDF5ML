import unittest
import h5py
import numpy as np
import os
from prediction_matrix_generate.merge_hdf5 import merge_f_pointer_hdf5


class TestMergeHDF5(unittest.TestCase):

    def setUp(self):

        i1 = [[500], [600], [700], [800], [900], [400]] #6
        ih1 = [["eid"], [""], [""]]
        d1 = [[22, 0, 0, 0, 1],
              [23, 0, 1, 0, 1],
              [32, 1, 0, 0, 0],
              [40, 0, 1, 0, 0],
              [ 5, 0, 1, 1, 0],
              [35, 0, 0, 0, 1]]

        dh1 = [["age", "v1", "v2", "v3", "v4"], ["", "", "", "", ""], ["", "", "", "", ""]]

        i2 = [[101, 41], [101, 42], [103, 43], [103, 44], [22, 45], [22, 46], [23, 47], [25, 50], [25, 52]] # 9
        ih2 = [["pid", "id"], ["", ""], ["", ""]]
        d2 = [[1,0,1],
              [0,0,0],
              [1,1,1],
              [1,0,1],
              [0,0,0],
              [1,1,1],
              [0,0,0],
              [0,1,1],
              [1,1,0]]

        dh2 = [["dv1", "dv2", "dv3"], ["", "", ""], ["", "", ""], ["", "", ""]]

        self.linking_list = [(400,41), (500, 43), (700, 47), (900, 52), (600, 2222222), (9, 9)]

        f_path1 = "./files/test_m1.hdf5"
        if os.path.exists(f_path1):
            os.remove(f_path1)

        f_path2 = "./files/test_m2.hdf5"
        if os.path.exists(f_path2):
            os.remove(f_path2)

        with h5py.File(f_path1, "w") as f5p1:
            f5p1["/x/identifier/core_array"] = np.array(i1)
            f5p1["/x/identifier/column_annotations"] = np.array(ih1)

            f5p1["/d1/data/core_array"] = d1
            f5p1["/d1/data/column_annotations"] = dh1

        with h5py.File(f_path2, "w") as f5p2:
            f5p2["/identifier/core_array"] = np.array(i2)
            f5p2["/identifier/column_annotations"] = np.array(ih2)

            f5p2["/d2/data/core_array"] = d2
            f5p2["/d2/data/column_annotations"] = dh2

        self.f_path1 = f_path1
        self.f_path2 = f_path2

    def test_join_hdf5(self):

        f51 = h5py.File(self.f_path1)
        f52 = h5py.File(self.f_path2)

        write_hdf5_file_path = "./files/test_joined.hdf5"

        if os.path.exists(write_hdf5_file_path):
            os.remove(write_hdf5_file_path)

        w5p2 = h5py.File(write_hdf5_file_path, "w")

        merge_f_pointer_hdf5(f51, f52, w5p2, "/x/identifier/core_array", "/identifier/core_array", 0, 1,
                             self.linking_list, path_name_prefix_1="/left", path_name_prefix_2="/right")

        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()

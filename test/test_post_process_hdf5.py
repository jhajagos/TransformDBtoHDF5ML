import os
import unittest

import h5py
import numpy as np

import prediction_matrix_generate.post_process_hdf5 as pp


class TestNormalizeCounts(unittest.TestCase):
    def setUp(self):
        if os.path.exists("./files/test_hdf5.hdf5"):
            os.remove("./files/test_hdf5.hdf5")

        f5 = h5py.File("./files/test_hdf5.hdf5", "w")
        ca = f5.create_dataset("/lab/categories/column_annotations", dtype="S128", shape=(3,7))
        lab_tests = [["Hemoglobin","Hemoglobin", "Blood Glucose", "Blood Glucose", "Blood Glucose", "WBC", "WBC"],
                     ["Normal", "Abnormal", "Low", "Normal", "High", "Normal", "Abnormal"],
                     ["", "", "", "", "", "", ""]]
        lab_test_array = np.array(lab_tests, dtype="S128")
        ca[...] = lab_test_array

        lab_test_values = [[0, 1, 5, 2, 1, 10, 1], [0, 0, 0, 0, 0, 0, 1], [5, 0, 0, 10, 0, 15, 1]]

        lab_test_corearray = np.array(lab_test_values)
        corea = f5.create_dataset("/lab/categories/core_array", shape=(3,7))
        corea[...] = lab_test_corearray

    def test_apply_rules(self):
        rules = [
           {
               "path": "/lab/categories/",
               "write_path": "/lab/category_present/",
               "rule": "zero_or_one"
           },
           {
                "path": "/lab/categories/",
                "write_path": "/lab/category_count_normalized/",
                "rule": "normalize_category_count"
           }
        ]
        pp.main("./files/test_hdf5.hdf5", rules, chunks=2)

        f5t = h5py.File("./files/test_hdf5.hdf5", "r")
        present_array = f5t["/lab/category_present/core_array"][...]
        self.assertEqual(6 + 1 + 4, np.sum(present_array))

        normalized_array = f5t["/lab/category_count_normalized/core_array"][...]
        self.assertEquals(7, np.sum(normalized_array))

if __name__ == '__main__':
    unittest.main()

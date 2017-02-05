__author__ = 'janos'

import json
import os
import unittest
import h5py
import numpy as np
import document_hdf5


class RunHDF5Mapping(unittest.TestCase):

    def setUp(self):

        # Build single file
        directory = "./files/"
        data_json_files = ["fake_inpatient_readmission_data_1.json", "fake_inpatient_readmission_data_2.json",
                           "fake_inpatient_readmission_data_3.json"]

        new_data_dict = {}
        for data_json_file in data_json_files:
            with open(os.path.join(directory, data_json_file), "r") as fj:
                data_dict = json.load(fj)

                for key in data_dict:
                    new_data_dict[key] = data_dict[key]

            new_data_file_name = os.path.join(directory, "fake_inpatient_readmission_data_all.json")
            with open(new_data_file_name, "w") as fjw:
                json.dump(new_data_dict, fjw, sort_keys=True, indent=4, separators=(',', ': '))

            with open(os.path.join(directory, "test_all_file_batch.json"), "w") as fjw:
                json.dump([{"batch_id": 1, "data_json_file": new_data_file_name}], fjw, sort_keys=True, indent=4, separators=(',', ': '))

    def test_filter_and_first_set(self):

        document_hdf5.main("filter_test", "./files/test_single_file_batch.json", "./files/configuration_to_build_matrix.json")
        f5 = h5py.File("./files/filter_test_1.hdf5",'r')

        lab_first = f5["/independent/classes/lab/first/core_array"][...]
        lab_first_day = f5["/independent/classes/lab/first_day/core_array"][...]


        self.assertNotEqual(lab_first.tolist(), lab_first_day.tolist())


    def test_mapping_single_file(self):

        if os.path.exists("./files/transaction_test_1.hdf5"):
            os.remove("./files/transaction_test_1.hdf5")

        document_hdf5.main("transaction_test", "./files/test_single_file_batch.json", "./files/configuration_to_build_matrix.json")
        f5 = h5py.File("./files/transaction_test_1.hdf5",'r')
        dca = f5["/independent/classes/discharge/core_array"][...]
        self.assertEquals(dca.shape, (2, 5))

        dcac = f5["/independent/classes/discharge/column_annotations"][...]
        self.assertEquals(dcac.shape, (3, 5))

        lab_count = f5["/independent/classes/lab/count/core_array"][...]
        lab_count_c = f5["/independent/classes/lab/count/column_annotations"][...]

        self.assertEqual(lab_count.tolist(), [[4.,  0.], [4.,  1.]])
        self.assertEqual(lab_count_c.tolist(), [['BUN', 'Troponin'], ['value', 'value'], ['count', 'count']])

        lab_category_count = f5["/independent/classes/lab/category/core_array"][...]
        lab_category_count_c = f5["/independent/classes/lab/category/column_annotations"][...]

        self.assertEqual(lab_category_count.tolist(), [[4.,  0.,  0.,  0.], [ 1.,  1.,  2.,  1.]])
        self.assertEqual(lab_category_count_c.tolist(), [['BUN', 'BUN', 'BUN', 'Troponin'],
                                                         ['high', 'low', 'normal', 'extreme'],
                                                         ['', '', '', '']])

        self.assertEquals(np.sum(lab_category_count), np.sum(lab_count), "Sums should be equal")

        diagnosis1 = f5["/independent/classes/diagnosis/core_array"][...]
        diagnosis_c = f5["/independent/classes/diagnosis/column_annotations"]

        self.assertTrue(np.sum(diagnosis1) > 0)

    def test_mapping_single_compressed_file(self):

        if os.path.exists("./files/transaction_test_1.hdf5"):
            os.remove("./files/transaction_test_1.hdf5")

        document_hdf5.main("transaction_test", "./files/test_all_file_batch_gz.json", "./files/configuration_to_build_matrix.json")

    def test_mapping_split_file(self):

        document_hdf5.main("transaction_test_split", "./files/test_simple_batch.json", "./files/configuration_to_build_matrix.json")
        document_hdf5.main("transaction_test_combined", "./files/test_all_file_batch.json", "./files/configuration_to_build_matrix.json")

        f5s1 = h5py.File("./files/transaction_test_split_1.hdf5", 'r')
        f5s2 = h5py.File("./files/transaction_test_split_2.hdf5", 'r')
        f5s3 = h5py.File("./files/transaction_test_split_3.hdf5", 'r')

        f5c = h5py.File("./files/transaction_test_combined_1.hdf5", 'r')

        lab_category_count_1 = f5s1["/independent/classes/lab/category/core_array"][...]
        lab_category_count_2 = f5s2["/independent/classes/lab/category/core_array"][...]
        lab_category_count_3 = f5s3["/independent/classes/lab/category/core_array"][...]

        lab_category_count_c = f5c["/independent/classes/lab/category/core_array"][...]

        # Concatenate to create a single file
        lab_category_count_cs = np.concatenate((lab_category_count_1, lab_category_count_2, lab_category_count_3))

        self.assertEquals(lab_category_count_c.shape, lab_category_count_cs.shape)
        self.assertEquals(np.sum(lab_category_count_c), np.sum(lab_category_count_cs))

        # Read the combined file
        f5ca = h5py.File("./files/transaction_test_split_combined.hdf5", "r")
        lab_category_count_ca = f5ca["/independent/classes/lab/category/core_array"][...]

        self.assertEqual(lab_category_count_c.shape, lab_category_count_ca.shape)
        self.assertEquals(np.sum(lab_category_count_c), np.sum(lab_category_count_ca))

        # print(np.sum(lab_category_count_c))

        # TODO write test that the two printed matrices are equal
        # import pprint
        # pprint.pprint(lab_category_count_c.tolist())
        # pprint.pprint(lab_category_count_cs.tolist())

        #self.assertEquals(lab_category_count_c.tolist(), lab_category_count_cs.tolist())


if __name__ == '__main__':
    unittest.main()

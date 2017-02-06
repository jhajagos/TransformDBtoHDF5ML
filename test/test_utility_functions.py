import unittest
import h5py
from utility_functions import *


class TestUtilityFunctions(unittest.TestCase):

    def setUp(self):
        self.hp = h5py.File("./test/test_matrix_combined.hdf5")

    def test_select_columns(self):

        column_annotations = self.hp["/independent/classes/discharge/column_annotations"][...]

        column_indices_g, column_details_g = find_column_indices(column_annotations, ("gender",))
        l_column_indices_g = column_indices_g.tolist()
        l_column_details_g = column_details_g.tolist()

        self.assertEqual(len(l_column_indices_g), 2)
        self.assertEqual(len(l_column_details_g), 3)
        self.assertEqual(len(l_column_details_g[0]), 2)
        self.assertEqual(l_column_details_g[0], ["gender", "gender"])
        self.assertEqual(l_column_details_g[1], ["F", "M"])

        column_indices_m, column_details_m = find_column_indices(column_annotations, ["gender", "M", ""])
        self.assertEqual(column_indices_m.tolist(), [2])

        bun_high_indices, bun_high_annot = find_column_indices_hdf5(self.hp, "/independent/classes/lab/day/category", ("BUN", "high"))
        bun_high_values = self.hp["/independent/classes/lab/day/category/core_array"][:,bun_high_indices]

        self.assertEqual(bun_high_values.tolist(), [2.0, 0.0])

    def test_select_multiple_columns(self):

        column_dict = find_multiple_column_indices_hdf5(self.hp, [("/independent/classes/discharge",
                                                                  ("gender","M"), "age"),
                                                                  ("/independent/classes/lab/day/category",
                                                                     ("BUN","high"))])
        print(column_dict)
        self.assertEqual(len(column_dict), 2)

    def test_find_rows_with_rows(self):

        indices_array_1 = query_rows_hdf5(self.hp, [("/independent/classes/discharge", "age", 58)])

        self.assertEqual(len(indices_array_1), 1)
        self.assertEqual(indices_array_1[0], 1)

        indices_array_2 = query_rows_hdf5(self.hp, [("/independent/classes/discharge", "age", (50, 58))])

        self.assertEqual(len(indices_array_2), 2)
        self.assertEqual(indices_array_2.tolist(), [0,1])

        indices_array_3 = query_rows_hdf5(self.hp, [("/independent/classes/lab/last_item", "BUN", 0.5, ">")])
        print(indices_array_3)
        self.assertEqual(len(indices_array_3), 1)

        indices_array_4 = query_rows_hdf5(self.hp, [("/independent/classes/discharge", "age", (50, 58)),
                                                    ("/independent/classes/lab/last_item", "Troponin", 0.0, ">")])
        self.assertEqual(len(indices_array_4), 1)

if __name__ == '__main__':
    unittest.main()

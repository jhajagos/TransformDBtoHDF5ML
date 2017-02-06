import unittest

"""
C:\Users\janos\GitHub\TransformDBtoHDF5ML\test>python ..\scripts\build_document_mapping_from_db.py -r ".\files\runtime_config_test.json" -c ".\files\test_mapping_document.json"
C:\Users\janos\GitHub\TransformDBtoHDF5ML\test>python ..\scripts\build_hdf5_matrix_from_document.py -a trans_test -c .\files\configuration_to_build_matrix.json -b .\files\transactions_batches.json
"""


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()

__author__ = 'jhajagos'

import gzip
import json
import os
import unittest

import sqlalchemy as sa
import db_document


schema = """
create table encounters (
  encounter_id int,
  medical_record_number int,
  drg char(3),
  patient_gender char(1),
  patient_age int,
  day_from_start int
);

create table diagnoses (
  encounter_id int,
  sequence_id int,
  diagnosis_code varchar(10),
  diagnosis_description varchar(255),
  ccs_code char(3),
  ccs_description varchar(255)
);

create table laboratory_tests (
  encounter_id int,
  test_name varchar(255),
  code varchar(100),
  numeric_value float,
  non_numeric_value varchar(255),
  test_status char(1),
  minutes_since_midnight int
)
"""

encounters = [(1000, 10, "050", "M", 30, 1),
              (1001, 10, "051", "M", 30, 28),
              (1003, 11, "288", "F", 20, 10),
              (1005, 50, "001", "U", 85, 15),
              (1009, 50, "001", "U", 85, 15),
              (1010, 50, "001", "U", 85, 15),
              (999, 22, "002", "U",  85, 15),
              (20000, 22, "002", "U",  85, 15),
              ]

diagnoses = [
    (1000, 1, "25000", "DIABETES UNCOMPL TYPE II", 49, "DiabMel no c"),
    (1000, 2, "4011", "BENIGN HYPERTENSION", 98, "HTN"),
    (1003, 1, "1309", "TOXOPLASMOSIS NOS",  8, "Oth infectns"),
    (1005, 1, "36504", "OCULAR HYPERTENSION", 88, "Glaucoma")
    ]

laboratory_tests = [
    (1000, "HEMOGLOBIN A1C/HEMOGLOBIN.TOTAL", "4548-4", 9.0, None, "H", 60*12),
    (1000, "C. difficile toxin B gene (tcdB), PCR", "54067-4", None, "Positive", "H", 60*13),
    (1000, "WHITE BLOOD CELL COUNT", "6690-2", 9.5, None, "N", 60*12 + 10),
    (1000, "WHITE BLOOD CELL COUNT", "6690-2", 0.1, None, "L", 60 * 15),
    (1001, "HEMOGLOBIN A1C/HEMOGLOBIN.TOTAL", "4548-4", 5.0, None, "N", 60*9),
    (1005, "C. difficile toxin B gene (tcdB), PCR", "54067-4", None, "Negative", "L", 60*9)
]


class TestDBMappingJSON(unittest.TestCase):

    def setUp(self):

        if os.path.exists("./files/test_db.db3"):
            os.remove("./files/test_db.db3")

        engine = sa.create_engine("sqlite:///./files/test_db.db3")
        connection = engine.connect()

        for statement in schema.split(";"):
            connection.execute(statement)

        meta_data = sa.MetaData(connection, reflect=True)

        encounters_obj = meta_data.tables["encounters"]
        for encounter in encounters:
            connection.execute(encounters_obj.insert(encounter))

        diagnoses_obj = meta_data.tables["diagnoses"]
        for diagnosis in diagnoses:
            connection.execute(diagnoses_obj.insert(diagnosis))

        laboratory_tests_obj = meta_data.tables["laboratory_tests"]
        for lab_test in laboratory_tests:
            connection.execute(laboratory_tests_obj.insert(lab_test))
        connection.close()

    def test_json_mapping_split_file(self):

        mapping_jsons, order_json = db_document.main_json("./files/test_mapping_document.json",
                                                                             "./files/runtime_config_test_split.json")

        mapping_json_one = mapping_jsons[0]
        mapping_json_two = mapping_jsons[1]

        with open(mapping_json_one, "r") as f:
            mapping_dict = json.load(f)
            self.assertEquals(len(mapping_dict), 5)
            keys = ['1000','1001','1005', '1009']

            for key in keys:
                self.assertTrue(key in mapping_dict.keys())


        with open(mapping_json_two, "r") as f:
            mapping_dict = json.load(f)
            self.assertEquals(len(mapping_dict), 3)
            keys = ['1010', '999', '20000']

            for key in keys:
                self.assertTrue(key in mapping_dict.keys())

    def test_json_mapping_one_file(self):

        mapping_jsons, order_json = db_document.main_json("./files/test_mapping_document.json", "./files/runtime_config_test.json")

        mapping_json_one = mapping_jsons[0]

        with open(mapping_json_one, "r") as f:
            mapping_dict = json.load(f)
            self.assertEquals(len(mapping_dict), 8)
            keys = ['999', '1005', '1003', '1001', '1000', '1009', '1010', '20000']

            for key in keys:
                self.assertTrue(key in mapping_dict.keys())

    def test_json_mapping_one_file_with_ujson_and_compression(self):
        mapping_jsons, order_json = db_document.main_json("./files/test_mapping_document.json",
                                                          "./files/runtime_config_test_gz_ujson.json")

        mapping_json_one = mapping_jsons[0]
        with gzip.open(mapping_json_one, "rb") as f:
            mapping_dict = json.loads(f.read().decode("ascii"))
            self.assertEquals(len(mapping_dict), 8)
            keys = ['999', '1005', '1003', '1001', '1000', '1009', '1010', '20000']

            for key in keys:
                self.assertTrue(key in mapping_dict.keys())


if __name__ == '__main__':
    unittest.main()

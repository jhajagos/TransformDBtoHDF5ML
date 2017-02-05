import json
import os
import pprint
import unittest

import pymongo

from scripts import dump_documents_from_doc_db


class TestDumpJSON(unittest.TestCase):

    def setUp(self):

        with open("./files/runtime_config_test.json", "r") as f:
            test_load = json.load(f)
            pprint.pprint(test_load)

            """
            u'mongo_db_config': {u'collection_name': u'mapped_encounters',
                      u'connection_string': u'mongodb://localhost',
                      u'database_name': u'encounters',

            """

            self.test_config = test_load

        collection_name = "load_document"
        connection_string = test_load["mongo_db_config"]["connection_string"]

        directory = "./files/"
        data_json_files = ["fake_inpatient_readmission_data_1.json", "fake_inpatient_readmission_data_2.json",
                           "fake_inpatient_readmission_data_3.json"]

        cs = pymongo.MongoClient(connection_string)
        database_name = test_load["mongo_db_config"]["database_name"]

        collection = cs[database_name][collection_name]
        collection.delete_many({})

        for data_json_file in data_json_files:
            data_json_file_name = os.path.join(directory, data_json_file)
            with open(data_json_file_name, "r") as f:
                data_dicts = json.load(f)
                for key in data_dicts:
                    data_dict = data_dicts[key]
                    collection.insert(data_dict)

    def test_extract(self):

        batches_dict_1 = dump_documents_from_doc_db.main({"independent.classes.discharge.gender": "F"}, "./files/",
                                                         "test_dump_docs_1",
                                                         self.test_config["mongo_db_config"])
        print(batches_dict_1[0]["data_json_file"])
        with open(batches_dict_1[0]["data_json_file"], "r") as f:
            data_dict_1 = json.load(f)
            self.assertEquals(2, len(data_dict_1))

        batches_dict_2 = dump_documents_from_doc_db.main({"independent.classes.discharge.gender": "M"}, "./files/",
                                                           "test_dump_docs_2",
                                                         self.test_config["mongo_db_config"])

        with open(batches_dict_2[0]["data_json_file"], "r") as f:
            data_dict_2 = json.load(f)
            self.assertEquals(3, len(data_dict_2))


if __name__ == '__main__':
    unittest.main()

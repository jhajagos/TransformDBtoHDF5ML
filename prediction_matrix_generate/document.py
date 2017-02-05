try:
    import ujson
except ImportError:
    import json as ujson

from utility_functions import data_dict_load
import pymongo
import os
import json
import shutil
import gzip


def write_keyed_json_file(base_directory, base_name, nth_file, file_batches_dict, query_results_dict, key_orders,
                          use_gzip_compression=True, use_ujson=True):
    """Write query"""

    json_file_name = os.path.join(base_directory, base_name + "_" + str(nth_file) + ".json")
    sort_order_file_name = os.path.join(base_directory, base_name + "_" + str(nth_file) + "_key_order.json")

    with open(json_file_name, "w") as fw:
        if not use_ujson:
            json.dump(query_results_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            ujson.dump(query_results_dict, fw)

    if use_gzip_compression:
        with open(json_file_name, 'rb') as f_in, gzip.open(json_file_name + ".gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        os.remove(json_file_name)
        json_file_name += ".gz"

    with open(sort_order_file_name, "w") as fw:
        json.dump(key_orders, fw)

    file_batches_dict += [{"batch_id": nth_file, "data_json_file": json_file_name,
                           "sort_order_name": sort_order_file_name}]

    return file_batches_dict


def write_document_from_main(query_to_run, base_directory, base_name, runtime_config, size_of_batches=5000,
                             overwritten_collection_name=None):
    """Write a document from document database"""

    connection_string = runtime_config["connection_string"]
    database_name = runtime_config["database_name"]
    client = pymongo.MongoClient(connection_string)

    if overwritten_collection_name is None:
        collection_name = runtime_config["collection_name"]
    else:
        collection_name = overwritten_collection_name

    database = client[database_name]
    collection = database[collection_name]

    cursor = collection.find(query_to_run)
    i = 0
    j = 0

    file_batches_dict = []
    query_results_dict = {}
    key_orders = []

    for row_dict in cursor:

        row_dict.pop("_id", None)

        query_results_dict[i] = row_dict
        key_orders += [i]

        if i > 0 and i % size_of_batches == 0:

            j += 1
            file_batches_dict = write_keyed_json_file(base_directory, base_name, j, file_batches_dict,
                                                      query_results_dict, key_orders)
            key_orders = []
            query_results_dict = {}
            print("Extracted %s documents" % i)

        i += 1

    if len(query_results_dict) > 0:
        j += 1
        file_batches_dict = write_keyed_json_file(base_directory, base_name, j, file_batches_dict, query_results_dict,
                                                  key_orders)

    batch_file_name = os.path.join(base_directory, base_name + "_batches.json")
    with open(batch_file_name, "w") as fw:
        json.dump(file_batches_dict, fw)

    return file_batches_dict


def load_document_main(json_json_files_to_process, runtime_json, collection_name_override=None):
    with open(runtime_json, "r") as fj:
        runtime_config = json.load(fj)

    mongo_db_config = runtime_config["mongo_db_config"]
    connection_string = mongo_db_config["connection_string"]
    database_name = mongo_db_config["database_name"]

    if collection_name_override is None:
        collection_name = mongo_db_config["collection_name"]
    else:
        collection_name = collection_name_override

    refresh_collection = mongo_db_config["refresh_collection"]
    print("Connecting to MongoDB '%s'" % connection_string)
    client = pymongo.MongoClient(connection_string)

    database = client[database_name]
    collection = database[collection_name]

    if refresh_collection:
        collection.delete_many({})

    initial_collection_count = collection.count()

    with open(json_json_files_to_process, "r") as fj:
        json_files_to_to_process = json.load(fj)

        j = 0
        for json_file_dict in json_files_to_to_process:
            json_file_name = json_file_dict["data_json_file"]
            print("Loading '%s'" % json_file_name)

            data_dict = data_dict_load(json_file_name)
            i = 0
            for datum_key in data_dict:

                collection.insert(data_dict[datum_key],
                                  check_keys=False)  # Have to makes sure keys do not have a "." or "$" in it

                if i > 0 and i % 500 == 0:
                    print("Inserted '%s' documents" % i)
                i += 1
            j += i

        print("Inserted '%s' total documents" % j)

    print("Added %s items in collection '%s' housed in database '%s'" % (
    collection.count() - initial_collection_count, collection_name, database_name))
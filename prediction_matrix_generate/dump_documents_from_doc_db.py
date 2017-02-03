"""
This program extracts a collection from a MongoDB instance and formats in a way the
pipeline works.
"""

try:
    import ujson
except ImportError:
    import json as ujson

import pymongo
import os
import json
import sys
import shutil
import gzip
import pprint


def write_keyed_json_file(base_directory, base_name, nth_file, file_batches_dict, query_results_dict, key_orders,
                          use_gzip_compression=True, use_ujson=True):
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


def main(query_to_run, base_directory, base_name, runtime_config, size_of_batches=5000, overwritten_collection_name=None):

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

if __name__ == "__main__":
    query_to_run_json = sys.argv[1]

    with open(query_to_run_json, "r") as f:
        query_to_run = json.load(f)

    pprint.pprint(query_to_run)

    base_directory = sys.argv[2]
    base_name = sys.argv[3]
    runtime_config_json = sys.argv[4]

    if len(sys.argv) > 5:
        collection_name = sys.argv[5]

    with open(runtime_config_json, "r") as f:
        runtime_config = json.load(f)

    if "batch_size" in runtime_config["source_db_config"]:
        batch_size = runtime_config["source_db_config"]["batch_size"]
    else:
        batch_size = 5000

    main(query_to_run, base_directory, base_name, runtime_config["mongo_db_config"],
         size_of_batches=batch_size, overwritten_collection_name=collection_name)
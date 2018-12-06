import elasticsearch as es
import os
import json
import argparse
import re
import sys

try:
    from prediction_matrix_generate.utility_functions import data_dict_load
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    from prediction_matrix_generate.utility_functions import data_dict_load


def dictionary_transform_date(object_to_scan, object_class=None):
    re_datetime = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$")

    if object_class == [].__class__:
        new_object = []
    elif object_class == {}.__class__:
        new_object = {}
    else:
        new_object = {}

    if new_object == [].__class__:
        for i in range(len(object_to_scan)):
            value = object_to_scan[i]
            if value.__class__ == {}.__class__:
                new_object[i] = dictionary_transform_date(value, {}.__class__)
            elif value.__class__ == [].__class__:
                new_object[i] = dictionary_transform_date(value, [].__class__)
            elif value.__class__ == u"".__class__:
                if re_datetime.match(value):
                    new_object[i] = "T".join(value.split(" "))
                else:
                    new_object[i] = value
            elif value == "None":
                pass
            else:
                new_object[i] = value
    else:
        for key in object_to_scan:
            value = object_to_scan[key]
            if value.__class__ == {}.__class__:
                new_object[key] = dictionary_transform_date(value, {}.__class__)
            elif value.__class__ == [].__class__:
                new_object[key] = dictionary_transform_date(value, [].__class__)
            elif value.__class__ == u"".__class__:
                if re_datetime.match(value):
                    new_object[key] = "T".join(value.split(" "))
                else:
                    new_object[key] = value
            elif value == "None":
                pass
            else:
                new_object[key] = value

    return new_object


def main(batch_file_name, elastic_search_host, index_name):

    elastic_server = es.Elasticsearch(elastic_search_host)
    elastic_server.indices.create(index_name, ignore=400)

    i = 0
    with open(batch_file_name) as f:
        batch_files_list = json.load(f)

    for batch_dict in batch_files_list:
        json_file_name =  batch_dict["data_json_file"]
        print("Loading '%s'" % json_file_name)
        document_dict = data_dict_load(json_file_name)

        for key in document_dict:
            document = document_dict[key]
            cleaned_document = dictionary_transform_date(document)
            try:
                elastic_server.index(index=index_name, doc_type="mapped_document", body=cleaned_document)
            except:
                print(cleaned_document)
                raise

            i += 1
    print("Loaded %s documents" % i)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Load generated JSON files into an elastic search store")

    arg_parse_obj.add_argument("-b", "--batch_file_name", dest="batch_file_name", default="./")
    arg_parse_obj.add_argument("-n", "--index-name", dest="index_name", default="extracted_documents")
    arg_parse_obj.add_argument("-e", "--host-elastic-search", dest="host_elastic_search", default="localhost")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.batch_file_name, arg_obj.host_elastic_search, arg_obj.index_name)

import elasticsearch as es
import os
import json
import argparse
import re
from prediction_matrix_generate.utility_functions import data_dict_load


def dictionary_transform_date(dict, object_class):
    re_datetime = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$")

    if object_class == [].__class__:
        new_object = []
    elif object_class == {}.__class__:
        new_object = {}

    for key in dict:
        value = dict[key]
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

    main(arg_obj.batch_file_nname, arg_obj.host_elastic_search, arg_obj.index_name)

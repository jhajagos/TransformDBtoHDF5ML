import csv
import json
import argparse


def main(csv_filename, criteria_field="to_include"):
    """
        Generate a JSON file that specifies which fields to filter from an HDF5 file
        container. Field selection is based CSV on which fields are specified by a column
        "to_include" (user settable) when it equals 1. This can be used by the python program
        compact_subset_hdf5.py to select columns.
    """

    path_field_selection = {}
    path_field_order = []
    with open(csv_filename, "rb") as fw:
        csv_list_dict = csv.DictReader(fw)

        c1_field = "c1"
        c2_field = "c2"
        c3_field = "c3"
        path_field = "path"

        for row_dict in csv_list_dict:

            if row_dict[criteria_field] == "1":
                path = row_dict[path_field]

                if path not in path_field_selection:
                    path_field_selection[path] = []
                    path_field_order += [path]
                else:
                    pass

                c1 = row_dict[c1_field]
                c2 = row_dict[c2_field]
                c3 = row_dict[c3_field]

                fields_to_select = []

                if len(c1.strip()):
                    fields_to_select += [c1]

                if len(c2.strip()):
                    fields_to_select += [c2]

                if len(c3.strip()):
                    fields_to_select += [c3]

                path_field_selection[path] += [fields_to_select]

        path_fields_list = []
        for path in path_field_order:
            path_fields_list += [[path] + path_field_selection[path]]

        json_file_name = csv_filename + ".json"

        with open(json_file_name, "wb") as fw:
            json.dump(path_fields_list, fw, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":

    argparse_obj = argparse.ArgumentParser(description="Generate a JSON file that specifies which fields to filter from an HDF5 file container.")
    argparse_obj.add_argument("-f", "--csv-filename", dest="csv_filename",
                              help="CSV filename to read to determine which fields to filter")
    argparse_obj.add_argument("-c", "--column-name", dest="column_name", default="to_include",
                              help="Optional field to use for field selection")

    arg_obj = argparse_obj.parse_args()
    main(arg_obj.csv_filename, arg_obj.column_name)
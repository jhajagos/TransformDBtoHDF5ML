import csv
import json
import sys


def main(csv_file_name, criteria_field="to_include"):

    path_field_selection = {}
    path_field_order = []
    with open(csv_file_name, "rb") as fw:
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

        json_file_name = csv_file_name + ".json"

        with open(json_file_name, "wb") as fw:
            json.dump(path_fields_list, fw, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    main(sys.argv[1])
__author__ = 'janos'

from utility_functions import *
try:
    import ujson as json
except ImportError:
    import json

import h5py
import numpy as np
import os
import datetime
from utility_functions import data_dict_load


def filter_list_of_interest(list_to_filter, filter_to_apply):
    """Filter an embedded list with a simple value check
        {"field": "sequence_id", "value": 1}"""
    new_filtered_list = []
    filter_field = filter_to_apply["field"]
    value_to_filter_on = filter_to_apply["value"]

    if value_to_filter_on.__class__ == [].__class__:
        filter_type = "list"
    else:
        filter_type = "single_item"

    for item in list_to_filter:
        if filter_field in item:
            value_of_item = item[filter_field]
            if filter_type == "single_item":
                if value_to_filter_on == value_of_item:
                    new_filtered_list += [item]
            else:
                if value_of_item in value_to_filter_on: # list
                    new_filtered_list += [item]
    return new_filtered_list


def get_entry_from_path(dict_with_path, path_list):
    """Traverses a path list in a dict of dicts"""
    while len(path_list):
        key = path_list[0]
        path_list = path_list[1:]

        if dict_with_path is not None:
            if key in dict_with_path:
                dict_with_path = dict_with_path[key]
            else:
                return None
        else:
            None

    return dict_with_path


def generate_column_annotations_categorical_list(categorical_list_dict, column_annotations):
    """Generate column annotations when it is a column list"""
    position_map = categorical_list_dict["position_map"]
    name = categorical_list_dict["name"]
    descriptions = categorical_list_dict["description"]

    position_map_reverse = {}
    for k in position_map:
        v = position_map[k]
        position_map_reverse[v] = k

    n_categories = categorical_list_dict["n_categories"]

    for i in range(n_categories):
        value = position_map_reverse[i]

        if value in descriptions:
            description = descriptions[value]
        else:
            pass

        column_annotations[0, i] = name.encode("ascii", errors="replace")
        column_annotations[1, i] = value.encode("ascii", errors="replace")
        if description is not None:
            column_annotations[2, i] = description.encode("ascii", errors="replace")
        column_annotations[3,i] = "categorical_list"

    return column_annotations


def generate_column_annotations_variables(variables_dict, column_annotations):
    """Generate column annotations based on variables"""

    for variable_dict in variables_dict["variables"]:
        offset_start = variable_dict["offset_start"]
        variable_type = variable_dict["type"]
        if variable_type in ("categorical", "categorical_list"):
            position_map = variable_dict["position_map"]

            position_map_reverse = {}
            for k in position_map:
                v = position_map[k]
                position_map_reverse[v] = k

            for i in range(len(position_map.keys())):

                value = position_map_reverse[i]

                if variable_type == "categorical":
                    field_value = variable_dict["cell_value"]
                else:
                    field_value = variable_dict["name"]

                descriptions = variable_dict["description"]

                if variable_type == "categorical":
                    if value in descriptions:
                        description = descriptions[value]
                    else:
                        description = ""
                else: # categorical_list
                    if field_value in descriptions:
                        description = descriptions[field_value]
                    else:
                        description = ""

                column_annotations[0, offset_start + i] = field_value
                column_annotations[1, offset_start + i] = value
                column_annotations[2, offset_start + i] = description
                column_annotations[3, offset_start + i] = variable_type
        else:
            field_name = variable_dict["cell_value"]

            if variable_type == "numeric_list":
                variable_name = variable_dict["name"]
                if "description" in variable_dict:
                    descriptions = variable_dict["description"]
                    if variable_name in descriptions:
                        description = descriptions[variable_name]
                    else:
                        description = ""
                else:
                    description = ""

                column_annotations[0, offset_start] = variable_name
                column_annotations[1, offset_start] = field_name
                column_annotations[2, offset_start] = description
                column_annotations[3, offset_start] = variable_dict["process"]
            else:
                column_annotations[0, offset_start] = field_name
                column_annotations[3, offset_start] = variable_type

    return column_annotations


def expand_template_dict(data_dict, template_list_dict):
    """Expand out to more detail based on encoded data in a data_dict"""

    new_templates = []
    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]
        if "export_path" in template_dict:
            export_path = template_dict["export_path"]
        else:
            export_path = template_dict["path"]

        if template_type == "classes_templates":
            entry_classes = []

            for data_key in data_dict:
                data = data_dict[data_key]
                entries = get_entry_from_path(data, path)
                if entries is not None:
                    for key in entries:
                        if key not in entry_classes:
                            entry_classes += [key]

            template_class = template_dict["class_template"]
            entry_classes_dict = []
            entry_classes.sort()
            for entry in entry_classes:
                new_template_dict = template_class.copy()
                new_template_dict["name"] = entry
                entry_classes_dict += [new_template_dict]

            new_type = template_dict["class_type"]
            new_template_dict = {"path": path, "type": new_type, new_type: entry_classes_dict, "export_path": export_path}

            new_templates += [new_template_dict]

    template_list_dict += new_templates

    return template_list_dict


def add_offsets_to_translation_dict(template_list_dict):
    """Add offsets to the dicts so as we know where variables start and end"""

    for template_dict in template_list_dict:

        template_type = template_dict["type"]

        if template_type == "variables":
            variable_dicts = template_dict["variables"]
            offset_start = 0

            for variable_dict in variable_dicts:

                variable_type = variable_dict["type"]
                if variable_type in ("categorical", "categorical_list"):
                    n_categories = variable_dict["n_categories"]
                    offset_end = offset_start + n_categories
                else:
                    offset_end = offset_start + 1

                variable_dict["offset_start"] = offset_start
                variable_dict["offset_end"] = offset_end

                offset_start = variable_dict["offset_end"]

        elif template_type == "categorical_list":
            n_categories = template_dict["n_categories"]
            offset_end = n_categories

            template_dict["offset_start"] = 0
            template_dict["offset_end"] = offset_end

    return template_list_dict


def build_translation_dict(data_dict, template_list_dict):
    """For categorical variables build lookup dicts"""
    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]

        if template_type == "variables":
            variables_dict = template_dict[template_type]
            for variable_dict in variables_dict:
                item_dict = {}
                variable_type = variable_dict["type"]
                if variable_type == "categorical":
                    description_dict = {}
                    label_dict = {}

                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        if dict_of_interest is not None:
                            if variable_dict["cell_value"] in dict_of_interest:
                                value_of_interest = str(dict_of_interest[variable_dict["cell_value"]])
                                if value_of_interest in item_dict:
                                    item_dict[value_of_interest] += 1
                                else:
                                    item_dict[value_of_interest] = 1
                                    if "description" in variable_dict:
                                        if variable_dict["description"] in dict_of_interest:
                                            description_dict[value_of_interest] = dict_of_interest[variable_dict["description"]]
                                    if "label" in variable_dict:
                                        if variable_dict["label"] in dict_of_interest:
                                            label_dict[value_of_interest] = dict_of_interest[variable_dict["label"]]

                    data_keys = item_dict.keys()
                    data_keys.sort()

                    position_map = {}
                    for i in range(len(data_keys)):
                        position_map[data_keys[i]] = i
                    variable_dict["position_map"] = position_map
                    variable_dict["labels"] = label_dict
                    variable_dict["description"] = description_dict
                    variable_dict["n_categories"] = len(position_map.keys())

                elif variable_type == "categorical_list":
                    description_dict = {}
                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)

                        variable_name = variable_dict["name"]
                        if dict_of_interest is not None and variable_name in dict_of_interest:
                            list_of_interest = dict_of_interest[variable_name]
                            item_value_field = variable_dict["cell_value"]

                            if len(list_of_interest):
                                first_item = list_of_interest[0]

                                if "description" in variable_dict:
                                    description_field = variable_dict["description"]
                                    description = first_item[description_field]
                                    description_dict[variable_name] = description

                            for item in list_of_interest:
                                if item_value_field in item:
                                    item_value = item[item_value_field]
                                    if item_value in item_dict:
                                        item_dict[item_value] += 1
                                    else:
                                        item_dict[item_value] = 1

                    data_keys = item_dict.keys()
                    data_keys.sort()

                    position_map = {}
                    for i in range(len(data_keys)):
                        position_map[data_keys[i]] = i
                    variable_dict["position_map"] = position_map
                    variable_dict["n_categories"] = len(position_map.keys())
                    variable_dict["description"] = description_dict

                elif variable_type == "numeric_list":
                    description_dict = {}
                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        variable_name = variable_dict["name"]
                        if dict_of_interest is not None and variable_name in dict_of_interest:
                            list_of_interest = dict_of_interest[variable_name]

                            if len(list_of_interest):
                                first_item = list_of_interest[0]

                                if "description" in variable_dict:
                                    description_field = variable_dict["description"]
                                    description = first_item[description_field]
                                    description_dict[variable_name] = description

                    variable_dict["description"] = description_dict

        elif template_type == "categorical_list":

            description_dict = {}
            label_dict = {}
            item_dict = {}

            for data_key in data_dict:
                datum_dict = data_dict[data_key]
                if datum_dict is not None:
                    dicts_of_interest = get_entry_from_path(datum_dict, path)
                    if dicts_of_interest is not None:
                        for dict_of_interest in dicts_of_interest:

                            if template_dict["field"] in dict_of_interest:
                                value_of_interest = str(dict_of_interest[template_dict["field"]])
                                if value_of_interest in item_dict:
                                    item_dict[value_of_interest] += 1
                                else:
                                    item_dict[value_of_interest] = 1
                                    if "description" in template_dict:
                                        description_dict[value_of_interest] = dict_of_interest[template_dict["description"]]
                                    if "label" in template_dict:
                                        label_dict[value_of_interest] = dict_of_interest[template_dict["label"]]

            data_keys = item_dict.keys()
            data_keys.sort()

            position_map = {}
            for i in range(len(data_keys)):
                position_map[data_keys[i]] = i

            template_dict["position_map"] = position_map
            template_dict["labels"] = label_dict
            template_dict["description"] = description_dict
            template_dict["n_categories"] = len(position_map.keys())

    return template_list_dict


def build_hdf5_matrix(hdf5p, data_dict, data_translate_dict_list, data_sort_key_list=None):
    """Each class will be a dataset in a hdf5 matrix"""

    data_items_count = len(data_dict)
    if data_sort_key_list is None:
        data_sort_key_list = data_dict.keys()
        data_sort_key_list.sort()

    for data_translate_dict in data_translate_dict_list:
        template_type = data_translate_dict["type"]

        if "export_path" in data_translate_dict:
            export_path = data_translate_dict["export_path"]
        else:
            export_path = data_translate_dict["path"]

        path = data_translate_dict["path"]

        hdf5_base_path = "/".join(export_path)

        if template_type == "variables":
            if len(data_translate_dict["variables"]):
                offset_end = data_translate_dict["variables"][-1]["offset_end"]
            else:
                offset_end = 0

        if template_type == "categorical_list":
            offset_end = data_translate_dict["offset_end"]

        if template_type in ("variables", "categorical_list"):
            core_array = np.zeros(shape=(data_items_count, offset_end))
            column_annotations = np.zeros(shape=(4, offset_end), dtype="S128")

        if template_type == "variables":

            for variable_dict in data_translate_dict["variables"]:
                offset_start = variable_dict["offset_start"]
                variable_type = variable_dict["type"]
                cell_value_field = variable_dict["cell_value"]

                if variable_type == "categorical":
                    position_map = variable_dict["position_map"]
                    i = 0
                    for data_key in data_sort_key_list:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)

                        if dict_of_interest is not None:
                            if cell_value_field in dict_of_interest:
                                field_value = str(dict_of_interest[cell_value_field])
                                field_value_position = position_map[field_value]
                                core_array[i, offset_start + field_value_position] = 1
                        i += 1

                else:
                    i = 0

                    if "process" in variable_dict:
                        process = variable_dict["process"]
                        variable_name = variable_dict["name"]
                    else:
                        process = None
                        variable_name = None

                    if "filter" in variable_dict:
                        filter_to_apply = variable_dict["filter"]
                    else:
                        filter_to_apply = None

                    if "position_map" in variable_dict:
                        position_map = variable_dict["position_map"]
                    else:
                        position_map = {}

                    for data_key in data_sort_key_list:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        if dict_of_interest is not None:

                            if (variable_name in dict_of_interest) or (dict_of_interest.__class__ == [].__class__): # An embedded list
                                if dict_of_interest.__class__ == {}.__class__:
                                    list_of_interest = dict_of_interest[variable_name]
                                else:
                                    list_of_interest = dict_of_interest

                                if list_of_interest is not None:

                                    if filter_to_apply is not None:
                                        list_of_interest = filter_list_of_interest(list_of_interest, filter_to_apply)

                                    if len(list_of_interest):

                                        if variable_type == 'numeric_list':

                                            if process in ("median", "count", "last_item", "first_item", "min", "max"):
                                                process_list = []
                                                counter = 0
                                                for item in list_of_interest:
                                                    if cell_value_field in item:
                                                        cell_value = item[cell_value_field]
                                                        if cell_value is not None:
                                                            process_list += [cell_value]
                                                        counter += 1

                                                if len(process_list):
                                                    process_array = np.array(process_list)
                                                    if process == "median":
                                                        median_value = np.median(process_array)
                                                        core_array[i, offset_start] = median_value
                                                    elif process == "min":
                                                        min_value = np.min(process_array)
                                                        core_array[i, offset_start] = min_value
                                                    elif process == "max":
                                                        max_value = np.max(process_array)
                                                        core_array[i, offset_start] = max_value
                                                    elif process == "last_item":
                                                        core_array[i, offset_start] = process_list[-1]
                                                    elif process == "first_item":
                                                        core_array[i, offset_start] = process_list[0]
                                                    elif process == "count":
                                                        core_array[i, offset_start] = counter  # Handles None values

                                        elif variable_type == 'categorical_list':
                                            if process == "count_categories":
                                                for item in list_of_interest:
                                                    if cell_value_field in item:
                                                        cell_value = item[cell_value_field]
                                                        if cell_value is not None:
                                                            position = position_map[str(cell_value)]
                                                            core_array[i, offset_start + position] += 1

                            if cell_value_field in dict_of_interest:
                                field_value = dict_of_interest[cell_value_field]
                                if field_value is not None:
                                    if variable_type == "datetime":
                                        try:
                                            time_obj = datetime.datetime.strptime(field_value, "%Y-%m-%d %H:%M:%S") # Seconds since January 1, 1970 Unix time
                                        except ValueError:
                                            try:
                                                time_obj = datetime.datetime.strptime(field_value, "%Y-%m-%d") # Seconds since January 1, 1970 Unix time
                                            except ValueError:
                                                time_obj = datetime.datetime.strptime(field_value, "%Y-%m-%d %H:%M")

                                        field_value = (time_obj - datetime.datetime(1970, 1, 1)).total_seconds()
                                core_array[i, offset_start] = field_value
                        i += 1

        elif template_type == "categorical_list":  # position of item in the categorical list is coded
            if "process" in data_translate_dict:
                process = data_translate_dict["process"]
            else:
                process = None

            cell_value_field = data_translate_dict["cell_value"]
            position_map = data_translate_dict["position_map"]
            i = 0
            for data_key in data_sort_key_list:
                datum_dict = data_dict[data_key]
                dict_of_interest = get_entry_from_path(datum_dict, path)

                if dict_of_interest is not None:
                    j = 0
                    for item_dict in dict_of_interest:
                        if cell_value_field in item_dict:
                            field_value = item_dict[cell_value_field]
                            if process == "list_position":
                                position = position_map[str(field_value)]
                                if core_array[i, position] == 0:
                                    core_array[i, position] = j + 1
                            elif process == "occurs_in_list":
                                position = position_map[str(field_value)]
                                core_array[i, position] = 1
                            elif process == "occurs_n_times_in_list":
                                position = position_map[str(field_value)]
                                core_array[i, position] += 1
                        j += 1
                i += 1

        if template_type in ("variables", "categorical_list"):

            hdf5_core_array_path = "/" + hdf5_base_path + "/core_array/"
            hdf5_column_annotation_path = "/" + hdf5_base_path + "/column_annotations/"

            core_data_set = hdf5p.create_dataset(hdf5_core_array_path, shape=(data_items_count, offset_end), dtype="float64",
                                                 compression="gzip")
            core_data_set[...] = core_array

            print("\n***************************")

            print(hdf5_core_array_path, hdf5_column_annotation_path)
            print("Core array:")
            print(core_array)

            column_header_path = "/" + hdf5_base_path + "/column_header/"
            column_header_set = hdf5p.create_dataset(column_header_path, shape=(4,), dtype="S32")
            column_header_set[...] = np.array(["field_name", "value", "description", "process"])
            print("\nAnnotations:")
            print(np.transpose(column_header_set[...]))

            if template_type == "variables":
                column_annotations = generate_column_annotations_variables(data_translate_dict, column_annotations)
                print(column_annotations)

            elif template_type == "categorical_list":
                column_annotations = generate_column_annotations_categorical_list(data_translate_dict, column_annotations)

            print(column_annotations)

            print("***************************")

            column_data_set = hdf5p.create_dataset(hdf5_column_annotation_path, shape=(4, offset_end), dtype="S128")
            column_data_set[...] = column_annotations


def merge_data_translate_dicts(data_translate_dict_1, data_translate_dict_2):
    """Merge two list translations dicts together"""
    if len(data_translate_dict_1) == 0:
        return data_translate_dict_2
    else:
        if len(data_translate_dict_1) == len(data_translate_dict_2):

            merged_data_translate_dict = []
            for i in range(len(data_translate_dict_1)):

                merged_item_dict = {}
                item_dict_1 = data_translate_dict_1[i]
                item_dict_2 = data_translate_dict_2[i]

                for key in item_dict_1:
                    if key not in ('variables',):
                        merged_item_dict[key] = item_dict_1[key]

                if "variables" in item_dict_1 and len(item_dict_1["variables"]) == 0:  #Case where no variables in item_dict_1
                    merged_item_dict = item_dict_2

                elif "variables" in item_dict_1:
                    variables_1 = item_dict_1["variables"]
                    variables_2 = item_dict_2["variables"]
                    running_offset = 0
                    merged_variables = []

                    if "name" in variables_1[0]:

                        variable_1_name_dict = {}
                        variable_2_name_dict = {}

                        for ii in range(len(variables_1)):
                            var_name = variables_1[ii]["name"]
                            variable_1_name_dict[var_name] = ii

                        for ii in range(len(variables_2)):
                            var_name = variables_2[ii]["name"]
                            variable_2_name_dict[var_name] = ii

                        all_variable_names = []
                        for var_name in variable_1_name_dict:
                            if var_name not in all_variable_names:
                                all_variable_names += [var_name]

                        for var_name in variable_2_name_dict:
                            if var_name not in all_variable_names:
                                all_variable_names += [var_name]

                        all_variable_names.sort()

                        all_variables_merged_right_join = []

                        for var_name in all_variable_names:
                            if var_name in variable_1_name_dict:
                                all_variables_merged_right_join += [variables_1[variable_1_name_dict[var_name]]]
                            else:
                                all_variables_merged_right_join += [variables_2[variable_2_name_dict[var_name]]]

                        n_variables = len(all_variables_merged_right_join)

                    else:
                        n_variables = len(variables_1)

                    for j in range(n_variables):
                        if "name" in variables_1[0]:
                            variable_1 = all_variables_merged_right_join[j]
                            if all_variable_names[j] not in variable_2_name_dict:
                                variable_2 = variable_1
                            else:
                                variable_2 = variables_2[variable_2_name_dict[all_variable_names[j]]]
                        else:
                            variable_1 = variables_1[j]
                            variable_2 = variables_2[j]

                        new_variable = {}

                        if "position_map" not in variable_1:
                            for key in variable_1:
                                new_variable[key] = variable_1[key]
                            new_variable["offset_start"] = running_offset
                            new_variable["offset_end"] = running_offset + 1

                            running_offset += 1

                        else:
                            new_variable, running_offset = remap_position_map(variable_1, variable_2, new_variable, running_offset)

                        merged_variables += [new_variable]

                    merged_item_dict["variables"] = merged_variables
                else:
                    merged_item_dict, running_offset = remap_position_map(item_dict_1, item_dict_2, merged_item_dict, 0)

                merged_data_translate_dict += [merged_item_dict]

            return merged_data_translate_dict

        else:
            raise RuntimeError, "Translations dictionaries are of different length"


def remap_position_map(variable_1, variable_2, new_variable, running_offset):
    """Remaps positions"""
    # Merge dicts
    for key in variable_1:
        if key not in ("n_categories", "offset_start", "offset_end"):
            key_variable_1_value = variable_1[key]
            key_variable_2_value = variable_2[key]

            if key_variable_1_value.__class__ != {}.__class__:
                new_variable[key] = key_variable_1_value

            else:

                merged_dict = {}
                for item_key in key_variable_1_value:
                    merged_dict[item_key] = key_variable_1_value[item_key]

                for item_key in key_variable_2_value:
                    if item_key not in merged_dict:
                        merged_dict[item_key] = key_variable_2_value[item_key]

                if key == "position_map":
                    new_values = [kk for kk in merged_dict]

                    new_values.sort()
                    new_position_map_dict = {}
                    for l in range(len(new_values)):
                        new_position_map_dict[new_values[l]] = l

                    new_variable["position_map"] = new_position_map_dict
                    new_variable["n_categories"] = len(new_position_map_dict)
                    new_variable["offset_start"] = running_offset
                    new_variable["offset_end"] = new_variable["offset_start"] + new_variable["n_categories"]
                    running_offset = new_variable["offset_end"]
                else:
                    new_variable[key] = merged_dict
    return new_variable, running_offset


def combine_exported_hdf5_files_into_single_file(h5p_master, hdf5_files, total_row_count):
    """Combine HDF5 files into a single file"""

    core_array_list = []
    annotations_arrays = []

    h5template = h5py.File(hdf5_files[0], "r")

    path_list = get_all_paths(h5template["/"])

    for path in path_list:
        if path.split("/")[-1] == "core_array":
            core_array_list += [path]
        else:
            annotations_arrays += [path]

    for annotations_array in annotations_arrays:
        copy_data_set(h5template, h5p_master, annotations_array)


    core_array_path_dict = {}
    core_array_path_position = {}
    for core_array_path in core_array_list:
        new_core_data_set = create_data_set_with_new_number_of_rows(h5template, h5p_master, core_array_path, total_row_count)
        core_array_path_dict[core_array_path] = new_core_data_set
        core_array_path_position[core_array_path] = 0

    for hdf5_file_name in hdf5_files:

        print("Inserting contents of '%s'" % hdf5_file_name)

        with h5py.File(hdf5_file_name, "r") as h5pc:

            for core_array_path in core_array_path_dict:
                ds1 = core_array_path_dict[core_array_path]
                new_starting_position = copy_into_data_set_starting_at(ds1, h5pc, core_array_path, core_array_path_position[core_array_path])
                core_array_path_position[core_array_path] = new_starting_position


def main(hdf5_base_name, batch_json_file_name, data_template_json, output_directory=None, clean=True):
    """Convert a JSON file to a HDF5 matrix format using a template"""

    if data_template_json.__class__ != [].__class__:
        data_template_json = [data_template_json]

    with open(batch_json_file_name) as fj:  # Load names of files to process
        batch_list_dict = json.load(fj)

    data_json_files = [x["data_json_file"] for x in batch_list_dict]

    sort_order_dict = {}
    batch_ids = []
    for list_dict in batch_list_dict:
        batch_id = list_dict["batch_id"]
        batch_ids += [batch_id]
        if "sort_order_file_name" in list_dict:
            sort_order_file_name = list_dict["sort_order_file_name"]
            sort_order_dict[batch_id] = sort_order_file_name

    if output_directory is None:
        output_directory = os.path.abspath(os.path.split(data_json_files[0])[0])

    generated_data_templates_names = []
    ks = 0
    for data_json_file in data_json_files:  # For each subset generate a template
        batch_number = batch_ids[ks]

        data_dict = data_dict_load(data_json_file)

        data_template_list = []
        for json_filename in data_template_json:
            with open(json_filename, "r") as f:
                data_template_list += json.load(f)

        data_template_list = expand_template_dict(data_dict, data_template_list)
        data_translate_dict = build_translation_dict(data_dict, data_template_list)
        data_translate_dict = add_offsets_to_translation_dict(data_translate_dict)
        data_translate_dict_json_name = os.path.join(output_directory, hdf5_base_name + "_" + str(batch_number) + "_data_template.json")

        generated_data_templates_names += [data_translate_dict_json_name]

        print("Generated: '%s'" % data_translate_dict_json_name)

        with open(data_translate_dict_json_name, "w") as fjw:
            try:
                json.dump(data_translate_dict, fjw, indent=4, separators=(", ", ": "), sort_keys=True)
            except:
                json.dump(data_translate_dict, fjw)

        ks += 1

    master_data_translate_dict = []
    for data_template_name in generated_data_templates_names:  # Combine templates into a single master template
        print("Merging '%s' to master data template" % data_template_name)
        with open(data_template_name) as fj:
            data_translate_dict = json.load(fj)

            master_data_translate_dict = merge_data_translate_dicts(master_data_translate_dict, data_translate_dict)

    master_data_translate_dict_name = os.path.join(output_directory, hdf5_base_name + "_master_data_template.json")
    with open(master_data_translate_dict_name, "w") as fjw:
        try:
            json.dump(master_data_translate_dict, fjw, indent=4, separators=(", ", ": "), sort_keys=True)
        except TypeError:
            json.dump(master_data_translate_dict, fjw)

    generated_hdf5_file_names = []
    ks = 0
    total_number_of_rows = 0
    for data_json_file in data_json_files:  # Export each subset into a HDF5 matrix
        batch_number = batch_ids[ks]

        data_dict = data_dict_load(data_json_file)

        total_number_of_rows += len(data_dict)

        hdf5_file_name = os.path.join(output_directory, hdf5_base_name + "_" + str(batch_number) + ".hdf5")
        generated_hdf5_file_names += [hdf5_file_name]

        with h5py.File(hdf5_file_name, "w") as f5p:
            if batch_number in sort_order_dict:
                sort_order_json_name = sort_order_dict[batch_number]
                with open(sort_order_json_name, "r") as fj:
                    sort_order_list = json.load(fj)
            else:
                sort_order_list = None

            build_hdf5_matrix(f5p, data_dict, master_data_translate_dict, sort_order_list)

        ks += 1

    print("Exported %s rows across %s files" % (total_number_of_rows, len(data_json_files)))

    all_hdf5_file_name = os.path.join(output_directory, hdf5_base_name + "_combined.hdf5")
    combined_hdf5 = h5py.File(all_hdf5_file_name, "w")

    combine_exported_hdf5_files_into_single_file(combined_hdf5, generated_hdf5_file_names, total_number_of_rows)

    if clean:
        for hdf5_file_name in generated_hdf5_file_names:
            os.remove(hdf5_file_name)

        for data_template_name in generated_data_templates_names:
            os.remove(data_template_name)
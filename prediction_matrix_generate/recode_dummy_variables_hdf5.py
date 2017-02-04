import h5py
import sys
import utility_prediction as upx
import numpy as np
import csv


def main(file_name):
    f5 = h5py.File(file_name, "r")
    all_paths = upx.get_all_paths(f5["./"])

    column_annotation_paths = [p for p in all_paths if p[ -1 * len("/column_annotations"):] == "/column_annotations"]
    core_array_paths = [c for c in all_paths if c[ -1 * len("/core_array"):] == "/core_array"]
    i = 0

    annotation_csv_dict = {}
    annotation_path_list = []
    for column_annotation_path in column_annotation_paths:
        annotation_path = column_annotation_path[:-1 * len("/column_annotations")]
        print(annotation_path)
        annotation_path_list += [annotation_path]
        annotation_csv_dict[annotation_path] = file_name + "." + str(i) + ".csv"

        column_annotation = f5[column_annotation_path][...]
        column_annotation_list = column_annotation[0, :].tolist()
        column_annotation_fields = column_annotation[1, :].tolist()

        fields = np.unique(column_annotation_list).tolist()

        with open(annotation_csv_dict[annotation_path], "wb") as fw:
            csv_writer = csv.writer(fw)
            csv_writer.writerow(fields)

            core_array_rows = f5[core_array_paths[i]].shape[0]

            for j in range(core_array_rows):
                result = {}
                core_array_row = f5[core_array_paths[i]][j, :]
                for k in range(len(column_annotation_list)):
                    if core_array_row[k] == 1:
                        result[column_annotation_list[k]] = column_annotation_fields[k]
                    else:
                        if core_array_row[k] > 0:
                            if column_annotation_list[k] not in result:
                                if int(core_array_row[k]) - core_array_row[k] > 0.0:
                                    result[column_annotation_list[k]] = core_array_row[k]
                                else:
                                    result[column_annotation_list[k]] = int(core_array_row[k])
                if j % 1000 == 0 and j > 0:
                    print("Read '%s' rows" % j)

                row_to_write = []
                for field in fields:
                    if field in result:
                        row_to_write += [result[field]]
                    else:
                        row_to_write += ['']

                csv_writer.writerow(row_to_write)
        i += 1

    # Build a single CSV file
    csv_dict_path = {}
    for annotation_path in annotation_csv_dict:
        f = open(annotation_csv_dict[annotation_path], "rb")
        csv_dict_path[annotation_path] = csv.reader(f)

    header = []
    for annotation_path in annotation_path_list:
        ap_header = csv_dict_path[annotation_path].next()
        header += ap_header

    master_csv_file_name = file_name + ".recode.csv"
    with open(master_csv_file_name, "wb") as fw:
        master_csv_writer = csv.writer(fw)
        master_csv_writer.writerow(header)

        for i in range(core_array_rows):
            master_row = []
            for path in annotation_path_list:
                master_row += csv_dict_path[path].next()
            master_csv_writer.writerow(master_row)


if __name__ == "__main__":
    main("Z:/sbm_microbio_mapped_combined.hdf5")
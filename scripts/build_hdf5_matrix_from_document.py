import sys
from db_document import main

if __name__ == "__main__":
    #TODO: Add option for mapping from an existing master file
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    elif len(sys.argv) == 2 and sys.argv[1] == "help":
        print("Usage: python build_hdf5_matrix_from_document.py data_file_base_name batch_dict.json data_template.json")
    else:
        main("matrix_build", "./test/test_simple_batch.json", "./test/configuration_to_build_matrix.json")
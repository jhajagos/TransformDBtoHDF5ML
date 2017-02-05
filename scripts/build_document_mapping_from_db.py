import sys
from db_document import main_json

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python build_document_mapping_from_db.py inpatient_config.json runtime_config.json")
    else:
        main_json(sys.argv[1], sys.argv[2])

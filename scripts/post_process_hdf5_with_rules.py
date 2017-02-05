import sys
import json
from post_process_hdf5 import main

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("""Usage: python post_process_hdf5.py data.hdf5 rules.json""")
    else:
        hdf5_file = sys.argv[1]
        rules_files = sys.argv[2]

        with open(rules_files, "r") as f:
            rules = json.load(f)

    main(hdf5_file, rules)

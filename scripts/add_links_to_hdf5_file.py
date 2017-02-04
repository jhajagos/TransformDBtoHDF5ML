"""
    Add hardlinks in a HDF5 file from a CSV file
"""

__author__ = 'jhajagos'

import csv
import h5py
import sys


def main(hdf5_file_name, csv_file_with_links, add_map=1):

    with open(csv_file_with_links, "r") as fc:
        dict_reader = csv.DictReader(fc)

        with h5py.File(hdf5_file_name, "r+") as fwj:

            for row_dict in dict_reader:
                map_from = row_dict["map_from"]
                map_to = row_dict["map_to"]

                if len(map_from.strip()):
                    if len(map_to.strip()):
                        if map_from != map_to:
                            if map_from in fwj:
                                print(map_from, map_to)
                                if add_map:
                                    fwj[map_to] = fwj[map_from]


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("""Usage: python add_links_to_hdf5_file.py hdf5_file.hdf5 mapping.csv [0]
        With [0] show mapping and by default [1] will write paths to the HDF5 file

Requirements:
    mapping.csv has to have two columns with names "map_from" and "map_to"
    "map_from" path must exist in the HDF5 file and symbolic link to "map_to" """)
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
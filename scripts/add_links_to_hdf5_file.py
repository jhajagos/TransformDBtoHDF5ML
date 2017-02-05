"""
    Add hardlinks in a HDF5 file from a CSV file. This is used for
    supporting past analyses when you change the structure of the file and
    need backwards.
"""

__author__ = 'jhajagos'

import csv
import h5py
import argparse


def main(hdf5_file_name, csv_file_with_links, add_map=1):
    """Add hard links to a HDF5 file"""

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

    arg_parser_obj = argparse.ArgumentParser()

    arg_parser_obj.add_argument("-f", "--hdf5_filename", dest="hdf5_filename", help="HDF5 file which to add links to")
    arg_parser_obj.add_argument("-c", "--csv_filename", dest="csv_filename",
                                help="CSV file which contains mappings. The file must have two columns 'map_from' and 'map_to'")

    arg_obj = arg_parser_obj.parse_args()
    main(arg_obj.hdf5_filename, arg_obj.hdf5_filename)
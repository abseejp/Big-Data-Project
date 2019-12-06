# The purpose of this script is to read in a list of dataset json files and merge them into a single task1.json

import os
import json

def cat_json(output_filename, input_filenames):
    with file(output_filename, "w") as outfile:
        first = True
        for infile_name in input_filenames:
            with file(infile_name) as infile:
                if first:
                    outfile.write('[')
                    first = False
                else:
                    outfile.write(',')
                outfile.write(mangle(infile.read()))
        outfile.write(']')

if __name__ == "__main__":
    
    output = "task1.json"
    files = []

    for file in os.listdir("/scratch/mva271/NYCOpenData/datasets.txt"):
        files.append(str(file))

    cat_json(output, files)
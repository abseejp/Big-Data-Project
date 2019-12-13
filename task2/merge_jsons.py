#!/usr/bin/env python
# coding: utf-8

# The purpose of this script is to read in a list of dataset json files and merge them into a single task1.json

import os
import json

def cat_json(output_filename, input_filenames):
    with open(output_filename, "w") as outfile:
        first = True
        for infile_name in input_filenames:
            with open(infile_name) as infile:
                if first:
                    outfile.write('{ "predicted_types": [')
                    first = False
                else:
                    outfile.write(',')
                outfile.write(infile.read())
        outfile.write('] }')

if __name__ == "__main__":
    
    output = "task2.json"
    files = []
    directory = "/home/mva271/"

    for file in os.listdir(directory):
        if(file != 'profiling.py' or file != 'run_task2.sh' or file != 'merge_jsons.py' or file != 'task1-outputs' or file != '2019BD-project-results'):
            files.append(directory+str(file))

    cat_json(directory+output, files)
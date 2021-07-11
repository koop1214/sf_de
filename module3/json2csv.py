import csv
import json


def json2csv(in_path, out_path, headers):
    with open(in_path, "r") as json_file, open(out_path, "w") as csv_file:
        dictionary = json.load(json_file)

        file_writer = csv.writer(csv_file, delimiter=",", lineterminator="\n")
        file_writer.writerow(headers)

        for key, value in dictionary.items():
            file_writer.writerow([key, value])

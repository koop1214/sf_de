import csv
import json


def json2csv(in_path, out_path, headers):
    with open(in_path, "r") as json_file, open(out_path, "w") as csv_file:
        dictionary = json.load(json_file)

        file_writer = csv.writer(csv_file, delimiter=",", lineterminator="\n")
        file_writer.writerow(headers)

        for key, value in dictionary.items():
            file_writer.writerow([key, value])


if __name__ == '__main__':
    json2csv('/home/deng/Data/phone.json', '/tmp/ai/phone.csv', ['country_code', 'phone_code'])
    json2csv('/home/deng/Data/names.json', '/tmp/ai/names.csv', ['country_code', 'country_name'])
    json2csv('/home/deng/Data/iso3.json', '/tmp/ai/iso3.csv', ['iso2_country_code', 'iso3_country_code'])
    json2csv('/home/deng/Data/currency.json', '/tmp/ai/currency.csv', ['country_code', 'currency_code'])
    json2csv('/home/deng/Data/continent.json', '/tmp/ai/continent.csv', ['country_code', 'continent_code'])
    json2csv('/home/deng/Data/capital.json', '/tmp/ai/capital.csv', ['country_code', 'capital'])

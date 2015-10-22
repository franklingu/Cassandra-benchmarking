#!/usr/bin/python
import re
import csv
import os
import json


def read_csv(fn):
    results = {}
    with open(fn, 'rb') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')
        for row in rows:
            m = re.search('Total Transactions', row[1])
            if len(row) == 7 and m:
                temp = results.get(row[0])
                if not temp:
                    results[row[0]] = {row[1]: float(row[2]), row[3]: float(row[4])}
                else:
                    results[row[0]] = {row[1]: float(row[2]) + temp.get(row[1]),
                                       row[3]: float(row[4]) + temp.get(row[3])}
                results[row[0]]['Throughput'] = results[row[0]][row[1]] / results[row[0]][row[3]]
    return results


def traverse_all_csvs(path_to_dir):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(path_to_dir):
        for fn in filenames:
            m = re.search('^collections-([\-D0-9]*).csv$', fn)
            if m:
                files.append(fn)
        break
    return files

if __name__ == '__main__':
    results = {}
    files = traverse_all_csvs(os.path.dirname(os.path.realpath(__file__)))
    for fn in files:
        m = re.search('^collections-([\-D0-9]*).csv$', fn)
        results[m.group(1)] = read_csv(fn)
    print json.dumps(results, indent=4, separators=(',', ': '))
    with open('compilation.json', 'w') as outfile:
        json.dump(results, outfile, sort_keys=True, indent=4, separators=(',', ': '))

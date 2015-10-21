#!/usr/bin/python
import re
import csv
import os
import sys


def read_file(fname):
    with open(fname) as f:
        contents = f.readlines()
        return contents


def parse_lines(lines):
    temp_results = []
    results = []
    has_exception = False
    for line in lines:
        m = re.search('^(.*):\s?(.*):\s?([0-9]*\.[0-9]*|[0-9]*)', line)
        m1 = re.search('Exception', line)
        if m1:
            has_exception = True
        if m:
            temp_results.append([m.group(1), m.group(2), m.group(3)])
    for ls in temp_results:
        try:
            rls = results[-1]
            if rls[0] == ls[0]:
                rls.extend([ls[1], ls[2]])
            else:
                results.append(ls)
        except Exception, e:
            results.append(ls)
    return results, has_exception


def write_to_csv(fname, parsed):
    with open(fname, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(parsed)


def traverse_all_logs(path_to_dir):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(path_to_dir):
        for fn in filenames:
            m = re.search('^err_[0-9]*.log$', fn)
            if m:
                files.append(fn)
        break
    return files

if __name__ == '__main__':
    files = traverse_all_logs(os.path.dirname(os.path.realpath(__file__)))
    collections = []
    has_exception = False
    for idx, f in enumerate(files):
        lines = read_file(f)
        parsed, has_except = parse_lines(lines)
        if has_except:
            has_exception = True
        if idx == 0:
            parsed.insert(0, ['Type', 'Measurement 1', 'Data 1', 'Measurement 2', 'Data 2', 'Measurement 3', 'Data 3'])
        parsed.append(['-', '-', '-', '-', '-', '-', '-'])
        collections.extend(parsed)
    write_to_csv('collections.csv', collections)
    if has_exception:
        sys.stderr.write("Parser: There some exceptions in the output\n")
    else:
        sys.stderr.write("Parser: Execution seems fine\n")

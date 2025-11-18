#! /usr/bin/env python3
'''
Build a Markov chain from sequential interaction data
'''

# imports
from csv import reader, Sniffer
from gzip import open as gopen
from niemarkov import MarkovChain
from pathlib import Path
from sys import stdout
import argparse

# constants
DEFAULT_BUFSIZE = 1048576 # 1 MB

# print log message
def print_log(s, end='\n', f=stdout):
    print(s, end=end, file=f); f.flush()

# parse + check user args
def parse_args():
    # parse args
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--input', required=True, type=str, help="Input Interaction Data File (CSV/TSV)")
    parser.add_argument('-cu', '--column_user', required=True, type=str, help="Input Column Name: User")
    parser.add_argument('-ci', '--column_item', required=True, type=str, help="Input Column Name: Item")
    parser.add_argument('-ct', '--column_time', required=True, type=str, help="Input Column Name: Time")
    parser.add_argument('-o', '--output', required=True, type=str, help="Output Markov Chain File (Pickle)")
    parser.add_argument('-t', '--threshold', required=False, type=float, default=float('inf'), help="Session Delta Time Threshold")
    parser.add_argument('-m', '--markov_order', required=False, type=int, default=1, help="Markov Chain Order")
    parser.add_argument('-q', '--quiet', action="store_true", help="Suppress Log Output")
    args = parser.parse_args()

    # check args for validity and return
    ## -i / --input
    args.input = Path(args.input)
    if not args.input.is_file():
        raise ValueError("File not found: %s" % args.input)
    ## -c* / --column_*
    for k in ['column_user', 'column_item', 'column_time']:
        v = getattr(args, k).strip()
        if len(v) == 0:
            raise ValueError("Argument '%s' cannot be empty" % k)
        setattr(args, k, v)
    ## -o / --output
    args.output = Path(args.output)
    if args.output.exists():
        raise ValueError("File exists: %s" % args.output)
    ## -t / --threshold
    if args.threshold <= 0:
        raise ValueError("Session delta time threshold must be positive: %s" % args.threshold)
    ## -m / --markov_order
    if args.markov_order < 1:
        raise ValueError("Markov chain order must be positive: %s" % args.markov_order)
    ## return
    return args

# load data from input interaction CSV/TSV
def load_interactions(p, column_user, column_item, column_time):
    # open file
    if p.suffix.lower() == '.gz':
        f = gopen(p, mode='rt')
    else:
        f = open(p, mode='rt', buffering=DEFAULT_BUFSIZE)

    # load data
    delim = Sniffer().sniff(f.read(DEFAULT_BUFSIZE)).delimiter
    f.seek(0)
    data = dict() # data[user] = list of (time, item) tuples
    for row_num, row in enumerate(reader(f, delimiter=delim)):
        row_stripped = [s.strip() for s in row]
        if row_num == 0:
            col2ind = {col:ind for ind, col in enumerate(row_stripped)}
            ind_user, ind_item, ind_time = (col2ind[col] for col in (column_user, column_item, column_time))
        else:
            curr_user = row_stripped[ind_user]
            curr_item = row_stripped[ind_item]
            curr_time = row_stripped[ind_time]
            try:
                curr_time = int(curr_time)
            except:
                try:
                    curr_time = float(curr_time)
                except:
                    raise ValueError("Time must be a number: %s" % curr_time)
            if curr_user not in data:
                data[curr_user] = list()
            data[curr_user].append((curr_time, curr_item))

    # close file and finalize data
    f.close()
    for vals in data.values():
        vals.sort() # sort interactions chronologically
    return data

# build a Markov chain from loaded interactio data using NieMarkov
def build_niemarkov(data, markov_order=1, threshold=float('inf')):
    mc = MarkovChain(order=markov_order)
    for user_inspections_list in data.values():
        split_inds = [0]
        for i in range(1, len(user_inspections_list)):
            if (user_inspections_list[i][0] - user_inspections_list[i-1][0]) > threshold:
                split_inds.append(i)
        split_inds.append(len(user_inspections_list))
        for i in range(len(split_inds)-1):
            path_start = split_inds[i]
            path_end = split_inds[i+1]
            if (path_end - path_start) > markov_order:
                mc.add_path([user_inspections_list[path_ind][1] for path_ind in range(path_start, path_end)])
    return mc

# program execution
if __name__ == '__main__':
    args = parse_args()
    if not args.quiet:
        print_log("Loading interaction data from: %s ..." % args.input, end=' ')
    data = load_interactions(args.input, args.column_user, args.column_item, args.column_time)
    if not args.quiet:
        print_log("done")
        print_log("Building %d-order Markov chain..." % args.markov_order, end=' ')
    mc = build_niemarkov(data, markov_order=args.markov_order, threshold=args.threshold)
    if not args.quiet:
        print_log("done")
        print_log("Saving Markov chain to file: %s ..." % args.output, end=' ')
    mc.dump(args.output)
    if not args.quiet:
        print_log("done")

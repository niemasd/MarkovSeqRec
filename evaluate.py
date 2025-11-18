#! /usr/bin/env python3
'''
Build a Markov chain from sequential interaction data
Use a prebuilt Markov chain to recommend products to users from sequential interaction data
'''

# imports
from csv import reader, Sniffer
from gzip import open as gopen
from json import load as jload
from pathlib import Path
from scipy.stats import kendalltau
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
    parser.add_argument('-i', '--input', required=True, type=str, help="Input Recommendations File (JSON)")
    parser.add_argument('-p', '--purchases', required=True, type=str, help="Input Purchases File (CSV/TSV)")
    parser.add_argument('-cu', '--column_user', required=True, type=str, help="Input Column Name: User")
    parser.add_argument('-ci', '--column_item', required=True, type=str, help="Input Column Name: Item")
    parser.add_argument('-o', '--output', required=True, type=str, help="Output Evaluations File (TSV)")
    args = parser.parse_args()

    # check args for validity and return
    ## -i / --input and -p / --purchases
    args.input = Path(args.input)
    args.purchases = Path(args.purchases)
    for p in [args.input, args.purchases]:
        if not p.is_file():
            raise ValueError("File not found: %s" % p)
    ## -c* / --column_*
    for k in ['column_user', 'column_item']:
        v = getattr(args, k).strip()
        if len(v) == 0:
            raise ValueError("Argument '%s' cannot be empty" % k)
        setattr(args, k, v)
    ## -o / --output
    args.output = Path(args.output)
    if args.output.exists():
        raise ValueError("File exists: %s" % args.output)
    ## return
    return args

# load data from input interaction CSV/TSV
def load_purchases(p, column_user, column_item):
    # open file
    if p.suffix.lower() == 'gz':
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
            ind_user, ind_item = (col2ind[col] for col in (column_user, column_item))
        else:
            curr_user = row_stripped[ind_user]
            curr_item = row_stripped[ind_item]
            if curr_user not in data:
                data[curr_user] = set()
            data[curr_user].add(curr_item)
    return data

# evaluate recommendations using Kendall's tau-b
def evaluate(recs, purchases):
    evals = dict() # evals[user] = (number of recommendations purchased, max possible number of recommendations purchased)
    for user, user_recs in recs.items():
        try:
            user_purchases = purchases[user]
        except KeyError:
            continue # skip users who didn't buy anything
        num_recs_purchased = sum(1 for item in user_recs if item in user_purchases)
        optimal_num_recs_purchased = min(len(user_purchases), len(user_recs))
        evals[user] = (num_recs_purchased, optimal_num_recs_purchased)
    return evals

# program execution
if __name__ == '__main__':
    args = parse_args()
    print_log("Loading recommendations from: %s ..." % args.input)
    if args.input.suffix.lower() == '.gz':
        f = gopen(args.input, mode='rt')
    else:
        f = open(args.input, mode='rt', buffering=DEFAULT_BUFSIZE)
    recs = jload(f)
    f.close()
    print_log("Loading purchase data from: %s ..." % args.purchases, end=' ')
    purchases = load_purchases(args.purchases, args.column_user, args.column_item)
    print_log("done")
    print_log("Evaluating recommendations...", end=' ')
    evals = evaluate(recs, purchases)
    print_log("done")
    print_log("Saving evaluations to file: %s ..." % args.output, end=' ')
    if args.output.suffix.lower() == 'gz':
        f = gopen(args.output, 'wt')
    else:
        f = open(args.output, 'wt')
    f.write('User\t# Recs Purchased\tOptimal # Recs Purchased\n')
    for user, user_eval in evals.items():
        f.write('%s\t%s\t%s\n' % (user, user_eval[0], user_eval[1]))
    f.close()
    print_log("done")

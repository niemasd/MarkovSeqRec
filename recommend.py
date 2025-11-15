#! /usr/bin/env python3
'''
Build a Markov chain from sequential interaction data
Use a prebuilt Markov chain to recommend products to users from sequential interaction data
'''

# imports
from csv import reader, Sniffer
from gzip import open as gopen
from json import dump as jdump
from niemarkov import MarkovChain, random_choice
from pathlib import Path
from pandas import read_csv
from random import choice
import argparse

# constants
DEFAULT_BUFSIZE = 1048576 # 1 MB

# parse + check user args
def parse_args():
    # parse args
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-m', '--markov', required=True, type=str, help="Input Markov Chain File (Pickle)")
    parser.add_argument('-i', '--input', required=True, type=str, help="Input Interaction Data File (CSV/TSV)")
    parser.add_argument('-d', '--item_details', required=True, type=str, help="Input Item Details File (CSV/TSV)")
    parser.add_argument('-cu', '--column_user', required=True, type=str, help="Input Column Name: User")
    parser.add_argument('-ci', '--column_item', required=True, type=str, help="Input Column Name: Item")
    parser.add_argument('-ct', '--column_time', required=True, type=str, help="Input Column Name: Time")
    parser.add_argument('-n', '--num_recs', required=True, type=int, help="Number of Recommendations")
    parser.add_argument('-o', '--output', required=True, type=str, help="Output Recommendations File (JSON)")
    parser.add_argument('--no_pseudocount', action="store_true", help="Don't Add Pseudocounts")
    args = parser.parse_args()

    # check args for validity and return
    ## -m / --markov and -i / --input and -d / --item_details
    args.markov = Path(args.markov)
    args.input = Path(args.input)
    args.item_details = Path(args.item_details)
    for p in [args.markov, args.input, args.item_details]:
        if not p.is_file():
            raise ValueError("File not found: %s" % p)
    ## -c* / --column_*
    for k in ['column_user', 'column_item', 'column_time']:
        v = getattr(args, k).strip()
        if len(v) == 0:
            raise ValueError("Argument '%s' cannot be empty" % k)
        setattr(args, k, v)
    ## -n / --num_recs
    if args.num_recs < 0:
        raise ValueError("Number of recommendations must be positive: %s" % args.num_recs)
    ## -o / --output
    args.output = Path(args.output)
    if args.output.exists():
        raise ValueError("File exists: %s" % args.output)
    ## return
    return args

# load data from input interaction CSV/TSV
def load_interactions(p, column_user, column_item, column_time):
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
            ind_user, ind_item, ind_time = (col2ind[col] for col in (column_user, column_item, column_time))
        else:
            user_item_time = [row_stripped[ind] for ind in (ind_user, ind_item, ind_time)]
            for i in range(3):
                try:
                    user_item_time[i] = int(user_item_time[i])
                except:
                    try:
                        user_item_time[i] = float(user_item_time[i])
                    except:
                        pass
            curr_user, curr_item, curr_time = user_item_time
            if type(curr_time) not in {float, int}:
                raise ValueError("Time must be a number: %s" % curr_time)
            if curr_user not in data:
                data[curr_user] = list()
            data[curr_user].append((curr_time, curr_item))

    # close file and finalize data
    f.close()
    for vals in data.values():
        vals.sort() # sort interactions chronologically
    return data

# load data from item details CSV/TSV
def load_item_details(p, column_item):
    # open file
    if p.suffix.lower() == 'gz':
        f = gopen(p, mode='rt')
    else:
        f = open(p, mode='rt', buffering=DEFAULT_BUFSIZE)

    # load data
    delim = Sniffer().sniff(f.read(DEFAULT_BUFSIZE)).delimiter
    f.seek(0)
    df = read_csv(f, delimiter=delim)
    f.close()
    return df

# produce recommendations for all users
def recommend(mc, data, item_details, num_recs):
    # create initial recommendations by Markov chain random walk
    recs = dict()
    for user, inspections in data.items():
        # find final node for this user
        final_items = [item for t, item in inspections[-mc.order:]]
        try:
            final_node = tuple([None]*(mc.order-len(final_items)) + [mc.label_to_state[item] for item in final_items])
            mc.get_label(final_node)
        except:
            final_node = None
        if final_node: # find most similar node with transitions
            node_dists = dict()
            for v in mc.transitions: # only check nodes with transitions
                v_dist = sum(1 for i in range(len(v)) if final_items[i] != mc.labels[v[i]])
                if v_dist not in node_dists:
                    node_dists[v_dist] = list()
                node_dists[v_dist].append(v)
            final_node = choice(node_dists[min(node_dists.keys())])

        # get recommendations from random walk
        curr_recs_set = set(); curr_recs_list = list()
        for curr_node in mc.random_walk(final_node, start_is_node=True):
            if len(curr_recs_set) == num_recs:
                break
            next_item = mc.labels[curr_node[-1]]
            if next_item not in curr_recs_set:
                curr_recs_set.add(next_item)
                curr_recs_list.append(next_item)
        recs[user] = curr_recs_list
    return recs

# program execution
if __name__ == '__main__':
    args = parse_args()
    print("Loading Markov chain from file: %s ..." % args.markov, end=' ')
    mc = MarkovChain.load(args.markov)
    print("done")
    if not args.no_pseudocount:
        print("Adding pseudocounts to Markov chain...", end=' ')
        mc.add_pseudocount()
        print("done")
    print("Loading interaction data from: %s ..." % args.input, end=' ')
    data = load_interactions(args.input, args.column_user, args.column_item, args.column_time)
    print("done")
    print("Loading item details from: %s ..." % args.item_details, end=' ')
    item_details = load_item_details(args.item_details, args.column_item) # TODO
    print("done")
    print("Producing recommendations...", end=' ')
    recs = recommend(mc, data, item_details, args.num_recs)
    print("done")
    print("Saving recommendations to file: %s ..." % args.output, end=' ')
    if args.output.suffix.lower() == 'gz':
        f = gopen(args.output, 'wt')
    else:
        f = open(args.output, 'wt')
    jdump(recs, f)
    f.close()
    print("done")

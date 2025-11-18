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
from random import choice, shuffle
from sys import stdout
import argparse

# constants
DEFAULT_BUFSIZE = 1048576 # 1 MB
DEFAULT_NUM_STEPS = 1000

# print log message
def print_log(s, end='\n', f=stdout):
    print(s, end=end, file=f); f.flush()

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
    parser.add_argument('--num_steps', required=False, type=int, default=DEFAULT_NUM_STEPS, help="Maximum Number of Steps in Random Walk")
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
    data = dict() # data[item][attribute] = value
    for row_num, row in enumerate(reader(f, delimiter=delim)):
        row_stripped = [s.strip() for s in row]
        if row_num == 0:
            header = row_stripped
            col2ind = {col:ind for ind, col in enumerate(row_stripped)}
            ind_item = col2ind[column_item]
            del col2ind[column_item]
        else:
            data[row_stripped[ind_item]] = {col:row_stripped[ind] for col, ind in col2ind.items()}
    f.close()
    return data

# produce recommendations for all users
def recommend(mc, data, item_details, num_recs, num_steps=DEFAULT_NUM_STEPS):
    # make sure number of recommendations is at most the total number of items
    all_items = set(item_details.keys())
    num_recs = min(num_recs, len(all_items))

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
        for curr_node in mc.random_walk(final_node, start_is_node=True, num_steps=num_steps):
            if len(curr_recs_set) == num_recs:
                break
            next_item = mc.labels[curr_node[-1]]
            if next_item not in curr_recs_set:
                curr_recs_set.add(next_item)
                curr_recs_list.append(next_item)

        # if too few recommendations, fill them randomly (currently uniformly, maybe more sophisticated later)
        if len(curr_recs_list) < num_recs:
            remaining_items = list(all_items)
            shuffle(remaining_items)
            while len(curr_recs_list) < num_recs:
                next_item = remaining_items.pop()
                if next_item not in curr_recs_set:
                    curr_recs_set.add(next_item)
                    curr_recs_list.append(next_item)
        recs[user] = curr_recs_list
    return recs

# program execution
if __name__ == '__main__':
    args = parse_args()
    print_log("Loading Markov chain from file: %s ..." % args.markov, end=' ')
    mc = MarkovChain.load(args.markov)
    print_log("done")
    if not args.no_pseudocount:
        print_log("Adding pseudocounts to Markov chain...", end=' ')
        mc.add_pseudocount()
        print_log("done")
    print_log("Loading interaction data from: %s ..." % args.input, end=' ')
    data = load_interactions(args.input, args.column_user, args.column_item, args.column_time)
    print_log("done")
    print_log("Loading item details from: %s ..." % args.item_details, end=' ')
    item_details = load_item_details(args.item_details, args.column_item)
    print_log("done")
    print_log("Producing recommendations...", end=' ')
    recs = recommend(mc, data, item_details, args.num_recs)
    print_log("done")
    print_log("Saving recommendations to file: %s ..." % args.output, end=' ')
    if args.output.suffix.lower() == 'gz':
        f = gopen(args.output, 'wt')
    else:
        f = open(args.output, 'wt')
    jdump(recs, f)
    f.close()
    print_log("done")

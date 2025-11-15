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
    parser.add_argument('-cu', '--column_user', required=True, type=str, help="Input Column Name: User")
    parser.add_argument('-ci', '--column_item', required=True, type=str, help="Input Column Name: Item")
    parser.add_argument('-ct', '--column_time', required=True, type=str, help="Input Column Name: Time")
    parser.add_argument('-o', '--output', required=True, type=str, help="Output Recommendations File (JSON)")
    parser.add_argument('-q', '--quiet', action="store_true", help="Suppress Log Output")
    args = parser.parse_args()

    # check args for validity and return
    ## -m / --markov and -i / --input
    args.markov = Path(args.markov)
    args.input = Path(args.input)
    for p in [args.markov, args.input]:
        if not p.is_file():
            raise ValueError("File not found: %s" % p)
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

# get a recommendation from a specific node
def get_recommendation(final_items, mc, item_dists=None):
    try:
        final_node = tuple([None]*(mc.order-len(final_items)) + [mc.label_to_state[item] for item in final_items])
        transitions = mc[final_node]
    except:
        transitions = dict()
    if len(transitions) == 0: # no transitions, so find most similar node with transitions
        node_dists = dict()
        for v in mc.transitions: # only check nodes with transitions
            if item_dists is None:
                v_dist = sum(1 for i in range(len(v)) if final_items[i] != mc.labels[v[i]])
            else:
                raise NotImplementedError("TODO GET TRANSITIONS OF CLOSEST NODE USING ITEM DISTS")
            if v_dist not in node_dists:
                node_dists[v_dist] = list()
            node_dists[v_dist].append(v)
        final_node = choice(node_dists[min(node_dists.keys())])
        transitions = mc[final_node]
    if final_node in transitions:
        del transitions[final_node]
    return mc.labels[random_choice(transitions)[-1]]

# produce recommendations for all users
def recommend(mc, data, item_dists=None):
    return {user:get_recommendation([item for t, item in inspections[-mc.order:]], mc, item_dists=item_dists) for user, inspections in data.items()}

# program execution
if __name__ == '__main__':
    args = parse_args()
    if not args.quiet:
        print("Loading Markov chain from file: %s ..." % args.markov, end=' ')
    mc = MarkovChain.load(args.markov)
    if not args.quiet:
        print("done")
        print("Loading interaction data from: %s ..." % args.input, end=' ')
    data = load_interactions(args.input, args.column_user, args.column_item, args.column_time)
    if not args.quiet:
        print("done")
        print("Producing recommendations...", end=' ')
    item_dists = None # TODO CALCULATE PAIRWISE ITEM DISTANCES IF ITEM DETAILS ARE GIVEN
    recs = recommend(mc, data, item_dists=item_dists)
    if not args.quiet:
        print("done")
        print("Saving recommendations to file: %s ..." % args.output, end=' ')
    if args.output.suffix.lower() == 'gz':
        f = gopen(args.output, 'wt')
    else:
        f = open(args.output, 'wt')
    jdump(recs, f)
    f.close()
    if not args.quiet:
        print("done")

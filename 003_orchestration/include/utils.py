# include/utils.py

import pickle

def dump_pickle(obj, filename):
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)

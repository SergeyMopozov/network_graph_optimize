'''
Created on Sep 3, 2019

@author: mrmopoz
'''

from pandas import read_pickle
#import networkx as nx



def load_volumes_on_derections(filename):
    volumes = read_pickle(filename)
    return volumes



from networkx import write_gpickle, DiGraph
from pandas import read_csv

#logging
import logging
logging.basicConfig(filename='../log/graph_optimize_log.log', \
                    format='%(asctime)s\t%(levelname)s,\t%(funcName)s\t%(message)s', \
                    level=logging.DEBUG)
'''
class Graph:
    
    def __init__(self):
        self.graph = nx.DiGraph()
    
    def load_edges(self, filename):
        edges = read_csv(filename)
        self.graph.add_edges_from(edges)
    
    def load_nodes(self, filename):
        nodes = read_csv(filename)
        self.graph.add_nodes_from(nodes)
    
    
    def create_graph(self, nodes, edges):
        graph = nx.DiGraph()
        
        for n in nodes:
            graph.load_nodes(n)
        
        for e in edges:
            graph.load_edges(e)
        
        return graph
    
    
    def save_graph(self, filename):
        self.graph.to_pickle(filename)
        
'''

def create_graph():
    logging.info('Start create graph')
    # 0 level nodes - core nodes
    core = read_csv('../ref/nodes_0_core.csv', index_col=0)
    # 1st level nodes
    #ufps = read_csv('../ref/nodes_1_ufps.csv', index_col=0)
    # 2nd level nodes
    #pochtamts = read_csv('../ref/nodes_2_pochtamts.csv', index_col=0)
    #3rd level nodes
    #ops = read_csv('../ref/nodes_3_ops_pochtamts.csv', index_col=0)
    
    # read core transport nodes
    auto_nodes =  read_csv('../ref/0_nodes_core_auto.csv', index_col=0)
    # TODO: update grpah for contain of all node 
    #auto_nodes =  read_csv('../ref/cuted_auto_graph.csv', index_col=0)
    aero_nodes = read_csv('../ref/0_nodes_core_aero.csv', index_col=0)
    
    # DONE: add railway nodes
    railway_nodes = read_csv('../ref/0_nodes_core_rail.csv', index_col=0)
    
    master = node_dict_from_df(core)
    total_core_nodes_in, total_core_nodes_out, \
    master_nodes_internal, master_nodes_internal_edge = generate_internal_nodes_and_edges(master)
    
    # create transport edges and nodes
    auto_nodes_1, auto_nodes_2, auto_edges = create_edges(auto_nodes)
    aero_nodes_1, aero_nodes_2, aero_edges = create_edges(aero_nodes)
    rail_nodes_1, rail_nodes_2, rail_edges = create_edges(railway_nodes)
    
    # create graph
    russian_post_graph = DiGraph()
    
    # this part of code create fullconnected core graph of 82 master nodes
    # adding master nodes (82 nodes) and internal edges
    russian_post_graph.add_nodes_from(master_nodes_internal)
    russian_post_graph.add_edges_from(master_nodes_internal_edge)
    
    
    # add auto nodes between masters
    russian_post_graph.add_nodes_from(auto_nodes_1)
    russian_post_graph.add_nodes_from(auto_nodes_2)
    
    # add avia nodes between masters
    russian_post_graph.add_nodes_from(aero_nodes_1)
    russian_post_graph.add_nodes_from(aero_nodes_2)
    
    # add rail nodes between masters
    russian_post_graph.add_nodes_from(rail_nodes_1)
    russian_post_graph.add_nodes_from(rail_nodes_2)
    
    
    # add transport edges between master nodes
    russian_post_graph.add_edges_from(auto_edges)
    russian_post_graph.add_edges_from(aero_edges)
    russian_post_graph.add_edges_from(rail_edges)
    
    logging.info('Graph created')
    logging.info('Seve graph on disk...')
    write_gpickle(russian_post_graph, '../result/base_graph')
    return total_core_nodes_in, total_core_nodes_out, russian_post_graph


def node_dict_from_df(df):
    # convert df to node dict for graph
    # df must contains columns ['idx', 'name', 'lat', 'lng', 'type', 'capacity', 
    #                             'time_transit', 'time_sort', 'time_limit',\
    #                             'cost_transit', 'cost_sort', 'avg_cost_transit', 'avg_cost_sort']
    nodes = [(row['idx'], { 'type': row['type'], 
                            'time': row['time_transit'], 
                            'capacity': row['capacity'], 
                            'cost': row['cost_transit'], 
                            'avg_cost': row['avg_cost_transit']}) for _, row in df.iterrows()]
    return nodes


def generate_internal_nodes_and_edges(master_nodes):
    total_nodes_in = []
    total_nodes_out = []
    master_nodes_internal = []
    master_nodes_internal_edge = []
    
    for node in master_nodes:
        node_in = (str(node[0])+ '_in',
                   {'idx': node[1].get('idx', 1),
                    'time': node[1].get('time', 1),
                    'cost': node[1].get('cost', 1),
                    'avg_cost': node[1].get('avg_cost', 1),
                    'type': node[1].get('type', 1),
                    'capacity': node[1].get('capacity', 1)})
        
        node_out = (str(node[0]) + '_out', {'name': node[0]})
#        node_sort_in = (str(node[0])+ '_sort_in',\
#                        {'name': node[0],'time': node[1].get('time_sort', 1),'cost': node[1].get('cost_sort', 1)})
#        node_sort_out = (str(node[0])+ '_sort_out' , {'name': node[0]})

        total_nodes_in.append(node_in[0])
        total_nodes_out.append(node_out[0])
        
        master_nodes_internal.append(node_in)
        master_nodes_internal.append(node_out)
#        master_nodes_internal.append(node_sort_in)
#        master_nodes_internal.append(node_sort_out)
        master_nodes_internal_edge.append((node_in[0], node_out[0], \
                                           {'time': node_in[1].get('time', 1) , 
                                            'cost':node_in[1].get('cost',1),
                                            'avg_cost': node[1].get('avg_cost', 1),
                                            'type': node[1].get('type', 1),
                                            'capacity': node[1].get('capacity', 1)}))
    
#        master_nodes_internal_edge.append((node_in[0], node_sort_in[0]))
#        master_nodes_internal_edge.append((node_sort_in[0], node_sort_out[0], \
#                                          {'time': node_sort_in[1].get('time', 1) , 'cost':node_sort_in[1].get('cost',1)}))
#        master_nodes_internal_edge.append((node_sort_out[0], node_out[0], {}))
        
    return total_nodes_in, total_nodes_out, master_nodes_internal, master_nodes_internal_edge


def create_edges(nodes):
    #create edges
    # direct
    transport_nodes = [(str(row['from']) + '_' + str(row['to']) + '_' + str(row['type']), 
                         {'from': row['from'],
                          'to': row['to'],
                          'dist': row['dist'], 
                          'time': row['time'], 
                          'cost':row['cost'],
                          'avg_cost':row['avg_cost'], 
                          'type': row['type'], 
                          'time_limit': row['time_limit'],
                          'capacity': row['capacity']})  for _, row in nodes.iterrows()]
    # return
    transport_nodes_2 = [(str(row['to']) + '_' + str(row['from']) + '_' + str(row['type']), 
                         {'from': row['to'], 
                          'to': row['from'],
                          'dist': row['dist_ret'], 
                          'time': row['time_ret'], 
                          'cost':row['cost'],
                          'avg_cost':row['avg_cost'],
                          'type': row['type'],
                          'time_limit': row['time_limit'],
                          'capacity': row['capacity']}) for _, row in nodes.iterrows()]

    new_edges_transport = []
    
    for node in transport_nodes:
        new_edges_transport.append((str(node[1].get('from')) + '_out', node[0],\
                              {'time': node[1].get('time', 1), 
                               'dist':node[1].get('dist',1),
                               'cost':node[1].get('cost',1),
                               'avg_cost':node[1].get('avg_cost',1),
                               'time_limit': node[1].get('time_limit', 1),
                               'capacity': node[1].get('capacity', 1),
                               'type':node[1].get('type',1)}))
        
        new_edges_transport.append((node[0], str(node[1].get('to')) + '_in'))


    for node in transport_nodes_2:
        new_edges_transport.append((str(node[1].get('from')) + '_out', node[0],\
                              {'time': node[1].get('time', 1), 
                               'dist':node[1].get('dist',1),\
                               'cost':node[1].get('cost',1),
                               'avg_cost':node[1].get('avg_cost',1),
                               'time_limit': node[1].get('time_limit', 1),
                               'capacity': node[1].get('capacity', 1),
                               'type':node[1].get('type',1)}))
        
        new_edges_transport.append((node[0], str(node[1].get('to')) + '_in'))
        
    return transport_nodes, transport_nodes_2, new_edges_transport






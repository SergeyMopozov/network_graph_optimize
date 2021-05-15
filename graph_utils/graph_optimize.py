'''
Created on Sep 18, 2019

@author: mrmopoz
'''
from graph_utils.cost_functions import update_weights
from graph_utils.data_loader import load_volumes_on_derections
from graph_utils.graph_algorithm import calculate_routeplan, calculate_nodes_load, \
                                        calculate_nodes_cost, calculate_metrics
from graph_utils.graph import create_graph
#import pandas as pd
from pandas import merge
#logging
import logging
logging.basicConfig(filename='../log/graph_optimize_log.log', \
                    format='%(asctime)s\t%(levelname)s,\t%(funcName)s\t%(message)s', \
                    level=logging.DEBUG)

# optimize function
def optimize_graph(graph, volumes, point_from, point_to, SLA_MIN=0.98, LOAD_GRADIENT=0.001):
    
    # restriction for optimization   
    NODES_LOAD_CHANGE = 1
    SLA = 1
    load_list = []
    logging.info('Start cycle')
    iteration = 0
    logging.info('Start iteration: '+ str(iteration))
    # minimize cost of transportation on graph wrhil restriction is not premited
    while (SLA > SLA_MIN) & (NODES_LOAD_CHANGE > LOAD_GRADIENT):
        print('Iteration: ', iteration)
        if iteration == 0:
            path_func = 'cost'
        else:
            path_func = 'avg_cost'
        # calculate route plan - how volumes must run throuh the graph with minimum cost
        logging.info('Calculate route plan')
        # TODO: if add static route_plan we get current distribution of volumes on graph
        route_plan = calculate_routeplan(graph, point_from, point_to, path_weight_func=path_func )
        #route_plan = pd.read_pickle('../result/route_table_2019-10-01_12:29')
        # calculate nodes loads 
        logging.info('Calculate nodes load')
        nodes_load, load_file_name = calculate_nodes_load(graph, route_plan, volumes)
        #nodes_load = pd.read_pickle('../result/nodes_load_2019-10-01_12:34')
        #load_file_name = '../result/nodes_load_2019-10-01_12:34'
        load_list.append(load_file_name)
        # calculate cost in nodes with current loads
        
        logging.info('Calculate nodes cost')
        nodes_cost = calculate_nodes_cost(graph, nodes_load)
        #nodes_cost = pd.read_pickle('../result/nodes_cost_2019-10-04_18:18')
        
        # calculate metrics
        logging.info('Calculate metrics')
        if iteration == 0:
            prev_nodes_load = load_list[iteration]
        else:
            prev_nodes_load = load_list[iteration - 1]
            
        NODES_LOAD_CHANGE, SLA = calculate_metrics(volumes, nodes_load, prev_nodes_load, route_plan)
        
        if iteration == 0:
            NODES_LOAD_CHANGE = 1
        print('NODES_LOAD_CHANGE: ', NODES_LOAD_CHANGE)
        print('SLA: ', SLA)
        
        # udpdate weights on 
        logging.info('Update graph weights')
        graph = update_weights(graph, nodes_cost)
        
 
        if iteration == 10:
            break
        iteration += 1
        
        print('\n')
    return graph, route_plan, nodes_load, nodes_cost


def calculate_product_synergy(result):
    nodes_loads = result[0][2]
    nodes_costs = result[0][3]
    #merge product loads on nodes
    for res in result[1:]:
        nodes_loads = merge(nodes_loads, res[2], on='left')
    nodes_loads = nodes_loads.sum(axis=1)
    
    for res in result[1:]:
        nodes_costs = merge(nodes_costs, res[3], on='left')
    nodes_costs = nodes_costs.sum(axis=1)
        
    # calculate costs for total load
    synergy_nodes_costs = calculate_nodes_cost(nodes_loads)
    delta_synergy = nodes_costs.sum(axis=0) - synergy_nodes_costs.sum(axis=0)
    # update weigts for total cost
    graph = update_weights(res[0][0], synergy_nodes_costs)
    
    return graph, nodes_loads, nodes_costs, synergy_nodes_costs, delta_synergy


def main():
    # main stream of graph optimization    
    volumes = load_volumes_on_derections('')
    #point_from, point_to, 
    graph = create_graph()
    
    products = ['mail', 'standart', 'express']
    result = []
    for product in products:
        result.append(optimize_graph(graph, volumes[product], volumes[product]['point_from'], volumes[product]['point_to']))
        
    
    graph, nodes_loads, nodes_costs, synergy_nodes_costs, delta_synergy = calculate_product_synergy(result)
    
    return graph, nodes_loads, nodes_costs, synergy_nodes_costs, delta_synergy





    


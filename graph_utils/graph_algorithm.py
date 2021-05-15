


#import pandas as pd
from pandas import Series, DataFrame, concat, merge, read_pickle
#import numpy as np
from numpy import ceil, inf, nan
from itertools import product
from networkx import dijkstra_path
from datetime import datetime
from graph_utils.cost_functions import minimal_auto_cost_func, avia_cost, sort_center_cost


import logging
logging.basicConfig(filename='../log/graph_optimize_log.log', \
                    format='%(asctime)s\t%(levelname)s,\t%(funcName)s\t%(message)s', \
                    level=logging.DEBUG)


# calculate path amounts
def path_amount(G, nodes, amount):
    w =0
    for ind, nd in enumerate(nodes[1:]):
        prev = nodes[ind]
        w += G[prev].get(nd).get(amount, 0)
    return w

def edges_from_path(nodes):
    # get edges between nodes
    w = []
    for ind,nd in enumerate(nodes[1:]):
        prev = nodes[ind]
        w.append((prev,nd))
    return w

def get_value(row, key):
    if type(row) == dict:
        return row.get(key, 0)
    else:
        return None

def calculate_routeplan(graph, point_from, point_to, path_weight_func='avg_cost'):
    # find all path from all nodes to all nodes and save result in table with summary  
    # таблица путей по нарпвлениям
   
    pairs = [pair for pair in product(point_from, point_to)]
    pairs_matrix = []
    for pair in pairs:
        try:
            # TODO: think about find all shortest path and choice optimal from all variable table
            # TODO: think about find alternative path with limit by capcity on edges
            path = dijkstra_path(graph, pair[0], pair[1], weight=path_weight_func)
                       
            row = []
            row.append(pair)
            row.append(path)
            row.append(path_amount(graph, path, 'time'))
            row.append(path_amount(graph, path, 'dist'))
            #row.append(path_amount(graph, path, 'cost'))
            pairs_matrix.append(row)
        except Exception as e:
            print(e)

    route_plan = DataFrame(pairs_matrix, columns=['pair', 'path', 'path_time', 'path_dist']) 
    route_plan['edges'] =  route_plan['path'].apply(edges_from_path)
    route_plan[['from', 'to']] = route_plan.pair.apply(lambda row: Series(row))
    
    logging.info('Route path found. Start saving...')
    # план направлений
    date = datetime.today().strftime('%Y-%m-%d_%H:%M')
    path = '../result/'
    file_name = path + 'route_table_' + date
    route_plan.to_pickle(file_name)
    return route_plan

def calculate_nodes_load(graph, route_plan, volumes):
 
    # calculate time shift path for current direction pair and sum all volumes timeseries with own timeshift
    #stack route plan
    logging.info('Start calculating load...')
    logging.info('Stacking path...')
    
    nodes_load = route_plan[['pair']].join(route_plan['edges']\
                                       .apply(lambda row: Series(row)))
    nodes_load = nodes_load.melt('pair')
    nodes_load.columns = ['pair', 'edge_num', 'edge']
    nodes_load.dropna(inplace=True)
    # node_load - table with pair pirection and edge in path
    logging.info('Merge path with volumes...')
    # volumes - table with pair direction and time series with hour frequncy of volumes 
    nodes_load = merge(nodes_load, volumes, on='pair', how='inner') # only intersetst directions and volumes 
    
    # get time on edges
    logging.info('Get graph edges for gettin time...')
    graph_edges = DataFrame(graph.edges(data=True), columns = ['from', 'to', 'info'])
    graph_edges['edge'] = Series(zip(graph_edges['from'], graph_edges['to']))
    graph_edges['time'] = graph_edges['info'].apply(lambda row: get_value(row, 'time')).replace(inf, nan) # return nan for inf edges
    # convert time in seconds to hours
    graph_edges['time'] = graph_edges['time'].apply(lambda row: ceil(row/3600))
    graph_edges = graph_edges[['edge', 'time']]
    
    # table content 'pair', 'edge_num', 'edge', 'volumes', 'time'
    nodes_load = merge(nodes_load, graph_edges, on='edge', how='left')
    
    
    
    # group by pair and calculate cumulative shift for each eadge in path for current pair diraection
    #TODO: think how rewrite this part of code. it's very slow
    logging.info('Calculate cumulative time shifting...')
    frames_cummulate = []
    
    for pair in nodes_load['pair'].unique():
       
        temp = nodes_load[nodes_load['pair'] == pair].copy(deep=True)
        temp.sort_values('edge_num', inplace=True)
        temp['shift'] = temp['time'].cumsum()
        frames_cummulate.append(temp)
    
    nodes_load = concat(frames_cummulate)
    nodes_load.reset_index(drop=True, inplace=True)
    
    # table content 'pair', 'edge_num', 'edge', 'volumes', 'time', shift
    # TODO: rewrite this part of code non pandas style for aggregate data
    # summurize time series with own shift for current edge
    logging.info('Sum load time series with shift')
    frames_cummulate = []
    for edge in nodes_load['edge'].unique():
        
        sub_frame = nodes_load[nodes_load['edge'] == edge].copy(deep=True).reset_index(drop=True)
        
        for idx, row in sub_frame.iterrows():
            if idx == 0:
                shifted_volume = row['volumes'].tshift(int(row['shift']))
            else:
                shifted_volume = shifted_volume.add(row['volumes'].tshift(int(row['shift'])), fill_value=0)
        # DONE: !!! most add number of direction
        # added count of summing components 
        frames_cummulate.append((edge, sub_frame, shifted_volume, sub_frame.shape[0]))
    
    # TODO: think how save previus result and agregated data     
    #nodes_load = pd.merge(nodes_load, pd.DataFrame(frames_cummulate, columns=['edge', 'total_volumes']), on='edge', how='left')
    nodes_load = DataFrame(frames_cummulate, columns=['edge', 'components', 'total_volumes', 'components_count'])
    
    
    
    
    logging.info('Edge load calculated. Start saving...')
    date = datetime.today().strftime('%Y-%m-%d_%H:%M')
    path = '../result/'
    file_name = path + 'nodes_load_' + date
    nodes_load.to_pickle(file_name)
    
    return nodes_load, file_name

def calculate_nodes_cost(graph, nodes_load):
    # calculate nodes cost timeseries
    # each cell of pdataframe contain ts (1 hour sampling) with loads
    # we need calculate costs for procecing this volumes 
    
    logging.info('Get graph edges for gettin time...')
    graph_edges = DataFrame(graph.edges(data=True), columns = ['from', 'to', 'info'])
    graph_edges['edge'] = Series(zip(graph_edges['from'], graph_edges['to']))
    graph_edges['time'] = graph_edges['info'].apply(lambda row: get_value(row, 'time')).replace(inf, nan) # return inf for inf edges
    graph_edges['dist'] = graph_edges['info'].apply(lambda row: get_value(row, 'dist')).replace(inf, nan) # return inf for inf edges
    graph_edges['type'] = graph_edges['info'].apply(lambda row: get_value(row, 'type')).replace(inf, nan) # return inf for inf edges
    # DONE: change  'cost' to ' avg_cost' prev use not correct data control change
    graph_edges['avg_cost'] = graph_edges['info'].apply(lambda row: get_value(row, 'avg_cost')).replace(inf, nan)
    # convert time in seconds to hours
    graph_edges['time'] = graph_edges['time'].apply(lambda row: ceil(row/3600))
       
    graph_edges = graph_edges[['edge', 'info', 'time', 'dist','type', 'avg_cost']]
    
    
    logging.info('Start calculating costs...')
    result = []
    for edge in nodes_load['edge'].unique():
        # get dataframe with volumes
        total_cost = nodes_load[nodes_load['edge'] == edge]['total_volumes'].iloc[0].resample('D').sum()
        dist = graph_edges[graph_edges['edge'] == edge]['dist'].iloc[0]
        time = graph_edges[graph_edges['edge'] == edge]['time'].iloc[0]
        edge_type = graph_edges[graph_edges['edge'] == edge]['type'].iloc[0]
        if edge_type == 'auto':
            # FIXME: !!!!!!calculation need initialize of avg_cost in frist iterration
            total_cost['cost'] = total_cost['sum_mass_kg']*graph_edges[graph_edges['edge'] == edge]['avg_cost'].iloc[0]
            total_cost[['total_cost', 'avg_loads', 'min_capacity', 'amount', 'types_list']] = \
                    total_cost.apply(lambda row: \
                                        Series(minimal_auto_cost_func(dist, time, row['sum_mass_kg'])), axis=1)
        
        if edge_type == 'avia':
            total_cost['cost'] = total_cost['sum_mass_kg']*graph_edges[graph_edges['edge'] == edge]['avg_cost'].iloc[0]
            total_cost[['total_cost', 'avg_loads', 'min_capacity', 'amount', 'types_list']] = \
                    total_cost.apply(lambda row: \
                                        Series(avia_cost(time, row['sum_mass_kg'])), axis=1)
        
        if edge_type == 'sort_center':
            total_cost['cost'] = total_cost['sum_mass_kg']*graph_edges[graph_edges['edge'] == edge]['avg_cost'].iloc[0]
            total_cost[['total_cost', 'avg_loads', 'min_capacity', 'amount', 'types_list']] = \
                    total_cost.apply(lambda row: \
                                        Series(sort_center_cost(time, row['sum_mass_kg'])), axis=1)
        
        
        result.append((edge, total_cost))
        
    nodes_cost = DataFrame(result, columns=['edge', 'total_cost'])
           
    logging.info('Edge cost calculated. Start saving...')
    date = datetime.today().strftime('%Y-%m-%d_%H:%M')
    path = '../result/'
    file_name = path + 'nodes_cost_' + date
    nodes_cost.to_pickle(file_name)
    logging.info('Edge cost calculated. Start saving...')
    file_name = path + 'graph_edges_' + date
    graph_edges.to_pickle(file_name)
    return nodes_cost

def calculate_metrics(volumes, nodes_load, prev_loads_filename, route_plan):
    SLA = 1
    NODE_LOAD_CHANGE = 1
    
    # TODO: extract volumes from each cell
    control_terms = read_pickle('../ref/control_terms_for_pair')
    
    try:
        logging.info('Calculate SLA...')
        
        stack_date = []
        for _, row in volumes.iterrows():
            df = row['volumes'].copy().resample('D').sum().reset_index()
            df['pair'] =   Series([row['pair'] for _ in range(len(df))])
            stack_date.append(df)
            
        volumes = concat(stack_date)
        
        fact_terms = merge(volumes, route_plan[['pair', 'path_time']], on='pair', how='left')
        fact_terms = merge(fact_terms, control_terms, on='pair', how='left')
        fact_terms['path_time'] = fact_terms['path_time']/3600/24
        fact_terms['compare'] = fact_terms['path_time'] > fact_terms['max']
        fact_terms['out_of_terms'] = fact_terms['count_rpo'] * fact_terms['compare']
        
        date = datetime.today().strftime('%Y-%m-%d_%H:%M')
        path = '../result/'
        file_name = path + 'fact_terms_' + date
        fact_terms.to_pickle(file_name)

        
        SLA = 1 - (fact_terms['out_of_terms'].sum()/ fact_terms['count_rpo'].sum()) 
        logging.info('SLA equal:' + str(SLA))
    except Exception as e:
        logging.error('Exception in calculatin SLA: ' + str(e))
    
    # TODO: extract total volumes from each cell
    try:
        logging.info('Calculate NODE_LOAD_CHANGE...')
        prev_nodes_load = read_pickle(prev_loads_filename)
        
        stack_date = []
        for _, row in prev_nodes_load.iterrows():
            df = row['total_volumes'].copy().resample('D').sum().reset_index()
            df['edge'] =   Series([row['edge'] for _ in range(len(df))])
            stack_date.append(df)
            
        prev_nodes_load = concat(stack_date)
        
        stack_date = []
        for _, row in nodes_load.iterrows():
            df = row['total_volumes'].copy().resample('D').sum().reset_index()
            df['edge'] =   Series([row['edge'] for _ in range(len(df))])
            stack_date.append(df)
            
        nodes_load = concat(stack_date)
        load_change = merge(nodes_load, prev_nodes_load, on=['date_','edge'])
        date = datetime.today().strftime('%Y-%m-%d_%H:%M')
        path = '../result/'
        file_name = path + 'load_change_' + date
        load_change.to_pickle(file_name)
        
        # TODO: think how chage this metric for calculating absolute change of flow for edge with 0 traffic in previous or current splitting  
        NODE_LOAD_CHANGE = abs(max(((load_change['sum_mass_kg_x'] - load_change['sum_mass_kg_y'])/ load_change['sum_mass_kg_y']).mean(),\
                               ((load_change['count_rpo_x'] - load_change['count_rpo_y'])/ load_change['count_rpo_y']).mean()))
        
        logging.info('NODE_LOAD_CHANGE equal:' + str(NODE_LOAD_CHANGE))
    except Exception as e:
        logging.error('Exception in calculating NODE_LOAD_CHANGE: ' + str(e))
    
    return NODE_LOAD_CHANGE, SLA
    
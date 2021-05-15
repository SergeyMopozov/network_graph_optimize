'''
Created on Sep 3, 2019

@author: mrmopoz
'''



from pandas import read_csv, DataFrame, merge, Series, concat
from numpy import ceil, log1p, exp, inf, nan, isnan
from networkx import write_gpickle
from datetime import datetime

#logging
import logging
logging.basicConfig(filename='../log/graph_optimize_log.log', \
                    format='%(asctime)s\t%(levelname)s,\t%(funcName)s\t%(message)s', \
                    level=logging.DEBUG)


def minimal_auto_cost_func(dist, time, volume, pair=(None, None)):
    # функция издержек для транспортировки по плечу в зависмости от объема
    # принимает 3 атрибута:
    #                     dist - расстояние в км
    #                     time - время в часах
    #                     volume - перевозимая масса в кг
    # плотность груза кг/м3
    ro = 100
    volume_step = {0.5:1.5, 0.9:7., 1.5:12., 3:15., 5:18., 7:36., 10:48., 20:80.}
    # справочник типоразмеров
    capacity_step = [0.5, 0.9, 1.5, 3, 5, 7, 10, 20]
    # справочник стоимости пробега
    # FIXME: change cost distribution
    dist_rate = {0.5: 8.1 , 0.9: 10.7, 1.5: 9.9, 3: 18.3, 5: 14.6, 7: 19.7, 10: 27.4, 20: 36.0 }
    # справочник стоимости часа
    time_rate = {'up_to_12': 210, 'after_12': 1.75*210 }
    
    time_cost = lambda time : time_rate['after_12']*time if time > 12 else time_rate['up_to_12']*time
    


    # область поиска оптимальной комбинации ТС
    target_loads = [loads/100 for loads in range(10, 100, 5)]
    total_cost_list = []
    total_capacity_list = []
    avg_loads_list = []
    car_types_list = []
    
    for target_load in target_loads:
        #print('target_load: ', target_load)
        
        current_volume = volume
        
        # подобрать типоразмеры ТС
        cars_types = []
        
        # повторять пока не разместиться весь объем
        if current_volume == 0:
            cars_types.append({'type': 0.5, 'load':current_volume/(volume_step[0.5]*ro)})
        
        while current_volume != 0:
            # найть наиболее подходящее ТС начиная с наибольшей грузоподъемности
            for cap in reversed(capacity_step):
                #print('type of car:', cap)
                amount = int(current_volume // (volume_step[cap]*ro))

                # если количество больше 1 то добавляем ТС в список
                if amount > 1:
                    #print('>1: ', amount)
                    #if cap == 20:
                    for _ in range(amount):
                        cars_types.append({'type': cap, 'load': 1})
                        current_volume  = current_volume - volume_step[cap]*ro
                    break; 

                # если количество равно 1 то добавляем ТС в список
                if amount == 1:
                    #print('==1')
                    cars_types.append({'type': cap, 'load': 1})
                    current_volume  = current_volume - volume_step[cap]*ro
                    break;
                # если  количество равно 0 
                if amount == 0:
                    #print('==0')
                    load =  current_volume / (volume_step[cap]*ro)
                    if load >= target_load:
                        cars_types.append({'type': cap, 'load':load})
                        current_volume  = 0
                        break;
                    # для объемов меньше 0,5
                    if current_volume < volume_step[0.5]*ro:
                        cars_types.append({'type': 0.5, 'load':current_volume/(volume_step[0.5]*ro)})
                        current_volume  = 0
                        break;

        total_cost = 0
        total_capacity = 0
        for _, car in enumerate(cars_types, 1):
            item_dist_cost =  dist_rate[car['type']] * dist
            item_time_cost = time_cost(time)
            item_total_cost = item_dist_cost + item_time_cost
            total_capacity += volume_step[car['type']] * ro

            #print(i, ':')
            #print('car type: ', car['type'])
            #print('car load: ', car['load']*100, '%')
            #print('dist cost: ', item_dist_cost)
            #print('time cost: ', item_time_cost)
            total_cost += item_total_cost
        
        total_cost_list.append(total_cost)
        total_capacity_list.append(total_capacity)
        avg_loads_list.append(volume / total_capacity)
        car_types_list.append(cars_types)
        #print('total volume: ', volume_1)
        #print('total capacity: ', total_capacity)
        #print('average load: ', avg_loads)
        
    
    min_total_cost = min(total_cost_list)
    min_index = total_cost_list.index(min_total_cost)
    min_capacity = total_capacity_list[min_index]
    min_avg_loads = avg_loads_list[min_index]
    min_car_types = car_types_list[min_index]    
    amount = len(min_car_types)
    
    return min_total_cost, min_avg_loads, min_capacity, amount, min_car_types


def avia_cost(time, volume, pair=(None, None)):
    # time in hours, volumes in kg
    # own plane TU-204-100, max load = 20000 kg
    time_cost = 447308 # расчет из проекта собственной авиакомпании
    capacity = 20000
    min_type_types = ['TU-204-100']
    if volume == 0:
        amount = 1
    else:
        amount = ceil(volume / capacity)
    min_total_cost = amount * time * time_cost
    min_avg_loads = volume / (capacity * amount)
    min_capacity = amount * capacity
    
    return min_total_cost, min_avg_loads, min_capacity, amount, min_type_types


def railway_cost_function(dist, volume, tariff_data=read_csv('../ref/railway_tarif_ref.csv')):
    
    
    # time in hours, volumes in kg
    #cost_per_1_vagon = 10e6
    capacity = 8000
    min_type_types = ['vagon']
    if volume == 0:
        amount = 1
    else:
        amount = ceil(volume / capacity)
    cost = tariff_data[(tariff_data['from'] <= dist) & (tariff_data['to'] >= dist) ]['post_nds'].iloc[0]
    min_total_cost = amount * cost
    min_avg_loads = volume / (capacity * amount)
    min_capacity = amount * capacity
    # TODO: calculate needs of total amount not on 1 edge
    #CAPEX = cost_per_1_vagon * amount
    
    return min_total_cost, min_avg_loads, min_capacity, amount, min_type_types


def sort_center_cost(time, volume, operation='sort', pair=(None, None), model=None):
    # TODO: calculate costs of sort center depend in operation type
    # calculate total cost (calculate total volumes -> area -> fix cost for planing mode)
    # load total cost for ASIS mode with current  capcity
    # calculate variable cost depend of volume and time for operation
    
    # CAPEX
    specific_CAPEX_per_m2 = 50000 # in rubles
    # squer model
    b0 = -0.71556093
    b1 = 0.860973
    # ceil_coef can be 100, 200, 500, 1000 minimal discrete for construction of sort_center
    ceil_coef = 500
    total_area = ceil(exp(log1p(volume) * b1 + b0)/ceil_coef) * ceil_coef 
    CAPEX_construction =  total_area * specific_CAPEX_per_m2
    
    
    CAPEX_sort_equipment = 0
    CAPEX_other_equipment = 0
    TOTAL_CAPEX = CAPEX_construction + CAPEX_sort_equipment + CAPEX_other_equipment
    
    # OPEX
    # FIX COST
    # set cost per square for 1 day
    b0_fix_OPEX = 2.44783918
    b1_fix_OPEX = 1.04516277
     
    FIX_OPEX =  exp(b0_fix_OPEX + b1_fix_OPEX * log1p(total_area)) # set fix cost cost in dependance of volumes or from refereance
    
    cost_per_hour = 276 # cost for 1 man per hour
    VAR_OPEX = 0
    
    b0_worker = -3.495886
    b1_worker = 0.703425
    if operation == 'sort':
        worker_count = ceil(exp(log1p(volume) * b1_worker + b0_worker)) # ts count of workers need
        VAR_OPEX = worker_count * cost_per_hour * 24 
    
    TOTAL_OPEX = VAR_OPEX + FIX_OPEX
    
    capacity = ceil(exp((log1p(total_area) - b0)/b1))
    avg_loads = volume / capacity
    
    specification = {'total_square': total_area, 'TOTAL_CAPEX': TOTAL_CAPEX, 'CAPEX_construction': CAPEX_construction, 
                     'CAPEX_sort_equipment': CAPEX_sort_equipment, 'CAPEX_other_equipment':  CAPEX_other_equipment, 'VAR_OPEX': VAR_OPEX,
                     'FIX_OPEX': FIX_OPEX}
    
    return TOTAL_OPEX, avg_loads, capacity, worker_count, specification


def update_weights(graph, nodes_cost):
    # FIXME: update all weights for edge between 2 nodes if it multi edge
    stack_date = []
    for _, row in nodes_cost.iterrows():
        df = row['total_cost'].copy().reset_index()
        df['edge'] =   Series([row['edge'] for _ in range(len(df))])
        
        stack_date.append(df.set_index('date_'))
        #break
    nodes_cost = concat(stack_date, sort=False)
    
    # calculate sum cost for period
    nodes_cost = nodes_cost.groupby('edge', as_index=False).agg({'sum_mass_kg':'sum', 'count_rpo':'sum', 'total_cost':'sum'})
    nodes_cost['avg_kg'] = (nodes_cost['total_cost'] / nodes_cost['sum_mass_kg']).replace(inf, nan).fillna(0)
    nodes_cost['avg_count'] = (nodes_cost['total_cost'] / nodes_cost['count_rpo']).replace(inf, nan).fillna(0)
  
    graph_edges = DataFrame(graph.edges(data=True), columns = ['from', 'to', 'info'])
    graph_edges['edge'] = Series(zip(graph_edges['from'], graph_edges['to']))
    graph_edges = merge(graph_edges, nodes_cost, on='edge', how='left')
    graph_edges['info'] = graph_edges.apply(lambda row: _update_info(row['info'], row['avg_kg'], row['total_cost']), axis=1)


    # DONE: think you add new edges or ubdate current using this method?  ANSWER: methode update edges in graph
    new_edges = [(row['from'], row['to'], row['info']) for _, row in graph_edges.iterrows()]
    graph.add_edges_from(new_edges)
    
    logging.info('Graph updated')
    logging.info('Save graph on disk...')

    date = datetime.today().strftime('%Y-%m-%d_%H:%M')
    path = '../result/'
    file_name = path + 'updated_graph_' + date
    write_gpickle(graph, file_name)
    
    return graph


def _update_info(dict_info, avg_cost, total_cost):
    # TODO: think about how update potential edges without calculated mass
    # maybe use minimal cost through calculating cost for current volume for different transport
    # then need change graph structure to check ecvivalents between edges for different transport
    if 'cost' in dict_info.keys():
        if isnan(total_cost):
            dict_info['avg_cost'] = dict_info['cost']
        else:
            dict_info['cost'] = total_cost
            dict_info['avg_cost'] = avg_cost
    '''
    if 'cost' in dict_info.keys():

        # update cost for avia edge in dependenies of volume
        if dict_info['type'] == 'avia':
            dict_info['cost'] = avia_cost(dict_info['time'], mass)[0]
        # update cost for auto edge in dependenies of volume
        if dict_info['type'] == 'auto':
            dict_info['cost'] = minimal_auto_cost_func(dict_info['dist'], dict_info['time'], mass)[0]
            
        # update cost for sort edge in dependenies of volume   
        if dict_info['type'] == 'sort_center':
            dict_info['cost'] = sort_center_cost(dict_info['time'])[0]
        # calculate avarage cost   
        dict_info['avg_cost'] = dict_info['cost'] / mass
    '''   
    return dict_info


"""


@author: mrmopoz
"""
# logging
from graph_utils.graph import create_graph
from graph_utils.graph_optimize import optimize_graph
from graph_utils.data_loader import load_volumes_on_derections
import logging

logging.basicConfig(filename='../log/graph_optimize_log.log',
                    format='%(asctime)s\t%(levelname)s,\t%(funcName)s\t%(message)s',
                    level=logging.DEBUG)

logging.info('Optimization is starting...')
logging.info('Create graph')
point_from, point_to, graph = create_graph()

logging.info('Load volumes')
volumes = load_volumes_on_derections('../data/volumes_2018_10_2019_02')

logging.info('Start optimization')
graph, route_plan, nodes_load, nodes_cost = optimize_graph(graph, volumes,
                                                           point_from, point_to,
                                                           SLA_MIN=0.98, LOAD_GRADIENT=0.01)

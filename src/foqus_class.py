#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# foqus_class.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import os
import logging

import pandas as pd
import olca_schema as olca
from netlolca.NetlOlca import NetlOlca
import prommis.uky.uky_flowsheet as uky
from prommis.uky.costing.ree_plant_capcost import QGESSCostingData
from pyomo.environ import TransformationFactory
from pyomo.core.base.var import Var
from idaes.core.util.model_diagnostics import DiagnosticsToolbox
import foqus_lib.framework.graph.graph as gr
import foqus_lib.framework.graph.node as nd
import foqus_lib.framework.graph.edge as ed
import foqus_lib.framework.graph.nodeVars as nv
from foqus_lib.framework.uq.Distribution import Distribution
from foqus_lib.framework.session.session import session
from foqus_lib.framework.optimizer.problem import objectiveFunction

import src as lca_prommis


##############################################################################
# CLASSES
##############################################################################
class NetlFoqus(object):
    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Global Variables
    # ////////////////////////////////////////////////////////////////////////

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Initialization
    # ////////////////////////////////////////////////////////////////////////
    def __init__(self):
        """Initialize NetlFoqus class."""
        self.vars = []  # all variables
        self.dv = []    # user-defined decision variables
        self.exchanges = pd.DataFrame()  # user-defined exchanges
        self.exchanges_vars = []  # variables of user-defined exchanges
        self.fs = None  # foqus flowsheet object
        self.prommis_node = None  # ProMMiS node object
        self.olca_node = None     # openLCA node object
        self.edge = None    # edge object
        self.logger = logging.getLogger(__name__)
        # self.session = session(useCurrentWorkingDir=True) # TBD


    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Property Definitions
    # ////////////////////////////////////////////////////////////////////////
    @property
    def dv_names(self):
        # Decision variable names
        return [x.ipvname for x in self.dv]

    @property
    def ndv(self):
        # Non-decision variables
        return [x for x in self.vars if x not in self.dv]

    @ndv.setter
    def ndv(self, value):
        raise AttributeError("ndv is a read-only property")

    @property
    def has_session(self):
        return self.fs is not None #TODO: turns out this is not a sign that the graph has a session
        #                                 the session has to be defined separately using the session module from foqus

    @has_session.setter
    def has_session(self, value):
        raise AttributeError("has_session is a read-only property")

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Function Definitions
    # ////////////////////////////////////////////////////////////////////////
    def add_decision_variable(self, var_name):
        """Add a decision variable to the FOQUS flowsheet.

        Parameters
        ----------
        var_name : str
            The name of the decision variable to be added.

        Returns
        -------
        None
        """
        if var_name not in self.vars:
            raise ValueError(
                f"Variable {var_name} not found in flowsheet variables."
            )
        if var_name in self.dv_names:
            self.logger.warning(
                f"Variable {var_name} is already a decision variable."
            )
        else:
            self.dv.append(nv.NodeVars(ipvname=var_name))

            # All decision variables are assumed inputs to prommis node;
            # add it if prommis_node exists; Python correctly links the object
            # found in self.dv to the one in prommis_node.inVars
            if self.prommis_node is not None:
                self.prommis_node.inVars[var_name] = self.dv[-1]

    def add_edge(self, from_node, to_node):
        """Add an edge to the FOQUS flowsheet.

        Parameters
        ----------
        from_node : nd.Node
            The starting node of the edge.
        to_node : nd.Node
            The ending node of the edge.

        Returns
        -------
        ed.Edge
            The created edge object.
        """
        if not self.has_session:
            raise RuntimeError(
                "Flowsheet session not created. Call create_session() first."
            )

        # check if the from_node is a valid object
        if not isinstance(from_node, gr.Node):
            raise TypeError("from_node must be a valid FOQUS node object")
        # check if the to_node is a valid object
        if not isinstance(to_node, gr.Node):
            raise TypeError("to_node must be a valid FOQUS node object")

        # Add edge to edges list; uses node names
        self.fs.addEdge(from_node.name, to_node.name)

        # Find and return the created edge
        return self.fs.edges[-1]


    def add_node(self, node_name):
        """Add a node to the FOQUS flowsheet.

        Parameters
        ----------
        node_name : str
            The name of the node to be added.

        Returns
        -------
        nd.Node
            The created node object.
        """
        if not self.has_session:
            raise RuntimeError(
                "Flowsheet session not created. Call create_session() first."
            )

        node = self.fs.addNode(node_name)
        return node

    def create_session(self, session_name):
        """Create a FOQUS flowsheet session.

        Parameters
        ----------
        session_name : str
            The name of the FOQUS flowsheet session.

        Returns
        -------
        None
        """
        self.fs = gr.Graph(session_name)

    def set_dv_max(self, var_name, max_val):
        """Set the maximum value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        max_val : float
            The maximum value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setMax(max_val)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_min(self, var_name, min_val):
        """Set the minimum value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        min_val : float
            The minimum value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setMin(min_val)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_value(self, var_name, value):
        """Set the value for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        value : float
            The value to set.

        Returns
        -------
        None
        """
        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.setValue(value)
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def set_dv_dist(self, var_name, distribution):
        """Set the distribution for a decision variable.

        Parameters
        ----------
        var_name : str
            The name of the decision variable.
        distribution : distribution object
            The distribution to set.

        Returns
        -------
        None

        Throws
        ------
        ValueError
            If the decision variable type is not found.
            See Distribution.fullNames or Distribution.psuadeNames
        """
        # Distribution is title case (e.g., "Uniform" or "U")
        if isinstance(distribution, str):
            distribution = distribution.title()
        my_dist = Distribution(distribution)

        for dv in self.dv:
            if dv.ipvname == var_name:
                dv.dist = my_dist
                return
        raise ValueError(f"Decision variable {var_name} not found.")

    def init_uky(self):
        """
        Initialize the UKy flowsheet.

        Parameters
        ----------
        None

        Throws
        ------
        TypeError
            If the producing_node or receiving_node is not a valid FOQUS node object
        ValueError
            If the exchanges table is not found


        Returns
        -------
        the UKy flosheet model
        
        """
        # Creates a session with ProMMiS and openLCA nodes connected by an edge.
        # Reads the UKy exchange table and populates the variable list.

        # Create FOQUS flowsheet
        self.create_session("UKy REE Flowsheet")
        self.prommis_node = self.add_node("ProMMiS")
        self.olca_node = self.add_node("openLCA")
        self.edge = self.add_edge(self.prommis_node, self.olca_node)

        # Get UKy exchange table
        my_vars, my_df, my_cm = get_uky_vars_exchanges()

        self.vars = my_vars

        # Add exchange variables names from exchange flow names:
        self.exchanges = my_df

        return my_cm
    
    def initialize_intermediate_variables(self, producing_node, receiving_node):
        """
        Initialize the intermediate variables.

        Parameters
        ----------
        producing_node : gr.Node
            The producing node producing an output.
        receiving_node : gr.Node
            The receiving node receiving the outputs of the producing node.

        Returns
        -------
        None
        """
        exchanges_names = self.exchanges['Flow_Name'].tolist()
        if not isinstance(producing_node, gr.Node):
            raise TypeError("producing_node must be a valid FOQUS node object")
        if not isinstance(receiving_node, gr.Node):
            raise TypeError("receiving_node must be a valid FOQUS node object")
        for var_name in exchanges_names:
            self.vars.append(nv.NodeVars(opvname=var_name, dtype = float))
            self.exchanges_vars.append(nv.NodeVars(opvname=var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set output properties for %s: value=%f" % (var_name, value)
            )
            if producing_node is not None:
                producing_node.outVars[var_name] = self.exchanges_vars[-1]    

        for var_name in exchanges_names:
            input_var_name = var_name + "_input"
            self.vars.append(nv.NodeVars(ipvname=input_var_name, dtype = float))
            self.exchanges_vars.append(nv.NodeVars(ipvname=input_var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set input properties for %s: value=%f" % (input_var_name, value)
            )
            if receiving_node is not None:
                receiving_node.inVars[input_var_name] = self.exchanges_vars[-1]

    def connect_intermediate_variables(self, node1, node2):
        """
        Connect the intermediate variables.
        Parameters
        ----------
        node1 : gr.Node
            The producing node.
        node2 : gr.Node
            The receiving node.

        Returns
        -------
        None

        """
        for var_out in node1.outVars:
            var_in = next(var for var in node2.inVars if var.startswith(var_out))
            self.edge.addConnection(var_out, var_in)
            logging.info(
                "Connected %s to %s" % (var_out, var_in)
            )
    
    def initiate_output_variables(self, node, var_name, var_value):
        """
        Initiate an output variable for a given node.
        Parameters
        ----------
        node : gr.Node
            The node to initiate the output variable for.
        var_name : str
            The name of the output variable.
        var_value : float
            The value of the output variable.

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        self.vars.append(nv.NodeVars(opvname=var_name, dtype = float))
        self.vars[-1].setValue(var_value)
        if node is not None:
            node.outVars[var_name] = self.vars[-1]
        logging.info(
            "Initiated output variable %s for node %s: value=%f" % (var_name, node.name, var_value)
        )
        return self.vars[-1]

    def define_node_script(self, node, script):
        """
        This function defines the script for a give node

        Parameters
        ----------
        node : gr.Node
            The node to define the script for.
        script : str
            The script to define for the node.

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        
        node.pythonCode = script
        logging.info(
            "Defined script for node %s: script=%s" % (node.name, script)
        )

    def set_node_scriptMode(self, node, script_mode):
        """
        Set the script mode for a given node.
        
        Parameters
        ----------
        node : gr.Node
            The node to set the script mode for.
        script_mode : str
            The script mode to set.
            Options include:
            'pre' --> runs the script before the model
            'total' --> runs the script instead of the model
            'post' --> runs the script after the model

        Returns
        -------
        None
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        
        if script_mode not in ['pre', 'total', 'post']:
            raise ValueError("script_mode must be either 'pre', 'total', or 'post'")
        node.scriptMode = script_mode
        logging.info(
            "Set script mode for node %s: script_mode=%s" % (node.name, script_mode)
        )
        
    def run_standalone_node_script(self, node):
        """
        Run the script for a given node.
        This will run the script as if the node 
        scriptMode is 'total'.
        
        Parameters
        ----------
        node : gr.Node
            The node to run the script for.
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        if node.pythonCode is None:
            raise ValueError("node has no script to run")
        
        node.runPython()
        logging.info(
            "Run script for node %s" % (node.name)
        )
        
    def run_node_script (self, node):
        """
        Run the script for a given node.
        This function will run the script based on 
        the set node scriptMode.

        Options include:
        'pre' --> runs the script before the model
        'total' --> runs the script instead of the model
        'post' --> runs the script after the model
        
        Parameters
        ----------
        node : gr.Node
            The node to run the script for.
        """
        if not isinstance(node, gr.Node):
            raise TypeError("node must be a valid FOQUS node object")
        if node.pythonCode is None:
            raise ValueError("node has no script to run")
        
        node.runCalc()
        logging.info(
            "Run script for node %s with script mode %s" % (node.name, node.scriptMode)
        )

    # def create_session(self, useCurrentWorkingDir):
    #     """
    #     Create a session.
    #     Parameters
    #     ----------
    #     session_name : str
    #         The name of the session.
    #     useCurrentWorkingDir : bool
    #         Whether to use the current working directory.
    #     """
    #     if useCurrentWorkingDir is not None:
    #         my_session = session(useCurrentWorkingDir)
    #     else:
    #         my_session = session(useCurrentWorkingDir=True)
        
    #     my_session.flowsheet = self
        
    #     return my_session

###############################################################################
# FUNCTIONS
###############################################################################
def get_uky_vars_exchanges():
    """Run the UKy flowsheet and process LCA data to create an initial
    exchange table.

    Returns
    -------
    pandas.DataFrame
    """
    # Build the ConcreteModel from UKy flowsheet
    m, _ = uky.main()

    # Extract all potential PrOMMiS variables
    all_vars = []
    for my_var in m.component_objects(Var):
        all_vars.append(my_var.name)

    # Create LCA exchanges (for PrOMMiS outputs to openLCA inputs)
    prommis_data = lca_prommis.data_lca.get_lca_df(m)

    df = lca_prommis.convert_lca.convert_flows_to_lca_units(
        prommis_data,
        hours=1,
        mol_to_kg=True,
        water_unit='m3'
    )

    REO_list = [
        "Yttrium Oxide",
        "Lanthanum Oxide",
        "Cerium Oxide",
        "Praseodymium Oxide",
        "Neodymium Oxide",
        "Samarium Oxide",
        "Gadolinium Oxide",
        "Dysprosium Oxide",
    ]

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Solid Feed',
        new_flow_name='374 ppm REO Feed',
        value_2_merge=REO_list
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Roaster Product',
        new_flow_name='73.4% REO Product'
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Wastewater',
        new_flow_name='Wastewater',
        merge_column='Category'
    )

    df = lca_prommis.final_lca.merge_flows(
        df,
        merge_source='Solid Waste',
        new_flow_name='Solid Waste',
        merge_column='Category'
    )

    finalized_df = lca_prommis.final_lca.finalize_df(
        df=df,
        reference_flow='73.4% REO Product',
        reference_source='Roaster Product',
        water_type='raw fresh water'
    )
    return (all_vars, finalized_df, m)

def initiate_lca_model(client, 
                        process_name, 
                        process_description, 
                        lca_df_finalized, 
                        impact_method_uuid, 
                        parameter_set_name, 
                        parameter_set_description, 
                        is_baseline):
    """
    Initiate the LCA model.
    Parameters
    ----------
    client : olca_ipc.Client
        The IPC client object.
    process_name : str
        The name of the process.
    process_description : str
        The description of the process.
    lca_df_finalized : pandas.DataFrame
        The finalized LCA dataframe.
    impact_method_uuid : str
        The UUID of the impact method.

    Returns
    -------
    total_impacts : dict
        The total impacts of the process.
    my_parameters : list
        The parameters of the process.
    ps_uuid : str
        The UUID of the product system.
    """
    process, my_parameters = lca_prommis.create_lca.create_new_process(client,
                                                                        lca_df_finalized,
                                                                        process_name,
                                                                        process_description)
    ps = lca_prommis.create_ps.create_ps(client, process.id)
    ps_uuid = ps.id
    # create baseline parameter set
    parameter_set = lca_prommis.run_analysis.create_parameter_set(client, 
                                                                process.id, 
                                                                ps.id, 
                                                                parameter_set_name, 
                                                                parameter_set_description, 
                                                                is_baseline)
    
    result = lca_prommis.run_analysis.run_analysis(client, 
                                                    ps.id, 
                                                    impact_method_uuid, 
                                                    parameter_set.parameters)
    result.wait_until_ready()
    total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

    return total_impacts, my_parameters, ps_uuid

def initialize_decision_variables(nf_obj, m):
    """
    Initialize the decision variables.
    Parameters
    ----------
    nf_obj : NetlFoqus
        The NetlFoqus object.
    m : pyomo.environ.ConcreteModel
        The model.

    Returns
    -------
    None
    """
    for dv in nf_obj.dv:
        # Execution string to get current value from model
        my_str = "m.%s.get_values().values()" % dv.ipvname
        my_val = [x for x in eval(my_str)][0]
        # Use half and double for min/max
        my_min = my_val * 0.5
        my_max = my_val * 2.0
        # Set decision variable properties
        dv.setValue(my_val)
        dv.setMin(my_min)
        dv.setMax(my_max)
        # NOTE: distribution is uniform by default; can be changed later
        logging.info(
            "Set decision properties for %s: value=%f, min=%f, max=%f" % (
                dv.ipvname, my_val, my_min, my_max
            )
        )

def setup_optimizer(session, solver_name):
    """
    This function setups the optimizer for the session

    It creates a problem object, assisngs the solver name, 
    and appends the decision variables to the problem
    
    Parameters
    ----------
    session : foqus_lib.framework.session.session
        The session object.
    solver_name : str
        The name of the solver.

    Returns
    -------
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    """
    # TODO: Add error handling for solver name
    #       solver name should be checked for a list
    problem = session.optProblem
    problem.solver = solver_name
    problem.v = nf.dv   # setup decision variables
                        # TODO: check how decision variables are stored in nf.dv 
    return problem

def create_problem_objective(problem, objectives_list, node_name):
    """
    This function creates a problem objective
    
    Parameters
    ----------
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    objectives_list : list
        The list of objective names.
        example: ['GWP', 'CED']

    Returns
    -------
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    """
    # TODO: Add error handling for objectives list
    #       each objective should be checked against 
    #       the outputVars    
    for objective in objectives_list:
        objective_function = objectiveFunction()
        objective_function.pycode = "f[node_name][objective]"
        problem.obj.append(objective_function)
    problem.objtype = problem.OBJ_TYPE_EVAL
    return problem

#def create_problem_constraint() # In progress

def setup_solver_options(problem, 
                         use_defaults = False, 
                         algorithm = None, 
                         max_func_eval = None, 
                         max_time = None, 
                         tol_func_rel = None, 
                         lower_bound = None, 
                         upper_bound = None):

    """
    This function setups the solver options 
    The user has the option to simply use the default solver options
    or to specify the solver options manually
    Parameters
    ----------
    use_defaults : bool
        Whether to use the default solver options.
    algorithm : str
        The algorithm to use.
    max_func_eval : int
        The maximum number of function evaluations.
    max_time : float
        The maximum time in hours.
    tol_func_rel : float
        The relative tolerance for the function.
    lower_bound : float
        The lower bound.
    upper_bound : float
        The upper bound.

    Returns
    -------
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    """

    if use_defaults:
        problem.solverOptions[problem.solver] = {
        "Solver": "BOBYQA",   
        "maxeval": 100,       
        "maxtime": 60,        
        "tolfunrel": 1e-4,    
        "lower": 0.0,         
        "upper": 10.0         
        }
    else:
        # TODO: Add error handling for algorithm
        #       each algorithm should be checked for a list
        #       BOBYQA, COBYLA, DIRECT, etc.
        problem.solverOptions[problem.solver] = {
            "Solver": algorithm,
            "maxeval": max_func_eval,
            "maxtime": max_time,
            "tolfunrel": tol_func_rel,
            "lower": lower_bound,
            "upper": upper_bound
        }

    return problem

def run_optimization(problem, session):
    """
    This function runs the optimization
    
    Parameters
    ----------
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    session : foqus_lib.framework.session.session
        The session object.

    Returns
    -------
    solver : foqus_lib.framework.optimizer.optimization
        The solver object.
    problem : foqus_lib.framework.optimizer.problem
        The problem object.
    """

    my_solver = problem.run(session)
    my_solver.join() # wait for results
    logging.info("Optimization completed")

    return my_solver, problem
    
#
# SANDBOX
#
if __name__ == "__main__":
    # Import and initialize NetlFoqus w/ UKy flowsheet
    import src.foqus_class as foqus_class
    from src.foqus_class import NetlFoqus
    import os
    import logging

    import pandas as pd
    import olca_schema as olca
    from netlolca.NetlOlca import NetlOlca
    import prommis.uky.uky_flowsheet as uky
    from prommis.uky.costing.ree_plant_capcost import QGESSCostingData
    from pyomo.environ import TransformationFactory
    from pyomo.core.base.var import Var
    from idaes.core.util.model_diagnostics import DiagnosticsToolbox
    import foqus_lib.framework.graph.graph as gr
    import foqus_lib.framework.graph.node as nd
    import foqus_lib.framework.graph.edge as ed
    import foqus_lib.framework.graph.nodeVars as nv
    from foqus_lib.framework.uq.Distribution import Distribution

    import src as lca_prommis

    nf = NetlFoqus()
    m = nf.init_uky()

    # Add a decision variable
    my_var1 = "fs.leach_liquid_feed.flow_vol"
    nf.add_decision_variable(my_var1)

    # [FH] we need at least two decision variables
    my_var2 = "fs.load_sep.split_fraction"
    nf.add_decision_variable(my_var2)

    # Help with initializing decision variables
    foqus_class.initialize_decision_variables(nf, m)

    # Initialize intermediate variables
    nf.initialize_intermediate_variables(nf.prommis_node, nf.olca_node)

    # connect intermediate variables
    nf.connect_intermediate_variables(nf.prommis_node, nf.olca_node)

    # initiate lca_model
    lca_df_finalized = nf.exchanges
    netl = NetlOlca()
    netl.connect()
    netl.read()
    
    process_name = "TESTING - REO Extraction From Coal Mining Refuse | UKy Flowsheet"

    process_description = """This process involves the production of a Rare 
    Earth Oxide solid extraction from coal mining refuse. The scope of this 
    work starts with the leaching of size-reduced REE-rich feedstock (REE: 
    Rare Earth Elements) and ends with the recovery of mixed REO solids. The 
    process consists of six main stages: 1) Mixing and Leaching, 2) Rougher 
    Solvent Extraction, 3) Cleaner Solvent Extraction, 4) Precipitation, 
    5) Solid-Liquid (S/L) separation, and 6) Roasting. This process does not 
    account for upstream processes leading to the production of REE-rich 
    feedstock nor does it account for Downstream processes leading to the 
    separation of REE contained in the REO. The main product is a rare earth 
    oxide solid with no other by-products or co-products. The functional 
    unit is 1 kg of recovered REO solids. The material and energy inputs 
    shown in the system boundary figure below have been shortlisted and 
    estimated based on the UKy flowsheet output, as well as other relevant 
    literature."""

    impact_method_uuid = '60cb71ff-0ef0-4e6c-9ce7-c885d921dd15'

    parameter_set_name = "Baseline"
    parameter_set_description = "Baseline parameter set for the process"
    is_baseline = True
    total_impacts, my_parameters, ps_uuid = foqus_class.initiate_lca_model (netl, 
                                                                            process_name, 
                                                                            process_description, 
                                                                            lca_df_finalized, 
                                                                            impact_method_uuid, 
                                                                            parameter_set_name, 
                                                                            parameter_set_description, 
                                                                            is_baseline)
    
    # create output variables 
    for impact_category in total_impacts['name']:
        nf.initiate_output_variables(nf.olca_node, 
                                    impact_category, 
                                    total_impacts.loc[total_impacts['name'] == impact_category, 'amount'].values[0])
        logging.info(
            "Initiated output variable %s for node %s: amount=%f" % (impact_category, nf.olca_node.name, total_impacts.loc[total_impacts['name'] == impact_category, 'amount'].values[0])
        )
    
    olca_node_script = """
    import pandas as pd
    import olca_schema as olca

    from netlolca.NetlOlca import NetlOlca

    import src as lca_prommis

    params = my_parameters.copy() # get a copy of the parameters df

    for count, row in params.iterrows():
        if x[row['parameter_description']] is not None:
            row['parameter_value'] = float(x[row['parameter_description']]) 
        else:
            row['parameter_value'] = 0

    parameter_set_name = "Baseline" # get the parameter_set name

    param_set_ref = lca_prommis.run_analysis.update_parameter ( netl, 
                                                                ps_uuid, 
                                                                parameter_set_name, 
                                                                params)

    result = lca_prommis.run_analysis.run_analysis (netl, 
                                                    ps_uuid, 
                                                    impact_method_uuid, 
                                                    param_set_ref)
    result.wait_until_ready()
    total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

    # save the total impacts to the node outputs
    for result in total_impacts:
        f[result['name']] = result['value']
    """

    nf.define_node_script(nf.olca_node, olca_node_script)

    prommis_node_script = """
    import os
    import pandas as pd
    import olca_schema as olca
    from netlolca.NetlOlca import NetlOlca
    import prommis.uky.uky_flowsheet as uky 
    import src as lca_prommis

    from pyomo.environ import TransformationFactory
    from idaes.core.util.model_diagnostics import DiagnosticsToolbox
    from prommis.uky.costing.ree_plant_capcost import QGESSCostingData
    import prommis.uky.uky_flowsheet as uky

    home_dir = os.path.expanduser("~")

    m = uky.build()

    uky.set_operating_conditions(m)

    if "Leach liquid feed" in x:
        m.fs.leach_liquid_feed.flow_vol.fix(x["Leach liquid feed"])

    if "split_fraction" in x:
        m.fs.load_sep.split_fraction[0.0, 'recycle'].fix(x["split_fraction"])

    uky.set_scaling(m)

    scaling = TransformationFactory("core.scale_model")
    scaled_model = scaling.create_using(m, rename=False)

    if uky.degrees_of_freedom(scaled_model) != 0:
        raise AssertionError("Degrees of freedom != 0")

    uky.initialize_system(scaled_model)
    uky.solve_system(scaled_model)

    uky.fix_organic_recycle(scaled_model)
    scaled_results = uky.solve_system(scaled_model)

    if not uky.check_optimal_termination(scaled_results):
        raise RuntimeError("Solver failed to terminate optimally")

    # Propagate results back to original model
    results = scaling.propagate_solution(scaled_model, m)

    # 5. Add Costing (Optional, but likely needed for optimization)
    uky.add_costing(m)

    # Costing initialization
    QGESSCostingData.costing_initialization(m.fs.costing)
    QGESSCostingData.initialize_fixed_OM_costs(m.fs.costing)
    QGESSCostingData.initialize_variable_OM_costs(m.fs.costing)

    # Final solve with costing
    uky.solve_system(m)

    prommis_data = lca_prommis.data_lca.get_lca_df(m)

    df = lca_prommis.convert_lca.convert_flows_to_lca_units(prommis_data, hours=1, mol_to_kg=True, water_unit='m3')

    REO_list = [
        "Yttrium Oxide",
        "Lanthanum Oxide",
        "Cerium Oxide",
        "Praseodymium Oxide",
        "Neodymium Oxide",
        "Samarium Oxide",
        "Gadolinium Oxide",
        "Dysprosium Oxide",
    ]

    df = lca_prommis.final_lca.merge_flows(df, merge_source='Solid Feed', new_flow_name='374 ppm REO Feed', value_2_merge=REO_list)


    df = lca_prommis.final_lca.merge_flows(df, merge_source='Roaster Product', new_flow_name='73.4% REO Product')


    df = lca_prommis.final_lca.merge_flows(df, merge_source='Wastewater', new_flow_name='Wastewater', merge_column='Category') 

    df = lca_prommis.final_lca.merge_flows(df, merge_source='Solid Waste', new_flow_name='Solid Waste', merge_column='Category') 

    finalized_df = lca_prommis.final_lca.finalize_df(
            df=df, 
            reference_flow='73.4% REO Product', 
            reference_source='Roaster Product',
            water_type='raw fresh water'
        )

    output_dir = os.path.join(home_dir, 'output')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    finalized_df.to_csv(os.path.join(output_dir, "finalized_df.csv"), index=False)

    for _, row in finalized_df.iterrows():
        f[row['Flow_Name']] = row['LCA_Amount']
    """

    nf.define_node_script(nf.prommis_node, prommis_node_script)

    my_session = nf.create_session(True) # create session
    
    problem = setup_optimizer(my_session, "NLopt") # first step in setting up optimizer

    problem = create_problem_objective(problem, ["GWP", "CED"], nf.prommis_node.name) # create problem objective

    # TODO: create function to setup problem constraint (In progress)

    problem = setup_solver_options(problem, True) # setup solver options

    my_solver, problem = run_optimization(problem, my_session) # run optimization
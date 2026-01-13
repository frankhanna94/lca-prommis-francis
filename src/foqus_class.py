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
        self.exchanges_vars = []  # user-defined exchanges
        self.fs = None  # foqus flowsheet object
        self.prommis_node = None  # ProMMiS node object
        self.olca_node = None     # openLCA node object
        self.edge = None    # edge object
        self.logger = logging.getLogger(__name__)


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
        return self.fs is not None

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
    
    def initialize_intermediate_variables(self):
        exchnages_names = self.exchanges['Flow_Name'].tolist()
        for var_name in exchnages_names:
            self.exchanges_vars.append(nv.NodeVars(opvname=var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set output properties for %s: value=%f" % (var_name, value)
            )
            if self.prommis_node is not None:
                self.prommis_node.outVars[var_name] = self.exchanges_vars[-1]    

        for var_name in exchnages_names:
            input_var_name = var_name + "_input"
            self.exchanges_vars.append(nv.NodeVars(ipvname=input_var_name, dtype = float))
            value = self.exchanges.loc[self.exchanges['Flow_Name'] == var_name, 'LCA_Amount'].values[0]
            self.exchanges_vars[-1].setValue(value)
            logging.info(
                "Set input properties for %s: value=%f" % (input_var_name, value)
            )
            if self.olca_node is not None:
                self.olca_node.inVars[input_var_name] = self.exchanges_vars[-1]

    def connect_intermediate_variables(self, node1, node2):
        for var_out in node1.outVars:
            var_in = next(var for var in node2.inVars if var.startswith(var_out))
            self.edge.addConnection(var_out, var_in)
            logging.info(
                "Connected %s to %s" % (var_out, var_in)
            )

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

def initiate_lca_model(client, process_name, process_description, lca_df_finalized, impact_method_uuid):
    process = lca_prommis.create_lca.create_new_process(client,lca_df_finalized,process_name,process_description)
    ps = lca_prommis.create_ps.create_ps(client, process.id)
    result = lca_prommis.run_analysis.run_analysis(client, ps.id, impact_method_uuid)
    result.wait_until_ready()
    total_impacts = lca_prommis.generate_total_results.generate_total_results(result)

def initialize_decision_variables(nf_obj, m):
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

    
#
# SANDBOX
#
if __name__ == "__main__":
    # Import and initialize NetlFoqus w/ UKy flowsheet
    from foqus_class import NetlFoqus
    nf = NetlFoqus()
    m = nf.init_uky()

    # Add a decision variable
    my_var = "fs.leach_liquid_feed.flow_vol"
    nf.add_decision_variable(my_var)

    # Help with initializing decision variables
    foqus_class.initialize_decision_variables(nf, m)

    # Initialize intermediate variables
    nf.initialize_intermediate_variables()

    # connect intermediate variables
    nf.connect_intermediate_variables(nf.prommis_node, nf.olca_node)

    # initiate lca_model
    lca_df_finalized = nf.exchanges
    netl = NetlOlca()
    netl.connect()
    netl.read()
    
    process_name = "REO Extraction From Coal Mining Refuse | UKy Flowsheet"

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

    foqus_class.initiate_lca_model(netl, process_name, process_description, lca_df_finalized, impact_method_uuid)
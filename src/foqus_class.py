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
        self.exchanges = []  # user-defined exchanges
        self.fs = None  # foqus flowsheet object
        self.prommis_node = None  # ProMMiS node object
        self.olca_node = None     # openLCA node object
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
        edge = self.add_edge(self.prommis_node, self.olca_node)

        # Get UKy exchange table
        my_vars, my_df, my_cm = get_uky_vars_exchanges()

        self.vars = my_vars

        # Add exchange variables names from exchange flow names:
        for var_name in my_df['Flow_Name'].tolist():
            self.exchanges.append(var_name)

        return my_cm


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
    initialize_decision_variables(nf, m)

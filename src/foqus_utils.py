# Import dependencies
#####################
import foqus_lib.framework.graph.graph as gr
import foqus_lib.framework.graph.node as nd
import foqus_lib.framework.graph.edge as ed
import foqus_lib.framework.graph.nodeVars as nv

# Function to create foqus flowsheet with two nodes and an edge
#--------------------------------------------------------------
def create_foqus_flowsheet(session_name, node1_name, node2_name, edge_name):
    """
    This function creates a foqus flowsheet with two nodes and an edge.
    The nodes are created with the given names and the edge is created with the given name.
    The function returns the flowsheet object.
    """

    # create the flowsheet
    flowsheet = gr.Graph(session_name)

    # create the nodes
    node1 = flowsheet.addNode(node1_name, 1, 1, 1)
    node2 = flowsheet.addNode(node2_name, 2, 1, 1)

    # create the edge
    flowsheet.addEdge(node1, node2, edge_name)
    edge = flowsheet.edges[0]

    # return the flowsheet objects
    return flowsheet, node1, node2, edge

# Function to add a node to an existing flowsheet
#------------------------------------------------
def add_node_to_flowsheet(flowsheet, node_name):
    """
    This function adds a node to an existing flowsheet.
    The node is created with the given name and the function returns the node object.

    Parameters:
    ----------
    flowsheet: the flowsheet object
    node_name: the name of the node

    Returns:
    -------
    node: the node object

    Raises:
    ValueError: if the node name is already in use
    TypeError: if the flowsheet is not a valid object
    """

    # check if the flowsheet is a valid object
    if not isinstance(flowsheet, gr.Graph):
        raise TypeError("flowsheet must be a valid foqus flowsheet object")
    
    # check if the node name is already in use
    if node_name in flowsheet.nodes:
        raise ValueError(f"Node name {node_name} is already in use")
    
    node = flowsheet.addNode(node_name, 1, 1, 1)
    return node


# Function to add an edge to an existing flowsheet
#-------------------------------------------------
def add_edge_to_flowsheet(flowsheet, node1, node2, edge_name):
    """
    This function adds an edge to an existing flowsheet.

    Parameters:
    flowsheet: the flowsheet object
    node1: the first node object
    node2: the second node object
    edge_name: the name of the edge

    Returns:
    edge: the edge object

    Raises:
    ValueError: if the edge name is already in use
    ValueError: if node1 and node2 are already connected by an edge
    TypeError: if the flowsheet, node1, node2, or edge_name is not a valid object
    """

    # check if the flowsheet is a valid object
    if not isinstance(flowsheet, gr.Graph):
        raise TypeError("flowsheet must be a valid foqus flowsheet object")
    # check if the node1 is a valid object
    if not isinstance(node1, gr.Node):
        raise TypeError("node1 must be a valid foqus node object")
    # check if the node2 is a valid object
    if not isinstance(node2, gr.Node):
        raise TypeError("node2 must be a valid foqus node object")

    # check if the edge name is already in use
    if edge_name in flowsheet.edges:
        raise ValueError(f"Edge name {edge_name} is already in use")
    
    # check if node1 and node2 are already connected by an edge
    for edge in flowsheet.edges:
        dict = edge.saveDict()
        nodes = [dict["start"], dict["end"]]
        if node1 in nodes and node2 in nodes:
            raise ValueError(f"Node {node1} and {node2} are already connected by an edge")

    # add the edge
    flowsheet.addEdge(node1, node2, edge_name)
    edge = flowsheet.edges[len(flowsheet.edges) - 1]

    # return the edge object
    return edge


# Function to create a new input variable
#----------------------------------------
def create_input_variable (variable_name, value, min, max, unit, distribution, description):
    """
    This function creates a new input variable.

    Parameters:
    ----------
    variable_name: the name of the variable
    value: the value of the variable
    min: the minimum value of the variable
    max: the maximum value of the variable
    unit: the unit of the variable
    distribution: the distribution of the variable
    description: the description of the variable (optional)
    
    Returns:
    -------
    var_object: the variable object
    """

    # create the variable
    var = nv.NodeVars()
    var.ipvname = variable_name
    var.setname(var.ipvname, var.opvname)
    
    # var value
    var.setValue(value)
    
    # min and max
    var.setMin(min)
    var.setMax(max)

    # set unit
    var.unit = unit

    # set distribution - this takes a distribution object
    var.dist = distribution

    # set description
    if description is not None: 
        var.desc = description
    else:
        var.desc = ""

    # return the variable object
    return var

# Function to create new output variable
#---------------------------------------
def create_output_variable(variable_name, value, unit, description):
    """
    This function creates a new output variable.

    Parameters:
    ----------
    variable_name: the name of the variable
    value: the value of the variable
    unit: the unit of the variable
    description: the description of the variable (optional)

    Returns:
    -------
    var_object: the variable object
    """
    # create the variable
    var = nv.NodeVars()
    var.opvname = variable_name
    var.setname(var.opvname, var.opvname)

    # set value
    var.setValue(value)

    # set unit
    var.unit = unit
    
    # set description
    if description is not None:
        var.desc = description
    else:
        var.desc = ""
    
    # return the variable object
    return var

# Function to create node inputs/outputs
#---------------------------------------

def add_node_variable(node, var_object, input_or_output, variable_name):
    """
    This function adds an input to a node.

    Parameters:
    node: the node object
    var_object: the variable object
    input_or_output: the type of variable to add, either "input" or "output"
    """

    # check if the node is a valid object
    if not isinstance(node, gr.Node):
        raise TypeError("node must be a valid foqus node object")
    # check if the var_object is a valid object
    if not isinstance(var_object, nv.NodeVars):
        raise TypeError("var_object must be a valid foqus variable object")
    # check if the variable name is already in use
    if var_object.name in node.inputs:
        raise ValueError(f"Variable {var_object.name} is already in use")

    # add the variable to the node
    if input_or_output == "input":
        node.inVarsVector[f"{variable_name}"] = var_object
    elif input_or_output == "output":
        node.outVarsVector[f"{variable_name}"] = var_object
    else:
        raise ValueError(f"Invalid input or output type: {input_or_output}")

    return


# Function to pass values from an output to an input
#---------------------------------------------------
def pass_values (gr, edge_object, output_variable, input_variable):
    """
    This function passes values from an output to an input.

    Parameters:
    ----------
    edge_object: the edge object (from add_edge_to_flowsheet)
    output_variable: the output variable name (string)
    input_variable: the input variable name (string)

    Returns:
    -------
    None
    """
    # check if the edge_object is a valid object
    if not isinstance(edge_object, ed.Edge):
        raise TypeError("edge_object must be a valid foqus edge object")
    # check if the variable names exist
    # output variable
    start_node_name = edge_object.start
    for node in gr.nodes:
        if node.name == start_node_name:
            node_obj = gr.nodes[node]
    if output_variable not in node_obj.outVarsVector.keys():
        raise ValueError(f"Output variable {output_variable} not found in node {start_node_name}")
    # input variable
    end_node_name = edge_object.end
    for node in gr.nodes:
        if node.name == end_node_name:
            node_obj = gr.nodes[node]
    if input_variable not in node_obj.inVarsVector.keys():
        raise ValueError(f"Input variable {input_variable} not found in node {end_node_name}")

    # pass the value from the output to the input
    edge_object.addConnection(output_variable, input_variable)

    return

# Function to define a node script
#---------------------------------

def define_node_script(node_obj, script):
    """
    This function defines a node script.

    Parameters:
    ----------
    node_obj: the node object
    script: the script to define

    Returns:
    -------
    None
    """
    # check if the node is a valid object
    if not isinstance(node_obj, gr.Node):
        raise TypeError("node must be a valid foqus node object")

    # script should be doc string
    if not isinstance(script, str):
        raise TypeError("script must be a valid foqus script object")
    
    try:
        node_obj.pythonCode = script
    except Exception as e:
        raise ValueError(f"Error defining node script: {e}")

    return

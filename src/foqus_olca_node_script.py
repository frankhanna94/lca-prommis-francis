# olca_node_script
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


# TODO: add 'parameter_description' column to params df
# TODO: standardize flows naming in openLCA and parameters_df
# TODO: make sure the code returns the parameter set name before
#       we get to the olca_node script update the parameters in openLCA
# TODO: make sure the same impact category names are used in both:
#       'total_impacts' and the f[] node output variables
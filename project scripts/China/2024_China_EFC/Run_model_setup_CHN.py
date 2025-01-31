import model_setup as ms




# Step 1: Initialize the Config object
# config = ms.ModelConfig('project scripts/China/2024_China_EFC/CHN_2024_EFC.toml')
config = ms.ModelConfig('./config/UKR.toml')

# Step 2: Initialize the SolutionIndexCreator object with the config
# solution_index_creator = ms.SolutionIndexCreator(config)

# # Step 3: Call create_solution_index method
# # You can specify force_update=True if you want to force the regeneration of the solution index
# solution_index = solution_index_creator.create_solution_index(force_update=True)
capacity_setup = ms.CapacitySetup(config)
capacity_setup.setup_capacity()
capacity_setup.setup_plant_parameters()
capacity_setup.export_plant_parameters()


# load_setup = ms.LoadSetup(config)
# load_setup.create_demand_inputs()
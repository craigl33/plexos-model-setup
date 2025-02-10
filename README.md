# PLEXOS Model Setup

A Python package for automating PLEXOS model setup and configuration, providing tools for capacity setup, demand configuration, and solution file index creation.

## Table of Contents
- [PLEXOS Model Setup](#plexos-model-setup)
  - [Description](#description)
  - [Project Structure](#project-structure)
  - [Core Reference Files](#core-reference-files)
  - [TOML Configuration Guide](#toml-configuration-guide)
  - [Installation](#installation)
  - [Features](#features)
  - [Usage](#usage)
  - [Development Status](#development-status)
  - [Contributing](#contributing)

## Description

This package automates PLEXOS model setup with functionality for:
- Capacity setup from DW (functional version complete) or old WEO Excel sheet style (partially integrated)
- Demand setup from combined demand sheet
- Automated solution file index creation

## Project Structure

```
plexos-model-setup/
├── .github/
│   └── workflows/          # CI/CD workflows
├── config/                 # Configuration files
├── functions/             # Core utility functions
├── model_setup/           # Main model setup modules
├── plexos_xml_editing/    # XML editing utilities
├── project scripts/       # Project-specific scripts
├── templates and indices/ # Template files and indices
├── environment.yml        # Dependencies and environment config
├── pyproject.toml         # Project metadata
├── README.md             # This file
└── setup.py              # Package installation script
```

### Key Components

- **model_setup/**: Core implementation modules
  - `capacity_setup.py`: Handles generator capacity configuration
  - `data_processor.py`: Data processing utilities
  - `load_setup.py`: Load and demand configuration
  - `model_config.py`: Configuration management
  - `solution_index.py`: Solution file index creation
  - `utils.py`: General utility functions

## Core Reference Files

### Generator Parameters File

The generator parameters file (either `generator_parameters.xlsx` or `generator_parameters_legacy.xlsx`) is the central reference for all generator technical specifications and operational parameters. This file contains:

1. **Indices Sheet**
   - Maps each generator technology to its operational class
   - Defines fuel types and categorizations
   - Specifies CHP types and CCS configurations
   - Links technologies to cost data sources

2. **Technical Parameters Sheets**
   - Heat rates and efficiencies
   - Minimum stable levels
   - Ramp rates
   - Start costs and times
   - Maintenance requirements (MFOR)
   - Storage parameters

3. **Regional Configuration**
   - Regional splitting factors for capacity allocation
   - Technology-specific regional distributions
   - Hydro and subcritical coal splitting ratios

### All Model Indices

The `all_model_indices.csv` file serves as the master technology mapping system, containing crucial classifications that determine how technologies are treated in the model.

### Template and Index Files

The `templates and indices/` directory contains critical files for model operation:

- **Index Files**:
  - `all model indices.csv`: Master index mapping technologies to their properties
  - `weo_plexos_index.csv`: Maps WEO technologies to PLEXOS
  - `parameters_index.csv`: Defines PLEXOS parameters
  - `legacy indices sheet.csv`: Support for older models

- **Support Files**:
  - `capacity_data_categories_index.csv`: Categories for capacity data
  - `AnnexA_gencapacity.csv`: Capacity adjustment factors
  - `Enduse_mapping_light.xlsx`: End-use sector mappings

## TOML Configuration Guide

[Previous TOML configuration section remains unchanged]

## Installation

1. Clone the repository:
```bash
git clone git clone plexos-model-setup
```

2. Create and activate the conda environment using the provided `environment.yml` file:
```bash
conda env create -f environment.yml
```

This creates a conda environment named `plexos-model-setup` and installs all relevant packages.

If problems occur during installation, try:
1. Updating conda
2. Creating a fresh environment
3. Installing packages individually

### Other repositories required

Note that plexos-model-setup uses functionality developed in other packages (riselib, gis-script) that require the installation of these packages in the plexos-model-setup environment. To do so you, can install in editable mode that allows you to develop the package while using its functionality. 

```bash

pip install -e ../riselib/ --config-settings editable_mode=strict  
pip install -r ../riselib/requirements.txt

```

Note that config settings here allow for Pylance functionality to work with the code in editable mode, enabling autocomplete features in VS Code.

If just installing for use of the package functions, one can install normally from GitLab as follows:

```bash

pip install git+ssh://git@gitlab.iea.org/iea/ems/rise/riselib --no-dependencies
pip install git+ssh://git@gitlab.iea.org/iea/ems/rise/gis-script --no-dependencies


```

## Features

### 1. Capacity Setup
- Support for multiple data sources:
  - Direct Data Warehouse (DW) integration
  - WEO Excel sheet import
  - Manual capacity configuration
- Regional capacity splitting
- Generator parameter configuration

### 2. Demand Setup
- Load profile configuration from combined demand sheets
- Demand response modeling
- Regional demand splitting
- Time pattern generation

### 3. Solution Index Creation
- Automated index generation for:
  - Generators
  - Demand response
  - Nodes and regions
  - Transmission lines
  - Emissions
  - Reserves
  - Variables
  - Fuels and contracts

## Usage

Basic usage example:

```python
import model_setup as ms

# Initialize configuration
config = ms.ModelConfig('config/your_config.toml')

# Setup capacity
capacity_setup = ms.CapacitySetup(config)
capacity_setup.setup_capacity()
capacity_setup.setup_plant_parameters()
capacity_setup.export_plant_parameters()

# Create solution index
solution_index = ms.SolutionIndexCreator(config)
solution_index.create_solution_index()
```

## Development Status

Current development priorities:

- ✅ DW capacity setup integration
- 🏗️ WEO Excel sheet integration (partially complete)
- 🏗️ Demand setup testing and validation
- 🏗️ Solution index creation refinement
- 📝 Custom column support for solution index
- 📝 Documentation improvements

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Acknowledgments

This project is developed and maintained by the IEA modeling team.
# PLEXOS Model Setup

A Python package for automating PLEXOS model setup and configuration, providing tools for capacity setup, demand configuration, and solution file index creation.

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

### Template and Index Files

The `templates and indices/` directory contains critical files for model operation:

- **generator_parameters.xlsx** / **generator_parameters_legacy.xlsx**
  - Contains technical parameters for different generator types
  - Includes sheets for:
    - Generator indices and classifications
    - Regional splitting factors
    - Technology splitting ratios
    - Heat rates and efficiencies
    - Operating constraints
    - Cost parameters

- **Index Files**:
  - `all model indices.csv`: Master index mapping technologies to their properties (75 rows, 39 columns)
    - Maps technologies to fuel types, operating classes, and categories
    - Contains inertia parameters, storage durations, and flexibility categories
    - Defines CHP types and CCS configurations
  
  - `weo_plexos_index.csv`: Maps WEO technologies to PLEXOS (50 rows, 17 columns)
    - Links WEO data sources to PLEXOS technologies
    - Specifies data processing methods and classifications
    - Contains product and flow mappings

  - `parameters_index.csv`: Defines PLEXOS parameters (43 rows, 8 columns)
    - Maps parameter labels to PLEXOS names
    - Specifies default values and filename conventions
    - Identifies capacity-relative parameters

  - `legacy indices sheet.csv`: Support for older models (164 rows, 32 columns)
    - Maps legacy WEO technologies
    - Contains historical categorizations and mappings

- **Support Files**:
  - `capacity_data_categories_index.csv`: Categories for capacity data
  - `AnnexA_gencapacity.csv`: Capacity adjustment factors
  - `Enduse_mapping_light.xlsx`: End-use sector mappings

### Key Components

- **model_setup/**: Core implementation modules
  - `capacity_setup.py`: Handles generator capacity configuration
  - `data_processor.py`: Data processing utilities
  - `load_setup.py`: Load and demand configuration
  - `model_config.py`: Configuration management
  - `solution_index.py`: Solution file index creation
  - `utils.py`: General utility functions

- **functions/**: Support utilities
  - `convert_plexos.py`: PLEXOS conversion utilities
  - `read_weo.py`: World Energy Outlook data processing
  - `xml_exploring.py`: XML manipulation tools

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

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/plexos-model-setup.git
```

2. Create and activate the conda environment:
```bash
conda env create -f environment.yml
conda activate plexos-model-setup
```

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

The `all_model_indices.csv` file serves as the master technology mapping system, containing crucial classifications that determine how technologies are treated in the model:

1. **Technology Identification**
   - PLEXOS technology names
   - WEO technology mappings
   - Operating classes
   - Fuel type assignments

2. **Technical Classifications**
   - VRE (Variable Renewable Energy) categorization
   - Storage duration parameters
   - Inertia contributions (HIGH/LOW)
   - CHP and CCS type specifications

3. **Operational Categories**
   - Flexibility categories
   - Capacity categories
   - Stack categories for reporting
   - Quick start capabilities

This classification system ensures consistent treatment of technologies across different model components and supports proper parameter assignment and regional splitting.

## TOML Configuration Guide

The package uses TOML configuration files (located in `config/`) for model setup. Below is a detailed breakdown of the key configuration sections:

### Path Configuration
```toml
[path]
project_folder = "path/to/project/"
data_folder = "path/to/data/"
generation_folder = "path/to/generation/"
load_folder = "path/to/load/"
plants_list_path = "path/to/generator_capacity.xlsx"
generator_parameters_path = "path/to/generator_parameters.xlsx"
model_xml_path = "path/to/model.xml"
```

### Parameters
```toml
[parameters]
model_region = "Region_Name"
regions = ["R1", "R2", "R3"]  # List of model regions
model_type = "Manual"  # Can be "WEO" or "Manual"
Annex_A_adjust = "False"
has_aluminium = "False"
```

### Manual Setup Configuration
```toml
[manual_setup]
plants_list_header = 0
plants_list_sheet = "AllGenerators"
capacity_columns = "Q:Y"  # Excel column range for capacity data
name_column = "PLEXOS_Name"
tech_column = "WEO_Tech"
reg_column = "PLEXOS_GRP1"
classification_column = "Category1"
legacy_indices_flag = true
```

### Portfolio Assignments
```toml
[portfolio_assignments]
P1 = { name = "Validation", year = "2021", setup_method = "manual",
       publication = 'None', scenario_code = "2021", 
       capacity_sheet = "AllGenerators", cap_col = "Q", 
       load_scaling = 0}
# Additional portfolios P2-Pn can be defined similarly
```

### Output Configuration
```toml
[outputs]
output_structure = "suffix"  # or "folders"
path_gen_outputs = "path/to/outputs/parameters/"
path_load_outputs = "path/to/outputs/demand/"
overwrite_flag = false
fill_na_flag = false
```

### Solution Index Sources
```toml
[solution_index_source]
transmission = "xml export"
generation = "xml export"
demand_response = "xml export"
regions_setup = "xml export"
fuel = "xml export"
emission = "xml export"
reserve = "xml export"
variable = "xml export"
fuel_contracts = "xml export"
```

### Key Configuration Notes:

1. **Setup Methods**:
   - `manual`: For detailed plant-level models
   - `data warehouse`: For WEO data-based models
   - `weo_excel`: For legacy WEO Excel-based models
   - `load only`: For demand-side only configurations

2. **Output Structures**:
   - `suffix`: Uses file suffixes for different portfolios
   - `folders`: Creates separate folders for each portfolio

3. **Index Flags**:
   - `legacy_indices_flag`: For compatibility with older model structures
   - `regional_split_flag`: For WEO-style models with manual input data

4. **Portfolio Configuration**:
   - Each portfolio (P1, P2, etc.) can have different years, methods, and scenarios
   - Capacity columns can be specified either by Excel column letters or names

## Configuration

The package uses these TOML configuration files (located in `config/`) to specify:
- File paths and locations
- Portfolio assignments
- Model parameters
- Output settings
- Regional configurations

Example configuration files are provided for reference (e.g., `CHN_2024_EFC.toml`, `UKR.toml`).

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

## License

[Your license information here]

## Acknowledgments

This project is developed and maintained by the IEA modeling team.
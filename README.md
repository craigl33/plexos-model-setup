# plexos-model-setup
A package to setup the PLEXOS input parameters based on input data
-capacity setup from DW (functional version complete) or old WEO Excel sheet style (not fully integrated but partially set up)
-demand setup from combined demand sheet. TODO: only the initial version and testing that it runs has been done. Outputs need to be
tested with PLEXOS
-automated solution file index creation: TODO: only the initial version of this was made, it still needs to be simplified and
harmonised with the solution index processing. Recommend to set up the ability to add custom columns using a separate file
and keep the main default index relatively simple, to mainly just the columns needed by all the models

## Description


## Setup
Just clone the repository to create a local copy:

    git clone plexos-model-setup

TODO: the yml has not been updated since the latest code development and probably should be remade
To install the dependencies, it is recommended to use a virtual environment. Both can be done automatically with the `environment.yml` file:

    conda env create -f environment.yml

This creates a conda environment named `plexos-model-setup` and installs all relevant packages.

If problems occur, see the Troubleshooting section in the [Documentation](docs/Documentation.md).

## Usage
the input files and TOML file need to be set up first. The China model one can be used as a template if a dedicated one is not present
the TOML contains some guidance on the file requirements but this could be improved

"""Intialise and load all assets from the data_engineering package"""

import pkgutil
import warnings
from importlib import import_module

from dagster import Definitions, ExperimentalWarning, load_assets_from_package_module

# Define the root package
PROJECT_FOLDER = "data_pipeline"
ASSET_FOLDER = "assets"

# Ignore experimental warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)

# Load all modules from the root package
modules = list(pkgutil.iter_modules([f"{PROJECT_FOLDER}/{ASSET_FOLDER}"]))

# Initialize an empty list to store all assets
all_assets = []

# Iterate over each module
for module_info in modules:
    # Import the module
    module = import_module(f"{PROJECT_FOLDER}.{ASSET_FOLDER}.{module_info.name}")

    # Load assets from the module
    assets = load_assets_from_package_module(
        package_module=module,
        group_name=module_info.name,
    )

    # Add the assets to the list of all assets
    all_assets.extend(assets)

# Create a Definitions object with all assets
defs = Definitions(all_assets)

# This file is the actual code for the Python runnable export-to-stages
import os

from dataiku import SQLExecutor2, api_client
from dataiku.runnables import Runnable


def success(message):
    return f"<div class=\"alert alert-success\">{message}</div>"


def error(message):
    return f"<div class=\"alert alert-error\">{message}</div>"


def resolve_table_name(dataset_params):
    catalog_name = dataset_params['catalog']
    schema_name = dataset_params['schema']
    table_name = dataset_params['table']
    if catalog_name and schema_name:
        resolved_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    elif not catalog_name and schema_name:
        resolved_table_name = f"{schema_name}.{table_name}"
    elif catalog_name and not schema_name:
        resolved_table_name = f"{catalog_name}..{table_name}"
    else:
        resolved_table_name = table_name
    return resolved_table_name


class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        os.environ['DKU_CURRENT_PROJECT_KEY'] = project_key
        self.config = config
        self.plugin_config = plugin_config

    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        dataset_name = f"{self.config['input_dataset']}"

        project = api_client().get_project(self.project_key)
        dss_dataset = project.get_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return error(f"'{dss_dataset.name}' is not a Snowflake dataset")

        mandatory_params = [{"name": "Snowflake stage", "id": "stage"}]

        for param in mandatory_params:
            if param['id'] not in self.config or not self.config[param['id']]:
                return error(f"The parameter '{param['name']}' is not specified")

        fully_qualified_stage_name = self.config['stage']
        output_path = f"{self.project_key}/{dataset_name}"
        dataset_params = dss_dataset.get_settings().get_raw_params()

        executor = SQLExecutor2(dataset=dataset_name)
        executor.query_to_df(f"COPY INTO @{fully_qualified_stage_name}/{output_path}/ FROM {resolve_table_name(dataset_params)} OVERWRITE = TRUE")
        return success(f"The {dataset_name} dataset has been successfully exported to the {fully_qualified_stage_name} stage")

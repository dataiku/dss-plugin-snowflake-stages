# This file is the actual code for the Python runnable export-to-stages
import os

from dataiku import SQLExecutor2, Dataset
from dataiku.runnables import Runnable


def success(message):
    return f"<div class=\"alert alert-success\">{message}</div>"


def error(message):
    return f"<div class=\"alert alert-error\">{message}</div>"


def resolve_table_name(dataset_connection_info):
    catalog_name = dataset_connection_info.get('catalog')
    schema_name = dataset_connection_info.get('schema')
    table_name = dataset_connection_info.get('table')
    if catalog_name and schema_name:
        resolved_table_name = f"\"{catalog_name}\".\"{schema_name}\".\"{table_name}\""
    elif not catalog_name and schema_name:
        resolved_table_name = f"\"{schema_name}\".\"{table_name}\""
    elif catalog_name and not schema_name:
        resolved_table_name = f"\"{catalog_name}\"..\"{table_name}\""
    else:
        resolved_table_name = f"\"{table_name}\""
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

        dataset_connection_info = Dataset(dataset_name).get_location_info()["info"]

        if dataset_connection_info.get("databaseType") != 'Snowflake':
            return error(f"'{dataset_name}' is not a Snowflake dataset")

        mandatory_params = [{"name": "Snowflake stage", "id": "stage"}]

        for param in mandatory_params:
            if param['id'] not in self.config or not self.config[param['id']]:
                return error(f"The parameter '{param['name']}' is not specified")

        fully_qualified_stage_name = self.config['stage']
        output_path = f"{self.project_key}/{dataset_name}"

        file_format_param = self.config['file_format']
        file_format = f"FILE_FORMAT = (FORMAT_NAME = {file_format_param})" if file_format_param and file_format_param != 'default' else ''
        overwrite = 'OVERWRITE = TRUE' if self.config["overwrite"] else ''
        sql_copy_query = f"COPY INTO @{fully_qualified_stage_name}/{output_path}/ FROM {resolve_table_name(dataset_connection_info)} {file_format} {overwrite}"

        executor = SQLExecutor2(dataset=dataset_name)
        executor.query_to_df(sql_copy_query)
        return success(f"The <b>{dataset_name}</b> dataset has been successfully exported to the <b>{fully_qualified_stage_name}</b> stage in the <b>{output_path}</b> path.")

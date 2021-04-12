# This file is the actual code for the Python runnable export-to-stages
from dataiku.runnables import Runnable
import os
from dataiku import SQLExecutor2, Dataset, api_client
from dataikuapi.dss.dataset import DSSDataset

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

        # Public API
        project = api_client().get_project(self.project_key)
        dss_dataset = project.get_dataset(dataset_name)
        dataset_params = dss_dataset.get_settings().get_raw_params()
        connection_definition = api_client().get_connection(dataset_params['connection']).get_definition()

        catalog_name = dataset_params['catalog'] or connection_definition['params']['db']
        schema_name = dataset_params['schema'] or connection_definition['params']['defaultSchema']
        table_name = dataset_params['table']
        # fully_qualified_table_name = '.'.join(filter(None, [catalog_name,  schema_name, table_name]))
        fully_qualified_table_name = f"\"{catalog_name}\".\"{schema_name}\".\"{table_name}\""

        stage_name = self.config['stage']  # what if no catalog or schema in the connection when we fetch stages?
        fully_qualified_stage_name = f"\"{catalog_name}\".\"{schema_name}\".\"{stage_name}\""

        output_path = f"{self.project_key}/{dataset_name}"

        executor = SQLExecutor2(dataset=dataset_name)
        iter = executor.query_to_df(f"COPY INTO @{fully_qualified_stage_name}/{output_path}/ FROM {fully_qualified_table_name} OVERWRITE = TRUE")
        return f"{iter}"




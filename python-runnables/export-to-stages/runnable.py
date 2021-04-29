# This file is the actual code for the Python runnable export-to-stages
import logging
from dataiku import SQLExecutor2, Dataset, set_default_project_key
from dataiku.runnables import Runnable


class ExportToStageRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        set_default_project_key(self.project_key)

    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        If `run` is successful, we use the method success() to return an HTML message.
        In case of an error, we don't return the error in such an HTML message but we raise an Error instead
        so it is considered as a failed step if called from a scenario.
        """
        if 'input_dataset' in self.config:
            # the macro is run from the flow
            dataset_name = self.config.get('input_dataset')
        else:
            # the macro is run from a scenario
            dataset_name = self.config.get('dataset')

        if not dataset_name:
            logging.error('The mandatory param `dataset` is missing or invalid to export dataset to Snowflake stage')
            raise ValueError(f"The mandatory parameter `Dataset to export` is invalid")

        # We use the API `dataiku.core.dataset.Dataset.get_location_info` rather than `dataikuapi.dss.dataset.DSSDataset.get_settings().get_raw_params()`
        # because it expands variables if any in the connection settings (see https://doc.dataiku.com/dss/latest/variables/index.html)
        dataset_connection_info = Dataset(dataset_name).get_location_info()["info"]

        if dataset_connection_info.get("databaseType") != 'Snowflake':
            logging.error('Cannot export non Snowflake dataset `%s.%s` to Snowflake stage', self.project_key, dataset_name)
            raise ValueError(f"'{dataset_name}' is not a Snowflake dataset")

        mandatory_params = [{"name": "Snowflake stage", "id": "stage"}]

        for param in mandatory_params:
            if param['id'] not in self.config or not self.config.get(param['id']):
                logging.error('The mandatory param `%s` is missing or invalid to export dataset `%s.%s` to Snowflake stage',
                              param['name'], self.project_key, dataset_name)
                raise ValueError(f"The parameter '{param['name']}' is invalid")

        fully_qualified_stage_name = self.config.get('stage')

        output_path = (self.config.get('path') or self.project_key).strip(' ').strip('/')
        destination = f"{output_path}/{dataset_name}" if output_path else dataset_name

        file_format_param = self.config.get('file_format') or 'default'
        file_format = '' if file_format_param == 'default' else f"FILE_FORMAT = (FORMAT_NAME = {file_format_param})"

        overwrite = 'OVERWRITE = TRUE' if self.config.get("overwrite") else ''

        sql_copy_query = f"COPY INTO @{fully_qualified_stage_name}/{destination} FROM {resolve_table_name(dataset_connection_info)} {file_format} {overwrite}"

        logging.info("Exporting dataset `%s.%s` with the copy command: `%s`", self.project_key, dataset_name, sql_copy_query)

        executor = SQLExecutor2(dataset=dataset_name)
        executor.query_to_df(sql_copy_query)

        logging.info("Successfully exported dataset `%s.%s` in Snowflake stage `%s` to `%s`",
                     self.project_key, dataset_name, fully_qualified_stage_name, destination)

        return success('The dataset has been successfully exported in stage <strong>%s</strong> to <strong>%s_*</strong>'
                       % (fully_qualified_stage_name.replace('"', ''), destination))


def success(message):
    return f"<div class=\"alert alert-success\">{message}</div>"


def resolve_table_name(dataset_connection_info):
    """
    In Snowflake, the namespace (database and schema) is inferred from the current database (or catalog in DSS terms) and schema in use for the session.
    We only qualify the table name with the Dataset catalog and schema if explicitly defined. Otherwise, Snowflake will qualify it itself from the session
    namespace. For more info, see https://docs.snowflake.com/en/sql-reference/name-resolution.html
    """
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

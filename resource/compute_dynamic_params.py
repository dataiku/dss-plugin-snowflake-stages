import logging
from dataiku import SQLExecutor2, api_client, default_project_key


def do(payload, config, plugin_config, inputs):
    """
    Currently we can't dynamically recalculate the choices of a param B depending on the value of a param A (see https://app.clubhouse.io/dataiku/story/53713)
    This is why we have this tradeoff for macros created from a scenario: list all the Snowflake objects, grouped by connection,
    rather than only the ones from the connection of the selected dataset.
    Macros directly created from a dataset currently don't let the user change the input dataset, and list the Snowflake objects from the connection of this
    dataset.
    """

    # if the macro is run from the flow, the input_dataset param is set...
    dataset_name = config.get('input_dataset')

    # ...thus we know whether we should display the macro in "flow" or "scenario" style
    if dataset_name:
        return macro_from_dataset_params(payload['parameterName'], dataset_name)
    else:
        return macro_from_scenario_params(payload['parameterName'])


def macro_from_scenario_params(parameter_name):
    """
    Params of type `SELECT` don't let us group the choices by connection. This is why we use this hack of displaying the connections as selectable
    choices and indenting to the right the valid choices. If the user was to select a connection rather than a valid choice, he would then get an error.
    """

    # We start by fetching all snowflake datasets in the current project and group them per connection
    datasets_per_connection = dict()
    for dataset in get_snowflake_datasets():
        connection = dataset['params']['connection']
        if connection not in datasets_per_connection:
            datasets_per_connection[connection] = []
        datasets_per_connection[connection] += [dataset.name]

    # Only if there are multiple Snowflake connections, we need to display the different inputs grouped by connections
    multiple_connections = len(datasets_per_connection) > 1

    # indent only if we have multiple Snowflake connections
    def do_indentation(choice):
        return {'value': choice['value'], 'label': f"  {choice['label']}"}
    indent = do_indentation if multiple_connections else (lambda choice: choice)

    if parameter_name == 'dataset':
        choices = []
        for connection in datasets_per_connection:
            if multiple_connections:
                choices += [connection_choice(connection)]
            choices += [indent(dataset_choice(dataset)) for dataset in datasets_per_connection[connection]]
        return {'choices': choices}

    if parameter_name == 'stage':
        choices = []
        for connection in datasets_per_connection:
            if multiple_connections:
                choices += [connection_choice(connection)]
            try:
                choices += [indent(stage_choice(row)) for row in get_stages(connection=connection)]
            except Exception as e:
                logging.exception('Error while fetching Snowflake stages on connection `%s`', connection, exc_info=e)
                choices += [indent(failed_connection("stages"))]
        return {"choices": choices}

    if parameter_name == 'file_format':
        choices = [indent(default_format_choice)]
        for connection in datasets_per_connection:
            if multiple_connections:
                choices += [connection_choice(connection)]
            try:
                choices += [indent(file_format_choice(row)) for row in get_file_formats(connection=connection)]
            except Exception as e:
                logging.exception('Error while fetching Snowflake file formats on connection `%s`', connection, exc_info=e)
                choices += [indent(failed_connection("file formats"))]
        return {"choices": choices}


def macro_from_dataset_params(parameter_name, dataset_name):
    if parameter_name == 'dataset':  # this param is only used to display the dataset name to the user in the macro modal
        return {"choices": [{"value": "default", "label": dataset_name}]}

    if parameter_name == 'stage':
        if not is_dataset_valid(dataset_name):
            return {"choices": [invalid_dataset_choice]}

        try:
            choices = [stage_choice(row) for row in get_stages(dataset=dataset_name)]
        except Exception as e:
            logging.exception('Error while fetching Snowflake stages for dataset `%s`', dataset_name, exc_info=e)
            choices = [failed_connection("stages")]
        return {"choices": choices}

    if parameter_name == 'file_format':
        if not is_dataset_valid(dataset_name):
            return {"choices": [invalid_dataset_choice]}

        try:
            choices = [default_format_choice] + \
                      [file_format_choice(row) for row in get_file_formats(dataset=dataset_name)]
        except Exception as e:
            logging.exception('Error while fetching Snowflake file formats for dataset `%s`', dataset_name, exc_info=e)
            choices = [failed_connection("file formats")]
        return {"choices": choices}


def get_snowflake_datasets():
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    return [dataset for dataset in project.list_datasets() if dataset.type == 'Snowflake']


def is_dataset_valid(dataset_name):
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    dss_dataset = project.get_dataset(dataset_name)
    return dss_dataset.get_settings().type == 'Snowflake'


def get_stages(**kwargs):
    query = "SHOW STAGES"
    logging.info("Fetching Snowflake stages with %s: `%s`", kwargs, query)
    return SQLExecutor2(**kwargs).query_to_iter(query).iter_tuples()


def get_file_formats(**kwargs):
    query = "SHOW FILE FORMATS IN ACCOUNT"
    logging.info("Fetching Snowflake file formats with %s: `%s`", kwargs, query)
    return SQLExecutor2(**kwargs).query_to_iter(query).iter_tuples()


def connection_choice(connection):
    return {
        "value": None,
        "label": f"From connection {connection}:"
    }


def failed_connection(what):
    return {
        "value": None,
        "label": f"⚠️ Failed getting {what}"
    }


# Given that we fully qualify all the file formats, our "default" option won't override an actual file format
default_format_choice = {
    "value": "default",
    "label": "DEFAULT"
}

invalid_dataset_choice = {
    "value": None,
    "label": "⚠️ Invalid input dataset"
}


def dataset_choice(dataset_name):
    return {
        "value": dataset_name,
        "label": dataset_name
    }


def stage_choice(row):
    catalog = row[2]
    schema = row[3]
    name = row[1]
    comment = row[8]
    return {
        "value": f"\"{catalog}\".\"{schema}\".\"{name}\"",
        "label": f"{catalog}.{schema}.{name} {'(' + comment + ')' if comment else ''}"
    }


def file_format_choice(row):
    catalog = row[2]
    schema = row[3]
    name = row[1]
    comment = row[6]
    return {
        "value": f"\"{catalog}\".\"{schema}\".\"{name}\"",
        "label": f"{catalog}.{schema}.{name} {'(' + comment + ')' if comment else ''}"
    }

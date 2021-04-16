from dataiku import SQLExecutor2, api_client, default_project_key


def do(payload, config, plugin_config, inputs):
    # if the macro is run from the flow, the input_dataset param is set...
    dataset_name = config.get('input_dataset')

    # ...thus we know whether we should display the macro in "flow" or "scenario" style
    if dataset_name:
        return macro_from_dataset_params(payload['parameterName'], dataset_name)
    else:
        return macro_from_scenario_params(payload['parameterName'])


def macro_from_scenario_params(parameter_name):
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
    indent = lambda choice: {'value': choice['value'], 'label': f"⠀⠀{choice['label']}"} if multiple_connections else lambda choice: choice

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
            choices += [indent(snowflake_object_choice(row)) for row in get_stages(connection=connection)]
        return {"choices": choices}

    if parameter_name == 'file_format':
        choices = [indent(default_format_choice)]
        for connection in datasets_per_connection:
            if multiple_connections:
                choices += [connection_choice(connection)]
            choices += [indent(snowflake_object_choice(row)) for row in get_file_formats(connection=connection)]
        return {"choices": choices}


def macro_from_dataset_params(parameter_name, dataset_name):
    if parameter_name == 'dataset':  # this param is only used to display the dataset name to the user in the macro modal
        return {"choices": [{"value": "default", "label": dataset_name}]}

    if parameter_name == 'stage':
        if not is_dataset_valid(dataset_name):
            return {"choices": [invalid_dataset_choice]}

        choices = [snowflake_object_choice(row) for row in get_stages(dataset=dataset_name)]
        return {"choices": choices}

    if parameter_name == 'file_format':
        if not is_dataset_valid(dataset_name):
            return {"choices": [invalid_dataset_choice]}

        choices = [default_format_choice] + \
                  [snowflake_object_choice(row) for row in get_file_formats(dataset=dataset_name)]
        return {"choices": choices}


def get_snowflake_datasets():
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    return filter(lambda dataset: dataset.type == 'Snowflake', project.list_datasets())


def is_dataset_valid(dataset_name):
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    dss_dataset = project.get_dataset(dataset_name)
    return dss_dataset.get_settings().type == 'Snowflake'


def get_stages(**kwargs):
    return SQLExecutor2(**kwargs).query_to_iter("SHOW STAGES").iter_tuples()


def get_file_formats(**kwargs):
    return SQLExecutor2(**kwargs).query_to_iter("SHOW FILE FORMATS IN ACCOUNT").iter_tuples()


def connection_choice(connection):
    return {
        "value": None,
        "label": f"From connection {connection}:"
    }


# Given that file formats are fully qualified, our "default" option won't override an actual file format
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


def snowflake_object_choice(row):
    catalog = row[2]
    schema = row[3]
    name = row[1]
    return {
        "value": f"\"{catalog}\".\"{schema}\".\"{name}\"",
        "label": f"{catalog}.{schema}.{name}"
    }

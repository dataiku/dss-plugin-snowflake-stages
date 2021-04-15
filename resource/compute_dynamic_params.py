from dataiku import SQLExecutor2, api_client, default_project_key


def qualify_name(row, quoted=True):
    catalog_column_index = 2
    schema_column_index = 3
    object_column_index = 1
    if quoted:
        return f"\"{row[catalog_column_index]}\".\"{row[schema_column_index]}\".\"{row[object_column_index]}\""
    else:
        return f"{row[catalog_column_index]}.{row[schema_column_index]}.{row[object_column_index]}"


def file_format_label(row):
    qualified_file_format_name = f"{qualify_name(row, False)}"
    comment = f"({row[6]})" if row[6] else ''
    return f"{qualified_file_format_name} {comment}"


def get_dss_dataset(dataset_name):
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    dss_dataset = project.get_dataset(dataset_name)
    return dss_dataset


def do(payload, config, plugin_config, inputs):
    dataset_name = config.get('input_dataset')

    if dataset_name:
        return macro_from_dataset_params(payload['parameterName'], dataset_name)
    else:
        return macro_from_scenario_params(payload['parameterName'])


def macro_from_scenario_params(parameter_name):
    project_key = default_project_key()
    project = api_client().get_project(project_key)
    snowflake_datasets = filter(lambda dataset: dataset.type == 'Snowflake', project.list_datasets())

    dataset_per_connection = dict()
    for dataset in snowflake_datasets:
        connection = dataset['params']['connection']
        if connection not in dataset_per_connection:
            dataset_per_connection[connection] = []
        dataset_per_connection[connection] += [dataset.name]

    multiple_connections = len(dataset_per_connection) > 1

    prefix = '⠀⠀' if multiple_connections else ''

    if parameter_name == 'dataset':
        choices = []
        for connection in dataset_per_connection:
            if multiple_connections:
                choices += [{"value": "", "label": f"In connection {connection}:"}]
            choices += [{"value": dataset, "label": f"{prefix}{dataset}"} for dataset in dataset_per_connection[connection]]

        return {'choices': choices}

    if parameter_name == 'stage':
        choices = []
        for connection in dataset_per_connection:
            executor = SQLExecutor2(connection=connection)
            stage_iter = executor.query_to_iter("SHOW STAGES")
            if multiple_connections:
                choices += [{"value": "", "label": f"In connection {connection}:"}]
            choices += [{"value": qualify_name(stage_tuple),
                        "label": f"{prefix}{qualify_name(stage_tuple, False)}"} for stage_tuple in stage_iter.iter_tuples()]
        return {"choices": choices}

    if parameter_name == 'file_format':
        # Given that file formats are fully qualified, our "default" option won't override an actual file format
        choices = [{"value": "default", "label": f"{prefix}DEFAULT"}]
        for connection in dataset_per_connection:
            executor = SQLExecutor2(connection=connection)
            file_format_iter = executor.query_to_iter("SHOW FILE FORMATS IN ACCOUNT")
            if multiple_connections:
                choices += [{"value": "", "label": f"In connection {connection}:"}]
            choices += [{"value": qualify_name(file_format_tuple),
                        "label": f"{prefix}{file_format_label(file_format_tuple)}"} for file_format_tuple in file_format_iter.iter_tuples()]
        return {"choices": choices}

    return {}


def macro_from_dataset_params(parameter_name, dataset_name):
    if parameter_name == 'dataset':  # this param is only used to display the dataset name to the user in the macro modal
        return {"choices": [{"value": "default", "label": dataset_name}]}

    if parameter_name == 'stage':
        dss_dataset = get_dss_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        executor = SQLExecutor2(dataset=dataset_name)
        stage_iter = executor.query_to_iter("SHOW STAGES")
        choices = [{"value": qualify_name(stage_tuple),
                    "label": qualify_name(stage_tuple, False)} for stage_tuple in stage_iter.iter_tuples()]
        return {"choices": choices}

    if parameter_name == 'file_format':
        dss_dataset = get_dss_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        executor = SQLExecutor2(dataset=dataset_name)
        file_format_iter = executor.query_to_iter("SHOW FILE FORMATS IN ACCOUNT")
        # Given that file formats are fully qualified, our "default" option won't override an actual file format
        choices = [{"value": "default", "label": "DEFAULT"}] + \
                  [{"value": qualify_name(file_format_tuple),
                    "label": file_format_label(file_format_tuple)} for file_format_tuple in file_format_iter.iter_tuples()]
        return {"choices": choices}
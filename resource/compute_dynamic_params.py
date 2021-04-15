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
    dataset_name = config['input_dataset']
    if payload['parameterName'] == 'dataset':  # this param is only used to display the dataset name to the user in the macro modal
        return {"choices": [{"value": "default", "label": dataset_name}]}
    if payload['parameterName'] == 'stage':
        dss_dataset = get_dss_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        executor = SQLExecutor2(dataset=dataset_name)
        stage_iter = executor.query_to_iter("SHOW STAGES")
        choices = [{"value": qualify_name(stage_tuple),
                    "label": qualify_name(stage_tuple, False)} for stage_tuple in stage_iter.iter_tuples()]
        return {"choices": choices}
    if payload['parameterName'] == 'file_format':
        dss_dataset = get_dss_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        executor = SQLExecutor2(dataset=dataset_name)
        file_format_iter = executor.query_to_iter("SHOW FILE FORMATS IN ACCOUNT")
        # Given that file formats are fully qualified, our "default" option won't override an actual file format
        choices = [{"value": "default", "label": "default"}] + \
                  [{"value": qualify_name(file_format_tuple),
                    "label": file_format_label(file_format_tuple)} for file_format_tuple in file_format_iter.iter_tuples()]
        return {"choices": choices}

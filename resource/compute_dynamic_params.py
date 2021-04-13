from dataiku import SQLExecutor2, api_client, default_project_key


def qualify_stage_name(stage_tuple):
    catalog_column_index = 2
    schema_column_index = 3
    stage_column_index = 1
    return f"{stage_tuple[catalog_column_index]}.{stage_tuple[schema_column_index]}.{stage_tuple[stage_column_index]}"


def do(payload, config, plugin_config, inputs):
    if payload['parameterName'] == 'stage':
        project_key = default_project_key()
        dataset_name = f"{config['input_dataset']}"

        project = api_client().get_project(project_key)
        dss_dataset = project.get_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        executor = SQLExecutor2(dataset=dataset_name)
        stage_iter = executor.query_to_iter("SHOW STAGES")
        choices = [{"value": qualify_stage_name(stage_tuple), "label": qualify_stage_name(stage_tuple)} for stage_tuple in stage_iter.iter_tuples()]
        return {"choices": choices}

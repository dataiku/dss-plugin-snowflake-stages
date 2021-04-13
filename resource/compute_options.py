from dataiku import SQLExecutor2, Dataset, api_client, default_project_key

def do(payload, config, plugin_config, inputs):
    if payload['parameterName'] == 'stage':
        project_key = default_project_key()
        dataset_name = f"{config['input_dataset']}"

        project = api_client().get_project(project_key)
        dss_dataset = project.get_dataset(dataset_name)

        if dss_dataset.get_settings().type != 'Snowflake':
            return {"choices": [{"value": None, "label": "⚠️ Invalid input dataset"}]}

        dataset_params = dss_dataset.get_settings().get_raw_params()

        connection_definition = api_client().get_connection(dataset_params['connection']).get_definition()
        catalog_name = dataset_params['catalog'] or connection_definition['params']['db']
        schema_name = dataset_params['schema'] or connection_definition['params']['defaultSchema']

        executor = SQLExecutor2(dataset=dataset_name)
        stage_iter = executor.query_to_iter("SHOW STAGES")
        choices = [{"value": f"{catalog_name}.{schema_name}.{stage[1]}", "label": f"{catalog_name}.{schema_name}.{stage[1]}"} for stage in stage_iter.iter_tuples()]
        return {"choices": choices}
    else:
        return []  # TODO user should get an error if reached

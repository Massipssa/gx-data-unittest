import great_expectations as gx

from ruamel import yaml
from great_expectations.core.batch import RuntimeBatchRequest

if __name__ == '__main__':
    base_path = "../data"
    datasource_name = "my_filesystem_datasource"
    # data_asset: is collection of records within datasource
    data_asset_name = "my_data_asset"

    context = gx.get_context()
    datasource_config = {
        "name": datasource_name,
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"},
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": base_path,
                "default_regex": {
                    "group_names": [data_asset_name],
                    "pattern": "(.*)\\.csv",
                },
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))

    # add datasource to the context
    context.add_datasource(**datasource_config)
    """
    Batch: is unique subset of Data Asset
    Batch Request
        - Specifies one or more Batches within the Data Asset
        - They are the primary way of retrieving data for use in GX
    """

    # Here is a RuntimeBatchRequest using a path to a directory
    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"path": base_path},
        batch_identifiers={"default_identifier_name": 1234567890},
        batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": True}},
    )

    # Expectations suite
    suite_name = "test_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    print(validator.head())
    print(validator.active_batch.data.dataframe.count())

    # Checkpoint
    checkpoint = context.add_or_update_checkpoint(
        name="test_checkpoint",
        validator=validator
    )
    checkpoint_result = checkpoint.run()
    print(checkpoint_result)

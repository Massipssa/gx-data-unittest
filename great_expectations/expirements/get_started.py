import great_expectations as gx

if __name__ == '__main__':
    # Create context
    context = gx.get_context()

    # Connect to data
    validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )

    # Create expectations
    validator.expect_column_values_to_not_be_null("pickup_datetime")
    validator.expect_column_values_to_be_between("passenger_count", auto=True)
    validator.save_expectation_suite()

    # Validate data
    checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
    )
    checkpoint_result = checkpoint.run()
    # View result in HTML
    context.view_validation_result(checkpoint_result)
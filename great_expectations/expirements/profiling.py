import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.data_asset import DataAsset

if __name__ == '__main__':

    data_folder = "../data"
    context = gx.get_context()
    context.sources.add_pandas_filesystem(
        "taxi_multi_batch_datasource",
        base_directory=data_folder,
    ).add_csv_asset(
        "all_years",
        batching_regex="files.csv" #r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )

    all_years_asset: DataAsset = context.datasources[
        "taxi_multi_batch_datasource"
    ].get_asset("all_years")

    multi_batch_all_years_batch_request: BatchRequest = (
        all_years_asset.build_batch_request()
    )

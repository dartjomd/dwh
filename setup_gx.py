import datetime

import great_expectations as gx
from airflow.hooks.base import BaseHook


class GreatExpectationSetup:
    def __init__(self, conn_id: str):
        self._conn_id = conn_id

        # connect to DB once
        self._datasource, self._context = self._connect_to_db()

    def _get_db_connection_uri(self) -> str:
        """Return DB connection string using Airflow hooks"""

        conn = BaseHook.get_connection(self._conn_id)
        uri = conn.get_uri()

        # correct format of connection string
        if uri.startswith("mysql://"):
            uri = uri.replace("mysql://", "mysql+mysqlconnector://", 1)

        return uri

    def _connect_to_db(self):
        """Connect to data source and return (datasource, context)"""

        # db connection string
        uri = self._get_db_connection_uri()

        # connect GX
        context = gx.get_context(context_root_dir="great_expectations")

        # create or update data source
        datasource_name = "airflow_mysql_source"
        try:
            datasource = context.sources.add_mysql(
                name=datasource_name, connection_string=uri
            )
        except:
            datasource = context.sources.get_mysql(datasource_name)

        return datasource, context

    def _get_batch(self, data_asset):
        """Request data batch"""

        return data_asset.build_batch_request()

    def _get_data_asset(self, datasource, table_name: str):
        """Return data asset from data source"""

        asset_name = f"{table_name}_asset"
        try:
            return datasource.add_table_asset(name=asset_name, table_name=table_name)
        except:
            return datasource.get_asset(asset_name)

    def run_gx_validation(self, table_name: str, suite_name: str):

        # get table asset from data source
        data_asset = self._get_data_asset(self._datasource, table_name)

        # create batch request
        batch_request = self._get_batch(data_asset)

        # start data validation
        checkpoint_name = f"{table_name}_checkpoint"
        checkpoint = self._context.add_or_update_checkpoint(
            name=checkpoint_name,
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )

        # execute validation
        result = checkpoint.run()

        # generate HTML reports
        self._context.build_data_docs()

        if not result.success:
            raise Exception("Data Quality validation failed")
        else:
            print(f"Table {table_name} has passed quality check.")

        return result.success

    def create_expectation_suite_for_stage_table(
        self, table_name: str, suite_name: str
    ):
        """Create JSON file with validation rules in /expectations directory"""

        # get asset
        asset = self._get_data_asset(self._datasource, table_name)

        # create or update suite
        suite = self._context.add_or_update_expectation_suite(
            expectation_suite_name=suite_name
        )

        # create validator
        validator = self._context.get_validator(
            batch_request=asset.build_batch_request(), expectation_suite_name=suite_name
        )

        # validation rules
        validator.expect_table_columns_to_match_set(
            column_set=[
                "raw_id",
                "transaction_id",
                "transaction_date",
                "customer_id",
                "first_name",
                "city",
                "email",
                "product_id",
                "product_name",
                "product_category",
                "price",
                "quantity",
                "load_timestamp",
            ]
        )

        # email validator
        validator.expect_column_values_to_match_regex(
            "email", regex=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        )

        # transaction_id: not empty, unique
        validator.expect_column_values_to_not_be_null("transaction_id")
        validator.expect_column_values_to_be_unique("transaction_id")

        # price: cannot be less than 0
        validator.expect_column_values_to_be_between(
            "price",
            min_value=0.01,
            meta={"notes": "Price must be positive"},
        )

        # quantity: prevent from spikes
        validator.expect_column_values_to_be_between(
            "quantity", min_value=1, max_value=1000
        )

        # order_date: date mustn't be future
        today = datetime.date.today().strftime("%Y-%m-%d")
        validator.expect_column_values_to_be_between(
            "transaction_date", max_value=today
        )

        # save file to expectations/<suite_name>.json
        validator.save_expectation_suite(discard_failed_expectations=False)
        print(f"Validation rules file '{suite_name}' has been successfully created.")

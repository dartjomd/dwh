# airflow_ignore_file

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

        context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
        datasource_name = "my_mysql_datasource"
        uri = self._get_db_connection_uri()

        # Обновлено: add_or_update_sql для версии 1.x
        datasource = context.data_sources.add_or_update_sql(
            name=datasource_name, connection_string=uri
        )

        return datasource, context

    def _get_batch(self, data_asset):
        """Request data batch"""

        return data_asset.build_batch_request()

    def _get_data_asset(self, datasource, table_name: str):
        """Return data asset from data source"""

        asset_name = f"{table_name}_asset"
        query = f"SELECT * FROM {table_name}"

        try:
            return datasource.add_query_asset(name=asset_name, query=query)
        except:
            return datasource.get_asset(asset_name)

    def run_gx_validation(
        self, table_name: str, suite_name: str, auto_create_suite: bool = False
    ):
        """Run validation using ValidationDefinition (GX 1.x Native)"""
        # 1. Get data asset
        data_asset = self._get_data_asset(self._datasource, table_name)

        # 2. Get or create Batch Definition
        batch_def_name = f"batch_def_{table_name}"
        try:
            batch_definition = data_asset.get_batch_definition(batch_def_name)
        except (Exception, KeyError, LookupError):
            try:
                batch_definition = data_asset.add_batch_definition_whole_dataframe(
                    name=batch_def_name
                )
            except AttributeError:
                batch_definition = data_asset.add_batch_definition_daily(
                    name=batch_def_name, column="transaction_date"
                )

        # 3. Get Expectation Suite
        try:
            suite = self._context.suites.get(name=suite_name)
        except (Exception, KeyError):
            if auto_create_suite:
                self.create_expectation_suite_for_stage_table(table_name, suite_name)
                suite = self._context.suites.get(name=suite_name)
            else:
                raise Exception(f"Expectation Suite '{suite_name}' not found.")

        # 4. Create or get Validation Definition
        val_def_name = f"val_def_{table_name}"
        try:
            validation_definition = self._context.validation_definitions.get(
                val_def_name
            )
        except (Exception, KeyError):
            validation_definition = self._context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=val_def_name, data=batch_definition, suite=suite
                )
            )

        # 5. Run validation с правильным RunIdentifier
        print(f"--- Running GX Validation for {table_name} ---")

        # Импортируем RunIdentifier внутри метода для надежности
        from great_expectations.core.run_identifier import RunIdentifier

        run_id_obj = RunIdentifier(
            run_name=f"airflow_run_{table_name}", run_time=datetime.datetime.now()
        )

        # Теперь передаем ОБЪЕКТ, а не строку
        result = validation_definition.run(run_id=run_id_obj)

        # 6. Build Data Docs
        self._context.build_data_docs()

        # 7. Check result
        if not result.success:
            # Можно добавить логирование количества упавших записей
            print(f"Validation FAILED for {table_name}")
            raise Exception(f"Data Quality validation failed for table: {table_name}")

        print(f"--- Quality Check Passed for {table_name} ---")
        return result.success

    def create_expectation_suite_for_stage_table(
        self, table_name: str, suite_name: str
    ):
        """Create JSON file with validation rules in /expectations directory"""

        # get asset
        asset = self._get_data_asset(self._datasource, table_name)

        # create suite
        try:
            suite = self._context.suites.add_or_update(
                gx.ExpectationSuite(name=suite_name)
            )
        except:
            suite = self._context.suites.get(name=suite_name)

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

        validator.expect_column_unique_value_count_to_be_between(
            "transaction_id", min_value=None
        )

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

        # check that transaction date column is in format YYYYMMDD
        validator.expect_column_values_to_match_regex(
            "transaction_date", regex=r"^\d{4}-\d{2}-\d{2}$"
        )

        # order_date: date mustn't be future
        today = datetime.date.today().strftime("%Y-%m-%d")
        validator.expect_column_values_to_be_between(
            "transaction_date", max_value=today
        )

        # save file to expectations/<suite_name>.json
        suite_to_save = validator.get_expectation_suite()

        try:
            self._context.suites.delete(suite_name)
        except:
            pass

        self._context.suites.add(suite_to_save)
        print(f"Validation rules file '{suite_name}' has been successfully created.")


if __name__ == "__main__":
    # create JSON file once
    setup = GreatExpectationSetup(conn_id="mysql_dwh")
    setup.create_expectation_suite_for_stage_table("stg_raw_sales", "stg_sales_suite")

# dont move failed files in airflow
# telegram
# great expectations error logging
# ci
# cloud

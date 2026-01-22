# airflow_ignore_file

import datetime
import logging
from typing import Any, Tuple

# Основной модуль
import great_expectations as gx

from airflow.hooks.base_hook import BaseHook

from great_expectations.datasource.fluent.interfaces import Datasource, DataAsset
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import AbstractDataContext
from great_expectations.validation_definition import ValidationDefinition

from great_expectations.core.run_identifier import RunIdentifier

# initialize logger
logger = logging.getLogger("airflow.task")


class GreatExpectationSetup:
    def __init__(self, conn_id: str) -> None:
        self._conn_id: str = conn_id

        # connect to DB once
        self._datasource: Datasource
        self._context: AbstractDataContext
        self._datasource, self._context = self._connect_to_db()

    def _get_db_connection_uri(self) -> str:
        """Return DB connection string using Airflow hooks"""

        conn = BaseHook.get_connection(self._conn_id)
        uri: str = conn.get_uri()

        # correct format of connection string
        if uri.startswith("mysql://"):
            uri = uri.replace("mysql://", "mysql+mysqlconnector://", 1)

        return uri

    def _connect_to_db(self) -> Tuple[Datasource, AbstractDataContext]:
        """Connect to data source and return (datasource, context)"""

        context: AbstractDataContext = gx.get_context(
            context_root_dir="/opt/airflow/great_expectations"
        )
        datasource_name: str = "my_mysql_datasource"
        uri: str = self._get_db_connection_uri()

        datasource: Datasource = context.data_sources.add_or_update_sql(
            name=datasource_name, connection_string=uri
        )

        return datasource, context

    def _get_batch(self, data_asset: DataAsset) -> Any:
        """Request data batch"""

        return data_asset.build_batch_request()

    def _get_data_asset(self, datasource: Datasource, table_name: str) -> DataAsset:
        """Return data asset from data source"""

        asset_name: str = f"{table_name}_asset"
        query: str = f"SELECT * FROM {table_name}"

        try:
            return datasource.add_query_asset(name=asset_name, query=query)
        except Exception as e:
            logger.warning(
                "Asset %s is already existing. Trying to get it, error: %s",
                asset_name,
                e,
            )
            return datasource.get_asset(asset_name)

    def _get_batch_definition(
        self, data_asset: Any, batch_def_name: str
    ) -> BatchDefinition:
        """Get or create batch definition and return it"""

        try:
            return data_asset.get_batch_definition(batch_def_name)
        except (Exception, KeyError, LookupError):
            try:
                return data_asset.add_batch_definition_whole_dataframe(
                    name=batch_def_name
                )
            except AttributeError:
                return data_asset.add_batch_definition_daily(
                    name=batch_def_name, column="transaction_date"
                )

    def _get_expectation_suite(
        self, suite_name: str, table_name: str, auto_create_suite: bool
    ) -> ExpectationSuite:
        """Get expectation suite"""

        try:
            return self._context.suites.get(name=suite_name)
        except (Exception, KeyError):
            if auto_create_suite:
                self.create_expectation_suite_for_stage_table(table_name, suite_name)
                return self._context.suites.get(name=suite_name)
            else:
                raise Exception(f"Expectation Suite '{suite_name}' not found.")

    def _create_validation_definition(
        self,
        val_def_name: str,
        batch_definition: BatchDefinition,
        suite: ExpectationSuite,
    ) -> ValidationDefinition:
        """Create validation definition"""

        try:
            return self._context.validation_definitions.get(val_def_name)
        except (Exception, KeyError):
            return self._context.validation_definitions.add(
                ValidationDefinition(
                    name=val_def_name, data=batch_definition, suite=suite
                )
            )

    def run_gx_validation(
        self, table_name: str, suite_name: str, auto_create_suite: bool = False
    ) -> bool:
        """Run validation using ValidationDefinition (GX 1.x Native)"""

        # Get data asset
        data_asset: DataAsset = self._get_data_asset(self._datasource, table_name)

        # Get or create Batch Definition
        batch_def_name: str = f"batch_def_{table_name}"
        batch_definition: BatchDefinition = self._get_batch_definition(
            data_asset, batch_def_name
        )

        # Get Expectation Suite
        suite: ExpectationSuite = self._get_expectation_suite(
            suite_name, table_name, auto_create_suite
        )

        # Create or get Validation Definition
        val_def_name: str = f"val_def_{table_name}"
        validation_definition: ValidationDefinition = (
            self._create_validation_definition(val_def_name, batch_definition, suite)
        )

        # Create RunIdentifier instance
        run_id_obj: RunIdentifier = RunIdentifier(
            run_name=f"airflow_run_{table_name}",
            run_time=datetime.datetime.now(datetime.timezone.utc),
        )

        # Validate
        result = validation_definition.run(run_id=run_id_obj)

        # Build Data Docs
        self._context.build_data_docs()

        # Check result
        if not result.success:
            logger.error("Validation FAILED for %s", table_name)
            raise Exception(f"Data Quality validation failed for table: {table_name}")

        return bool(result.success)

    def create_expectation_suite_for_stage_table(
        self, table_name: str, suite_name: str
    ) -> None:
        """Create JSON file with validation rules in /expectations directory"""

        # get asset
        asset: DataAsset = self._get_data_asset(self._datasource, table_name)

        # create suite
        try:
            self._context.suites.add_or_update(ExpectationSuite(name=suite_name))
        except Exception:
            self._context.suites.get(name=suite_name)

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

        # save file to expectations/<suite_name>.json
        suite_to_save: ExpectationSuite = validator.get_expectation_suite()

        try:
            self._context.suites.delete(suite_name)
        except Exception:
            pass

        self._context.suites.add(suite_to_save)

        # log creation info
        logger.info(
            "Validation rules file '%s' has been successfully created.", suite_name
        )


if __name__ == "__main__":
    # create JSON file once
    setup = GreatExpectationSetup(conn_id="mysql_dwh")
    setup.create_expectation_suite_for_stage_table("stg_raw_sales", "stg_sales_suite")

import os
import time

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, InterfaceError
from mysql.connector.errors import DatabaseError

from airflow.hooks.base import BaseHook


MAX_DB_RETRY = 10
RETRY_DELAY = 10


class DB:
    def __init__(self):
        # define db engine
        self._engine: None | Engine = None
        self._create_engine()

    def _create_engine(self, conn_id: str = "mysql_dwh"):
        """Create DB engine by multiple attempts"""

        # get connection with Airflow connector hooks
        connection = BaseHook.get_connection(conn_id)
        db_uri = connection.get_uri()

        for i in range(1, MAX_DB_RETRY + 1):
            try:
                print(f"Attemp {i} of creating DB engine.")

                # create engine
                engine = create_engine(db_uri, echo=False)

                # make test request
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1 + 1")).fetchone()

                self._engine = engine
                print(
                    "Test request has been successfuly executed. DB engine has been created."
                )
                break
            except (OperationalError, InterfaceError, DatabaseError) as e:
                if i < MAX_DB_RETRY:
                    print(f"Connection error, will retry in {RETRY_DELAY} seconds.")
                    time.sleep(RETRY_DELAY)
                else:
                    print(
                        "FATAL: Fail to connect to db. This was last attemp to create DB engine."
                    )
                    raise ConnectionError(
                        "Failed to connect to database after all retries"
                    )
            except Exception as e:
                print(f"An unexpected error occurred during engine creation: {e}")
                raise ConnectionError("Failed to connect to database after all retries")

    def get_engine(self) -> Engine:
        """Get engine"""
        if not self._engine:
            raise RuntimeError("Engine is not initialized.")
        return self._engine

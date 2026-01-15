import os
import time

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError, InterfaceError
from mysql.connector.errors import DatabaseError


MAX_DB_RETRY = 10
RETRY_DELAY = 10
ENV_VARIABLES = ["_DB_HOST", "_DB_USER", "_DB_PASSWORD", "_DB_NAME"]


class DB:
    def __init__(self):
        self._DB_HOST = os.getenv("DB_HOST")
        self._DB_USER = os.getenv("DB_USER")
        self._DB_PASSWORD = os.getenv("DB_PASSWORD")
        self._DB_NAME = os.getenv("DB_NAME")
        self._DB_PORT = os.getenv("DB_PORT", "3306")

        # check if all ENV variables are present
        self._check_env_variables()

        # url to connect to bd
        self.DB_URL = self._get_db_url()

        # define db engine
        self._engine: None | Engine = None
        self._create_engine()

    def _check_env_variables(self):
        """Check if every ENV variable from global list is present"""
        missing_variables = [var for var in ENV_VARIABLES if not getattr(self, var)]

        if missing_variables:
            raise ValueError(
                f"{', '.join(missing_variables)} from .env file are absent."
            )

    def _get_db_url(self):
        """Concatenate ENV variables to create connection URL"""
        return f"mysql+mysqlconnector://{self._DB_USER}:{self._DB_PASSWORD}@{self._DB_HOST}:{self._DB_PORT}/{self._DB_NAME}"

    def _create_engine(self):
        """Create DB engine by multiple attempts"""
        for i in range(1, MAX_DB_RETRY + 1):
            try:
                print(f"Attemp {i} of creating DB engine.")

                # create engine
                engine = create_engine(self.DB_URL, echo=False)

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

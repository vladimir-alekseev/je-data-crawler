"""Various ways to save data."""
import csv
import logging
from dataclasses import asdict, fields
from typing import Any, Collection, Mapping, Sequence, Tuple

import mysql.connector
from mysql.connector import errorcode
from mysql.connector.connection import MySQLConnection, MySQLCursor

from data_crawler.data_objects import Vacancy


class Engine():
    """
    Base class for actual write engines.
    Meant to be instantiated by a Writer.
    Methods are overridden by subclasses if actually used.

    Fields:
    REQUIRED_CONFIG_FIELDS: Settings required to be present in config section for engine to function properly.

    Usage:
    1. connect()
    2. save_data_batch()
    3. disconnect()
    """
    REQUIRED_CONFIG_FIELDS: Tuple[str]

    def __init__(self, config: Mapping) -> None:
        """Sets required configuration settings for connecting to a data storage, authenticating and saving a data batch.

        Arguments:
            config {Mapping} -- Appropriate config section returned by configparser.
        """
        pass

    def _check_config_fields(self, config: Mapping) -> None:
        """Checks completeness of required config settings.

        Arguments:
            config {Mapping} -- Appropriate config section returned by configparser.

        Raises:
            KeyError: Whenever required setting is missing from a config section.
        """
        for required_field in self.REQUIRED_CONFIG_FIELDS:
            if required_field not in config:
                raise KeyError(
                    f"Required field is missing from config file section {config}: {required_field}")

    def connect(self) -> None:
        """Actions required to establish a connection to a data storage."""
        pass

    def save_data_batch(self, data_batch: Collection[Vacancy]) -> None:
        """Actions required to properly store passed data batch to a storage destination."""
        pass

    def disconnect(self) -> None:
        """Actions needed to disconect from a data storage. Used during normal functioning and by exception handlers."""
        pass


class LogOutput(Engine):
    """Outputs data to log. Nothing to set up and close."""

    def save_data_batch(self, data_batch: Collection[Vacancy]) -> None:
        for data_item in data_batch:
            logging.info(data_item)


class CSVOutput(Engine):
    """
    Outputs data to a local CSV file using predefined format.
    Uses standard python csv module.

    Fields:
    VACANCY_FIELDS: All the fields from Vacancy data object to maintain consistency when writing to CSV columns.
    """
    VACANCY_FIELDS: Sequence[str] = [field.name for field in fields(Vacancy)]
    REQUIRED_CONFIG_FIELDS: Tuple[str] = (
        "FILE_PATH",
        "CSV_DIALECT",
    )

    _file_path: str
    _csv_dialect: str

    _csv_file: object

    def __init__(self, config: Mapping[str, Any]) -> None:
        self._check_config_fields(config)

        self._file_path = config["FILE_PATH"]
        self._csv_dialect = config["CSV_DIALECT"]

    def connect(self) -> None:
        logging.info(
            f"Opening CSV file {self._file_path} for saving...")

        self._csv_file = open(self._file_path, 'a', newline="")

    def save_data_batch(self, data_batch: Collection[Vacancy]) -> None:
        """Saves data batch to a local CSV file using defined dialect. Raises errors on fields mismatch."""
        writer = csv.DictWriter(
            self._csv_file,
            fieldnames=self.VACANCY_FIELDS,
            # restval="",
            extrasaction="raise",
            dialect=self._csv_dialect,
        )

        writer.writerows(map(asdict, data_batch))

        logging.info("Data save completed")

    def disconnect(self) -> None:
        if self._csv_file:
            self._csv_file.close()


class MariaDBOutput(Engine):
    """
    Outputs data to a MariaDB instance.
    Uses MySQL Connector/Python module available at https://dev.mysql.com/downloads/connector/python/

    Fields:
    VACANCY_FIELDS: All the fields from Vacancy data object to maintain consistency when writing to table columns in MariaDB.
    """
    VACANCY_FIELDS: Sequence[str] = [field.name for field in fields(Vacancy)]
    REQUIRED_CONFIG_FIELDS: Tuple[str] = (
        "HOST",
        "USER",
        "PASSWORD",
        "DB",
        "TABLE",
    )

    _host: str
    _user: str
    _password: str
    _db: str
    _table: str

    _connection: MySQLConnection

    def __init__(self, config: Mapping[str, Any]) -> None:
        self._check_config_fields(config)

        self._host = config["HOST"]
        self._user = config["USER"]
        self._password = config["PASSWORD"]
        self._db = config["DB"]
        self._table = config["TABLE"]

    def connect(self) -> None:
        try:
            self._connection = mysql.connector.connect(
                user=self._user,
                password=self._password,
                host=self._host,
                database=self._db,
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error(
                    "Something is wrong with MariaDB username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error(f"Database {self._db} does not exist")
            else:
                logging.error(err)
            raise
        else:
            logging.info(
                f"Connected to MariaDB on {self._host} as {self._user}")

    def save_data_batch(self, data_batch: Collection[Vacancy]) -> None:
        """Saves data batch to a table in MariaDB. Only saves fields explicitly defined in Vacancy data object."""
        cursor: MySQLCursor = self._connection.cursor()

        add_vacancy_statement_template: str = "INSERT IGNORE INTO {table_name} ({fields}) VALUES ({value_formats});"
        add_vacancy_statement = add_vacancy_statement_template.format(
            table_name=self._table,
            fields=", ".join(self.VACANCY_FIELDS),
            value_formats=", ".join(
                [f"%({field_name})s" for field_name in self.VACANCY_FIELDS])
        )

        try:
            cursor.executemany(add_vacancy_statement,
                               tuple(map(asdict, data_batch)))

            if cursor.with_rows:
                logging.info(cursor.fetchall())
            else:
                logging.info(f"Vacancies added to DB: {cursor.rowcount}")

            self._connection.commit()
        except Exception as err:
            logging.error(str(err))
        finally:
            cursor.close()

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()

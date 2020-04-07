"""Controllers for manipulating data source handlers."""
import importlib
import logging
import sys
from typing import Collection, Mapping

from data_crawler.data_objects import Vacancy
from data_crawler.data_collection.data_sources import DataSource


class Fetcher(object):
    """Data source manager: checks data source validity, initializes data source, collects data batch."""
    VALID_DATA_SOURCES = (
        "HH_API",
        "CSVInput",
    )

    _data_source: DataSource

    def __init__(self, data_source_name: str, config: Mapping) -> None:
        """Validates data source name and passes config section for initialization.

        Arguments:
            data_source_name {str} -- Data source name from config file. Must match data source class name.
            config {Mapping} -- Config file section for appropriate data source

        Raises:
            ValueError: When provided data source name is invalid
        """
        if data_source_name in self.VALID_DATA_SOURCES:
            ValidDataSourceClass = getattr(
                importlib.import_module("data_crawler.data_collection.data_sources"),
                data_source_name)
            self._data_source = ValidDataSourceClass(config)
        else:
            raise ValueError(f"Invalid data source: {data_source_name}")

        logging.info(f"Data source initialized: {data_source_name}")

    def fetch_data(self) -> Collection[Vacancy]:
        """Controls connection to fetch data from data source."""
        try:
            self._data_source.connect()
        except Exception as err:
            logging.error(str(err))
            sys.exit()

        try:
            fetched_data = self._data_source.collect_data()
        except Exception as err:
            logging.error(str(err))
        finally:
            self._data_source.disconnect()

        return fetched_data

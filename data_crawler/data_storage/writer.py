"""Controllers for manipulating data storage writers."""
import importlib
import logging
import sys
from typing import Collection, Mapping

from data_crawler.data_objects import Vacancy
from data_crawler.data_storage.engines import Engine


class Writer(object):
    """
    Data storage writer: checks data storage engine validity,
    initializes engine, saves data batch to a storage locations.
    """
    VALID_ENGINES = (
        "LogOutput",
        "CSVOutput",
        "MariaDBOutput",
    )

    _engine: Engine

    def __init__(self, engine_name: Engine, config: Mapping) -> None:
        """Validates writer engine name and passes config section for initialization.
        
        Arguments:
            engine_name {Engine} -- Writer engine name from config file. Must match data source class name.
            config {Mapping} -- Config file section for appropriate writer engine
        
        Raises:
            ValueError: When provided engine name is invalid
        """
        if engine_name in self.VALID_ENGINES:
            ValidEngineClass = getattr(
                importlib.import_module("data_crawler.data_storage.engines"),
                engine_name)
            self._engine = ValidEngineClass(config)
        else:
            raise ValueError(f"Invalid writer engine: {engine_name}")

        logging.info(f"Writer engine initialized: {engine_name}")

    def save(self, data_batch: Collection[Vacancy]) -> None:
        """Controls connection to save data to storage location."""
        try:
            self._engine.connect()
        except Exception as err:
            logging.error(str(err))
            sys.exit()
        
        try:
            self._engine.save_data_batch(data_batch)
        except Exception as err:
            logging.error(str(err))
        finally:
            self._engine.disconnect()

import argparse
import configparser
import logging
import sys
from typing import Any, Collection, Mapping

from . import __version__
from .data_collection.fetcher import Fetcher
from .data_storage.writer import Writer


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect data for Jobs Explorer from various sources.",
        prog="data_crawler"
    )
    parser.add_argument("-c", "--config", type=str, nargs=1,
                        help="config file path (default: crawler.ini)", default="crawler.ini")
    parser.add_argument("-V", "--version", action="version",
                        version=f"{parser.prog} {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logs")

    args = parser.parse_args()

    # Initialize logger
    logging_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s %(levelname)s %(module)s.%(funcName)s: %(message)s"
    )

    # Initialize config settings
    config = configparser.ConfigParser()
    config_path = args.config
    config.read(config_path)

    if len(config) < 2:
        logging.error(f"Error reading config file {args.config[0]}")
        sys.exit()

    data_source_name = config["DEFAULT"]["DATA_SOURCE"]
    writer_engine_name = config["DEFAULT"]["WRITER_ENGINE"]

    # Prepare source and storage
    try:
        fetcher = Fetcher(data_source_name, config[data_source_name])
    except ValueError as err:
        logging.error(f"Fetcher initialization failed: {err}")
        sys.exit()

    try:
        writer = Writer(writer_engine_name, config[writer_engine_name])
    except ValueError as err:
        logging.error(f"Writer initialization failed: {err}")
        sys.exit()

    # Do stuff
    fetched_data = fetcher.fetch_data()
    if fetched_data:
        writer.save(fetched_data)


if __name__ == "__main__":
    main()

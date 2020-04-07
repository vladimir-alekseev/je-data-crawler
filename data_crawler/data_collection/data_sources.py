# pylint: disable=E1101
"""Various ways to access data sources used by crawler."""
import csv
import json
import logging
import re
from dataclasses import fields
from datetime import date, datetime, timedelta
from distutils.util import strtobool
from typing import Any, Collection, List, Mapping, Sequence, Tuple

import requests

from data_crawler.data_objects import Vacancy
from data_crawler.data_collection.parsers import VacancyDescriptionParser


class DataSource():
    """
    Base class for actual data source handling objects.
    Meant to be instantiated by a Fetcher.
    Methods are overridden by subclasses if actually used.

    Fields:
    DATA_SOURCE_NAME: Label put into Vacancy objects to identify their origin.
    REQUIRED_CONFIG_FIELDS: Settings required to be present in config section for engine to function properly.

    Usage:
    1. connect()
    2. collect_data()
    3. disconnect()
    """
    DATA_SOURCE_NAME: str
    REQUIRED_CONFIG_FIELDS: Tuple[str]

    def __init__(self, config: Mapping) -> None:
        """Sets required configuration settings for connecting to a data source, authenticating and saving a data batch.

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
        """Actions required to establish a connection to a data source."""
        pass

    def collect_data(self) -> Collection[Vacancy]:
        """Actions required to properly collect data from a data source."""
        pass

    def disconnect(self) -> None:
        """Actions needed to disconect from a data source. Used during normal functioning and by exception handlers."""
        pass


class HH_API(DataSource):
    """
    Handler object for HH.ru API.
    Implemented settings comply with API specification as 2018-07.

    Users requests library.
    """
    DATA_SOURCE_NAME: str = "hh.ru"
    REQUIRED_CONFIG_FIELDS: Tuple[str] = (
        "SEARCH_PERIOD_DAYS",
        "DEFAULT_PERIOD_OFFSET_DAYS",
        "MIN_SEARCH_PERIOD_DAYS",
        "SEARCH_PARAMS_SPECIALIZATION",
        "SEARCH_PARAMS_AREA",
        "SEARCH_PARAMS_PER_PAGE",
    )

    # HH API endpoint settings for vacancy search: https://github.com/hhru/api/blob/master/docs/vacancies.md
    VACANCIES_ENDPOINT_URL: str = "https://api.hh.ru/vacancies"
    DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S%z"
    MAX_RESULTS_PER_SEARCH: int = 2000

    USER_AGENT_STRING: str = "jobs_explorer"
    DEFAULT_HEADERS: Mapping[str, Any] = {
        "User-Agent": USER_AGENT_STRING
    }

    # HH API endpoint settings for individual vacancy
    VACANCY_ENDPOINT_HEADERS: Mapping[str, str] = DEFAULT_HEADERS.copy().update(
        {"Content-Type": "application/x-www-form-urlencoded"}
    )

    _search_period_days: int
    _default_period_offset_days: int
    _min_search_period_days: int

    _specialization_id: int
    _area_id: int
    _per_page: int

    def __init__(self, config: Mapping) -> None:
        self._check_config_fields(config)

        self._search_period_days = config.getint("SEARCH_PERIOD_DAYS")
        self._default_period_offset_days = config.getint(
            "DEFAULT_PERIOD_OFFSET_DAYS")
        self._min_search_period_days = config.getint("MIN_SEARCH_PERIOD_DAYS")

        self._specialization_id = config.getint("SEARCH_PARAMS_SPECIALIZATION")
        self._area_id = config.getint("SEARCH_PARAMS_AREA")
        self._per_page = config.getint("SEARCH_PARAMS_PER_PAGE")

    def collect_data(self) -> Collection[Vacancy]:
        """
        Actions needed to collect full data for matching vacancies.

        Collection routine:
        1. Query API with parameters from config file to count overall matching vacancies.
        2. Query API to collect matching vacancies' IDs.
        3. Query API by individual IDs to collect full vacancy data.

        By default, method collects vacancies found during 1-day period counting from yesterday.
        Period length and offset are set in config file.
        """
        parsed_vacancies: Collection[Vacancy] = None

        vacancy_ids = self._collect_vacancy_ids()
        if len(vacancy_ids):
            parsed_vacancies = self._collect_vacancy_data(vacancy_ids)
        else:
            logging.info("No new vacancy IDs found")

        logging.info(
            f"Data collection completed. Vacancies collected: {len(parsed_vacancies)}")
        return parsed_vacancies

    def _collect_vacancy_ids(self) -> Collection[str]:
        """
        Actions needed to collect full data for matching vacancies for the defined period.
        If matching vacancies count is greater than API limitation (2000 items as 2018-07),
        initial request is divided into subrequests with smaller period controlled by _min_search_period_days.

        Collection routine:
        1. Query API with parameters from config file to count overall matching vacancies.
        2. Query API to collect matching vacancies' IDs.

        If query response contains several pages, they are collected appropriately.
        """
        def _extract_vacancy_ids(search_results: Collection[Any]) -> Collection[str]:
            return [found_vacancy["id"] for found_vacancy in search_results["items"]]

        def _search_hh_vacancies(period_duration: int, period_offset: int = 0, subrequest: bool = False) -> Tuple[int, List[str]]:
            """Query API for gettings matching vacancies IDs

            Arguments:
                period_duration {int} -- Search period

            Keyword Arguments:
                period_offset {int} -- Days offset into the past from today (used for subqueries) (default: {0})
                subrequest {bool} -- Whether API results limitation check is needed (default: {False})

            Returns:
                Tuple[int, List[str]] -- Count of results found and matching vacancy IDs
            """
            results_count: int = 0
            found_ids: List[str] = []

            # Determine API query parameters based on provided settings
            current_response_page: int = 0
            search_params: Mapping[str, Any] = {
                "specialization": self._specialization_id,
                "area": self._area_id,
                "per_page": self._per_page,
                "date_from": (date.today() - timedelta(days=period_offset+self._default_period_offset_days) - timedelta(days=period_duration-1)).isoformat(),
                "date_to":   (date.today() - timedelta(days=period_offset+self._default_period_offset_days)).isoformat(),
                "page": current_response_page,
            }

            # Collect first results page and count found items
            search_results = self._get_api_data(
                self.VACANCIES_ENDPOINT_URL, search_params, self.DEFAULT_HEADERS)
            results_count += search_results["found"]
            found_ids.extend(_extract_vacancy_ids(search_results))

            if (results_count > self.MAX_RESULTS_PER_SEARCH) and not subrequest:
                logging.warning(
                    f"Search results limit exceeded! Vacancies found: {results_count}")
                return (results_count, found_ids)

            # Collect remaining results pages
            response_pages_count: int = search_results["pages"]
            if response_pages_count > 1:
                logging.info(f"Pages to load: {response_pages_count}")

            for current_response_page in range(1, response_pages_count):
                search_params.update({"page": current_response_page})
                search_results = self._get_api_data(
                    self.VACANCIES_ENDPOINT_URL, search_params, self.DEFAULT_HEADERS)
                found_ids.extend(_extract_vacancy_ids(search_results))

            return (results_count, found_ids)

        logging.info("Collecting hh.ru vacancy IDs...")

        total_vacancies_found, vacancy_ids = _search_hh_vacancies(
            self._search_period_days)

        # Divide into subrequests if single query results count exceeds API limitation
        if total_vacancies_found > self.MAX_RESULTS_PER_SEARCH:
            subrequests_needed: int

            # If subrequests do not cover initial search period in full, add 1 additional subrequest
            if self._search_period_days % self._min_search_period_days:
                subrequests_needed = self._search_period_days // self._min_search_period_days + 1
            else:
                subrequests_needed = self._search_period_days // self._min_search_period_days

            logging.info(
                f"Splitting search into {subrequests_needed} subrequests")

            subrequests_ids: List[str] = []
            for subrequest_index in range(subrequests_needed):
                logging.info(
                    f"Processing subrequest #{subrequest_index + 1} ...")

                subrequest_ids_batch = _search_hh_vacancies(
                    self._min_search_period_days,
                    period_offset=subrequest_index * self._min_search_period_days,
                    subrequest=True
                )[1]

                subrequests_ids.extend(subrequest_ids_batch)

            vacancy_ids = subrequests_ids

        return vacancy_ids

    def _collect_vacancy_data(self, vacancy_ids: Collection[str]) -> Collection[Vacancy]:
        """Query API by individual vacancy IDs to collect full data and populate Vacancy data objects."""
        def _strip_spaces_between_tags(text: str) -> str:
            return re.sub(r">\s+<", "><", text)

        parsed_vacancies: List[Vacancy] = []

        logging.info(f"Collecting data for {len(vacancy_ids)} vacancies...")

        description_parser: VacancyDescriptionParser = VacancyDescriptionParser()

        for vacancy_id in vacancy_ids:
            logging.debug(f"Collecting hh.ru vacancy ID {vacancy_id}")

            vacancy_data = self._get_api_data(
                url=self.VACANCIES_ENDPOINT_URL + "/" + vacancy_id,
                headers=self.VACANCY_ENDPOINT_HEADERS
            )

            vacancy_fields = {
                # Required fields
                "source": self.DATA_SOURCE_NAME,
                "name": vacancy_data["name"],
                "description": vacancy_data["description"],
                "date_published": datetime.strptime(
                    vacancy_data["published_at"], self.DATETIME_FORMAT).date(),
                "employer_name": vacancy_data["employer"]["name"] if vacancy_data["employer"] else None,
                # Miscellaneous fields
                "id_source": vacancy_data["id"],
                "salary_range_lower": vacancy_data["salary"]["from"] if vacancy_data[
                    "salary"] and ("from" in vacancy_data["salary"]) else None,  # Sometimes comes empty
                "salary_range_upper": vacancy_data["salary"]["to"] if vacancy_data["salary"] and ("to" in vacancy_data["salary"]) else None,
                "salary_currency": vacancy_data["salary"]["currency"] if vacancy_data["salary"] else None,
                "salary_gross_indicator": vacancy_data["salary"]["gross"] if vacancy_data["salary"] else None,
                "schedule_type": vacancy_data["schedule"]["name"] if vacancy_data["schedule"] else None,
                "employment_type": vacancy_data["employment"]["name"] if vacancy_data["employment"] else None,
                "region": vacancy_data["area"]["name"] if vacancy_data["area"] else None,
                "cover_letter_required": vacancy_data["response_letter_required"],
                # HH_API
                "experience_range_hh": vacancy_data["experience"]["name"] if vacancy_data["experience"] else None,
                "employer_id_hh": vacancy_data["employer"]["id"] if vacancy_data["employer"] and ("id" in vacancy_data["employer"]) else None,
                "test_required_hh": vacancy_data["test"]["required"] if vacancy_data["test"] else False,
                "test_included_hh": vacancy_data["has_test"]
            }

            # Parse HTML description
            description_parser.feed(_strip_spaces_between_tags(
                vacancy_fields["description"]))
            description_parser.close()
            vacancy_fields["description"] = description_parser.get_results()
            description_parser.reset()

            parsed_vacancies.append(Vacancy(**vacancy_fields))
        return parsed_vacancies

    def _get_api_data(self, url: str, params: Mapping[str, Any] = None, headers: Mapping[str, Any] = None) -> Collection[Any]:
        """Parses data from API JSON response

        Arguments:
            url {str} -- API endpoint

        Keyword Arguments:
            params {Mapping[str, Any]} -- API query parameters (default: {None})
            headers {Mapping[str, Any]} -- API query headers (default: {None})

        Raises:
            RuntimeError: When any error occurs during request.

        Returns:
            Collection[Any] -- Parsed data from JSON response.
        """
        try:
            response = requests.get(url=url, params=params, headers=headers)
        except requests.ConnectionError as connection_err:
            logging.error(
                f"Network error during API request: {connection_err}")
            raise RuntimeError from connection_err
        except requests.Timeout as timeout_err:
            logging.error(f"API request timed out: {timeout_err}")
            raise RuntimeError from timeout_err
        except requests.exceptions.RequestException as requests_err:
            logging.error(f"API request failed: {requests_err}")
            raise RuntimeError from requests_err
        except Exception as err:
            logging.error(f"API connection failed: {err}")
            raise RuntimeError from err

        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error(f"Unexpected HTTP response: {err}")
            raise RuntimeError from err
        else:
            extracted_data = response.json()

        return extracted_data


class CSVInput(DataSource):
    """
    Handler object for local CSV file input.
    Fetcher instance must handle connection closing appropriately.
    """
    DATA_SOURCE_NAME: str = "CSV Import"
    REQUIRED_CONFIG_FIELDS: Tuple[str] = (
        "FILE_PATH",
        "CSV_DIALECT",
    )

    _file_path: str
    _csv_dialect: str

    _csv_file: object

    def __init__(self, config: Mapping) -> None:
        self._check_config_fields(config)

        self._file_path = config["FILE_PATH"]
        self._csv_dialect = config["CSV_DIALECT"]

    def connect(self) -> None:
        logging.info(
            f"Opening CSV file {self._file_path} for import...")

        self._csv_file = open(self._file_path, 'r', newline="")

    def collect_data(self) -> Collection[Vacancy]:
        """
        Reads data from a local CSV file using defined dialect.
        Populates missing data on column mismatch.
        Replaces data source label in Vacancy data objects to mark data import batches.
        """
        def _reverse_csv_writer_changes(raw_fields: Mapping[str, Any]) -> Mapping[str, Any]:
            """Reverses changes introduced by csv.writer"""
            # csv.writer converts None to empty str and bool to str - it drives MariaDB crazy
            vacancy_fields: Mapping[str, Any] = {}

            for key, value in raw_fields.items():
                if value and value.strip():
                    if value == 'False' or value == 'True':
                        vacancy_fields[key] = bool(strtobool(value))
                    else:
                        vacancy_fields[key] = value
                else:
                    vacancy_fields[key] = None

            return vacancy_fields

        data_batch: List[Vacancy] = []

        reader = csv.DictReader(
            self._csv_file,
            restval="CSV Import Error",
            dialect=self._csv_dialect,
        )

        # Post-process parsed data to reverse changes made by csv.writer
        for raw_vacancy_fields in reader:
            raw_vacancy_fields["source"] = self.DATA_SOURCE_NAME
            data_batch.append(
                Vacancy(**_reverse_csv_writer_changes(raw_vacancy_fields)))

        logging.info(
            f"Data import completed. Vacancies imported: {len(data_batch)}")
        return data_batch

    def disconnect(self) -> None:
        if self._csv_file:
            self._csv_file.close()

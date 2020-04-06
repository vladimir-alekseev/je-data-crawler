"""Parsers for various fields in data objects"""
import re
from html.parser import HTMLParser
from typing import List, Tuple


class VacancyDescriptionParser(HTMLParser):
    """
    Parses description field of HH.ru vacancies.
    Mainly strips HTML tags and non-printable characters.

    Lifecycle:
    1. __init__
    2. parse() invokes tag-handling functions
    3. get_results() returns parsed description
    """
    _description_parts: List[str]

    def __init__(self):
        """
        Description parts will be appended to a list by tag-handling functions
        to form a complete text, stripped from tags and non-printable characters
        """
        super().__init__()
        self._description_parts = []

    # Tags found in vacancy description on HH.ru to be replaced by new lines.
    START_TAGS: Tuple[str] = (
        "p",
        "li"
    )

    END_TAGS: Tuple[str] = (
        "br",
        "ul",
        "ol"
    )

    def handle_starttag(self, tag, attrs) -> None:
        if tag in self.START_TAGS:
            self._description_parts.append("\n")

    def handle_endtag(self, tag) -> None:
        if tag in self.END_TAGS:
            self._description_parts.append("\n")

    def handle_data(self, data) -> None:
        # Strips non-printable characters
        if len(data.strip()) > 1:
            self._description_parts.append(data)

    def reset(self) -> None:
        super().reset()
        self._description_parts = []

    def get_results(self) -> str:
        """Converts parsing results to a complete description text.

        Returns:
            str -- Complete desctription text.
        """
        return ' '.join(self._description_parts)

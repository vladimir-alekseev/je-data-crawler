"""Data objects used by data crawler."""
from dataclasses import dataclass, field
from datetime import date


@dataclass(init=True, repr=True, eq=False, order=False, unsafe_hash=False, frozen=False)
class Vacancy():    
    """
    Defines a universal Vacancy object for using
    throughout data crawler:
    - API response saving
    - CSV saving/reading
    - Database table saving
    """
    # Required fields
    source: str
    name: str
    description: str = field(repr=False)
    date_published: date
    employer_name: str
    id_source: str # May not be unique across sources
    # Miscellaneous fields
    salary_range_lower: int = field(repr=False)
    salary_range_upper: int = field(repr=False)
    salary_currency: str = field(repr=False)
    salary_gross_indicator: bool = field(repr=False)
    schedule_type: str = field(repr=False)
    employment_type: str = field(repr=False)
    region: str = field(repr=False)
    cover_letter_required: bool = field(repr=False)
    # hh_api_2018_07
    employer_id_hh: str = field(repr=False)
    experience_range_hh: str = field(repr=False)
    test_required_hh: bool = field(repr=False)
    test_included_hh: bool = field(repr=False)

    def __eq__(self, other: "Vacancy") -> bool:
        if (self.id_source == other.id_source) and (self.source == other.source):
            return True
        else:
            return (self.name == other.name) and (self.description == other.description)

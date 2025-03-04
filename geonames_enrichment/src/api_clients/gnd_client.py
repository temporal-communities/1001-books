from __future__ import (
    annotations,
)  # Handle forward reference in typehint for __enter__ method
import logging
import re
import pandas as pd
from pandas._libs.missing import NAType  # for type hints
import requests  # for type hints
from types import TracebackType  # for type hints
import unicodedata

from src.http_client.http_client import HttpClient  # for type hints

logger = logging.getLogger(__name__)


class GndClient:
    """
    GndClient interacts with the lobid-gnd API to fetch geographic area codes and author/title variants.
    Can be used as a context manager.
    """

    def __init__(self, rate_limit: str, base_url: str = "https://lobid.org/gnd/") -> None:
        self.http_client = self._initialize_http_client(rate_limit)
        self.rate_limit = rate_limit
        self.base_url = base_url
        self.excluded_types = {
            "Person",
            "DifferentiatedPerson",
            "CorporateBody",
            "ConferenceOrEvent",
            "MusicalWork",
        }

    @staticmethod
    def _initialize_http_client(rate_limit: str) -> HttpClient:
        """Create and return a new HttpClient instance with rate limiting."""
        return HttpClient(rate_limit)

    def __enter__(self) -> GndClient:
        """Enable the use of GeoNamesClient as a context manager."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        """Ensure HttpClient is closed when exiting the context."""
        if self.http_client:
            self.http_client.__exit__(exc_type, exc_value, traceback)

    def get_gnd_areacode(self, row: pd.Series, n_pages: int, title_columns: list[str]) -> tuple[str | NAType, str | NAType, str | NAType]:
        """Retrieve the GND area code by querying the lobid-gnd API using author and title information from the input row."""

        author = row["Author"]
        work_wikidata_id = row.get("Work Wikidata ID", pd.NA)
        titles = self._extract_titles(row, title_columns)

        logger.debug(f"Processing titles: {titles} for author {author}")

        for title in titles:
            if pd.isna(title):
                continue
            logger.info(f"Retrieving data for {title} by {author}")
            title = self._normalize_input(title)
            # Fetch metadata for title from lobid-gnd API
            response = self._fetch_gnd_data(f"{self.base_url}search?q={title}&type=Titel&from=0&size={n_pages}&format=json")
            if response is None:
                logger.warning(f"Request for {title} failed, see logs.")
                break
            # Check number of results
            response_dict = response.json()
            if response_dict["totalItems"] > n_pages:
                logger.warning(f"More than {n_pages} results for {title} by {author}, try increasing n_pages.")
            if response_dict["totalItems"] == 0:
                logger.debug(f"No data found for {title} by {author}, trying next title...")
                continue

            # Check each result and return GND areacode
            for no, item in enumerate(response_dict.get("member", [])):
                if self._validate_gnd_result(
                    item, work_wikidata_id, title, author, no
                ):  # find_matching_result
                    geo_id, geo_label = self._extract_geocode(item)
                    return geo_id, geo_label, pd.NA

        return pd.NA, pd.NA, "No GND areacode"

    def _validate_gnd_result(self, item: dict, work_wikidata_id: str, title: str, author: str, no: int) -> bool:
        """Validate a lobid-gnd API result item by checking type, Wikidata ID, and author/title match."""

        # Check result item type
        logger.debug("Checking result item type...")
        type_dict = item.get("type", [])
        if any(result_type in self.excluded_types for result_type in type_dict):
            logger.debug(f"Result no {no} is wrong type ({type_dict}).")
            return False

        # Check if Wikidata ID matches
        logger.debug(f"Searching result no {no} for Wikidata ID...")
        same_as_lst = item.get("sameAs", [])
        wikidata_id = self._extract_wikidata_id(same_as_lst)
        if (
            pd.notna(wikidata_id)
            and pd.notna(work_wikidata_id)
            and wikidata_id == work_wikidata_id
        ):
            logger.debug(f"Found matching Wikidata ID: {wikidata_id}")
            return True

        # Check if title or alternative titles match
        logger.debug(
            f"No matching Wikidata ID, searching result no {no} for author name and title..."
        )
        all_titles = self._fetch_title_variants(item)
        if title not in all_titles:
            logger.debug("Result no {no} does not contain matching title.")
            return False

        # Check if first author name matches
        first_author = item.get("firstAuthor", pd.NA)
        if pd.isna(first_author):
            logger.debug(f"Result no {no} does not contain author name.")
            return False

        author_label = first_author[0].get("label", pd.NA)
        if self._normalize_input(author_label) == self._normalize_input(author):
            logger.debug(f"Found matching first author name: {author_label}, {author}")
            return True

        author_id = first_author[0].get("id", pd.NA)
        if pd.isna(author_id):
            logger.debug(f"Result no {no} does not contain author id.")
            return False

        # Check if variant auhtor name matches
        all_names = self._fetch_author_variants(author_id.split("/")[-1])
        if self._normalize_input(author) in all_names:
            logger.debug(f"Found matching author variant name: {author_label}, {author}")
            return True

        logger.info(f"No matching result found for {title} by {author}.")
        return False

    def _fetch_gnd_data(self, url: str) -> requests.Response | None:
        """Fetch data from the lobid-gnd API."""
        logger.info(f"Fetching data from {url}")
        return self.http_client.fetch_page(url)

    def _fetch_title_variants(self, item: dict) -> list[str]:
        """Extract and normalize all available titles from lobid-gnd API response."""
        logger.debug("Fetching title variants...")
        preferred_title = item.get("preferredName", pd.NA)
        variant_titles = item.get("variantName", [])
        all_titles = variant_titles + [preferred_title]
        logger.debug(f"All titles: {all_titles}.")
        return [self._normalize_input(title) for title in all_titles]

    def _fetch_author_variants(self, author_id: str) -> list[str]:
        """Fetch alternative author names from lobid-gnd API and normalize variant names."""
        logger.debug("Fetching author name variants...")
        metadata_response = self._fetch_gnd_data(f"{self.base_url}{author_id}.json")
        if metadata_response is None:
            logger.warning(f"Request to retrieve metadata for {author_id} failed, see logs.")
            return []
        metadata_json = metadata_response.json()
        variant_names = metadata_json.get("variantNameEntityForThePerson", [])

        all_names = []
        for variant in variant_names:
            # Collect forenames, surnames, and personal names into lists
            forenames = variant.get("forename", [])
            surnames = variant.get("surname", [])
            personal_names = variant.get("personalName", [])
            # Ensure all name components are lists
            forenames = [forenames] if isinstance(forenames, str) else forenames
            surnames = [surnames] if isinstance(surnames, str) else surnames
            personal_names = ([personal_names] if isinstance(personal_names, str) else personal_names)
            # Merge personal names into forenames
            forenames.extend(personal_names)
            # Format full name
            full_name = (
                f"{' '.join(surnames)}, {' '.join(forenames)}"
                if surnames and forenames
                else " ".join(forenames + surnames)
            )
            all_names.append(full_name)

        logger.debug(f"All names: {all_names}")
        return [self._normalize_input(name) for name in all_names]

    def _extract_wikidata_id(self, same_as_lst: list) -> str | NAType:
        """Extract Wikidata ID from lobid-gnd API response."""
        for elem in same_as_lst:
            id_url = elem.get("id", "")
            if "wikidata.org" in id_url:
                return id_url.split("/")[-1]
        return pd.NA

    def _extract_geocode(self, item: dict) -> tuple[str | NAType, str | NAType]:
        """Extract geographic area code from lobid-gnd API response."""
        geocode = item.get("geographicAreaCode", None)
        if geocode:
            logger.debug(f"Found geocode: {geocode}. Extracting only first.")
            geo_id = geocode[0]["id"]
            geo_label = geocode[0]["label"]
            logger.info(f"GND areacode is: {geo_id}")
            return geo_id, geo_label
        else:
            logger.info("No GND areacode")
            return pd.NA, pd.NA

    def _extract_titles(self, row: pd.Series, title_columns: list[str]) -> list[str]:
        """Extract titles from row for specified title columns."""
        titles = []
        for col in title_columns:
            value = row.get(col, pd.NA)
            if col == "Aliases" and isinstance(value, list):
                titles.extend(value)
            elif pd.notna(value):
                titles.append(value)
        return titles

    def _normalize_input(self, data: str) -> str:
        """Normalize input strings by removing accents, question marks, exclamation marks, lowercasing, and trimming whitespace."""
        if pd.isna(data):
            return ""
        data = data.lower()
        data = unicodedata.normalize("NFD", data)
        data = "".join(
            letter for letter in data if unicodedata.category(letter) != "Mn"
        )
        data = re.sub(r"(\w\.)\s(\w\.)", r"\1\2", data)
        data = re.sub(r"[!?]", "", data)
        data_normalized = data.strip()
        logger.debug(f"Input normalized: {data_normalized}.")
        return data_normalized

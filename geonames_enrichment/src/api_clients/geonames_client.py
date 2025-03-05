from __future__ import annotations  # Handle forward reference in typehint for __enter__ method
import logging
import pandas as pd
from pandas._libs.missing import NAType  # for type hints
import requests  # for type hints
from types import TracebackType  # for type hints
import xml.etree.ElementTree as ET
from src.http_client.http_client import HttpClient

logger = logging.getLogger(__name__)


class GeoNamesAPIError(Exception):
    """Exception raised when the GeoNames API maximum requests are exceeded."""

    def __init__(self, message: str) -> None:
        super().__init__(f"GeoNames API error: {message}. \n Try setting the rate limit for GeoNamesClient to '1000/hour'.")


class GeoNamesClient:
    """
    GeoNamesClient interacts with the GeoNames API to fetch latitude and longitude
    for a given GeoNames ID. Can be used as a context manager.
    """

    def __init__(self, username: str, rate_limit: str, base_url: str = "http://api.geonames.org/get",) -> None:
        self.username = username
        self.http_client = self._initialize_http_client(rate_limit)
        self.base_url = base_url

    @staticmethod
    def _initialize_http_client(rate_limit: str) -> HttpClient:
        """Create and return a new HttpClient instance with rate limiting."""
        return HttpClient(rate_limit)

    def __enter__(self) -> GeoNamesClient:
        """Enable the use of GeoNamesClient as a context manager."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        """Ensure HttpClient is closed when exiting the context"""
        if self.http_client:
            self.http_client.__exit__(exc_type, exc_value, traceback)

    def get_geonames_data(self, geoname_id: str) -> tuple[float | NAType, float | NAType]:
        """Retrieve latitude and longitude for a GeoName ID from the GeoNames API."""
        response = self._fetch_geonames_page(geoname_id)
        if response is None:
            logger.warning(f"Failed to fetch data for GeoNames ID: {geoname_id}")
            return pd.NA, pd.NA
        xml_content = ET.fromstring(response.content)
        status_element = xml_content.find("status")
        if status_element is not None and "message" in status_element.attrib:
            message = status_element.attrib["message"]
            logger.error(f"GeoNames API error: {message}")
            raise GeoNamesAPIError(message)

        # Extract name (for debugging), latitude, and longitude
        name = xml_content.findtext("name")
        logger.debug(f"GeoNames ID {geoname_id} resolves to name {name}")
        latitude = xml_content.findtext("lat")
        longitude = xml_content.findtext("lng")

        if latitude is None or longitude is None:
            logger.warning(f"Failed to fetch data for GeoNames ID: {geoname_id}")
            return pd.NA, pd.NA

        return float(latitude), float(longitude)

    def _fetch_geonames_page(self, geoname_id: str) -> requests.Response | None:
        """Fetch XML data for a specified GeoName ID from the GeoNames API."""
        logger.info(f"Fetching data for GeoNames ID: {geoname_id}")
        request_url = f"{self.base_url}?geonameId={geoname_id}&username={self.username}"
        return self.http_client.fetch_page(request_url)

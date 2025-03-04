import logging
import pandas as pd
from pandas._libs.missing import NAType  # for type hints
import pywikibot  # type:ignore


logger = logging.getLogger(__name__)


class WikidataClient:
    """
    WikidataClient interacts with the Wikidata API using pywikibot to fetch properties and labels.
    """

    def __init__(self):
        self.site = pywikibot.Site("wikidata", "wikidata")
        self.repo = self.site.data_repository()

    def get_wikidata_property(
        self, row: pd.Series, p_number: str, q_col: str
    ) -> str | NAType:
        """Fetch a specified Wikidata property (p_number) for a Wikidata Q-number in the row. If there is more than one claim, return first."""

        q_number = row[q_col]
        logger.info(f"Fetching {p_number} for {q_number}")
        if pd.isna(q_number):
            logger.debug("No Q-number")
            return pd.NA

        item = self._fetch_wikidata_item(q_number)

        countries = []
        # Retrieve value(s) for country of origin (P495)
        if p_number in item.claims:
            for claim in item.claims[p_number]:
                country_item = claim.getTarget()

                if (
                    country_item is None
                ):  # handle case that country_item is "unknown value" such as here: https://www.wikidata.org/wiki/Q4233718, in this case getTarget returns None and calling .labels causes and exception
                    logger.debug(f"No data for {q_number}")
                    return pd.NA

                country_name = country_item.labels.get("en", pd.NA)  # fallback_value
                countries.append(country_name)

        if len(countries) == 0:  # countries is empty list , or if not countries
            logger.debug(f"No data for {q_number}")
            return pd.NA

        logger.debug(
            f"Found claim for {p_number} for {q_number}: {countries}. Extracting only first."
        )
        country = countries[0]
        return country

    def get_wikidata_labels(self, row, q_col):
        """Retrieve the German label and all aliases for a Wikidata Q-number in the row."""

        q_number = row[q_col]

        if pd.isna(q_number):
            logger.debug("No Q-number")
            return pd.NA, pd.NA  # prev: '', []

        item = self._fetch_wikidata_item(q_number)

        german_label = item.labels.get("de", pd.NA)

        aliases = []
        for labels in item.aliases.values():
            aliases.extend(labels)
        if not aliases:
            aliases = pd.NA

        logger.debug(f"Found labels for {q_number}: {german_label}, {aliases}.")
        return german_label, aliases

    def _fetch_wikidata_item(self, q_number):
        """Fetch a Wikidata ItemPage by its Q-number. Automatically resolves redirects if necessary."""

        site = pywikibot.Site("wikidata", "wikidata")
        repo = site.data_repository()

        # Retrieve the Wikidata item using the Q-number
        item = pywikibot.ItemPage(repo, q_number)
        try:
            item.get()
        except (
            pywikibot.exceptions.IsRedirectPageError
        ):  # test case: wikidata:Q42191769
            logger.error(
                f"Page [[wikidata:{q_number}]] is a redirect page. Trying to resolve the redirect..."
            )
            item = item.getRedirectTarget()
            item.get()
        return item

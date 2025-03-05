import logging
import pandas as pd

# for type hints
from src.api_clients.geonames_client import GeoNamesClient
from src.api_clients.gnd_client import GndClient
from src.api_clients.wikidata_client import WikidataClient

from src.data_processing.data_mapping import wikidata_to_gnd, gnd_to_geonames, match_arealabel


logger = logging.getLogger(__name__)


def enrich_with_geolocation(df: pd.DataFrame, gnd_client: GndClient, wikidata_client: WikidataClient, geonames_client: GeoNamesClient, no_pages: int = 100,) -> pd.DataFrame:
    """ Enrich pandas Dataframe containing book metadata with GeoNames IDs, Wikidata properties, latitude and longitude."""
    logger.info("Starting geolocation enrichment...")

    # Add empty columns
    new_cols = [
        "GND Areacode",
        "GND Arealabel",
        "note",
        "German Title",
        "Aliases",
        "P495",
        "P19",
        "Geonames ID",
        "GND Mapping",
        "Latitude",
        "Longitude",
    ]
    df[new_cols] = pd.NA

    # Fetch GND Areacodes for Book Title and Original/Alt Title
    results = df.apply(
        gnd_client.get_gnd_areacode,
        axis=1,
        args=(no_pages, ("Book Title", "Original/Alt Title")),
    )
    df["GND Areacode"], df["GND Arealabel"], df["note"] = zip(*results)

    # Retrieve German Title and Aliases where GND Areacode is missing
    remaining_rows = df[df["GND Areacode"].isna()]
    german_labels, aliases = zip(
        *remaining_rows.apply(
            wikidata_client.get_wikidata_labels, 
            axis=1, 
            args=("Work Wikidata ID",)
        )
    )
    df.loc[remaining_rows.index, "German Title"] = pd.Series(german_labels, index=remaining_rows.index, dtype="str")
    df.loc[remaining_rows.index, "Aliases"] = pd.Series(aliases, index=remaining_rows.index, dtype="object")

    # Fetch GND Areacodes for German Title and Aliases
    remaining_rows = df[df["GND Areacode"].isna()]
    results = remaining_rows.apply(
        gnd_client.get_gnd_areacode,
        axis=1,
        args=(no_pages, ("German Title", "Aliases")),
    )
    gnd_areacode, gnd_arealabel, note = zip(*results)
    df.loc[remaining_rows.index, "GND Areacode"] = pd.Series(gnd_areacode, index=remaining_rows.index, dtype="str")
    df.loc[remaining_rows.index, "GND Arealabel"] = pd.Series(gnd_arealabel, index=remaining_rows.index, dtype="str")
    df.loc[remaining_rows.index, "note"] = pd.Series(note, index=remaining_rows.index, dtype="str")

    df["GND Areacode"] = df["GND Areacode"].str.strip()

    # Fetch P495 (Country of Origin) and P19 (Place of Birth)
    remaining_rows = df[df["GND Areacode"].isna() | (df["GND Areacode"] == "https://d-nb.info/standards/vocab/gnd/geographic-area-code#ZZ")]
    results = remaining_rows.apply(
        wikidata_client.get_wikidata_property, 
        axis=1, 
        args=("P495", "Work Wikidata ID")
    )
    df.loc[remaining_rows.index, "P495"] = results

    remaining_rows = df[(df["GND Areacode"].isna() | (df["GND Areacode"] == "https://d-nb.info/standards/vocab/gnd/geographic-area-code#ZZ")) & df["P495"].isna()]
    results = remaining_rows.apply(
        wikidata_client.get_wikidata_property,
        axis=1,
        args=("P19", "Author Wikidata ID"),
    )
    df.loc[remaining_rows.index, "P19"] = results

    # Map Wikidata P495 values to GND areacodes
    arealabel_dict = wikidata_to_gnd()
    df["GND Mapping"] = df["P495"].apply(match_arealabel, args=(arealabel_dict,))

    # Map GND IDs to Geonames IDs
    geonames_dict = gnd_to_geonames()
    df["GND Areacode"] = df["GND Areacode"].str.strip()
    df["GND Mapping"] = df["GND Mapping"].str.strip()

    remaining_rows = df[df["Geonames ID"].isna()]
    df.loc[remaining_rows.index, "Geonames ID"] = remaining_rows["GND Areacode"].apply(
        lambda x: geonames_dict.get(x, pd.NA)
    )

    remaining_rows = df[df["Geonames ID"].isna()]
    df.loc[remaining_rows.index, "Geonames ID"] = remaining_rows["GND Mapping"].apply(
        lambda x: geonames_dict.get(x, pd.NA)
    )

    # Fetch latitude and longitude from Geonames API
    geonames_df = df.copy()
    geonames_df["Geonames ID"] = geonames_df["Geonames ID"].str.extract(
        r"(\d+)", expand=False
    )

    latitude_list = []
    longitude_list = []
    for geoname_id in geonames_df["Geonames ID"]:
        if pd.notna(geoname_id):
            latitude, longitude = geonames_client.get_geonames_data(geoname_id)
            latitude_list.append(latitude)
            longitude_list.append(longitude)
        else:
            latitude_list.append(pd.NA)
            longitude_list.append(pd.NA)

    geonames_df["Latitude"] = latitude_list
    geonames_df["Longitude"] = longitude_list

    df.update(geonames_df)
    logger.info("Geolocation enrichment completed.")
    return df

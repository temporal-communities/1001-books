import logging
import pandas as pd
from pandas._libs.missing import NAType  # for type hints
from rdflib import Graph, Literal
from rdflib.namespace import RDFS, SKOS

logger = logging.getLogger(__name__)


def gnd_to_geonames() -> dict:
    """Map GND Areacodes to GeoNames URIs."""

    logger.debug("Retrieving GeoNames URIs for GND Areacodes...")
    g = Graph()

    # Load RDF-XML data from URL and parse for geonames URIs
    g.parse(
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code.rdf", format="xml"
    )

    geonames_dict = {}
    for subj, pred, obj in g:
        if pred == RDFS.seeAlso and str(obj).startswith("http://www.geonames.org/"):
            geonames_dict[str(subj)] = str(obj)

    # Map GND areacodes for Roman Empire (XT), Ancient Greece (XS), Arab Countries (XX) to GeoNames IDs for Italy, Greece, Arab Gulf countries
    # Map GND areacodes for Czechoslovakia (XA-CSHH) and Soviet Union to GeoNames IDs for Czech Republic, Russian Federation
    special_cases_dict = {
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XS": "http://www.geonames.org/390903",
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XT": "http://www.geonames.org/8354456",
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XX": "https://www.geonames.org/12218088",
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-CSHH": "http://www.geonames.org/3077311",
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-SUHH": "http://www.geonames.org/2017370",
    }
    geonames_dict.update(special_cases_dict)

    return geonames_dict


def wikidata_to_gnd() -> dict:
    """Map english language GND arealabels and Wikidata values for property P495 to GND areacodes."""

    logger.debug(
        "Mapping english language GND arealabels and Wikidata values to GND areacodes..."
    )
    g = Graph()

    # Load RDF-XML data from URL and parse for English language arealabels
    g.parse(
        "https://d-nb.info/standards/vocab/gnd/geographic-area-code.rdf", format="xml"
    )

    arealabel_dict = {}
    for subj, pred, obj in g:
        if pred == SKOS.prefLabel and isinstance(obj, Literal) and obj.language == "en":
            arealabel_dict[str(obj)] = str(subj)

    # Map Wikidata labels that do not correspond to GND arealabels to GND areacodes
    special_cases_dict = {
        "People's Republic of China": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XB-CN",
        "Yuan dynasty": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XB-CN",
        "Ming dynasty": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XB-CN",
        "England": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB",
        "Captaincy General of Guatemala": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XD-GT",
        "United Kingdom": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB",
        "Scotland": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB",
        "Russia": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-RU",
        "Russian Empire": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-RU",
        "United States of America": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XD-US",
        "Republic of Ireland": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-IE",
        "Kingdom of the Netherlands": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-NL",
        "German Democratic Republic": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-DE",
        "West Germany": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-DE",
        "Wales": "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB",
    }

    arealabel_dict.update(special_cases_dict)

    return arealabel_dict


def match_arealabel(p_value: str, gnd_arealabel_dict: dict) -> str | NAType:
    """Match GND arealabel to wikidata value for property P495 and return corresponding GND areacode"""
    for arealabel, areacode in gnd_arealabel_dict.items():
        if pd.notna(p_value) and arealabel == p_value:
            return areacode
    return pd.NA

import logging
from argparse import ArgumentParser

# API Clients
from src.api_clients.geonames_client import GeoNamesClient
from src.api_clients.gnd_client import GndClient
from src.api_clients.wikidata_client import WikidataClient

# Data Processing
from src.data_processing.io_utils import read_input_df, save_enriched_df
from src.data_processing.data_enrichment import enrich_with_geolocation
from src.data_processing.geo_mapping import make_map

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("geolocation_enrichment.log"),  # rename
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def main():
    parser = ArgumentParser()
    parser.add_argument("-i", "--input_path")
    parser.add_argument("-o", "--output_dir")
    parser.add_argument("-f", "--output_filename")
    parser.add_argument("-u", "--geonames_username")
    args = parser.parse_args()

    logger.info("Starting geolocation enrichment pipeline...")

    # Define paths
    input_path = args.input_path
    output_dir = args.output_dir
    output_filename = args.output_filename
    geonames_username = args.geonames_username

    # Define output map title
    map_titles = {
        "de": "Geografische Verteilung der Werke nach Geodaten aus GND und Wikidata",
        "en": "Geographical Distribution of Works Based on Geodata from GND and Wikidata",
    }

    # Step 1: Load input data
    logger.info("Loading input data...")
    df = read_input_df(input_path)

    # Step 2: Initialize API clients
    logger.info("Initializing API clients...")
    wikidata_client = WikidataClient()
    with GndClient("5/second") as gnd_client, GeoNamesClient(geonames_username, "1000/hour") as geonames_client:
        # Step 3: Enrich data with geolocation information
        logger.info("Enriching data with geolocation information...")
        enriched_df = enrich_with_geolocation(
            df,
            gnd_client=gnd_client,
            wikidata_client=wikidata_client,
            geonames_client=geonames_client,
        )

    # Step 4: Save enriched data
    logger.info("Saving enriched data...")
    save_enriched_df(enriched_df, filename=output_filename, output_dir=output_dir)
    logger.info(f"Enriched data saved to {output_dir}.")

    # Step 5: Generate map
    logger.info("Generating map...")
    make_map(enriched_df, map_title=map_titles["de"], output_dir=output_dir, lang="de")
    logger.info(f"Map saved to {output_dir}.")


if __name__ == "__main__":
    main()

# Geolocation enrichment 

## Contents

- `src/api_clients`: Clients for interacting with the lobid-gnd, GeoNames and Wikidata APIs 
- `src/http_client`: Generic HTTP client for making HTTP requests used by the GeoNames and lobid-gnd clients
- `src/data_processing`: Functions for I/O operations, data enrichment logic, mappings between Wikidata, GND and GeoNames data, and for creating the output map.

## Sample usage

**Installation:**

`$ pip install -r requirements.txt`

This also installs [visidata](https://github.com/saulpw/visidata). Remove visidata, importlib_metadata and zipp if you do not wish to open the enriched TSV file in visidata.

Recommended Python version: 3.12.0.

**Run pipeline:**

`$ python main.py -i "input/1001-books-plus-wikidata.tsv" -o "outputs" -f "1001-books-plus-wikidata-plus-geonames" -u "geonames-username"`

Running the enrichment pipeline requires a GeoNames username. To create a GeoNames account, visit https://www.geonames.org/login. 

Since execution speed was not a priority when writing the code, the implementation is entirely synchronous, which makes running the pipeline slower than it could be. Wall time for running the pipeline with default settings to enrich 1155 rows containing Geonames IDs from `1001-books-plus-wikidata.tsv` with rate limits of 5 requests/second for lobid-gnd and 2 requests/second for GeoNames is approximately 30 minutes. However, due to [GeoNames API usage restrictions](https://www.geonames.org/export/), the same application (identified by username) can only make 1000 requests per hour. If this is the case, set the rate limit for GeoNamesClient to "1000/hour". 

## Note on data mapping

In order to retrieve as many GeoNames IDs as possible, several GND areacodes which were not mapped to GeoNames IDs by the GND were manually mapped to GeoNames IDs. Wikidata entites with labels that did not correspond to GND arealabels were manually mapped to GND areacodes for the same reason. For the mappings, see `src/data_processing/data_mapping.py`. Mappings are mostly pragmatic and sometimes not historically or politically accurate (f.e. GND Areacode for Arab countries is mapped to GeoNames ID for Arab Gulf countries). The aim was to create points on the map geographically closest to the center of the larger political entity. 

GND records contain country codes based on the work’s place of origin, the author’s country of origin (or the country they were primarily based), or the language of the text ([Deutsche Nationalbibliothek 2024, p. 19](file:///Users/lipogg/Downloads/laendercodeleitfaden-3.pdf)). Wikidata property *P495 country of origin* (of a creative work) was chosen to augment the GND areacodes. 

For works without a Wikidata Work ID and for works with Wikidata Work ID but without property P495, *P19 place of birth* was retrieved as a fallback for the corresponding Wikidata author ID. An alternative mapping might have been *P27 country of citizenship*, however this property was available for only for a few of the remaining entities and P19 was chosen for its larger coverage.
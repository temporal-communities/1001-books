import polars as pl
import requests


def format_query(query: str, qids: list[str]) -> str:
    # Create a formatted string of Wikidata IDs prefixed with "wd:"
    formatted_qids = " ".join([f"wd:{qid}" for qid in qids])
    return query % formatted_qids


def query_wdqs(query: str, qids: list[str]) -> pl.DataFrame:
    url = "https://query.wikidata.org/sparql"

    formatted_query = format_query(query, qids)
    res = requests.post(
        url,
        data={"query": formatted_query},
        headers={"Accept": "text/tab-separated-values"},
    )
    print(f"Request took {res.elapsed.total_seconds()} seconds.")

    return (
        pl.read_csv(res.content, separator="\t")
        .rename(lambda s: s.strip("?"))
        .with_columns(
            pl.all()
            .str.strip_chars("<>")  # Remove < > around URLs
            .str.strip_prefix("http://www.wikidata.org/entity/"),
        )
    )

import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def read_input_df(df_path: str) -> pd.DataFrame:
    """Read and return pandas DataFrame from a TSV file with specified dtypes."""

    df_path = Path(df_path)
    # Load only header row to get column names
    input_cols = pd.read_csv(df_path, sep="\t", encoding="UTF-8", nrows=0).columns
    int_cols = ["ID", "Wilson score"]

    # Set all columns to str except the ones in int_cols, which are Int32 (pd.Int32Dtype())
    dtype_dict = {col: ("Int32" if col in int_cols else "str") for col in input_cols}

    input_df = pd.read_csv(df_path, sep="\t", encoding="UTF-8", dtype=dtype_dict, na_values=["NaN"])

    input_df.fillna(value=pd.NA, inplace=True)
    return input_df


def save_enriched_df(df: pd.DataFrame, filename: str, output_dir: str, debug_mode: bool = False) -> None:
    """Save pandas DataFrame as pickle and TSV file in the outputs directory."""

    columns_to_remove = ["GND Mapping", "German Title", "Aliases", "note"]
    if not debug_mode:
        # Clean up dataframe before export
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])

    # Define outputs directory and create it if it doesn't exist
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    # Create file paths using pathlib
    pkl_path = output_dir / f"{filename}.pkl"
    tsv_path = output_dir / f"{filename}.tsv"

    df.to_pickle(pkl_path)
    df.to_csv(tsv_path, sep="\t", encoding="utf-8", index=False)

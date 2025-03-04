import logging
import pandas as pd  # for type hints
import geopandas as gpd
import geodatasets
import matplotlib.pyplot as plt
from pathlib import Path

logger = logging.getLogger(__name__)


def make_map(df: pd.DataFrame, map_title: str, output_dir: str, lang: str = "en", scaling_factor: int = 5) -> None:
    """Generate world map of latitudes and longitudes from a pandas dataframe."""

    # Step 1: Validate input_df
    required_columns = {"Latitude", "Longitude"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing columns: {required_columns - set(df.columns)}")

    # Step 2: Prepare Geodataframe
    # Subset df only column containing Latitude and Longitude
    geonames_df = df.dropna(subset=["Latitude", "Longitude"], inplace=False)
    # Create Geodataframe with point geometries
    geonames_df["geometry"] = gpd.points_from_xy(
        geonames_df.Longitude, geonames_df.Latitude
    )
    geo_df = gpd.GeoDataFrame(geonames_df, geometry="geometry")
    # Aggregate occurrences
    grouped = (
        geo_df.groupby(["Latitude", "Longitude"]).size().reset_index(name="counts")
    )
    # Merge the counts back to the original Geodataframe
    geo_df = geo_df.merge(grouped, on=["Latitude", "Longitude"])
    # Scale the area of each point proportionally to the number of occurrences at each latitude and longitude
    geo_df["point_size"] = geo_df["counts"] * scaling_factor
    # Sort to prevent large bubbles overlapping small ones
    geo_df = geo_df.sort_values(by="point_size", ascending=False)

    # Step 3: Plot map
    # Load world map
    world = gpd.read_file(geodatasets.get_path("naturalearth.land"))
    # Create figure and axes object
    plt.figure(figsize=(15, 10))
    ax = plt.gca()  # Get the current axis
    # Plot the world map
    world.boundary.plot(ax=ax, alpha=0.4, color="black", linewidth=1)
    # Plot the points
    geo_df.plot(
        ax=ax,
        markersize=geo_df["point_size"],
        color="#1f77b4",
        edgecolor="white",
        alpha=0.6,
    )
    # Set labels and filename
    if lang == "de":
        xlab, ylab, filename = "Längengrad", "Breitengrad", "map-de"
    else:
        xlab, ylab, filename = "Longitude", "Latitude", "map-en"
    # Plot axes and title
    plt.title(map_title)
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.grid(False)

    # Step 4: Save map
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    map_path = output_dir / f"{filename}.png"
    plt.savefig(map_path, dpi=600)

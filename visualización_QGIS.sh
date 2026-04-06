python3 - << 'EOF'
import geopandas as gpd
import pandas as pd
from shapely import wkt

# Query 1
df1 = pd.read_csv("/mnt/hdfs/resultados/query1_schools.csv")
df1 = df1[df1["geo_wkt"] != "geo_wkt"].dropna(subset=["geo_wkt"])
df1["geometry"] = df1["geo_wkt"].apply(wkt.loads)

gpd.GeoDataFrame(df1, geometry="geometry", crs="EPSG:4326") \
    .drop(columns=["geo_wkt"]) \
    .to_file("/mnt/hdfs/resultados/query1_schools.geojson", driver="GeoJSON")

print(f"Query 1: {len(df1)} objetos exportados")

# Query 2
df2 = pd.read_csv("/mnt/hdfs/resultados/query2_tracks.csv")
df2 = df2[df2["building_wkt"] != "building_wkt"].dropna(subset=["building_wkt"])
df2["geometry"] = df2["building_wkt"].apply(wkt.loads)

gpd.GeoDataFrame(df2, geometry="geometry", crs="EPSG:4326") \
    .drop(columns=["building_wkt", "highway_wkt"]) \
    .to_file("/mnt/hdfs/resultados/query2_tracks.geojson", driver="GeoJSON")

print(f"Query 2: {len(df2)} objetos exportados")
EOF
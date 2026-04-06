cat > /mnt/hdfs/sedona_osm.py << 'EOF'
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Sedona_OSM_Brasil") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .getOrCreate()

sedona = SedonaContext.create(spark)
spark.sparkContext.setLogLevel("WARN")

HDFS = "hdfs://worker1:9000/user/hadoop/osm"

def load(path, name):
    df = sedona.read.format("csv") \
        .option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .load(path)
    df.createOrReplaceTempView(name)
    print(f"Cargado {name}: {df.count():,} registros")

load(f"{HDFS}/brazil_buildings.csv", "brazil_buildings")
load(f"{HDFS}/brazil_highways.csv", "brazil_highways")
load(f"{HDFS}/brazil_points.csv", "brazil_points")

sedona.sql("""
CREATE OR REPLACE TEMP VIEW buildings_geom AS
SELECT ST_GeomFromWKT(geo) AS geo, desc
FROM brazil_buildings
""")

sedona.sql("""
CREATE OR REPLACE TEMP VIEW highways_geom AS
SELECT ST_GeomFromWKT(geo) AS geo, desc
FROM brazil_highways
""")

# Consulta 1 - Range Query
t0 = time.time()
r1 = sedona.sql("""
SELECT ST_AsText(geo) AS geo_wkt, desc
FROM buildings_geom
WHERE ST_Contains(
    ST_GeomFromWKT('POLYGON ((-46.70 -23.60, -46.60 -23.60,
                              -46.60 -23.50, -46.70 -23.50,
                              -46.70 -23.60))'),
    geo
)
AND desc = 'school'
""")

print(f"Q1 - Escuelas en ventana: {r1.count():,} ({time.time() - t0:.2f}s)")
r1.write.mode("overwrite").option("header", "true") \
    .csv(f"{HDFS}/results/query1_schools")

# Consulta 2 - Spatial Join
t0 = time.time()
r2 = sedona.sql("""
SELECT ST_AsText(b.geo) AS building_wkt,
       ST_AsText(h.geo) AS highway_wkt
FROM buildings_geom b, highways_geom h
WHERE ST_Intersects(b.geo, h.geo)
AND h.desc = 'track'
""")

print(f"Q2 - Tracks intersectan edificios: {r2.count():,} ({time.time() - t0:.2f}s)")
r2.write.mode("overwrite").option("header", "true") \
    .csv(f"{HDFS}/results/query2_tracks")

# Consulta 3 - Área granjas
t0 = time.time()
r3 = sedona.sql("""
SELECT ST_Area(ST_Union_Aggr(geo)) AS area_total_grados
FROM buildings_geom
WHERE desc = 'farm'
""")

r3.show()
print(f"Q3 - Área granjas: ({time.time() - t0:.2f}s)")
r3.write.mode("overwrite").option("header", "true") \
    .csv(f"{HDFS}/results/query3_farms")

print("=== Completado ===")
spark.stop()
EOF
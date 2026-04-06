spark-submit --master yarn --deploy-mode client \
--executor-memory 6g --driver-memory 4g \
--executor-cores 4 --num-executors 4 \
- << 'EOF'
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Sedona_verify") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .getOrCreate()

sedona = SedonaContext.create(spark)

HDFS = "hdfs://worker1:9000/user/hadoop/osm"

for name, path in [
    ("buildings", f"{HDFS}/brazil_buildings.csv"),
    ("highways", f"{HDFS}/brazil_highways.csv"),
    ("points", f"{HDFS}/brazil_points.csv")
]:
    df = sedona.read.format("csv").option("header", "true").load(path)
    print(f"{name}: {df.count():,} registros")

spark.stop()
EOF
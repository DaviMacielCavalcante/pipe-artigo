from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestConnection") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("=" * 50)
print("Spark Session criada com sucesso!")
print(f"Spark Version: {spark.version}")
print("=" * 50)

# Testar acesso aos volumes
import os
print("\nArquivos em /opt/spark/work-dir/:")
for item in os.listdir("/opt/spark/work-dir/"):
    print(f"  - {item}")

print("\nSchemas em /opt/spark/work-dir/metadata/schemas/:")
for item in os.listdir("/opt/spark/work-dir/metadata/schemas/"):
    print(f"  - {item}")

spark.stop()
print("\nTeste concluído!")
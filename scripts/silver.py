from json.decoder import JSONDecodeError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
import json

def load_transformation_schema(category_name: str) -> dict | None:
    
    path = f"metadata/transformations/{category_name}.json"
    
    try:
        with open(path, 'r') as f:
            schema = json.load(f)
    except (FileNotFoundError, JSONDecodeError) as e:
        print(f"Erro: {e}")
        return None
    return schema

def adjust_data_type(category_name: str, spark: SparkSession):
    
    schema = load_transformation_schema(category_name)
    
    df = spark.read.parquet(f"data/{category_name}/2025-12.parquet")

    for k, v in schema.items():
        if v != "date":
            df = df.withColumn(k, F.col(k).cast((v)))
        else:
            df = df.withColumn(k, F.to_date(k, "yyyyMMdd"))
        
    return df

def standardizing_strings(df: DataFrame):
    
    df_columns = df.columns
    
    for cl in df_columns:
        if isinstance(df.schema[cl].dataType, StringType):
            df = df.withColumn(cl, F.trim(F.lower(F.col(cl))))
            df = df.withColumn(cl, F.when(F.col(cl) == "", None).otherwise(F.col(cl)))
            df = df.withColumn(cl, F.regexp_replace(cl, " ", "_"))
            if cl == "cnpj_ou_cpf_do_socio":
                df = df.withColumn(cl, F.coalesce(F.col(cl), F.lit("não_informado")))
                
    return df

def persisting_on_cassandra(df: DataFrame, category: str):
    
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "silver") \
        .option("table", category) \
        .mode("append") \
        .save()
        
    

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Teste") \
        .master("spark://spark-master:7077") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
        
    categories = ["cnaes", "empresas", "estabelecimentos", "motivos", "municipios", "naturezas", "paises", "qualificacoes", "simples", "socios"]
    
    for category in categories:
        df = adjust_data_type(category, spark) 
        standardized_df = standardizing_strings(df)
        persisting_on_cassandra(standardized_df, category)

    spark.stop()
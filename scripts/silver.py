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

def load_validation_schema(category_name: str) -> dict | None:
    
    path = f"metadata/validations/{category_name}.json"
    
    try:
        with open(path, 'r') as f:
            schema = json.load(f)
    except (FileNotFoundError, JSONDecodeError) as e:
        print(f"Erro: {e}")
        return None
    return schema

def validating_and_filtering_data(category_name: str, df: DataFrame):
    
    validation_schema = load_validation_schema(category_name)
    
    if not validation_schema:
        return df 
    
    validation_schema = validation_schema[category_name]
    
    for column, val in validation_schema.items():
        valid_values = val["valid_values"]
        allow_null = val["allow_null"]
        
        if allow_null:
            df = df.filter(F.col(column).isNull() | F.col(column).isin(valid_values))
        else:
            df = df.filter(F.col(column).isNotNull() & F.col(column).isin(valid_values))
        
    return df    

def adjust_data_type(spark: SparkSession, category_name: str):
    
    schema = load_transformation_schema(category_name)
    
    df = spark.read.parquet(f"data/bronze/{category_name}")

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

def persisting_locally(df: DataFrame, category: str):
    
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"data/silver/{category}")
        
    

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Teste") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
        
    categories = ["cnaes", "empresas", "estabelecimentos", "motivos", "municipios", "naturezas", "paises", "qualificacoes", "simples", "socios"]
    
    for category in categories:
        df = adjust_data_type(category, spark) 
        standardized_df = standardizing_strings(df)
        validated_df = validating_and_filtering_data(standardized_df, category)
        persisting_locally(validated_df, category)

    spark.stop()
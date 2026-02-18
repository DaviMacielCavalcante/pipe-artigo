from json.decoder import JSONDecodeError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql import DataFrame
from pathlib import Path
from datetime import datetime
import json
import time

def generate_silver_report(category_name: str, df: DataFrame) -> DataFrame:
    """
    Validate data and generate statistics report for Silver layer.

    Applies validation rules, counts removed records, and saves
    a JSON report to reports/silver/{category_name}_report.json.

    Parameters
    ----------
    category_name : str
        Category name (e.g., 'empresas', 'estabelecimentos').
    df : DataFrame
        DataFrame after type adjustment and string standardization.

    Returns
    -------
    DataFrame
        Validated DataFrame with invalid records removed.

    Notes
    -----
    - Calls validating_and_filtering_data() internally.
    - Reports are saved to reports/silver/ directory.
    - Categories without validation rules will show zero removals.
    """
    start = time.time()
    
    rows_before = df.count()
    
    validated_df = validating_and_filtering_data(category_name, df)
    
    rows_after = validated_df.count()
    
    results = {
        "category": category_name,
        "timestamp": datetime.now().isoformat(),
        "validation": {
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after
        },
        "performance": {}
    }
    
    end = time.time()
    
    results["performance"]["execution_time_seconds"] = round(end - start, 2)
    
    Path("reports/silver").mkdir(exist_ok=True, parents=True)
    with open(f"reports/silver/{category_name}_report.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    return validated_df

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
            if isinstance(df.schema[column].dataType, DateType):
                df = df.filter(F.col(column).isNull() | (F.col(column) >= valid_values))
            else:
                df = df.filter(F.col(column).isNull() | F.col(column).isin(valid_values))
        else:
            if isinstance(df.schema[column].dataType, DateType):
                df = df.filter(F.col(column).isNotNull() & (F.col(column) >= valid_values))
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
        df = adjust_data_type(spark, category) 
        standardized_df = standardizing_strings(df)
        validated_df = generate_silver_report(category, standardized_df)
        persisting_locally(validated_df, category)

    spark.stop()
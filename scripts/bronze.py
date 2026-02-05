from pathlib import Path
from json.decoder import JSONDecodeError
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json
import time

def load_schema(file: str) -> dict | None:
    """
    Load a specific JSON schema from the metadata/schemas/ directory.

    Parameters
    ----------
    file : str
        Name of the JSON file to be loaded (e.g., 'cnaes.json', 'empresas.json').
        The file must be located in ./metadata/schemas/

    Returns
    -------
    dict or None
        Dictionary containing the loaded schema from the JSON file if successful.
        Expected structure: {category: {columns: [...], separator: str, ...}}
        Returns None if the file is not found or contains invalid JSON.

    Examples
    --------
    >>> schema = load_schema("cnaes.json")
    >>> if schema:
    ...     print(schema["cnaes"]["columns"])
    ['codigo', 'descricao']
    
    >>> schema = load_schema("arquivo_invalido.json")
    Erro: [Errno 2] No such file or directory: './metadata/schemas/arquivo_invalido.json'
    >>> print(schema)
    None
    """
    path = f"./metadata/schemas/{file}"
    try:
        with open(path, 'r') as f:
            schema = json.load(f)
    except (FileNotFoundError, JSONDecodeError) as e:
        print(f"Erro: {e}")
        return None
    return schema

def generate_reports(category_name: str) -> dict:
    """
    Generate statistics report for a Receita Federal data category using PySpark.
    
    Processes the latest snapshot (most recent data) for the given category.

    Parameters
    ----------
    category_name : str
        Category name (e.g., "cnaes", "empresas", "municipios").
        Must match a {category_name}.json file in metadata/schemas/

    Returns
    -------
    dict
        Dictionary containing statistics with:
        - category: category name
        - timestamp: generation timestamp (ISO format)
        - total: aggregated statistics (rows, files, size_mb, unique_codes)
        
    Notes
    -----
    - Requires active SparkSession in global scope
    - Reads CSV files from cnpjs_receita_federal/2025-12/{Category}/*.csv
    - All columns are read as StringType to avoid type inference issues
    - Saves JSON report to reports/{category_name}_report.json
    
    Examples
    --------
    >>> # Create SparkSession first
    >>> spark = SparkSession.builder.appName("CNPJ").master("local[*]").getOrCreate()
    >>> 
    >>> # Generate report
    >>> result = generate_reports("cnaes")
    >>> print(result['total']['rows'])
    1359
    >>> 
    >>> # Stop Spark when done
    >>> spark.stop()
    """
    start = time.time()
    
    schema_json = load_schema(f"{category_name}.json")
    
    code_col = schema_json[category_name]["columns"][0]
    
    columns = schema_json[category_name]["columns"]
    schema = StructType([
            StructField(column, StringType(), True)
            for column in columns
        ])

    results = {
        "category": category_name,
        "timestamp": datetime.now().isoformat(),
        "total": {
            "rows": 0,
            "files": 0,
            "size_mb": 0,
            "unique_codes": 0
        },
        "performance": {}
    }
        
    month_str = "2025-12"
    
    csv_files = list(Path("cnpjs_receita_federal/").glob(f"{month_str}/{category_name.capitalize()}*/*.csv"))
    
    if not csv_files:
        print(f"Nenhum arquivo encontrado para {category_name}")
        return results
    
    df = spark.read \
    .option("header", "false") \
    .option("delimiter", ";") \
    .option("encoding", "ISO-8859-1") \
    .schema(schema) \
    .csv([str(file) for file in csv_files])
    
    rows = df.count()
    size = sum(file.stat().st_size / (1024**2) for file in csv_files)
    unique_codes = df.select(code_col).distinct().count()
            
    results["total"]["rows"] = rows
    results["total"]["files"] = len(csv_files)
    results["total"]["size_mb"] = round(size, 2)
    results["total"]["unique_codes"] = unique_codes
    
    end = time.time()
    
    total_time = end - start
    
    results["performance"]["execution_time_seconds"] = round(total_time, 2)

    Path("reports").mkdir(exist_ok=True)
    with open(f"reports/{category_name}_report.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        
    return results

def generate_bronze(category_name: str) -> bool:
    """
    Convert CSV files to Parquet format (Bronze layer).
    
    Parameters
    ----------
    category_name : str
        Category to process
        
    Returns
    -------
    bool
        True if successful, False if no files found
    """
    
    csv_files = list(Path("cnpjs_receita_federal/").glob(f"2025-12/{category_name.capitalize()}*/*.csv"))
    
    
    if not csv_files:
        print(f"Nenhum arquivo encontrado para {category_name}")
        return False
    
    schema_json = load_schema(f"{category_name}.json")
    
    columns = schema_json[category_name]["columns"]
    schema = StructType([
            StructField(column, StringType(), True)
            for column in columns
        ])
    
    df = spark.read \
    .option("header", "false") \
    .option("delimiter", ";") \
    .option("encoding", "ISO-8859-1") \
    .schema(schema) \
    .csv([str(file) for file in csv_files])
    
    Path(f"data/{category_name}").mkdir(exist_ok=True, parents=True)
    df.write.mode("overwrite").option("compression", "snappy").parquet(f"data/{category_name}/2025-12.parquet")
    
    return True

spark = SparkSession.builder \
    .appName("Teste") \
    .master("spark://spark-master:7077[4]") \
    .getOrCreate()
    
generate_reports("cnaes")
generate_reports("municipios")
generate_reports("paises")
generate_reports("naturezas")
generate_reports("motivos")
generate_reports("qualificacoes")
generate_reports("simples")
generate_reports("empresas")
generate_reports("estabelecimentos")
generate_reports("socios")

generate_bronze("cnaes")
generate_bronze("municipios")
generate_bronze("paises")
generate_bronze("naturezas")
generate_bronze("motivos")
generate_bronze("qualificacoes")
generate_bronze("simples")
generate_bronze("empresas")
generate_bronze("estabelecimentos")
generate_bronze("socios")

spark.stop()
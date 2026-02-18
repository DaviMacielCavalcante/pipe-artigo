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


def get_schema_for_category(category_name: str) -> dict:
    """
    Build a PySpark StructType schema from a JSON schema file.

    Parameters
    ----------
    category_name : str
        Category name (e.g., 'cnaes', 'empresas').
        Must match a {category_name}.json file in metadata/schemas/

    Returns
    -------
    dict
        Dictionary containing:
        - schema : StructType
            PySpark schema with all columns as StringType.
        - columns : list of str
            List of column names from the JSON schema.

    Examples
    --------
    >>> result = get_schema_for_category("cnaes")
    >>> result["columns"]
    ['codigo', 'descricao']
    """

    
    schema_json = load_schema(f"{category_name}.json")
    
    columns = schema_json[category_name]["columns"]
    schema = StructType([
            StructField(column, StringType(), True)
            for column in columns
        ])
    
    return {
        "schema": schema,
        "columns": columns
    }
    

def read_CSVs(category_name: str, spark: SparkSession):
    """
    Read CSV files for a Receita Federal data category into a Spark DataFrame.

    Locates CSV files in cnpjs_receita_federal/2025-12/{Category}*/,
    loads the corresponding JSON schema, and reads the files using PySpark.

    Parameters
    ----------
    category_name : str
        Category name (e.g., 'cnaes', 'empresas').

    Returns
    -------
    dict
        Dictionary containing:
        - csv_files : list of Path
            List of CSV file paths found.
        - schema_and_columns : dict
            Output of get_schema_for_category (schema and column names).
        - dataframe : pyspark.sql.DataFrame
            Spark DataFrame with the loaded data.

    Raises
    ------
    FileNotFoundError
        If no CSV files are found for the given category.
    """
    csv_files = list(Path("cnpjs_receita_federal/").glob(f"2025-12/{category_name.capitalize()}*/*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {category_name}")
    
    schema_and_columns = get_schema_for_category(category_name)
    
    df = spark.read \
    .option("header", "false") \
    .option("delimiter", ";") \
    .option("encoding", "ISO-8859-1") \
    .option("quote", '"') \
    .schema(schema_and_columns["schema"]) \
    .csv([str(file) for file in csv_files])
    
    return {
        "csv_files": csv_files, 
        "schema_and_columns": schema_and_columns, 
        "dataframe": df            
    }    

def generate_reports(category_name: str, spark: SparkSession) -> dict:
    """
    Generate statistics report for a Receita Federal data category using PySpark.

    Processes the latest snapshot (most recent data) for the given category.

    Parameters
    ----------
    category_name : str
        Category name (e.g., 'cnaes', 'empresas', 'municipios').
        Must match a {category_name}.json file in metadata/schemas/
    spark : SparkSession
        Active PySpark session passed to read_CSVs.

    Returns
    -------
    dict
        Dictionary containing statistics with:
        - category : str
            Category name.
        - timestamp : str
            Generation timestamp (ISO format).
        - total : dict
            Aggregated statistics (rows, files, size_mb, unique_codes).
        - performance : dict
            Execution time in seconds.

    Notes
    -----
    - Reads CSV files via read_CSVs.
    - All columns are read as StringType to avoid type inference issues.
    - Saves JSON report to reports/{category_name}_report.json

    Examples
    --------
    >>> spark = SparkSession.builder.appName("CNPJ").master("local[*]").getOrCreate()
    >>> result = generate_reports("cnaes", spark)
    >>> print(result['total']['rows'])
    1359
    >>> spark.stop()
    """
    start = time.time()
    
    category_pack = read_CSVs(category_name, spark)
    
    code_col = category_pack["schema_and_columns"]["columns"][0]
    
    
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
    
    df = category_pack["dataframe"]
    
    rows = df.count()
    size = sum(file.stat().st_size / (1024**2) for file in category_pack["csv_files"])
    unique_codes = df.select(code_col).distinct().count()
            
    results["total"]["rows"] = rows
    results["total"]["files"] = len(category_pack["csv_files"])
    results["total"]["size_mb"] = round(size, 2)
    results["total"]["unique_codes"] = unique_codes
    
    end = time.time()
    
    total_time = end - start
    
    results["performance"]["execution_time_seconds"] = round(total_time, 2)

    Path("reports/bronze").mkdir(exist_ok=True)
    with open(f"reports/bronze/{category_name}_report.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        
    return results

def generate_bronze(category_name: str, spark: SparkSession) -> bool:
    """
    Convert CSV files to Parquet format (Bronze layer).

    Parameters
    ----------
    category_name : str
        Category to process.
    spark : SparkSession
        Active PySpark session passed to read_CSVs.

    Returns
    -------
    bool
        True if successful.

    Raises
    ------
    FileNotFoundError
        If no CSV files are found for the given category (raised by read_CSVs).
    """
    
    category_pack = read_CSVs(category_name, spark)
    
    df = category_pack["dataframe"]
    
    Path(f"data/bronze/{category_name}").mkdir(exist_ok=True, parents=True)
    df.write.mode("overwrite").option("compression", "snappy").parquet(f"data/bronze/{category_name}")
    
    return True

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Teste") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
        
    categories = [
        "cnaes", 
        "municipios", 
        "paises", 
        "naturezas", 
        "motivos", 
        "qualificacoes", 
        "simples", 
        "empresas", 
        "estabelecimentos", 
        "socios"
    ]
    
    for category in categories:
        generate_reports(category, spark)
        generate_bronze(category, spark)

    spark.stop()
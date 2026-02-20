from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

def persisting_on_cassandra(df: DataFrame, category: str):
    
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "gold") \
        .option("table", category) \
        .mode("append") \
        .save()
        
def persisting_on_mongodb(df: DataFrame, collection: str, pk_col: str = None):
    
    if pk_col:
        df = df.withColumnRenamed(pk_col, "_id")
    
    df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://mongodb:27017") \
        .option("database", "cnpj_db") \
        .option("collection", collection) \
        .mode("overwrite") \
        .save()
        
def empresas_por_uf(spark: SparkSession):

    df_estabelecimentos = spark.read \
        .parquet("data/silver/estabelecimentos") 
    
    df_estabelecimentos_por_uf = df_estabelecimentos.groupBy("uf").agg(
       F.countDistinct("cnpj_basico").alias("quantidade")
    ).orderBy(F.desc("quantidade"))
    
    return df_estabelecimentos_por_uf

def capital_por_porte(spark: SparkSession):
    
    df_empresas = spark.read \
        .parquet("data/silver/empresas") 
        
    df_capital_por_porte = df_empresas.groupBy("porte_da_empresa").agg(
        F.avg("capital_social_da_empresa").alias("capital_medio")
    ).orderBy(F.desc("capital_medio"))
    
    df_capital_por_porte = df_capital_por_porte.withColumnRenamed("porte_da_empresa", "porte")
    df_capital_por_porte = df_capital_por_porte.filter(F.col("porte").isNotNull())
    
    return df_capital_por_porte

def socios_por_empresa(spark: SparkSession):
    
    df_empresas = spark.read \
        .parquet("data/silver/empresas")
        
    df_empresas = df_empresas.select("cnpj_basico", "razao_social_ou_nome_empresarial")
        
    df_socios = spark.read \
        .parquet("data/silver/socios")
        
    df_socios = df_socios.select("cnpj_basico", "cnpj_ou_cpf_do_socio")
    
    df_socios_por_empresa = df_empresas.join(df_socios, how="inner", on="cnpj_basico").groupBy("cnpj_basico").agg(
        F.count("cnpj_ou_cpf_do_socio").alias("quantidade_de_socios")
    )
    
    df_socios_por_empresa = df_socios_por_empresa.join(df_empresas, on="cnpj_basico").orderBy(F.desc("quantidade_de_socios")).select("cnpj_basico", "razao_social_ou_nome_empresarial", "quantidade_de_socios")
    
    return df_socios_por_empresa

def empresas_por_natureza(spark: SparkSession):
    
    df_empresas = spark.read \
        .parquet("data/silver/empresas")
        
    df_empresas = df_empresas.select("cnpj_basico", "natureza_juridica")
    
    df_naturezas = spark.read \
        .parquet("data/silver/naturezas")
        
    df_empresas_por_natureza =  df_empresas.groupBy("natureza_juridica").agg(
    F.count("cnpj_basico").alias("quantidade_de_empresas")
    )
    
    df_empresas_por_natureza = df_empresas_por_natureza.join(df_naturezas, how="inner", on=F.col("natureza_juridica") == F.col("codigo")).orderBy(F.desc("quantidade_de_empresas")).select("descricao", "quantidade_de_empresas")
    
    return df_empresas_por_natureza

def filiais_inaptas_por_empresa(spark: SparkSession):
    
    df_empresas = spark.read \
        .parquet("data/silver/empresas")
        
    df_empresas = df_empresas.select("cnpj_basico", "razao_social_ou_nome_empresarial")
    
    df_estabelecimentos = spark.read \
        .parquet("data/silver/estabelecimentos")
        
    df_estabelecimentos = df_estabelecimentos.select("cnpj_basico", "identificador_matriz_ou_filial", "situacao_cadastral")
    
    df_estabelecimentos = df_estabelecimentos.filter((F.col("identificador_matriz_ou_filial") == 2) & (F.col("situacao_cadastral") == 4))
    
    df_estabelecimentos = df_estabelecimentos.groupBy("cnpj_basico").agg(
        F.count("situacao_cadastral").alias("quantidade")
    )
    
    df_filiais_inaptas_por_empresa = df_empresas.join(df_estabelecimentos, on="cnpj_basico", how="inner").select("cnpj_basico", "razao_social_ou_nome_empresarial", "quantidade")
    
    return df_filiais_inaptas_por_empresa

def ranking_empresas_filiais_inaptas(df: DataFrame):
    
    window = Window.orderBy(F.desc("quantidade"))
    
    df = df.withColumn("rank", F.row_number().over(window))
    
    df = df.withColumn("bucket", 
        F.when(F.col("rank") <= 101, "top_100")
        .otherwise("outros")
    )
    
    df = df.select("bucket", "quantidade", "razao_social_ou_nome_empresarial", "cnpj_basico")
    
    return df
    

if __name__ == "__main__":
    
    spark = SparkSession.builder \
    .appName("Teste") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()        
      

    df_empresas_por_uf = empresas_por_uf(spark)
    persisting_on_cassandra(df_empresas_por_uf, "empresas_por_uf")
    persisting_on_mongodb(df_empresas_por_uf, "empresas_por_uf", "uf")
    

    df_capital_por_porte = capital_por_porte(spark)
    persisting_on_cassandra(df_capital_por_porte, "capital_por_porte")
    persisting_on_mongodb(df_capital_por_porte, "capital_por_porte", "porte")
    

    df_socios_por_empresa = socios_por_empresa(spark)
    persisting_on_cassandra(df_socios_por_empresa, "socios_por_empresa")
    persisting_on_mongodb(df_socios_por_empresa, "socios_por_empresa", "cnpj_basico")
    

    df_empresas_por_natureza = empresas_por_natureza(spark)
    persisting_on_cassandra(df_empresas_por_natureza, "empresas_por_natureza")
    persisting_on_mongodb(df_empresas_por_natureza, "empresas_por_natureza", "descricao")
    
    df_filiais_inaptas_por_empresa = filiais_inaptas_por_empresa(spark)
    persisting_on_cassandra(df_filiais_inaptas_por_empresa, "filiais_inaptas_por_empresa")
    persisting_on_mongodb(df_filiais_inaptas_por_empresa, "filiais_inaptas_por_empresa", "cnpj_basico")
    
    df_ranking = ranking_empresas_filiais_inaptas(df_filiais_inaptas_por_empresa)
    persisting_on_cassandra(df_ranking, "rank_empresas_filiais_inaptas")
    persisting_on_mongodb(df_ranking, "rank_empresas_filiais_inaptas")  # Sem pk_column!
    
    spark.stop()
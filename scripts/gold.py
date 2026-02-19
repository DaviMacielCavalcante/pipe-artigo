from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

def persisting_on_cassandra(df: DataFrame, category: str):
    
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "gold") \
        .option("table", category) \
        .mode("append") \
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
        F.avg("capital_social_da_empresa").alias("capital_social")
    ).orderBy(F.desc("capital_social"))
    
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
    
    df_socios_por_empresa = df_socios_por_empresa.join(df_empresas, on="cnpj_basico").orderBy(F.desc("quantidade_de_socios")).select("razao_social_ou_nome_empresarial", "quantidade_de_socios")
    
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
    
    df_empresas_por_natureza.show(10)


if __name__ == "__main__":
    
    spark = SparkSession.builder \
    .appName("Teste") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()        
    
    empresas_por_uf(spark)
    
    capital_por_porte(spark)
    
    socios_por_empresa(spark)
    
    empresas_por_natureza(spark)
    
    spark.stop()
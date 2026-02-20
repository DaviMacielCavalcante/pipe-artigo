docker compose up -d

Write-Host "Esperando o Cassandra..."
do {
    Start-Sleep -Seconds 5
    docker exec cassandra cqlsh -e "describe cluster" 2>$null
} until ($LASTEXITCODE -eq 0)
Write-Host "Cassandra pronto!"

docker exec -it cassandra cqlsh -f /opt/scripts/keyspaces.cql

docker exec -it cassandra cqlsh -f /opt/scripts/gold_tables.cql

docker exec -it spark-master bash -c "cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/bronze.py"

docker exec -it spark-master bash -c "cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/silver.py"

docker exec -it spark-master bash -c "cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 --master spark://spark-master:7077 scripts/gold.py"
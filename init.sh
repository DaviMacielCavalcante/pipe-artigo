docker compose up -d

echo "Esperando o Cassandra..."
until docker exec cassandra cqlsh -e "describe cluster" > /dev/null 2>&1; do
    sleep 5
done
echo "Cassandra pronto!"

docker exec -it cassandra cqlsh -f /opt/scripts/keyspaces.cql

docker exec -it cassandra cqlsh -f /opt/scripts/silver_tables.cql

#docker exec -it spark-master bash -c "cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/bronze.py"

docker exec -it spark-master bash -c "cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --master spark://spark-master:7077 scripts/silver.py"
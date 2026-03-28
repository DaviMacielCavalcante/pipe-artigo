CREATE KEYSPACE ycsb
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
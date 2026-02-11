CREATE KEYSPACE gold
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
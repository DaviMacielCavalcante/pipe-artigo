CREATE KEYSPACE bronze
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE KEYSPACE silver
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE KEYSPACE gold
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};


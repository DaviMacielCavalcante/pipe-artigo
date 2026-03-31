$configId = $args[0]
$instanceId = $args[1]
$seed = $args[2]
$instanceName = $args[3]

$params = @{}

for ($i = 4; $i -lt $args.Count; $i += 2) {
    $key = $args[$i] -replace "^--", ""
    $value = $args[$i + 1]
    $params[$key] = $value 
}

$cassandraYaml = Get-Content "./config/cassandra.yaml" -Raw

$cassandraYaml = $cassandraYaml -replace "concurrent_writes: \d+", "concurrent_writes: $($params['concurrent_writes'])"

$cassandraYaml = $cassandraYaml -replace "concurrent_reads: \d+", "concurrent_reads: $($params['concurrent_reads'])"

$cassandraYaml = $cassandraYaml -replace "write_request_timeout_in_ms: \d+", "write_request_timeout_in_ms: $($params['write_timeout'])"

$cassandraYaml = $cassandraYaml -replace "read_request_timeout_in_ms: \d+", "read_request_timeout_in_ms: $($params['read_timeout'])"

$cassandraYaml = $cassandraYaml -replace "row_cache_size_in_mb: \d+", "row_cache_size_in_mb: $($params['cache_size_mb'])"

$cassandraYaml = $cassandraYaml -replace "batch_size_warn_threshold_in_kb: \d+", "batch_size_warn_threshold_in_kb: $($params['batch_size'])"

$cassandraYaml | Set-Content "./config/cassandra.yaml"


$mongoConfig = Get-Content "./config/mongod.conf" -Raw

$mongoConfig = $mongoConfig -replace "wiredTigerConcurrentWriteTransactions: \d+", "wiredTigerConcurrentWriteTransactions: $($params['concurrent_writes'])"

$mongoConfig = $mongoConfig -replace "wiredTigerConcurrentReadTransactions: \d+", "wiredTigerConcurrentReadTransactions: $($params['concurrent_reads'])"


$cacheSizeGB = [math]::Round($params['cache_size_mb'] / 1024, 2)

$mongoConfig = $mongoConfig -replace "cacheSizeGB: [\d.]+", "cacheSizeGB: $cacheSizeGB"

$compressionMap = @{
    "lz4" = "zstd"
    "snappy" = "snappy"
    "none" = "none"
}

$mongoCompression = $compressionMap[$params['compression']]

$mongoConfig = $mongoConfig -replace "blockCompressor: \w+", "blockCompressor: $mongoCompression"

$mongoConfig | Set-Content "./config/mongod.conf"

docker restart cassandra
docker restart mongodb

do {
    Start-Sleep -Seconds 5
    $status = docker inspect --format='{{.State.Health.Status}}' cassandra
} while ($status -ne "healthy")


do {
    Start-Sleep -Seconds 5
    $status = docker inspect --format='{{.State.Health.Status}}' mongodb
} while ($status -ne "healthy")

docker exec cassandra cqlsh -e "DROP KEYSPACE IF EXISTS ycsb;"
docker exec cassandra cqlsh -e "CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec cassandra cqlsh -e "CREATE TABLE ycsb.usertable (y_id TEXT PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT);"

$cassandraCompressionMap = @{
    "lz4"    = "LZ4Compressor"
    "snappy" = "SnappyCompressor"
    "none"   = ""
}

$cassandraCompression = $cassandraCompressionMap[$params['compression']]

if ($cassandraCompression -eq "") {
    docker exec cassandra cqlsh -e "
    ALTER TABLE ycsb.usertable WITH compression = {'enabled': 'false'}"
} else {
    docker exec cassandra cqlsh -e "
    ALTER TABLE ycsb.usertable WITH compression = {'class': '$cassandraCompression'}"
}

$cassandraConsistencyMap = @{
    "low" = "ONE"
    "medium" = "LOCAL_QUORUM"
    "high" = "ALL"
}

$cassandraConsistency = $cassandraConsistencyMap[$params['consistency_level']]

$mongoConsistencyMap = @{
    "low" = "acknowledged"
    "medium" = "majority"
    "high" = "all"
}

$mongoConsistency = $mongoConsistencyMap[$params['consistency_level']]

$cassandraLoad = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=1000 -p cassandra.readconsistencylevel=$cassandraConsistency -p cassandra.writeconsistencylevel=$cassandraConsistency"

$cassandraLoad > cassandra_load.txt

$loadTime = Select-String -Path "cassandra_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRun = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=1000 -p cassandra.readconsistencylevel=$cassandraConsistency -p cassandra.writeconsistencylevel=$cassandraConsistency"

$cassandraRun > cassandra_run.txt

$runTime = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$totalTime = [int]$loadTime + [int]$runTime

Write-Output $totalTime
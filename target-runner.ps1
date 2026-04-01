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

$cassandraLoadTime = Select-String -Path "cassandra_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRun = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=1000 -p cassandra.readconsistencylevel=$cassandraConsistency -p cassandra.writeconsistencylevel=$cassandraConsistency"

$cassandraRun > cassandra_run.txt

$cassandraRunTime = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunOverallThroughput = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunReadAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunRead95thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunRead99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunUpdateAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraRunUpdate99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraTotalTime = [int]$cassandraLoadTime + [int]$cassandraRunTime

# Fazendo isso para evitar que as operações estourem o timeout
$mongoTimeout = [math]::Max([int]$params['write_timeout'], [int]$params['read_timeout'])

docker exec mongodb mongosh --eval "db.getSiblingDB('ycsb').dropDatabase()"

$mongoLoad = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=1000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.writeConcern=$mongoConsistency -p mongodb.url=mongodb://localhost:27017/ycsb"

$mongoLoad > mongo_load.txt

$mongoLoadTime = Select-String -Path "mongo_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRun = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=1000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.writeConcern=$mongoConsistency -p mongodb.url=mongodb://localhost:27017/ycsb"

$mongoRun > mongo_run.txt

$mongoRunTime = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunOverallThroughput = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunReadAverageLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunRead95thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunRead99thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunUpdateAverageLatency = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoRunUpdate99thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoTotalTime = [int]$mongoLoadTime + [int]$mongoRunTime

$totalTimeDbs = $cassandraTotalTime + $mongoTotalTime

Write-Output $totalTimeDbs

"metric,cassandra_value,mongodb_value,unit" | Set-Content "experiment_${configId}_${instanceName}.csv"

"time_load,$cassandraLoadTime,$mongoLoadTime,ms" | Add-Content "experiment_${configId}_${instanceName}.csv"

"time_run,$cassandraRunTime,$mongoRunTime,ms" | Add-Content "experiment_${configId}_${instanceName}.csv"

"total_time,$cassandraTotalTime,$mongoTotalTime,ms" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_overall_throughput,$cassandraRunOverallThroughput,$mongoRunOverallThroughput,ops_sec" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_read_average_latency,$cassandraRunReadAverageLatency,$mongoRunReadAverageLatency,us" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_read_p95_latency,$cassandraRunRead95thPercentileLatency,$mongoRunRead95thPercentileLatency,us" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_read_p99_latency,$cassandraRunRead99thPercentileLatency,$mongoRunRead99thPercentileLatency,us" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_update_average_latency,$cassandraRunUpdateAverageLatency,$mongoRunUpdateAverageLatency,us" | Add-Content "experiment_${configId}_${instanceName}.csv"

"run_update_p99_latency,$cassandraRunUpdate99thPercentileLatency,$mongoRunUpdate99thPercentileLatency,us" | Add-Content "experiment_${configId}_${instanceName}.csv"

type "experiment_1_run_1.csv"
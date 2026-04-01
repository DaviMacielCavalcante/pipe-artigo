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

function ConvertToMB($value) {
    if ($value -match "GiB") { return [double]($value -replace "GiB", "") * 1024 }
    if ($value -match "MiB") { return [double]($value -replace "MiB", "") }
    if ($value -match "GB")  { return [double]($value -replace "GB", "") * 1024 }
    if ($value -match "MB")  { return [double]($value -replace "MB", "") }
    if ($value -match "kB")  { return [double]($value -replace "kB", "") / 1024 }
    if ($value -match "B")   { return [double]($value -replace "B", "") / (1024*1024) }
    return 0
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
    "lz4"    = "zstd"
    "snappy" = "snappy"
    "none"   = "none"
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
    docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'enabled': 'false'};"
} else {
    docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'class': '$cassandraCompression'};"
}

$cassandraConsistencyMap = @{
    "low"    = "ONE"
    "medium" = "LOCAL_QUORUM"
    "high"   = "ALL"
}
$cassandraConsistency = $cassandraConsistencyMap[$params['consistency_level']]

$mongoConsistencyMap = @{
    "low"    = "acknowledged"
    "medium" = "majority"
    "high"   = "all"
}
$mongoConsistency = $mongoConsistencyMap[$params['consistency_level']]

$mongoTimeout = [math]::Max([int]$params['write_timeout'], [int]$params['read_timeout'])

"" | Set-Content "$PWD\stats_cassandra.csv"

$cassandraJob = Start-Job -ScriptBlock {
    param($container, $OutputFile)
    while ($true) {
        $stats = docker stats $container --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.BlockIO}}"
        $stats | Add-Content $OutputFile
        Start-Sleep -Seconds 1
    }
} -ArgumentList "cassandra", "$PWD\stats_cassandra.csv"

$cassandraLoad = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=100000 -p cassandra.readconsistencylevel=$cassandraConsistency -p cassandra.writeconsistencylevel=$cassandraConsistency -threads 20"
$cassandraLoad > cassandra_load.txt

$cassandraRun = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=100000 -p cassandra.readconsistencylevel=$cassandraConsistency -p cassandra.writeconsistencylevel=$cassandraConsistency -threads 20"
$cassandraRun > cassandra_run.txt

Stop-Job $cassandraJob
Remove-Job $cassandraJob

$cassandraLoadTime = Select-String -Path "cassandra_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunTime = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunOverallThroughput = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunReadAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunRead95thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunRead99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunUpdateAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunUpdate99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraTotalTime = [int]$cassandraLoadTime + [int]$cassandraRunTime

docker exec mongodb mongosh --eval "db.getSiblingDB('ycsb').dropDatabase()"

"" | Set-Content "$PWD\stats_mongo.csv"

$mongoJob = Start-Job -ScriptBlock {
    param($container, $OutputFile)
    while ($true) {
        $stats = docker stats $container --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.BlockIO}}"
        $stats | Add-Content $OutputFile
        Start-Sleep -Seconds 1
    }
} -ArgumentList "mongodb", "$PWD\stats_mongo.csv"

$mongoLoad = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=100000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.writeConcern=$mongoConsistency -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20"
$mongoLoad > mongo_load.txt

$mongoRun = cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=100000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.writeConcern=$mongoConsistency -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20"
$mongoRun > mongo_run.txt

Stop-Job $mongoJob
Remove-Job $mongoJob

$mongoLoadTime = Select-String -Path "mongo_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunTime = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunOverallThroughput = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunReadAverageLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunRead95thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunRead99thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunUpdateAverageLatency = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$mongoRunUpdate99thPercentileLatency = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$mongoTotalTime = [int]$mongoLoadTime + [int]$mongoRunTime

$cassandraMetrics = Get-Content "$PWD\stats_cassandra.csv" | Where-Object { $_ -ne "" }

$cassandraCpu = $cassandraMetrics | ForEach-Object {
    $fields = $_ -split ","
    [double]($fields[0] -replace "%", "")
}

$cassandraRam = $cassandraMetrics | ForEach-Object {
    $fields = $_ -split ","
    $used = ($fields[1] -split "/")[0].Trim()
    if ($used -match "GiB") {
        [double]($used -replace "GiB", "") * 1024
    } elseif ($used -match "MiB") {
        [double]($used -replace "MiB", "")
    } else {
        0
    }
}

$cassandraCpuAvg = ($cassandraCpu | Measure-Object -Average).Average
$cassandraCpuPeak = ($cassandraCpu | Measure-Object -Maximum).Maximum
$cassandraRamAvg = ($cassandraRam | Measure-Object -Average).Average
$cassandraRamPeak = ($cassandraRam | Measure-Object -Maximum).Maximum

$lastValuesCassandra = $cassandraMetrics[-1]
$ioCassandraFields = ($lastValuesCassandra -split ",")[2] -split "/"
$ioReadCassandra = ConvertToMB $ioCassandraFields[0].Trim()
$ioWriteCassandra = ConvertToMB $ioCassandraFields[1].Trim()

$mongoMetrics = Get-Content "$PWD\stats_mongo.csv" | Where-Object { $_ -ne "" }

$mongoCpu = $mongoMetrics | ForEach-Object {
    $fields = $_ -split ","
    [double]($fields[0] -replace "%", "")
}

$mongoRam = $mongoMetrics | ForEach-Object {
    $fields = $_ -split ","
    $used = ($fields[1] -split "/")[0].Trim()
    if ($used -match "GiB") {
        [double]($used -replace "GiB", "") * 1024
    } elseif ($used -match "MiB") {
        [double]($used -replace "MiB", "")
    } else {
        0
    }
}

$mongoCpuAvg = ($mongoCpu | Measure-Object -Average).Average
$mongoCpuPeak = ($mongoCpu | Measure-Object -Maximum).Maximum
$mongoRamAvg = ($mongoRam | Measure-Object -Average).Average
$mongoRamPeak = ($mongoRam | Measure-Object -Maximum).Maximum

$lastValuesMongo = $mongoMetrics[-1]
$ioMongoFields = ($lastValuesMongo -split ",")[2] -split "/"
$ioReadMongo = ConvertToMB $ioMongoFields[0].Trim()
$ioWriteMongo = ConvertToMB $ioMongoFields[1].Trim()

$totalTimeDbs = $cassandraTotalTime + $mongoTotalTime

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
"cpu_avg,$cassandraCpuAvg,$mongoCpuAvg,percent" | Add-Content "experiment_${configId}_${instanceName}.csv"
"cpu_peak,$cassandraCpuPeak,$mongoCpuPeak,percent" | Add-Content "experiment_${configId}_${instanceName}.csv"
"ram_avg,$cassandraRamAvg,$mongoRamAvg,megabytes" | Add-Content "experiment_${configId}_${instanceName}.csv"
"ram_peak,$cassandraRamPeak,$mongoRamPeak,megabytes" | Add-Content "experiment_${configId}_${instanceName}.csv"
"io_read,$ioReadCassandra,$ioReadMongo,megabytes" | Add-Content "experiment_${configId}_${instanceName}.csv"
"io_write,$ioWriteCassandra,$ioWriteMongo,megabytes" | Add-Content "experiment_${configId}_${instanceName}.csv"

Write-Output $totalTimeDbs
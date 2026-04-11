if (-not (Test-Path "campaign_info.txt")) {
    [Console]::Error.WriteLine("ERRO: campaign_info.txt nao encontrado na raiz do projeto.")
    exit 1
}

$campaignInfo = @{}
Get-Content "campaign_info.txt" | ForEach-Object {
    if ($_ -match "=") {
        $key, $value = $_ -split "=", 2
        $campaignInfo[$key.Trim()] = $value.Trim()
    }
}

if (-not $campaignInfo.ContainsKey("name") -or [string]::IsNullOrWhiteSpace($campaignInfo["name"])) {
    [Console]::Error.WriteLine("ERRO: chave 'name' nao encontrada ou vazia em campaign_info.txt.")
    exit 1
}

$campaignName = $campaignInfo["name"]
$csvPath = "experiments_$campaignName.csv"

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
$cassandraYaml = $cassandraYaml -replace "write_request_timeout: \d+ms", "write_request_timeout: $($params['write_timeout'])ms"
$cassandraYaml = $cassandraYaml -replace "read_request_timeout: \d+ms", "read_request_timeout: $($params['read_timeout'])ms"
$cassandraYaml = $cassandraYaml -replace "row_cache_size: \d+MiB", "row_cache_size: $($params['cache_size_mb'])MiB"
$cassandraYaml = $cassandraYaml -replace "batch_size_warn_threshold: \d+KiB", "batch_size_warn_threshold: $($params['batch_size'])KiB"
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

docker restart cassandra 2>&1 | Out-Null
docker restart mongodb 2>&1 | Out-Null

do {
    Start-Sleep -Seconds 5
    $status = docker inspect --format='{{.State.Health.Status}}' cassandra
} while ($status -ne "healthy")

do {
    Start-Sleep -Seconds 5
    $status = docker inspect --format='{{.State.Health.Status}}' mongodb
} while ($status -ne "healthy")

Start-Sleep -Seconds 10

docker exec cassandra cqlsh -e "DROP KEYSPACE IF EXISTS ycsb;" 2>&1 | Out-Null
docker exec cassandra cqlsh -e "CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};" 2>&1 | Out-Null
docker exec cassandra cqlsh -e "CREATE TABLE ycsb.usertable (y_id TEXT PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT);" 2>&1 | Out-Null

$cassandraCompressionMap = @{
    "lz4"    = "LZ4Compressor"
    "snappy" = "SnappyCompressor"
    "none"   = ""
}
$cassandraCompression = $cassandraCompressionMap[$params['compression']]

if ($cassandraCompression -eq "") {
    docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'enabled': 'false'};" 2>&1 | Out-Null
} else {
    docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'class': '$cassandraCompression'};" 2>&1 | Out-Null
}

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

cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=100000 -threads 20 2>&1" > cassandra_load.txt

cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run cassandra-cql -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=100000 -threads 20 2>&1" > cassandra_run.txt

Stop-Job $cassandraJob 2>&1 | Out-Null
Remove-Job $cassandraJob 2>&1 | Out-Null

$cassandraLoadTime = Select-String -Path "cassandra_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunTime = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunOverallThroughput = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunReadAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunRead95thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunRead99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunUpdateAverageLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
$cassandraRunUpdate99thPercentileLatency = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }

$cassandraTotalTime = [int]$cassandraLoadTime + [int]$cassandraRunTime

docker exec mongodb mongosh --eval "db.getSiblingDB('ycsb').dropDatabase()" 2>&1 | Out-Null

"" | Set-Content "$PWD\stats_mongo.csv"

$mongoJob = Start-Job -ScriptBlock {
    param($container, $OutputFile)
    while ($true) {
        $stats = docker stats $container --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.BlockIO}}"
        $stats | Add-Content $OutputFile
        Start-Sleep -Seconds 1
    }
} -ArgumentList "mongodb", "$PWD\stats_mongo.csv"

cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=100000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout  -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20 2>&1" > mongo_load.txt

cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=100000 -p mongodb.batchsize=$($params['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20 2>&1" > mongo_run.txt

Stop-Job $mongoJob 2>&1 | Out-Null
Remove-Job $mongoJob 2>&1 | Out-Null

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

if (-not (Test-Path $csvPath)) {
    "config_id,instance_name,banco,batch_size,concurrent_writes,concurrent_reads,write_timeout,read_timeout,compression,cache_size_mb,time_load,time_run,total_time,run_overall_throughput,run_read_average_latency,run_read_p95_latency,run_read_p99_latency,run_update_average_latency,run_update_p99_latency,cpu_avg,cpu_peak,ram_avg,ram_peak,io_read,io_write" | Set-Content $csvPath
}

$paramCols = "$($params['batch_size']),$($params['concurrent_writes']),$($params['concurrent_reads']),$($params['write_timeout']),$($params['read_timeout']),$($params['compression']),$($params['cache_size_mb'])"

"$configId,$instanceName,cassandra,$paramCols,$cassandraLoadTime,$cassandraRunTime,$cassandraTotalTime,$cassandraRunOverallThroughput,$cassandraRunReadAverageLatency,$cassandraRunRead95thPercentileLatency,$cassandraRunRead99thPercentileLatency,$cassandraRunUpdateAverageLatency,$cassandraRunUpdate99thPercentileLatency,$cassandraCpuAvg,$cassandraCpuPeak,$cassandraRamAvg,$cassandraRamPeak,$ioReadCassandra,$ioWriteCassandra" | Add-Content $csvPath

"$configId,$instanceName,mongodb,$paramCols,$mongoLoadTime,$mongoRunTime,$mongoTotalTime,$mongoRunOverallThroughput,$mongoRunReadAverageLatency,$mongoRunRead95thPercentileLatency,$mongoRunRead99thPercentileLatency,$mongoRunUpdateAverageLatency,$mongoRunUpdate99thPercentileLatency,$mongoCpuAvg,$mongoCpuPeak,$mongoRamAvg,$mongoRamPeak,$ioReadMongo,$ioWriteMongo" | Add-Content $csvPath

Write-Output $totalTimeDbs
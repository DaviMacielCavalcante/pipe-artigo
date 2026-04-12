$csvPath = "validation_results.csv"

$champion = @{
    batch_size = 32
    concurrent_writes = 16
    concurrent_reads = 16
    write_timeout = 20000
    read_timeout = 15000
    compression = "lz4"
    cache_size_mb = 512
}

$defaultCassandra = @{
    batch_size = 5
    concurrent_writes = 32
    concurrent_reads = 32
    write_timeout = 2000
    read_timeout = 5000
    compression = "lz4"
    cache_size_mb = 0
}

$defaultMongo = @{
    batch_size = 1
    concurrent_writes = 128
    concurrent_reads = 128
    write_timeout = 0
    read_timeout = 0
    compression = "snappy"
    cache_size_mb = 6656
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

function Invoke-Experiment {
    param($mode, $runNumber, $cassandraConfig, $mongoConfig)

    $cassandraYaml = Get-Content "./config/cassandra.yaml" -Raw
    $cassandraYaml = $cassandraYaml -replace "concurrent_writes: \d+", "concurrent_writes: $($cassandraConfig['concurrent_writes'])"
    $cassandraYaml = $cassandraYaml -replace "concurrent_reads: \d+", "concurrent_reads: $($cassandraConfig['concurrent_reads'])"
    $cassandraYaml = $cassandraYaml -replace "write_request_timeout: \d+ms", "write_request_timeout: $($cassandraConfig['write_timeout'])ms"
    $cassandraYaml = $cassandraYaml -replace "read_request_timeout: \d+ms", "read_request_timeout: $($cassandraConfig['read_timeout'])ms"
    $cassandraYaml = $cassandraYaml -replace "row_cache_size: \d+MiB", "row_cache_size: $($cassandraConfig['cache_size_mb'])MiB"
    $cassandraYaml = $cassandraYaml -replace "batch_size_warn_threshold: \d+KiB", "batch_size_warn_threshold: $($cassandraConfig['batch_size'])KiB"
    $cassandraYaml | Set-Content "./config/cassandra.yaml"

    $mongoConfigFile = Get-Content "./config/mongod.conf" -Raw
    $mongoConfigFile = $mongoConfigFile -replace "wiredTigerConcurrentWriteTransactions: \d+", "wiredTigerConcurrentWriteTransactions: $($mongoConfig['concurrent_writes'])"
    $mongoConfigFile = $mongoConfigFile -replace "wiredTigerConcurrentReadTransactions: \d+", "wiredTigerConcurrentReadTransactions: $($mongoConfig['concurrent_reads'])"
    $cacheSizeGB = [math]::Round($mongoConfig['cache_size_mb'] / 1024, 2)
    $mongoConfigFile = $mongoConfigFile -replace "cacheSizeGB: [\d.]+", "cacheSizeGB: $cacheSizeGB"

    $compressionMap = @{ "lz4" = "zstd"; "snappy" = "snappy"; "none" = "none" }
    $mongoCompression = $compressionMap[$mongoConfig['compression']]
    $mongoConfigFile = $mongoConfigFile -replace "blockCompressor: \w+", "blockCompressor: $mongoCompression"
    $mongoConfigFile | Set-Content "./config/mongod.conf"

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

    $cassandraCompressionMap = @{ "lz4" = "LZ4Compressor"; "snappy" = "SnappyCompressor"; "none" = "" }
    $cassandraCompression = $cassandraCompressionMap[$cassandraConfig['compression']]
    if ($cassandraCompression -eq "") {
        docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'enabled': 'false'};" 2>&1 | Out-Null
    } else {
        docker exec cassandra cqlsh -e "ALTER TABLE ycsb.usertable WITH compression = {'class': '$cassandraCompression'};" 2>&1 | Out-Null
    }

    $mongoTimeout = [math]::Max([int]$mongoConfig['write_timeout'], [int]$mongoConfig['read_timeout'])

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

    $cLoad = Select-String -Path "cassandra_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cRun = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cThr = Select-String -Path "cassandra_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cReadAvg = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cReadP95 = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cReadP99 = Select-String -Path "cassandra_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cUpdAvg = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cUpdP99 = Select-String -Path "cassandra_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $cTotal = [int]$cLoad + [int]$cRun

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

    cmd /c ".\ycsb-0.17.0\bin\ycsb.bat load mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p recordcount=100000 -p mongodb.batchsize=$($mongoConfig['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20 2>&1" > mongo_load.txt
    cmd /c ".\ycsb-0.17.0\bin\ycsb.bat run mongodb -s -P .\ycsb-0.17.0\workloads\workloada -p hosts=localhost -p operationcount=100000 -p mongodb.batchsize=$($mongoConfig['batch_size']) -p mongodb.socketTimeout=$mongoTimeout -p mongodb.url=mongodb://localhost:27017/ycsb -threads 20 2>&1" > mongo_run.txt

    Stop-Job $mongoJob 2>&1 | Out-Null
    Remove-Job $mongoJob 2>&1 | Out-Null

    $mLoad = Select-String -Path "mongo_load.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mRun = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], RunTime\(ms\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mThr = Select-String -Path "mongo_run.txt" -Pattern "\[OVERALL\], Throughput\(ops/sec\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mReadAvg = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mReadP95 = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 95thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mReadP99 = Select-String -Path "mongo_run.txt" -Pattern "\[READ\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mUpdAvg = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], AverageLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mUpdP99 = Select-String -Path "mongo_run.txt" -Pattern "\[UPDATE\], 99thPercentileLatency\(us\)" | ForEach-Object { ($_ -split ",")[2].Trim() }
    $mTotal = [int]$mLoad + [int]$mRun

    $cMetrics = Get-Content "$PWD\stats_cassandra.csv" | Where-Object { $_ -ne "" }
    $cCpu = $cMetrics | ForEach-Object { [double](($_ -split ",")[0] -replace "%", "") }
    $cRam = $cMetrics | ForEach-Object {
        $used = (($_ -split ",")[1] -split "/")[0].Trim()
        if ($used -match "GiB") { [double]($used -replace "GiB", "") * 1024 }
        elseif ($used -match "MiB") { [double]($used -replace "MiB", "") }
        else { 0 }
    }
    $cCpuAvg = ($cCpu | Measure-Object -Average).Average
    $cCpuPeak = ($cCpu | Measure-Object -Maximum).Maximum
    $cRamAvg = ($cRam | Measure-Object -Average).Average
    $cRamPeak = ($cRam | Measure-Object -Maximum).Maximum
    $cIoFields = (($cMetrics[-1] -split ",")[2]) -split "/"
    $cIoR = ConvertToMB $cIoFields[0].Trim()
    $cIoW = ConvertToMB $cIoFields[1].Trim()

    $mMetrics = Get-Content "$PWD\stats_mongo.csv" | Where-Object { $_ -ne "" }
    $mCpu = $mMetrics | ForEach-Object { [double](($_ -split ",")[0] -replace "%", "") }
    $mRam = $mMetrics | ForEach-Object {
        $used = (($_ -split ",")[1] -split "/")[0].Trim()
        if ($used -match "GiB") { [double]($used -replace "GiB", "") * 1024 }
        elseif ($used -match "MiB") { [double]($used -replace "MiB", "") }
        else { 0 }
    }
    $mCpuAvg = ($mCpu | Measure-Object -Average).Average
    $mCpuPeak = ($mCpu | Measure-Object -Maximum).Maximum
    $mRamAvg = ($mRam | Measure-Object -Average).Average
    $mRamPeak = ($mRam | Measure-Object -Maximum).Maximum
    $mIoFields = (($mMetrics[-1] -split ",")[2]) -split "/"
    $mIoR = ConvertToMB $mIoFields[0].Trim()
    $mIoW = ConvertToMB $mIoFields[1].Trim()

    $cParams = "$($cassandraConfig['batch_size']),$($cassandraConfig['concurrent_writes']),$($cassandraConfig['concurrent_reads']),$($cassandraConfig['write_timeout']),$($cassandraConfig['read_timeout']),$($cassandraConfig['compression']),$($cassandraConfig['cache_size_mb'])"
    $mParams = "$($mongoConfig['batch_size']),$($mongoConfig['concurrent_writes']),$($mongoConfig['concurrent_reads']),$($mongoConfig['write_timeout']),$($mongoConfig['read_timeout']),$($mongoConfig['compression']),$($mongoConfig['cache_size_mb'])"

    "$mode,$runNumber,cassandra,$cParams,$cLoad,$cRun,$cTotal,$cThr,$cReadAvg,$cReadP95,$cReadP99,$cUpdAvg,$cUpdP99,$cCpuAvg,$cCpuPeak,$cRamAvg,$cRamPeak,$cIoR,$cIoW" | Add-Content $csvPath
    "$mode,$runNumber,mongodb,$mParams,$mLoad,$mRun,$mTotal,$mThr,$mReadAvg,$mReadP95,$mReadP99,$mUpdAvg,$mUpdP99,$mCpuAvg,$mCpuPeak,$mRamAvg,$mRamPeak,$mIoR,$mIoW" | Add-Content $csvPath

    Write-Host "[$mode] run $runNumber/30 done | cassandra=$cTotal ms | mongodb=$mTotal ms"
}

if (-not (Test-Path $csvPath)) {
    "mode,run_number,banco,batch_size,concurrent_writes,concurrent_reads,write_timeout,read_timeout,compression,cache_size_mb,time_load,time_run,total_time,run_overall_throughput,run_read_average_latency,run_read_p95_latency,run_read_p99_latency,run_update_average_latency,run_update_p99_latency,cpu_avg,cpu_peak,ram_avg,ram_peak,io_read,io_write" | Set-Content $csvPath
}

for ($i = 1; $i -le 30; $i++) {
    Invoke-Experiment -mode "champion" -runNumber $i -cassandraConfig $champion -mongoConfig $champion
}

for ($i = 1; $i -le 30; $i++) {
    Invoke-Experiment -mode "default" -runNumber $i -cassandraConfig $defaultCassandra -mongoConfig $defaultMongo
}

Write-Host "Validacao completa. Resultados em $csvPath"
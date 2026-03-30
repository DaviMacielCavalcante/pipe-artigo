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
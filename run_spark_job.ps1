param(
  [Parameter(Mandatory=$true)][string]$JobPath
)

$compose = ".\docker\docker-compose.yml"

# Ensure service is running
$running = docker compose -f $compose ps --status running --services 2>$null
if ($running -notcontains "spark") {
  Write-Host "[INFO] spark service not running. Starting..." -ForegroundColor Yellow
  docker compose -f $compose up -d --build
}

$deltaArgs = @(
  "--packages","io.delta:delta-spark_2.12:3.3.1",
  "--conf","spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
  "--conf","spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
  "--conf","spark.sql.sources.partitionOverwriteMode=dynamic",
  "--conf","spark.sql.shuffle.partitions=8",
  "--conf","spark.jars.ivy=/root/.ivy2"
)

docker compose -f $compose exec -T spark `
  spark-submit `
  $deltaArgs `
  $JobPath

$outputFile = "C:\Users\DELL\.gemini\antigravity-ide\brain\3d7996ef-5664-4453-91db-3c09a259a178\event_engine_final_source_code.md"
$files = @(
  "README.md",
  "package.json",
  "tsconfig.json",
  "jest.config.js",
  "src\index.ts",
  "src\core\EventProcessingEngine.ts",
  "src\utils\LinkedList.ts",
  "src\utils\ReadyQueue.ts",
  "src\utils\RateTracker.ts",
  "src\utils\PercentileTracker.ts",
  "src\errors\EngineShutdownException.ts",
  "src\errors\WaitQueueFullException.ts",
  "src\types\public\EngineConfig.ts",
  "src\types\public\Event.ts",
  "src\types\public\EventHandler.ts",
  "src\types\public\HealthSnapshot.ts",
  "src\types\public\Logger.ts",
  "src\types\public\Metrics.ts",
  "src\types\public\PartitionMetrics.ts",
  "src\types\public\ShutdownReport.ts",
  "src\types\internal\EngineState.ts",
  "src\types\internal\InternalEvent.ts",
  "src\types\internal\PartitionState.ts",
  "src\types\internal\PartitionStatus.ts",
  "src\types\internal\WaitingSubmitter.ts",
  "src\__tests__\EventProcessingEngine.test.ts",
  "src\__tests__\Observability.test.ts",
  "src\__tests__\Stress.test.ts"
)

"# Event Engine - Complete Implementation Source Code`n`n" | Out-File $outputFile -Encoding utf8

foreach ($file in $files) {
    "## $file`n" | Out-File $outputFile -Append -Encoding utf8
    if ($file.EndsWith(".ts")) {
        "````typescript`n" | Out-File $outputFile -Append -Encoding utf8
    } elseif ($file.EndsWith(".json")) {
        "````json`n" | Out-File $outputFile -Append -Encoding utf8
    } elseif ($file.EndsWith(".js")) {
        "````javascript`n" | Out-File $outputFile -Append -Encoding utf8
    } elseif ($file.EndsWith(".md")) {
        "````markdown`n" | Out-File $outputFile -Append -Encoding utf8
    } else {
        "`````n" | Out-File $outputFile -Append -Encoding utf8
    }
    
    Get-Content $file -Encoding utf8 | Out-File $outputFile -Append -Encoding utf8
    "`````n`n" | Out-File $outputFile -Append -Encoding utf8
}

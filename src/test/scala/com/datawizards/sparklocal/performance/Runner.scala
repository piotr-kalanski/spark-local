package com.datawizards.sparklocal.performance

object Runner extends App {

  run()
  //run(false)

  def run(runTests: Boolean = true): Unit = {
    if(runTests) {
      val results =
        BenchmarksRunner.runDataSetBenchmarks(Benchmarks.peopleDataSetBenchmarks) ++
        BenchmarksRunner.runRDDBenchmarks(Benchmarks.peopleRDDBenchmarks)

      BenchmarkResultsSerialization.writeBenchmarkResults(results)
    }

    val results = BenchmarkResultsSerialization.readBenchmarkResults()
    ResultsVisualization.visualizeResults(results)
  }

}

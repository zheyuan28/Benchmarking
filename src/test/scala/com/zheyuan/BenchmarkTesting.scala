package com.zheyuan

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Benchmark

class BenchmarkTesting extends SparkFunSuite {

  lazy val sparkSession: SparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_232-b09 on Mac OS X 10.14.6
   * Intel(R) Core(TM) i7-8569U CPU @ 2.80GHz
   *
   * my test benchmark:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------
   * limit                                       44266 / 45075          0.0   442655252.2       1.0X
   * takeAsList                                     386 /  456          0.0     3860990.2     114.6X
   * take                                           283 /  325          0.0     2825618.8     156.7X
   * */
  test("limit vs. takeAsList vs. take") {
    val benchmark = new Benchmark(s"my test benchmark", 100)

    benchmark.addCase("limit") { _ =>
      sparkSession.read.format("csv")
        .option("header", "true")
        .load("src/test/resources/Data82777.csv")
        .limit(100).toJSON.foreach(x => print(x + ","))
    }

    benchmark.addCase("takeAsList") { _ =>
      val df = sparkSession.read.format("csv")
        .option("header", "true")
        .load("src/test/resources/Data82777.csv")
      val dfnew = sparkSession.createDataFrame(df.takeAsList(100), df.schema)
      dfnew.toJSON.foreach(x => print(x + ","))
    }

    benchmark.addCase("take") { _ =>
      sparkSession.read.format("csv")
        .option("header", "true")
        .load("src/test/resources/Data82777.csv")
        .toJSON.take(100).foreach(x => print(x + ","))
    }

    benchmark.run()
  }
}

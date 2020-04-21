package com.zheyuan

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.Benchmark

class BenchmarkTesting extends SparkFunSuite {

  lazy val sparkSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("microbenchmark")
    //    .config("spark.sql.shuffle.partitions", 1)
    //    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_232-b09 on Mac OS X 10.14.6
   * Intel(R) Core(TM) i7-8569U CPU @ 2.80GHz
   *
   * my test benchmark:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------
   * source.limit.transform                      15712 / 16282          0.0     3237586.1       1.0X
   * source.transform.limit                      19694 / 21789          0.0     4058139.3       0.8X
   * source.transform.takeAsList                 17399 / 17481          0.0     3585293.3       0.9X
   * source.transform.take                       16923 / 17130          0.0     3487191.9       0.9X
   * source.limit.transform.takeAsList           14580 / 14693          0.0     3004274.4       1.1X
   * source.limit.transform.take                 14307 / 14465          0.0     2947978.2       1.1X
   * source.takeAsList.transform                   1124 / 1125          0.0      231703.0      14.0X
   * source.sample.takeAsList.transform            1061 / 1119          0.0      218551.2      14.8X
   *
   * */
  test("limit vs. takeAsList vs. take") {
    import sparkSession.implicits._
    val benchmark = new Benchmark(s"my test benchmark", 4853)

    def source: DataFrame = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("src/test/resources/Data8277.csv")

//    def doTransform(in: DataFrame): DataFrame = {
//      in.withColumn("date", $"date".cast("date"))
//        .filter($"date" === "2007-03-11" && $"fog" === "Yes")
//        .withColumn("avg", avg($"avgwindspeed") over Window.partitionBy($"date"))
//        .groupBy($"date")
//        .agg(avg($"fastest2minwindspeed" * $"temperaturemax"))
//    }

    def doTransform(in: DataFrame): DataFrame = {
      in.filter($"Year" === "2018" && $"sex" === "2")
        .withColumn("avg", avg($"count") over Window.partitionBy($"Year"))
        .groupBy($"Area")
        .agg(avg($"Ethnic" * $"Age"))
    }

    benchmark.addCase("source.limit.transform") { _ =>
      source
        .limit(100)
        .transform(doTransform)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.transform.limit") { _ =>
      source
        .transform(doTransform)
        .limit(100)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.transform.takeAsList") { _ =>
      val df = source
        .transform(doTransform)

      sparkSession
        .createDataFrame(df.takeAsList(100), df.schema)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.transform.take") { _ =>
      source
        .transform(doTransform)
        .toJSON
        .take(100)
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.limit.transform.takeAsList") { _ =>
      val df = source
        .limit(2000)
        .transform(doTransform)

      sparkSession
        .createDataFrame(df.takeAsList(100), df.schema)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.limit.transform.take") { _ =>
      source
        .limit(2000)
        .transform(doTransform)
        .toJSON
        .take(100)
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.takeAsList.transform") { _ =>
      sparkSession
        .createDataFrame(source.takeAsList(100), source.schema)
        .transform(doTransform)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.addCase("source.sample.takeAsList.transform") { _ =>
      val sampled = source.sample(0.001)
      sparkSession
        .createDataFrame(sampled.takeAsList(100), sampled.schema)
        .transform(doTransform)
        .toJSON
        .foreach(x => print(x + ","))

      source.unpersist()
    }

    benchmark.run()
  }
}

package sss

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SparkJob {
  val flu = udf((s: String) => if (s == "C01.2") 1 else 0)

  val ili = udf((s:String) => {
    if (s == "A01.1" || s == "A02.1") 1 else 0
  })

  val pneumonia = udf((s:String) => if (s == "A01.1") 1 else 0)

  val sari = udf((s:String) => if (s == "B01.3") 1 else 0)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SSS")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")
    import spark.implicits.StringToColumn

    println("starting sss job")

    val source = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", "9876")
      .load()

    val visits = source.select(from_json($"value", jsonSchema()) as "data")
      .select($"data.*")

    val cases: DataFrame = visits.withColumn("flu", flu($"icd10"))
      .withColumn("ili", ili($"icd10"))
      .withColumn("pneumonia", pneumonia($"icd10"))
      .withColumn("sari", sari($"icd10"))
      .withColumn("ipd", when($"patient_type" === "IPD", 1).otherwise(0))
      .withColumn("opd", when($"patient_type" === "OPD", 1).otherwise(0))

    val summary = cases
      .groupBy($"subdistrict")
      .agg(
        sum($"flu") as "flu",
        sum($"ili") as "ili",
        sum($"sari") as "sari",
        sum($"pneumonia") as "pneumonia",
        sum($"ipd") as "ipd",
        sum($"opd") as "opd"
      )

    val writer = new JDBCSink("jdbc:postgresql://127.0.0.1:5432/sss", "pphetra", "pphetra")

    val query = summary
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }

  def jsonSchema() = {
    new StructType()
      .add("date", TimestampType)
      .add("gender", StringType)
      .add("icd10", StringType)
      .add("patient_type", StringType)
      .add("subdistrict", StringType)
  }
}

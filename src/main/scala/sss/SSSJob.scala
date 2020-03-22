package sss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable
;

object SSSJob {
  val dataSchema = new StructType(Array(
    new StructField("HN", StringType, false),
    new StructField("DISEASE", IntegerType, false),
    new StructField("DATEDEFINE", DateType, false),
    new StructField("TYPE", IntegerType, false),
    new StructField("ICD10", StringType, false),
    new StructField("PATIENT_LOCATION_CODE", StringType, false)
  ))

  val flu = udf((ary: mutable.WrappedArray[String]) => {
    if (ary.exists((s) => s.startsWith("J10") || s.startsWith("J11"))) 1 else 0
  })

  val pneumonia = udf((ary: mutable.WrappedArray[String]) => {
    if (ary.exists(s => s.matches(raw"^J1[2345678]") || s.startsWith("J85"))) 1 else 0
  })

  val ili = udf((ary: mutable.WrappedArray[String], patientType: Int) => {
    if (patientType == 2) { // OPD only
      //J00, J02.9, J06.9, J09,J10,J11
      if (ary.exists(s => s.matches(raw"^(J00|J02.9|J06.9|J09|J10|J11)"))) 1 else 0
    } else 0
  })

  val sari = udf((ary: mutable.WrappedArray[String], patientType: Int) => {
    if (patientType == 1) { // IPD only
      // J00-J22
      if (ary.exists(s => s.matches(raw"^J[01][0-9]") || s.matches(raw"^J2[0-2]"))) 1 else 0
    } else 0
  })


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SSS")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val source = spark.read.format("csv")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .option("header", "true")
      .schema(dataSchema)
      .option("path", "./hos_visits.csv")
      .load()


    val dfd = source
      .withColumn("ICD10", split(col("ICD10"), "\\|"))
      .withColumn("flu", flu(col("ICD10")))
      .withColumn("pnuemonia", pneumonia(col("ICD10")))
      .withColumn("ili", ili(col("ICD10"), col("TYPE")))
      .withColumn("sari", sari(col("ICD10"), col("TYPE")))

    val summary = dfd.groupBy(col("DATEDEFINE"), col("PATIENT_LOCATION_CODE"))
      .agg(sum(col("flu")) as "flu"
        ,sum(col("ili")) as "ILI"
        ,sum(col("sari")) as "SARI"
        ,sum(col("pnuemonia")) as "pnuemonia")

    summary.repartition(1).write.format("csv")
      .mode("overwrite")
      .option("path", "./summary_single.csv")
      .option("header", "TRUE")
      .save()

  }
}
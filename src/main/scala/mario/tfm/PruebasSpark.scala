package mario.tfm


import java.text.SimpleDateFormat
import org.apache.spark.sql.functions
.{stddev, variance, hour, minute, second}

import org.apache.spark.sql.functions.{col, to_utc_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue, Json}

object PruebasSpark extends App {

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  implicit val spark = SparkSession.builder
    .appName("SparkSession")
    .master("local[4]")
    .config("spark.network.timeout", 100000000)
    .config("spark.executor.heartbeatInterval", 100000000)
    .config("spark.io.compression.codec", "snappy")
    .config("spark.rdd.compress", "true")
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.mergeSchema", "true")
    .config("spark.sql.parquet.binaryAsString", "true")
    .getOrCreate

  import spark.sqlContext.implicits._

  def getId(id: String, dateFrom: Option[String],
            dateTo: Option[String])(implicit spark: SparkSession) = {

    import org.apache.spark.sql.types._

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/*")
      .filter("id == '" + id + "'").orderBy($"notifiedAt".desc)

    import org.apache.spark.sql.functions.to_timestamp
    //df.withColumn("ts", df("notifiedAt").cast(TimestampType)).show(2, false)

    if ((dateFrom isDefined) && (dateTo isDefined)) {

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)

      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\", "")

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)) {
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)

      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\", "")

      /*if (count isDefined) {
        val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
        result = c.toString()
      }*/

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)) {
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)
      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\", "")
    }
    else {
      data = df.toJSON.head(2)
      result = Json.toJson(data).toString().replace("\\", "")
    }

    //val data = df.filter("id == '" + id + "'").toJSON.collect()
    //val result = df.select(field).toJSON.collect()

    //Json.toJson(data).toString().replace("\\", "")
    result
  }

  def countGetId(id: String, dateFrom: Option[String],
                 dateTo: Option[String])(implicit spark: SparkSession) = {

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/*")
      .filter("id == '" + id + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)) {

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)

      val c: JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)) {
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)

      val c: JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)) {
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)
      val c: JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()
    }
    else {
      data = df.toJSON.collect()
      val c: JsValue = JsObject(Seq("count" -> JsNumber(df.count())))
      result = c.toString()
    }

    result
  }


  def getAggrMethodPropertyType(tipo: String, property: String,
                                aggrMethod: Option[String],
                                aggrPeriod: Option[String],
                                dateFrom: Option[String], dateTo: Option[String])
                               (implicit spark: SparkSession) = {

    val path: String = "hdfs://localhost:9000/" + tipo + "/*"
    var result = ""


    if ((aggrMethod isDefined) && (aggrPeriod isDefined)) {
      val method = aggrMethod.get
      val period = aggrPeriod.get
      var df = spark.emptyDataFrame
      if ((dateFrom isDefined) || (dateTo isDefined)) {
        df = getDataInThisTimeframe(path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }
      if (method == "avg") {
        if (period == "hour") {
          val dfAvg = df.groupBy(hour($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfAvg = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "max"){
        if (period == "hour") {
          val dfMax = df.groupBy(hour($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "min"){
        if (period == "hour") {
          val dfMin = df.groupBy(hour($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "sum"){
        if (period == "hour") {
          val dfSum = df.groupBy(hour($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfSum = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfSum = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "std"){
        if (period == "hour") {
          val dfStd = df.groupBy(hour($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }

      }
      else if (method == "var"){
        if (period == "hour") {
          val dfVar = df.groupBy(hour($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else {
        val c: JsValue = JsObject(Seq("result" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else if ((aggrMethod isDefined) && (aggrPeriod isEmpty)){
      val method = aggrMethod.get
      var df = spark.emptyDataFrame

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        df = getDataInThisTimeframe(path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }

      if (method == "avg") {
        val dfAvg = df.groupBy().avg()
        val data = dfAvg.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "sum") {
        val dfSum = df.groupBy().sum()
        val data = dfSum.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "max"){
        val dfMax = df.groupBy().max()
        val data = dfMax.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "min") {
        val dfMin = df.groupBy().min()
        val data = dfMin.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "std"){
        val dfStd = df.groupBy()
          .agg(stddev($"value"))
        val data = dfStd.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "var") {
        val dfVar = df.groupBy().agg(variance($"value"))
        val data = dfVar.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else {
        val c: JsValue = JsObject(Seq("result" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else {
      val c: JsValue = JsObject(Seq("result" -> JsString("Método no introducido")))
      result = c.toString()
    }
    result
  }

  def getDataInThisTimeframe(path: String, dateFrom: Option[String],
                             dateTo: Option[String],
                             isId: Boolean,
                             id: String) = {

    var df: DataFrame = spark.emptyDataFrame
    if (isId == true) {
      df = spark.read.parquet(path).filter("id == '" + id + "'")
    }
    else {
      df = spark.read.parquet(path)
    }

    var dfResult = spark.emptyDataFrame

    if ((dateFrom isDefined) && (dateTo isDefined)) {

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)

      dfResult = df2

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)) {
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") >= from)

      dfResult = df2

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)) {
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp($"notifiedAt", "Europe/Madrid") <= to)

      dfResult = df2
    }

    dfResult
  }


  def getAggrMethodPropertyId(id: String, property: String,
                                aggrMethod: Option[String],
                                aggrPeriod: Option[String],
                                dateFrom: Option[String], dateTo: Option[String])
                               (implicit spark: SparkSession) = {

    val path: String = "hdfs://localhost:9000/" +
      id.split(":")(2) + "/*"

    var data = Array[String]()
    var result = ""

    if ((aggrMethod isDefined) && (aggrPeriod isDefined)) {
      var df = spark.emptyDataFrame

      val method = aggrMethod.get
      val period = aggrPeriod.get

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        df = getDataInThisTimeframe(path, dateFrom, dateTo, true, id)
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .filter("id == '" + id + "'")
          .select("notifiedAt", property + ".value")
      }
      if (method == "avg") {
        if (period == "hour") {
          val dfAvg = df.groupBy(hour($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {

          val dfAvg = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "max"){
        if (period == "hour") {
          val dfMax = df.groupBy(hour($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "min"){
        if (period == "hour") {
          val dfMin = df.groupBy(hour($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "sum"){
        if (period == "hour") {
          val dfSum = df.groupBy(hour($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfSum = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfSum = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "std"){
        if (period == "hour") {
          val dfStd = df.groupBy(hour($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .agg(stddev($"value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }

      }
      else if (method == "var"){
        if (period == "hour") {
          val dfVar = df.groupBy(hour($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(hour($"notifiedAt"), minute($"notifiedAt"), second($"notifiedAt"))
            .agg(variance($"value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("result" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else {
        val c: JsValue = JsObject(Seq("result" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else if ((aggrMethod isDefined) && (aggrPeriod isEmpty)){
      var df = spark.emptyDataFrame
      if ((dateFrom isDefined) || (dateTo isDefined)) {
        df = getDataInThisTimeframe(path, dateFrom, dateTo, true,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .filter("id == '" + id + "'")
          .select("notifiedAt", property + ".value")
      }
      val method = aggrMethod.get
      if (method == "avg") {
        val dfAvg = df.groupBy().avg()
        val data = dfAvg.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "sum") {
        val dfSum = df.groupBy().sum()
        val data = dfSum.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "max"){
        val dfMax = df.groupBy().max()
        val data = dfMax.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "min") {
        df.show(false)
        val dfMin = df.groupBy().min()
        dfMin.show(false)
        val data = dfMin.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "std"){
        df.show(false)
        val dfStd = df
          //.groupBy()
          .agg(stddev($"value"))
        val data = dfStd.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "var") {
        df.show()
        val dfVar = df.groupBy().agg(variance($"value"))
        val data = dfVar.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else {
        val c: JsValue = JsObject(Seq("result" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else {
      val c: JsValue = JsObject(Seq("result" -> JsString("Método no introducido")))
      result = c.toString()
    }
    result
  }



  val m = Option("std")
  val p = Option.empty[String]
  val datf = Option.empty[String]
  val datt = Option.empty[String]

  val path = "hdfs://localhost:9000/OffStreetParking/*"
  val dfFull = spark.read.parquet(path).filter("id == '" + "urn:ngsi-ld:OffStreetParking:Downtown02" + "'")

  dfFull.show(false)
  val result = getAggrMethodPropertyId("urn:ngsi-ld:OffStreetParking:Downtown02",
    "availableSpotNumber", m, p,
    datf, datt)

  println(result)

  val dfPrueba = Seq(
    (8, "bat"),
    (64, "mouse"),
    (-27, "horse")
  ).toDF("number", "word")

  //val uuid = UUID.randomUUID().toString
  //df.write.parquet("hdfs://localhost:9000/" + uuid)
}

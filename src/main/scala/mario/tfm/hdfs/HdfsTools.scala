package mario.tfm.hdfs

import java.lang.NullPointerException
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.util.logging.Logger

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue, Json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.functions.{month,dayofmonth, hour, minute, second, stddev, variance}

object HdfsTools extends App {

  def sendParquet(data: JsObject)(implicit spark: SparkSession): Unit = {
    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

    import spark.implicits._

    Try(Json.parse(data.toString())) match {
      case Failure(t) => t
        complete(StatusCodes.BadRequest)
      case Success(h) =>
        var fulljson = Vector[JsObject]()
        var ds = spark.emptyDataset[String]

        try {
          val notifiedAt = (h \ "notifiedAt").as[JsValue]
          val context = (h \ "@context").as[JsValue]
          val data = (h \ "data").as[Vector[JsObject]]
          var folder: String = (data(0) \ "type").as[String]

          for (i<-data){
            val c: JsObject = i + ("notifiedAt" -> notifiedAt) + ("context" -> context)
            ds = ds.union(Seq(c.toString()).toDS)
            fulljson = fulljson :+ c
          }

          val df = spark.read.json(ds)

          // 8020
          val uuid = UUID.randomUUID().toString
          val date = new SimpleDateFormat("d-M-y").format(Calendar.getInstance().getTime)
          df.write.parquet("hdfs://localhost:9000/"+ folder + "/" +  date.toString + "/" + uuid)

        }
        catch {
          case _ => println("No funciona")
        }
    }
  }


  def getId(id: String, dateFrom: Option[String],
            dateTo: Option[String])(implicit spark: SparkSession) = {

    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)
    var result = ""

    try {
      var data = Array[String]()

      var df = spark.read.parquet("hdfs://localhost:9000/" +
        id.split(":")(2) + "/*/*" )
        .filter("id == '" + id + "'")

      if ((dateFrom isDefined) && (dateTo isDefined)){

        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
          .withColumnRenamed("context", "@context")
        df2.show()
        data = df2.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")

      }
      else if ((dateFrom isDefined) && (dateTo isEmpty)){
        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
          "Europe/Madrid") >= from)
          .withColumnRenamed("context", "@context")

        data = df2.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")

      }
      else if ((dateFrom isEmpty) && (dateTo isDefined)){
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
          "Europe/Madrid") <= to)
          .withColumnRenamed("context", "@context")

        data = df2.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else {
        val df2 = df.withColumnRenamed("context", "@context")
        data = df2.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
    }

    catch{
      case e: java.lang.ArrayIndexOutOfBoundsException =>
        val c: JsValue = JsObject(Seq("Error"
          -> JsString("El ID en NGSI-LD es representado con una URI con " +
          "estructura urn:ngsi-ld:<type of entity>:<unique identifier>")))
        result = c.toString()
    }

    result
  }

  def countGetId(id: String, dateFrom: Option[String],
                 dateTo: Option[String])(implicit spark: SparkSession) = {

    var result = ""

    try {
      var data = Array[String]()
      val df = spark.read.parquet("hdfs://localhost:9000/" +
        id.split(":")(2) + "/*/*")
        .filter("id == '" + id + "'")

      if ((dateFrom isDefined) && (dateTo isDefined)){

        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)

        val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
        result = c.toString()

      }
      else if ((dateFrom isDefined) && (dateTo isEmpty)){
        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)

        val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
        result = c.toString()
      }
      else if ((dateFrom isEmpty) && (dateTo isDefined)){
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
        result = c.toString()
      }
      else {
        data = df.toJSON.collect()
        val c : JsValue = JsObject(Seq("count" -> JsNumber(df.count())))
        result = c.toString()
      }
    }
    catch{
      case e: java.lang.ArrayIndexOutOfBoundsException =>
        val c: JsValue = JsObject(Seq("Error"
          -> JsString("El ID en NGSI-LD es representado con una URI con " +
          "estructura urn:ngsi-ld:<type of entity>:<unique identifier>")))
        result = c.toString()
    }

    result
  }

  def getType(tipo: String, dateFrom: Option[String],
              dateTo: Option[String])(implicit spark: SparkSession): String = {

    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*/*")
      .filter("type == '" + tipo + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .withColumnRenamed("context", "@context")

      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }

    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
        "Europe/Madrid") >= from)
        .withColumnRenamed("context", "@context")

      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .withColumnRenamed("context", "@context")

      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    else {
      val df2 = df.withColumnRenamed("context", "@context")
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }

    result
  }

  def countGetType(tipo: String, dateFrom: Option[String],
                 dateTo: Option[String])(implicit spark: SparkSession) = {

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*/*")
      .filter("type == '" + tipo + "'")

    import org.apache.spark.sql.functions.to_timestamp
    //df.withColumn("ts", df("notifiedAt").cast(TimestampType)).show(2, false)

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)

      val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)

      val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
      val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
      result = c.toString()
    }
    else {
      data = df.toJSON.collect()
      val c : JsValue = JsObject(Seq("count" -> JsNumber(df.count())))
      result = c.toString()
    }

    result
  }

  def getIdLastN(id: String, dateFrom: Option[String],
                 dateTo: Option[String], lastN: Option[String])
                (implicit spark: SparkSession) = {

    var result = ""
    var data = Array[String]()

    try {
      val df = spark.read.parquet("hdfs://localhost:9000/" +
        id.split(":")(2) + "/*/*")
        .filter("id == '" + id + "'")

      if ((dateFrom isDefined) && (dateTo isDefined)){

        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
          .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
          .withColumnRenamed("context", "@context")
          .orderBy(col("notifiedAt").desc)

        data = df2.toJSON.head(lastN.get.toInt)
        result = Json.toJson(data)
          .toString()
          .replace("\\\"", "")
      }
      else if ((dateFrom isDefined) && (dateTo isEmpty)){
        val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
        val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
          "Europe/Madrid") >= from)
          .withColumnRenamed("context", "@context")
          .orderBy(col("notifiedAt").desc)

        data = df2.toJSON.head(lastN.get.toInt)
        result = Json.toJson(data)
          .toString()
          .replace("\\\"", "")
      }
      else if ((dateFrom isEmpty) && (dateTo isDefined)){
        val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
        val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

        val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
          "Europe/Madrid") <= to)
          .withColumnRenamed("context", "@context")
          .orderBy(col("notifiedAt").desc)

        data = df2.toJSON.head(lastN.get.toInt)
        result = Json.toJson(data)
          .toString()
          .replace("\\\"", "")

      }
      else {
        val df2 = df.withColumnRenamed("context", "@context")
        data = df2.toJSON.head(lastN.get.toInt)
        result = Json.toJson(data)
          .toString()
          .replace("\\\"", "")
      }
    }

    catch{
      case e: java.lang.ArrayIndexOutOfBoundsException =>
        val c: JsValue = JsObject(Seq("Error"
          -> JsString("El ID en NGSI-LD es representado con una URI con " +
          "estructura urn:ngsi-ld:<type of entity>:<unique identifier>")))
        result = c.toString()
    }

    result
  }


  def getTypeLastN(tipo: String, dateFrom: Option[String],
                   dateTo: Option[String], lastN: Option[String])
                  (implicit spark: SparkSession) = {

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*/*")
      .filter("type == '" + tipo + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .withColumnRenamed("context", "@context")
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.head(lastN.get.toInt)
      result = Json.toJson(data)
        .toString()
        .replace("\\\"", "")
    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
        "Europe/Madrid") >= from)
        .withColumnRenamed("context", "@context")
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.head(lastN.get.toInt)
      result = Json.toJson(data)
        .toString()
        .replace("\\\"", "")
    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"),
        "Europe/Madrid") <= to)
        .withColumnRenamed("context", "@context")
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.head(lastN.get.toInt)
      result = Json.toJson(data)
        .toString()
        .replace("\\\"", "")
    }
    else {
      data = df
        .withColumnRenamed("context", "@context")
        .orderBy(col("notifiedAt").desc).toJSON.head(lastN.get.toInt)
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    result
  }

  def getAggrMethodPropertyType(tipo: String, property: String,
                                aggrMethod: Option[String],
                                aggrPeriod: Option[String],
                                dateFrom: Option[String], dateTo: Option[String],
                                lastN: Option[String])
                               (implicit spark: SparkSession) = {

    val path: String = "hdfs://localhost:9000/" + tipo + "/*/*"
    var result = ""

    if ((aggrMethod isDefined) && (aggrPeriod isDefined)) {
      val method = aggrMethod.get
      val period = aggrPeriod.get
      var df = spark.emptyDataFrame
      if ((dateFrom isDefined) || (dateTo isDefined)) {
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }
      if (lastN isDefined){
        df = df.orderBy(col("notifiedAt").desc).limit(lastN.get.toInt)
      }
      if (method == "avg") {
        if (period == "month") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "max"){
        if (period == "month") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }

        else if (period == "hour") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "min"){
        if (period == "month") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "sum"){
        if (period == "hour") {
          val dfSum = df.groupBy(hour(col("notifiedAt")))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfSum = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfSum = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "std"){
        if (period == "month") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }

      }
      else if (method == "var"){
        if (period == "month") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else {
        val c: JsValue = JsObject(Seq("Error" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else if ((aggrMethod isDefined) && (aggrPeriod isEmpty)){
      val method = aggrMethod.get
      var df = spark.emptyDataFrame

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        try {
          df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
        }
        catch {
          case e: org.apache.spark.sql.AnalysisException =>
            val c: JsValue = JsObject(Seq("Información"
              -> JsString("El campo <type> especificado no se encuentra " +
              "en nuestra base de datos.")))
            result = c.toString()
        }

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
        val dfStd = df
          .agg(stddev("value"))
        val data = dfStd.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "var") {
        val dfVar = df.agg(variance("value"))
        val data = dfVar.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else {
        val c: JsValue = JsObject(Seq("Error" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else {
      var df = spark.emptyDataFrame

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }

      df = spark.read.parquet(path)
        .select("notifiedAt", property + ".value")
      val data = df.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    result
  }

  def getDataInThisTimeframe(dF: DataFrame,
                              path: String, dateFrom: Option[String],
                             dateTo: Option[String],
                             isId: Boolean,
                             id: String) = {

    var df: DataFrame = dF
    var dfResult: DataFrame = dF

    if (isId == true) {
      df = dF.filter("id == '" + id + "'")
    }

    if ((dateFrom isDefined) && (dateTo isDefined)) {

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)

      dfResult = df2

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)) {
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)

      dfResult = df2

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)) {
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)

      dfResult = df2
    }

    dfResult
  }


  def getAggrMethodPropertyId(id: String, property: String,
                              aggrMethod: Option[String],
                              aggrPeriod: Option[String],
                              dateFrom: Option[String], dateTo: Option[String],
                              lastN: Option[String])
                             (implicit spark: SparkSession) = {

    val path: String = "hdfs://localhost:9000/" +
      id.split(":")(2) + "/*/*"

    var data = Array[String]()
    var result = ""

    if ((aggrMethod isDefined) && (aggrPeriod isDefined)) {
      var df = spark.emptyDataFrame

      val method = aggrMethod.get
      val period = aggrPeriod.get

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, true, id)
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .filter("id == '" + id + "'")
          .select("notifiedAt", property + ".value")
      }
      if (lastN isDefined){
        df = df.orderBy(col("notifiedAt").desc).limit(lastN.get.toInt)
      }

      if (method == "avg") {
        if (period == "month") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "max"){
        if (period == "month") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }

        else if (period == "hour") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "min"){
        if (period == "month") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "sum"){
        if (period == "hour") {
          val dfSum = df.groupBy(hour(col("notifiedAt")))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfSum = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfSum = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .sum()
          val data = dfSum.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else if (method == "std"){
        if (period == "month") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }

      }
      else if (method == "var"){
        if (period == "month") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "day") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "hour") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(month(col("notifiedAt")).as("month"),
            dayofmonth(col("notifiedAt")).as("day"),
            hour(col("notifiedAt")).as("hour"),
            minute(col("notifiedAt")).as("minute"),
            second(col("notifiedAt")).as("second"))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else {
          val c: JsValue = JsObject(Seq("Error" -> JsString("Periodo no válido")))
          result = c.toString()
        }
      }
      else {
        val c: JsValue = JsObject(Seq("Error" -> JsString("Método no válido")))
        result = c.toString()
      }
    }
    else {
      var df = spark.emptyDataFrame

      if ((dateFrom isDefined) || (dateTo isDefined)) {
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }

      df = spark.read.parquet(path)
        .select("notifiedAt", property + ".value")
      val data = df.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    result
  }
}

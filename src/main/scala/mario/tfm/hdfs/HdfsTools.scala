package mario.tfm.hdfs

import java.lang.NullPointerException
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.logging.Logger

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import play.api.libs.json.{JsNumber, JsObject, JsString, JsValue, Json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.functions.{hour, minute, second, stddev, variance}

object HdfsTools extends App {

  val spark = SparkSession.builder
    .appName("SparkSession")
    .master("local[4]")
    .getOrCreate
  import spark.implicits._
  lazy val logger = Logger.getLogger(this.getClass.getSimpleName)



  val jsonString = """{"id":"urn:ngsi-ld:Notification:5e78b78f455cbceffa17b499","type":"Notification","subscriptionId":"urn:ngsi-ld:Subscription:01","@context":"http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld","notifiedAt":"2020-03-23T13:20:15Z","data":[{"id":"urn:ngsi-ld:Vehicle:A4501","type":"Vehicle","brandName":{"type":"Property","value":"Mercedes"},"isParked":{"type":"Relationship","object":"urn:ngsi-ld:OffStreetParking:Downtown1","providedBy":{"type":"Relationship","object":"urn:ngsi-ld:Person:Bob"},"observedAt":"2018-12-04T12:00:00Z"}}, {"id":"urn:ngsi-ld:PARKING","type":"PARKING","brandName":{"type":"Property","value":"CARREFOUR"},"isParked":{"type":"Relationship","object":"urn:ngsi-ld:OffStreetParking:Downtown1","providedBy":{"type":"Relationship","object":"urn:ngsi-ld:Person:Bob"},"observedAt":"2018-12-04T12:00:00Z"}}]}"""

  Try(Json.parse(jsonString)) match {
    case Failure(t) => t
      complete(StatusCodes.BadRequest)
    case Success(h) =>
      var fulljson = Vector[JsObject]()
      var ds = spark.emptyDataset[String]

      try {
        /*val notifiedAt = (h \ "notifiedAt").as[JsValue]
        val context = (h \ "@context").as[JsValue]
        var data = (h \ "data").as[Vector[JsObject]]

        for (i<-data){
          val c: JsObject = i + ("notifiedAt" -> notifiedAt) + ("context" -> context)
          logger.info("Tamaño del DS antes: " + ds.count())
          ds = ds.union(Seq(c.toString()).toDS)
          logger.info("Tamaño del DS después: " + ds.count())
          fulljson = fulljson :+ c
        }

        val df = spark.read.json(ds)
        logger.info("DATAFRAME TOTAL")
        df.printSchema()
        df.collect().foreach(println)

        // 8020
        df.write.parquet("hdfs://localhost:9000/dataframe.parquet")*/

        val df = spark.read.parquet("hdfs://localhost:9000/dataframe.parquet")
        df.collect().foreach(println)

     }
     catch {
        case _ => println("No funciona")
     }
  }

  def sendParquet(data: JsObject)(implicit spark: SparkSession): Unit = {
    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

    import spark.implicits._

    Try(Json.parse(data.toString())) match {
      case Failure(t) => t
        complete(StatusCodes.BadRequest)
      case Success(h) =>
        logger.info("Ha parseado bien")
        var fulljson = Vector[JsObject]()
        var ds = spark.emptyDataset[String]

        try {
          val notifiedAt = (h \ "notifiedAt").as[JsValue]
          val context = (h \ "@context").as[JsValue]
          val data = (h \ "data").as[Vector[JsObject]]
          var folder: String = ""

          for (i<-data){
            val c: JsObject = i + ("notifiedAt" -> notifiedAt) + ("context" -> context)
            folder= (i \ "type").as[String]
            logger.info("NOMBRE FOLDER : "+ folder)
            logger.info(c.toString())
            logger.info("Tamaño del DS antes: " + ds.count())
            ds = ds.union(Seq(c.toString()).toDS)
            logger.info("Tamaño del DS después: " + ds.count())
            fulljson = fulljson :+ c
          }

          val df = spark.read.json(ds)
          logger.info("DATAFRAME TOTAL")
          df.printSchema()
          df.show(false)
          df.collect().foreach(println)

          // 8020
          val uuid = UUID.randomUUID().toString
          df.write.parquet("hdfs://localhost:9000/"+ folder + "/" + uuid)

        }
        catch {
          case _ => println("No funciona")
        }
    }
  }


  def getId(id: String, dateFrom: Option[String],
            dateTo: Option[String])(implicit spark: SparkSession) = {

    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)
    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
      id.split(":")(2) + "/*" )
      .filter("id == '" + id + "'")

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

      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)

      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    else {
      data = df.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }

    //val data = df.filter("id == '" + id + "'").toJSON.collect()
    //val result = df.select(field).toJSON.collect()

    //Json.toJson(data).toString().replace("\\", "")
    result
  }

  def countGetId(id: String, dateFrom: Option[String],
                 dateTo: Option[String])(implicit spark: SparkSession) = {

    import org.apache.spark.sql.types._

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
      id.split(":")(2) + "/*")
      .filter("id == '" + id + "'")

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

      /*if (count isDefined) {
        val c : JsValue = JsObject(Seq("count" -> JsNumber(df2.count())))
        result = c.toString()
      }*/

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

  def getType(tipo: String, dateFrom: Option[String],
              dateTo: Option[String])(implicit spark: SparkSession): String = {

    lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*")
      .filter("type == '" + tipo + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)

      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)

      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
      df2.show()
      data = df2.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }
    else {
      data = df.toJSON.collect()
      result = Json.toJson(data).toString().replace("\\\"", "")
    }

    result
  }

  def countGetType(tipo: String, dateFrom: Option[String],
                 dateTo: Option[String])(implicit spark: SparkSession) = {

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*")
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

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
      id.split(":")(2) + "/*")
      .filter("id == '" + id + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .orderBy(col("notifiedAt").desc)
      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else {
      data = df.orderBy(col("notifiedAt").desc).toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }

    result
  }


  def getTypeLastN(tipo: String, dateFrom: Option[String],
                   dateTo: Option[String], lastN: Option[String])
                  (implicit spark: SparkSession) = {

    import org.apache.spark.sql.types._

    var data = Array[String]()
    var result = ""
    val df = spark.read.parquet("hdfs://localhost:9000/" +
    tipo + "/*")
      .filter("type == '" + tipo + "'")

    if ((dateFrom isDefined) && (dateTo isDefined)){

      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else if ((dateFrom isDefined) && (dateTo isEmpty)){
      val dfrom = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateFrom.get)
      val from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dfrom)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") >= from)
        .orderBy(col("notifiedAt").desc)

      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else if ((dateFrom isEmpty) && (dateTo isDefined)){
      val dto = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(dateTo.get)
      val to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dto)

      val df2 = df.filter(to_utc_timestamp(col("notifiedAt"), "Europe/Madrid") <= to)
        .orderBy(col("notifiedAt").desc)
      data = df2.toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")

    }
    else {
      data = df.orderBy(col("notifiedAt").desc).toJSON.collect()
      result = Json.toJson(data).head(lastN.get.toInt).toString().replace("\\\"", "")
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
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .select("notifiedAt", property + ".value")
      }
      if (method == "avg") {
        if (period == "hour") {
          val dfAvg = df.groupBy(hour(col("notifiedAt")))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfAvg = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfMax = df.groupBy(hour(col("notifiedAt")))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfMin = df.groupBy(hour(col("notifiedAt")))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfSum = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")), second(col("notifiedAt")))
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
          val dfStd = df.groupBy(hour(col("notifiedAt")))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")), second(col("notifiedAt")))
            .agg(stddev("value"))
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
          val dfVar = df.groupBy(hour(col("notifiedAt")))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")), second(col("notifiedAt")))
            .agg(variance("value"))
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
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, false,"")
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
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, true, id)
          .select("notifiedAt", property + ".value")
      }
      else {
        df = spark.read.parquet(path)
          .filter("id == '" + id + "'")
          .select("notifiedAt", property + ".value")
      }
      if (method == "avg") {
        if (period == "hour") {
          val dfAvg = df.groupBy(hour(col("notifiedAt")))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {

          val dfAvg = df.groupBy(hour(col("notifiedAt")), minute(col("notifiedAt")))
            .avg()
          val data = dfAvg.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfAvg = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfMax = df.groupBy(hour(col("notifiedAt")))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMax = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .max()
          val data = dfMax.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMax = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfMin = df.groupBy(hour(col("notifiedAt")))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfMin = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .min()
          val data = dfMin.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfMin = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfSum = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
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
          val dfStd = df.groupBy(hour(col("notifiedAt")))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfStd = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .agg(stddev("value"))
          val data = dfStd.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfStd = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")),
            second(col("notifiedAt")))
            .agg(stddev("value"))
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
          val dfVar = df.groupBy(hour(col("notifiedAt")))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "min") {
          val dfVar = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")))
            .agg(variance("value"))
          val data = dfVar.toJSON.collect()
          result = Json.toJson(data).toString().replace("\\\"", "")
        }
        else if (period == "sec") {
          val dfVar = df.groupBy(hour(col("notifiedAt")),
            minute(col("notifiedAt")), second(col("notifiedAt")))
            .agg(variance("value"))
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
        val dF = spark.read.parquet(path)
        df = getDataInThisTimeframe(dF, path, dateFrom, dateTo, true,id)
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
          .agg(stddev("value"))
        val data = dfStd.toJSON.collect()
        result = Json.toJson(data).toString().replace("\\\"", "")
      }
      else if (method == "var") {
        df.show(false)
        val dfVar = df.agg(variance("value"))
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
}

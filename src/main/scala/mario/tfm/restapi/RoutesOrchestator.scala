package mario.tfm.restapi

import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.http.scaladsl.server.directives.PathDirectives.path

import scala.concurrent.ExecutionContext
import akka.util.Timeout
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import java.util.logging.Logger

import akka.http.scaladsl.{Http, HttpExt}
import mario.tfm.parameters.Config
import mario.tfm.hdfs.HdfsTools._

import mario.tfm.postgresql.DataToolsPostgres._
import mario.tfm.postgresql.PostgresTools._
import mario.tfm.restapi.Routes._
import org.apache.spark.sql.SparkSession

import mario.tfm.mysql.MysqlTools._

//#user-routes-class
trait RoutesOrchestator {
  //#user-routes-class

  // we leave these abstract, since they will be provided by the App
  implicit def system: ActorSystem
  implicit def ec: ExecutionContext = system.dispatcher
  implicit def http: HttpExt = Http(system)
  implicit def config: Config


  lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

  // other dependencies that UserRoutes use
  //def userRegistryActor: ActorRef

  // Required by the `ask` (?) method below
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration

  //#all-routes
  //#users-get-post
  //#users-get-delete

  lazy val RoutesOrchestator: Route =
   /*extractRequest { request =>
   println(s"Request uri: ${request.uri}")
   println(s"Request protocol: ${request.protocol}")
   println(s"Request entity: ${request.entity}")
   println(s"Request entity data bytes: ${request.entity.toString}")
   println(s"Request headers: ${request.headers}")
   println(s"Request method value: ${request.method.value}")
   println(s"Request method is ${request.method.name} and content-type is ${request.entity.contentType}")
   complete(s"Request method is ${request.method.name} and content-type is ${request.entity.contentType}")
 }*/

    if (config.persistence == "mysql"){

      // CREATE SQL CONNECTION
      implicit val connection = createConnectionMysql()

      concat (
        // rc de receive context
        pathPrefix("notification") {
          receiveContextMysql
        },

        path("subscribeContext") {
          subscribeContext
        },
      )
    }

    else if (config.persistence == "hdfs") {

      import org.apache.log4j.Logger
      import org.apache.log4j.Level

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      implicit val spark = SparkSession.builder
        .appName("SparkSession")
        .master("local[4]")
        .config("spark.network.timeout", 100000000)
        .config("spark.executor.heartbeatInterval",100000000)
        .config("spark.io.compression.codec", "snappy")
        .config("spark.rdd.compress", "true")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.parquet.binaryAsString", "true")
        .config("spark.hive.mapred.supports.subdirectories","true")
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
        .getOrCreate


      concat (
        // rc de receive context
        pathPrefix("notification") {
          receiveContextNoSql
        },

        path("subscribeContext") {
          subscribeContext
        },
        // http://host:port/path?key=authType&value=Basic345

        pathPrefix ("data"){

          concat (

            path("id" / Segment){
              (id) =>
                get {
                  parameters('dateFrom?, 'dateTo?, 'lastN?, 'count?){
                    (dateFrom: Option[String], dateTo: Option[String],
                     lastN: Option[String], count: Option[String]) =>

                      if (lastN isEmpty) {
                        if (count isDefined){
                          if (count.get.equals("true")){
                            val result = countGetId(id, dateFrom, dateTo)
                            complete(HttpEntity(ContentTypes.`application/json`, result))
                          }
                          else if (count.get.equals("false")){
                            val result = getId(id, dateFrom, dateTo)
                            complete(HttpEntity(ContentTypes.`application/json`, result))
                          }
                          else {
                            val c: JsValue = JsObject(Seq("Error" -> JsString("Valor de count no válido")))
                            complete(HttpEntity(ContentTypes.`application/json`, c.toString()))
                          }
                        }
                        else {
                          val result = getId(id, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                        }
                      }
                      else {
                        val result = getIdLastN(id, dateFrom, dateTo, lastN)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                  }
                }
            },
            path("type" / Segment){
              (tipo) =>
                get {
                  parameters('dateFrom?, 'dateTo?, 'lastN?, 'count?){
                    (dateFrom: Option[String], dateTo: Option[String],
                     lastN: Option[String], count: Option[String]) =>

                      if (lastN isEmpty) {
                        if (count isDefined){
                          if (count.get.equals("true")){
                            val result = countGetType(tipo, dateFrom, dateTo)
                            complete(HttpEntity(ContentTypes.`application/json`, result))
                          }
                          else if (count.get.equals("false")){
                            val result = getType(tipo, dateFrom, dateTo)
                            complete(HttpEntity(ContentTypes.`application/json`, result))
                          }
                          else {
                            val c: JsValue = JsObject(Seq("Error" -> JsString("Valor de count no válido")))
                            complete(HttpEntity(ContentTypes.`application/json`, c.toString()))
                          }
                        }
                        else {
                          val result = getType(tipo, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                        }
                      }
                      else {
                        val result = getTypeLastN(tipo, dateFrom, dateTo, lastN)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                  }
                }
            },

            path("id" / Segment / "property" / Segment){
              (id, property) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?, 'lastN?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String], lastN: Option[String]
                         ) =>
                          val result = getAggrMethodPropertyId(id, property,
                            aggrMethod, aggrPeriod, dateFrom, dateTo, lastN)
                          complete(HttpEntity(ContentTypes.`application/json`, result))

                      }
                    }
                  }

                )
            },
            path("id" / Segment / "property" / Segment / Segment){
              (id, property, property2) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?,
                      'lastN?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String],
                         lastN: Option[String]
                        ) =>
                          val result = getAggrMethodPropertyId(id,
                            property + "." + property2,
                            aggrMethod, aggrPeriod, dateFrom, dateTo, lastN)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            },
            path("type" / Segment / "property" / Segment){
              (tipo, property) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?,
                      'lastN?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String],
                         lastN: Option[String]
                        ) =>
                          val result = getAggrMethodPropertyType(tipo, property,
                            aggrMethod, aggrPeriod, dateFrom, dateTo, lastN)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            },
            path("type" / Segment / "property" / Segment / Segment){
              (tipo, property, property2) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?,
                      'lastN?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String],
                         lastN: Option[String]
                        ) =>
                          val result = getAggrMethodPropertyType(tipo,
                            property + "." + property2,
                            aggrMethod, aggrPeriod, dateFrom, dateTo, lastN)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            }
          )

        }
      )
    }

    else if (config.persistence == "postgres"){
      // CREATE SQL CONNECTION
      implicit val connection = createConnectionPostgres()

      concat (
        // rc de receive context
        pathPrefix("notification") {
          receiveContextPostgres
        },

        path("subscribeContext") {
          subscribeContext
        },
      )
    }

    else {
      val c: JsValue = JsObject(Seq("Error" -> JsString("Valor de persistence no válido " +
        "en el servidor.")))
      complete(StatusCodes.Forbidden -> HttpEntity(ContentTypes.`application/json`, c.toString()))
    }

  //#all-routes
}

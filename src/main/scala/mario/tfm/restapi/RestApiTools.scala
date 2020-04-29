package mario.tfm.restapi

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, MediaTypes, RequestEntity, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object RestApiTools {

  def postApplication(implicit  ec: ExecutionContext, http: HttpExt, d: String): Future[HttpResponse] = {
    var aux = ""
    Try(Json.parse(d)) match {
      case Failure(t) => t
        throw new Exception("JSON not valid")
      case Success(h) =>
        try {
          aux = Json.stringify(h.as[JsObject] - "@context")
        }
        catch {
          case _ => "No se ha podido acceder a los datos"
            complete(StatusCodes.BadRequest)
        }
        Marshal(aux).to[RequestEntity] flatMap { ent =>
          println(ent)
          //http.content_type == "application/json"
          val request = HttpRequest(method = HttpMethods.POST, uri = "http://orion:1026/ngsi-ld/v1/subscriptions/", entity = ent.withContentType(MediaTypes.`application/json`)).withHeaders(RawHeader("Link", "<http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""))
          println("REQUEST " + request)
          //ContentTypes.`application/json`  "http://orion:1026/ngsi-ld/v1/entities"
          http.singleRequest(request)
        }
    }
  }

}

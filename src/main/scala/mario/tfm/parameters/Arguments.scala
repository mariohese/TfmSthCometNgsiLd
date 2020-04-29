package mario.tfm.parameters

import java.util.logging.Logger

import scopt.OParser

object Arguments extends App {

  lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

  def getArgs (args: Array[String])= {
    var conf = Config()

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("isSql")
          .required()
          .action((x, c) => c.copy(isSql = x))
          .text("Put this option to true in case want data in Sql format")
        // Si se añade otro parámetro, poner una coma antes de poner opt,

      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config: Config) =>
        conf = config
      case _ =>
        System.exit(-1)
      // arguments are bad, error message will have been displayed
    }
    conf
  }


}


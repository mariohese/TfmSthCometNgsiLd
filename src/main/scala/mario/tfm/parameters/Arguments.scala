package mario.tfm.parameters

import java.io.File

import mario.tfm.parameters.Config
import scopt.OParser

object Arguments {

  def getArgs (args: Array[String])= {

    var conf = Config()

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        opt[String]("bootstrap_servers")
          .required()
          .action((x, c) => c.copy(bootstrap_servers = x))
          .text("Bootstrap servers separated by ;"),
        opt[String]("application_id")
          .required()
          .action((x, c) => c.copy(application_id = x))
          .text("Id for the application"),
        opt[String]("client_id")
          .required()
          .action((x, c) => c.copy(client_id = x))
          .text("Id for the client"),
        opt[String]("commit_interval_ms")
          .required()
          .action((x, c) => c.copy(commit_interval_ms = x))
          .text("Commit interval in milliseconds"),
        opt[String]("num_stream_threads")
          .required()
          .action((x, c) => c.copy(num_stream_threads = x))
          .text("Number of streaming threads"),
        opt[String]("az_account_name")
          .required()
          .action((x, c) => c.copy(az_account_name = x))
          .text("Name of the Azure Account"),
        opt[String]("az_path")
          .required()
          .action((x, c) => c.copy(az_path = x))
          .text("Azure path where to save the data"),
        opt[String]("az_account_key")
          .required()
          .action((x, c) => c.copy(az_account_key = x))
          .text("Azure Account Key"),
        opt[String]("az_container_name")
          .required()
          .action((x, c) => c.copy(az_container_name = x))
          .text("Azure Container Name"),
        opt[String]("topic_name")
          .required()
          .action((x, c) => c.copy(topic_name = x))
          .text("Name of the Kafka topic to connect to"),
        opt[String]("errors_topic")
          .required()
          .action((x, c) => c.copy(errors_topic = x))
          .text("Name of the Kafka topic where errors are sent"),
        opt[String]("format")
          .required()
          .action((x, c) => c.copy(format = x))
          .text("Format of the saved data")
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        conf = config
      case _ =>
        System.exit(-1)
      // arguments are bad, error message will have been displayed
    }
    conf
  }


}


package mario.tfm.json_ld

import java.io.{ FileInputStream, StringReader }
import java.util

import com.github.jsonldjava.core.{ JsonLdOptions, JsonLdProcessor }
import com.github.jsonldjava.utils.JsonUtils
import org.apache.commons.rdf.jsonldjava._
import org.apache.jena.riot.RiotException

class Lab extends App {

  def contextLoad() = {
    /*getClass.getClassLoader.getResource("jarcache.json")
    getClass.getClassLoader.getResource("contexts/vehicle.jsonld")

    println(classOf[Lab].getClassLoader.getResource("jarcache.json"))
    println(Thread.currentThread().getContextClassLoader().getResource("jarcache.json"))
    println(Thread.currentThread().getContextClassLoader().getResource("contexts/vehicle.jsonld"))
    Thread.currentThread().getContextClassLoader()
    Thread.currentThread().setContextClassLoader(classOf[Lab].getClassLoader())
    */

    val inputStream = new FileInputStream("C:\\Users\\mario\\IdeaProjects\\TFMAPI\\src\\main\\scala\\mario\\tfm\\json_ld\\vehicle.jsonld")
    val jsonObject = JsonUtils.fromInputStream(inputStream)
    val context = new util.HashMap()
    val options = new JsonLdOptions
    val compact = JsonLdProcessor.compact(jsonObject, context, options)
    System.out.println(JsonUtils.toPrettyString(compact))

    val keys = compact.entrySet()
    val it = keys.iterator()

    while (it.hasNext()) {
      it.next().getKey
    }

  }

}

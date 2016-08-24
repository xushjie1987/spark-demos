import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._

/**
  * Created by hadoop on 16-8-24.
  */
object SparkEsRead {

    def main(args: Array[String]) = {
        //
        val conf = new SparkConf().setAppName("Spark Es Read").setMaster("spark://10.128.6.72:7077")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", "10.128.6.72")
        conf.set("es.port", "9200")
        //
        val sc = new SparkContext(conf)
        //
        val docs = sc.esRDD("test_1/logs", "?q=log_level:ERROR")
        docs.foreach((x: (String,scala.collection.Map[String, AnyRef])) => {
            println(x)
            println(x._1)
            println(x._2)
            println(x.toString())
        })
    }

}

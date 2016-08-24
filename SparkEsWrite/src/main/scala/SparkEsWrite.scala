import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._

/**
  * Created by hadoop on 16-8-23.
  */
object SparkEsWrite {

    def main(args: Array[String]) = {
        //
        val conf = new SparkConf().setAppName("Spark Es Write").setMaster("spark://10.128.6.72:7077")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", "10.128.6.72")
        conf.set("es.port", "9200")
        //
        val sc = new SparkContext(conf)
        //
        val doc1 = Map("log_level" -> "INFO", "log_msg" -> "test1", "log_tm" -> "2016-08-23")
        val doc2 = Map("log_level" -> "ERROR", "log_msg" -> "test2", "log_tm" -> "2016-08-22")
        sc.makeRDD(Seq(doc1, doc2)).saveToEs("test_1/logs")
    }

}

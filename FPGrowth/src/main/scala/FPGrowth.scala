import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

/**
  * Created by hadoop on 16-8-24.
  */
object FPGrowth {

    def main(args: Array[String]) {
        //
        val conf = new SparkConf().setAppName(s"FPGrowth")
        val sc = new SparkContext(conf)
        val transactions = sc.textFile("/data/spark-1.6.2/input.txt").map(_.split("\\s+")).cache()
        //
        println(s"Number of transactions: ${transactions.count()}")
        //
        val model = new FPGrowth()
            .setMinSupport(0.2)
            .setNumPartitions(2)
            .run(transactions)
        //
        println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
        //
        model.freqItemsets.collect().foreach { itemset =>
            println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
        }
        //
        sc.stop()
    }

}

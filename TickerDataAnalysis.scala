import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object TickerDataAnalysis {
  
  case class ticker(ticker:String, date:String, open:Double, close:Double, split_ratio:Double,adj_open:Double, adj_close: Double, adj_vol:Double);
  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val tickerDataFile = "/user/bachiyld/wiki_prices.txt" 
    val tickerFile = sc.textFile(tickerDataFile).
                     map(x=> x.split(",")).
                     filter(x=> x(0)!="ticker").
                     filter(x=> (x(0).nonEmpty && x(1).nonEmpty && x(2).nonEmpty && x(3).nonEmpty && x(4).nonEmpty && x(8).nonEmpty && x(9).nonEmpty && x(12).nonEmpty && x(13).nonEmpty))
    val ticker_DF = tickerFile.map(x=> ticker(x(0),x(1),x(2).toDouble,x(5).toDouble,x(8).toDouble,x(9).toDouble,x(12).toDouble,x(13).toDouble)).toDF()
    ticker_DF.persist()
    
    val pattern = "yyyy-MM-dd"
    val ticker_data_parsed = ticker_DF.withColumn("date_stamp", unix_timestamp(ticker_DF("date"), pattern).cast("timestamp"))

    val ticker_data_rdd = ticker_data_parsed.filter("date_stamp='2017-01-03 00:00:00.0' or date_stamp='2017-08-25 00:00:00.0'").orderBy($"ticker",$"date_stamp".desc);
    val ticker_data_adj = ticker_data_rdd.select($"ticker", $"date_stamp", $"adj_open", $"adj_close").rdd
        
    val ticker_data_pair_rdd = ticker_data_rdd.map(x=> (x(0).toString,(x(2).toString.toDouble,x(3).toString.toDouble))).reduceByKey((x,y)=> (y._1,x._2))
    val ticker_result_rdd = ticker_data_pair_rdd.map(x=> ((x._1,x._2._1,x._2._2),(x._2._2 - x._2._1)*100/x._2._1)).sortBy(_._2,false)
    ticker_result_rdd.saveAsTextFile("/user/bachiyld/tickers/results/");
    sc.stop()

  }
}

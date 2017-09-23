/*
# Problem Statement: Determine Stocks given high return this year (source: moneycontrol.com)
# Data Source: NYSE data from Quandle.com (WIKI Prices)
# Description: 
Data source has ticker id (stock id), date, open price, close price, day high price, day low price, volume, adjusted open, 
adjusted close and adjusted volume.
Adjusted open and adjusted close are the driving factors for calculating return as these includes any corporate actions 
like stock split, stock merge etc taken on the stock. (Source: Investopedia.com)

# Spark Application development using Scala: 
Data is about size of 1.6GB stored in around 14 HDFS block which is good example for parallel computing.
Write code to open the file, filter out for junk data like fields with no values or null values.
Get the adjusted open for each stock on first trading day of the year and adjusted close price of the day on which calculation 
need to be done. This is done using core spark APIs and Spark SQL using data frames.
Calculate the return percentage based on the two prices extracted in earlier step, sort the data based on return in descending order and save the data to HDFS.
Build the application with SBT and ran it on cluster with default parameters and with some tuned parameters for better performance.
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object TickerDataAnalysis {
  
  //class to parse the input file and create dataframe
  case class ticker(ticker:String, date:String, open:Double, close:Double, split_ratio:Double,adj_open:Double, adj_close: Double, adj_vol:Double);
  def main(args: Array[String]) {

    //Initialize spark context, SQL context and import explicits.
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //get the data file from HDFS
    val tickerDataFile = "/user/bachiyld/wiki_prices.txt" 
    /*
    Tokenize the rows
    filter out header
    filter out null values
    parse the data using "ticker" case class and create a dataframe
    persit the DF.
    */
    val tickerFile = sc.textFile(tickerDataFile).
                     map(x=> x.split(",")).
                     filter(x=> x(0)!="ticker").
                     filter(x=> (x(0).nonEmpty && x(1).nonEmpty && x(2).nonEmpty && x(3).nonEmpty && x(4).nonEmpty && x(8).nonEmpty && x(9).nonEmpty && x(12).nonEmpty && x(13).nonEmpty))
    val ticker_DF = tickerFile.map(x=> ticker(x(0),x(1),x(2).toDouble,x(5).toDouble,x(8).toDouble,x(9).toDouble,x(12).toDouble,x(13).toDouble)).
                    toDF()
    ticker_DF.persist()
    
    /*convert the date pattern for filtering*/
    val pattern = "yyyy-MM-dd"
    val ticker_data_parsed = ticker_DF.
                             withColumn("date_stamp", unix_timestamp(ticker_DF("date"), pattern).
                             cast("timestamp"))

    /*filter out data for start day of the year and as of which on which precentage need to be calculated.
    Note: these arguments can passed as arugments.
    for each stock we will get two rows. Get the latest row on top by sorting.*/
    val ticker_data_rdd = ticker_data_parsed.
                          filter("date_stamp='2017-01-03 00:00:00.0' or date_stamp='2017-08-25 00:00:00.0'").
                          orderBy($"ticker",$"date_stamp".desc)
    val ticker_data_adj = ticker_data_rdd.
                          select($"ticker", $"date_stamp", $"adj_open", $"adj_close").
                          rdd
        
    
    /*Perform calculation to deterime percentage increment
    sort the stock based on high returns given.
    save the results to HDFS.
    */
    val ticker_data_pair_rdd = ticker_data_rdd.
                               map(x=> (x(0).toString,(x(2).toString.toDouble,x(3).toString.toDouble))).
                               reduceByKey((x,y)=> (y._1,x._2))
    val ticker_result_rdd = ticker_data_pair_rdd.
                            map(x=> ((x._1,x._2._1,x._2._2),(x._2._2 - x._2._1)*100/x._2._1)).
                            sortBy(_._2,false)
    ticker_result_rdd.saveAsTextFile("/user/bachiyld/tickers/results/");
    //exit the spark context.
    sc.stop()

  }
}

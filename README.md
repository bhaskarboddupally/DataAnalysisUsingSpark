# Stock market data analysis using Spark

Problem Statement: Determine Stocks given high return this year (source: moneycontrol.com)
Data Source: NYSE data from Quandle.com

Description: 
Data source has ticker id (stock id), date, open price, close price, day high price, day low price, volume, adjusted open, adjusted close and adjusted volume.
Adjusted open and adjusted close are the driving factors for calculating return as these includes any corporate actions like stock split, stock merge etc taken on the stock. (Source: Investopedia.com)

# Spark Application development using Scala: 
Data is about size of 1.6GB stored in around 14 HDFS block which is good example for parallel computing.
Write code to open the file, filter out for junk data like fields with no values or null values.
Get the adjusted open for each stock on first trading day of the year and adjusted close price of the day on which calculation need to be done. T
Calculate the return percentage based on the two prices extracted in earlier step, sort the data based on return in descending order and save the data to HDFS.
Build the application with SBT and run it on cluster with default parameters and with some tuned parameters for better performance.


package com.pavanpkulkarni.sparkstreaming

import org.apache.spark.sql.SparkSession

object SocketStreaming{
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession
			.builder()
			.master("local")
			.appName("Socket_Streaming")
			.getOrCreate()
		
		val socketStreamDf = spark.readStream
			.format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()
		
		import spark.implicits._
		
		val words = socketStreamDf.as[String].flatMap(_.split(" "))
		
		val wordCounts = words.groupBy("value").count()
		
		val query = wordCounts.writeStream
			.outputMode("complete")
			.format("console")
			.start()
		
		query.awaitTermination()
		
	}
	
}

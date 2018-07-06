package com.pavanpkulkarni.sparkstreaming


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object FileToFileStreaming{
	
	case class Customer(customer_id : String, customer_name: String, customer_location: String)
	
	def main(args: Array[String]): Unit = {
		
		//initialize the spark session
		val spark = SparkSession
			.builder()
			.master("local")
			.appName("File_Streaming")
			.getOrCreate()
		
		val schema = StructType(
			Array(StructField("customer_id", StringType),
				StructField("pid", StringType),
				StructField("pname", StringType),
				StructField("date", StringType)))
		
		//get the orders stream from the csv files.
		val ordersStreamDF = spark
			.readStream
			.option("header", "true")
			.schema(schema)
			.csv(args(0))
		
		import spark.implicits._
		
		//get the static customer data
		val customerDS = spark.read
			.format("csv")
			.option("header", true)
			.load("src/main/resources/input/cutomer_info/customer.csv")
			.map(x => Customer(x.getString(0), x.getString(1), x.getString(2)))
		
		val finalResult = ordersStreamDF.join(customerDS, "customer_id")
		
		//write the joined stream to json/parquet output.
		val query = finalResult
			.writeStream
			.queryName("count_customer")
			//.format("console")
			.outputMode("append")
			.format("json")
			.partitionBy("date")
			.option("path", "src/main/resources/output/")
			.option("checkpointLocation", "src/main/resources/chkpoint_dir")
			.start()

		query.awaitTermination()
	}
}


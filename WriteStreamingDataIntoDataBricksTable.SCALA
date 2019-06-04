// Prepare a dataframe with Content and Sentiment columns
val streamingDataFrame = incomingStream.selectExpr("cast (body as string) AS Content").withColumn("Sentiment", toSentiment($"Content"))

// Display the streaming data with the sentiment
//streamingDataFrame.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// Write streaming data into Parquet File format
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val result =
  streamingDataFrame
    .writeStream
    .format("parquet")
    .option("path", "/mnt/TwitterSentiment")  
    .option("checkpointLocation", "/mnt/temp/check")
    .start()

// Read data from Parquet files
val sentimentdata = spark.read.parquet("/mnt/TwitterSentiment") 

display(sentimentdata)

// Write Parquet data as a Data Table in Databricks
spark.read.parquet("/mnt/TwitterSentiment").write.mode(SaveMode.Overwrite) saveAsTable("twitter_sentiment")

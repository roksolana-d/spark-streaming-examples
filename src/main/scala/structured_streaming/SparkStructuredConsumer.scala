package structured_streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.window
import utils.SparkSqlFunctionsTemplates

object SparkStructuredConsumer {

  val generalFunctions = new SparkSqlFunctionsTemplates

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName( "KafkaStructuredSparkStreaming" )
      .set( "es.index.auto.create", "true" )

    val sparkSession = SparkSession
      .builder()
      .config( sparkConf )
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")

    val topic = "tweets"

    import sparkSession.implicits._

    val tweetsStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()

    val tweetsStructured = tweetsStream
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .select(from_json($"value", generalFunctions.buildTweetDataStruct()).as("tweet"))


    val userFollowersCount = "tweet.user.followers_count"
    val userLanguage = "tweet.user.lang"

    val languageFilterResults = generalFunctions.filterContainsStringValues(tweetsStructured, userLanguage, "en")
    val keywordsFilterResults = generalFunctions.outputSpecificField(generalFunctions.filterContainsStringValues(tweetsStructured, "tweet.text", "big data"), "tweet.text")

    val countByLanguage = generalFunctions.countColumnValues(tweetsStructured, userLanguage)

    val countByFollowers = generalFunctions.countColumnValues(tweetsStructured, userFollowersCount)

    val filterByFollowersNumber = generalFunctions.outputSpecificField(generalFunctions.filterBiggerThanIntValues(tweetsStructured, userFollowersCount, 100), userFollowersCount)

    val minFollowersCount = generalFunctions.minAndMaxColumnValues(tweetsStructured, userFollowersCount)

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val parseDate = udf((timeString: String) => {
      val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
      val parsedDate = dateFormat.parse(timeString)
      val timestamp = new Timestamp(parsedDate.getTime)
      timestamp
    })

    val tweetTimestamp = "tweet.timestamp_ms"
    val updatedTweets = tweetsStructured.withColumn("timestamp_ms",  from_unixtime($"tweet.timestamp_ms" / 1000))
 updatedTweets.printSchema()

    val windowedAggregation = updatedTweets.groupBy(window(updatedTweets.col("timestamp_ms"), "7 minutes", "3 minutes"),
      updatedTweets.col(userLanguage))
      .count()

//    val watermarkingAggregation = updatedTweets
////      .withWatermark("tweet.timestamp_ms", "7 minutes")
//      .groupBy(window(updatedTweets.col("timestamp_ms"), "7 minutes", "3 minutes"),
//      updatedTweets.col(userLanguage))
//      .count()

    val outputModeAppend = "append"
    val outputModeComplete = "complete"
    val outputFormat = "console"

    val outputExample = watermarkingAggregation
      .writeStream
      .outputMode(outputModeComplete)
      .format(outputFormat)
      .start()

    outputExample.awaitTermination()
  }
}

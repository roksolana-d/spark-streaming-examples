package legacy_streaming

import java.text.SimpleDateFormat

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._

object SparkLegacyConsumer {

  def formatDate(date: String) = {
    val initialDate = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy").parse(date)
    val esFormat = new SimpleDateFormat("MMMM DD YYYY, HH:mm:ss.SSS")
    esFormat.parse(esFormat.format(initialDate))
  }

  def convertToTweetMap(data: Array[String]) = {
    List(Map("tweetId" -> data(0).toLong,
      "text" -> data(1),
      "favoriteCount" -> data(2).toLong,
      "retweetCount" -> data(3).toLong,
      "geoLocation" -> data(4),
      "language" -> data(5),
      "createdAt" -> formatDate(data(6)),
      "inReplyToStatusId" -> data(7).toLong,
      "inReplyToUserId" -> data(8).toLong,
      "user" -> Map("userId" -> data(9).toLong,
        "userName" -> data(10),
        "userDescription" -> data(11),
        "userUrl" -> data(12),
        "userLocation" -> data(13),
        "userFollowersCount" -> data(14).toLong,
        "userFriendsCount" -> data(15).toLong,
        "userFavouritesCount" -> data(16).toLong,
        "userStatusesCount" -> data(17).toLong,
        "userCreatedAt" -> formatDate(data(18))
      )
    ))
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("KafkaSparkStreaming")
      .set("es.index.auto.create", "true")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(3))
    val sparkContext = streamingContext.sparkContext
    sparkContext.setLogLevel("ERROR")

    val sqlContext = new SQLContext(sparkContext)

    val numStreams = args(1).toInt
    val topics = Array("tweets")

    def kafkaParams(i: Int) = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val lines = (1 to numStreams).map(i => KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams(i))
    ))

    val messages = streamingContext.union(lines)

    val values = messages
      .map(record => record.value().toString)
    values.print()

    val es_index = "tweets-time/output"

    val wordsArrays = values.map(_.split(" !& "))

    wordsArrays.foreachRDD(rdd => rdd.flatMap(
      record => convertToTweetMap( record )
    ).saveToEs( es_index))


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
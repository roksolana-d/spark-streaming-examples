package streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import utils.TwitterConfigurations

object StringKafkaProducer extends App{

  val configs = new TwitterConfigurations
  val topic = args(0)
  val brokers = "localhost:9092"
  val props = new Properties()
  val keywords = configs.getKeywords(args(1))

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "KafkaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("batch.size", args(2))
  props.put("linger.ms", args(3))

  val producer = new KafkaProducer[String, String](props)


  def startTweetStream() = {

    val stream = new TwitterStreamFactory(configs.getTwitterConfig()).getInstance()

    val listener = new StatusListener {

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) =
        println(s"Track limited $numberOfLimitedStatuses tweets")

      override def onStallWarning(stallWarning: StallWarning) =
        println("Stream stalled")

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) =
        println("Status ${statusDeletionNotice.getStatusId} deleted")

      override def onScrubGeo(userId: Long, upToStatusId: Long) =
        println(s"Geo info scrubbed. userId:$userId, upToStatusId:$upToStatusId")

      override def onException(exception: Exception) =
        println("Exception occurred. " + exception.getMessage)

      override def onStatus(status: Status): Unit = {
        val tweetId = status.getId
        val tweetText = status.getText
        val tweetFavoriteCount = status.getFavoriteCount
        val retweetCount = status.getRetweetCount
        val tweetGeoLocation = status.getGeoLocation
        val tweetLanguage = status.getLang
        val tweetCreatedAt = status.getCreatedAt
        val tweetPlace = status.getPlace
        val inReplyToStatusId = status.getInReplyToStatusId
        val inReplyToUserId = status.getInReplyToUserId

        val user = status.getUser
        val userId = user.getId
        val userName = user.getName
        val userDescription = user.getDescription
        val userUrl = user.getURL
        val userLocation = user.getLocation
        val userFollowersCount = user.getFollowersCount
        val userFriendsCount = user.getFriendsCount
        val userFavouritesCount = user.getFavouritesCount
        val userStatusesCount = user.getStatusesCount
        val userCreatedAt = user.getCreatedAt
        val tweet = s"$tweetId !& $tweetText !& $tweetFavoriteCount !& $retweetCount !& $tweetGeoLocation !& $tweetLanguage !& " +
          s"$tweetCreatedAt !& $inReplyToStatusId !& $inReplyToUserId !& $userId !& $userName !& $userDescription !& $userUrl !& " +
          s"$userLocation !& $userFollowersCount !& $userFriendsCount !& $userFavouritesCount !& $userStatusesCount !& $userCreatedAt"


        println("[Tweet] " + tweet)
        val data = new ProducerRecord[String, String](topic, tweet)
        producer.send(data)
      }
    }

    stream.addListener(listener)
    val fq = new FilterQuery()
    fq.track(keywords.mkString(","))
    stream.filter(fq)
  }

  startTweetStream()

}

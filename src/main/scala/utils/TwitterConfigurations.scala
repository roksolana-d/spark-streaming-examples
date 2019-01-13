package utils

import java.io.File
import com.typesafe.config.ConfigFactory
import twitter4j.conf.ConfigurationBuilder
import scala.io.Source

class TwitterConfigurations {

  def getTwitterConfig() = {
    val twitterConfig = ConfigFactory.parseFile(new File("twitter_configs/twitter_config.conf"))

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    cb.setOAuthConsumerKey(twitterConfig.getString("consumerKey"))
    cb.setOAuthConsumerSecret(twitterConfig.getString("consumerSecret"))
    cb.setOAuthAccessToken(twitterConfig.getString("accessToken"))
    cb.setOAuthAccessTokenSecret(twitterConfig.getString("accessTokenSecret"))
    cb.setJSONStoreEnabled(true)
    cb.setIncludeEntitiesEnabled(true)

    cb.build()
  }

  def getKeywords(filename: String) = {
    val fileContents = Source.fromFile("twitter_configs/" + filename).getLines().mkString

    fileContents
      .split(", ")
      .map(word => "#" + word)
      .toList
  }

}

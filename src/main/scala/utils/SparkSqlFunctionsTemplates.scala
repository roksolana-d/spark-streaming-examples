package utils

import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class SparkSqlFunctionsTemplates {

  def minAndMaxColumnValues(inputDataFrame: DataFrame, colName: String) = {
    inputDataFrame
      .agg(min(inputDataFrame.col(colName)), max(inputDataFrame.col(colName)))
  }

  def countColumnValues(inputDataFrame: DataFrame, colName: String) = {
    inputDataFrame
      .groupBy(colName)
      .count()
      .orderBy()
  }

  def filterContainsStringValues(inputDataFrame: DataFrame, colName: String, condition: String) = {
    inputDataFrame
      .filter(inputDataFrame.col(colName).contains(condition))
  }

  def filterBiggerThanIntValues(inputDataFrame: DataFrame, colName: String, condition: Int) = {
    inputDataFrame
      .filter(inputDataFrame.col(colName) > condition)
  }

  def outputSpecificField(dataset: Dataset[Row], colName: String)= {
    dataset
      .selectExpr(colName)
  }

  def buildTweetDataStruct() ={
    new StructType()
      .add("created_at", DataTypes.StringType)
      .add("id", DataTypes.LongType)
      .add("text", DataTypes.StringType)
      .add("source", DataTypes.StringType)
      .add("timestamp_ms", DataTypes.StringType)
      .add("user", new StructType()
        .add("id", DataTypes.LongType)
        .add("name", DataTypes.StringType)
        .add("location", DataTypes.StringType)
        .add("url", DataTypes.StringType)
        .add("description", DataTypes.StringType)
        .add("followers_count", DataTypes.IntegerType)
        .add("friends_count", DataTypes.IntegerType)
        .add("favourites_count", DataTypes.IntegerType)
        .add("lang", DataTypes.StringType)
        .add("reply_count", DataTypes.IntegerType)
        .add("retweet_count", DataTypes.IntegerType)
        .add("favorite_count", DataTypes.IntegerType)
      )
      .add("entities", new StructType()
        .add("hashtags", new ArrayType(DataTypes.StringType, true)
        )
      )
  }
}

/**
 * Created by Lema on 9/21/15.
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming {

  def main(args: Array[String]) {


    val filters = args

    System.setProperty("twitter4j.oauth.consumerKey", "A6ufsokRXbEJwYlB8AYs6kiy3")
    System.setProperty("twitter4j.oauth.consumerSecret", "IxvVdrQTEfWTxw8SnAMaYpSPW4snDOQbrDMRHUsLHuSku78J61")
    System.setProperty("twitter4j.oauth.accessToken", "3002889342-IYRBCylpS3lWOybUxQv61gsf3ognkxbXFkk8PO1")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "U8heloG3mUBIQdYfYC1c2N6D0Ue6igXYqJC5sO4qa9PsK")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")


    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 10 second window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(1)
      SocketClient.sendCommandToRobot( rdd.count() +"is the total tweets analyzed")
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    ssc.start()

    ssc.awaitTermination()
  }


}

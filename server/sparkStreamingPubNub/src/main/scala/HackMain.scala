/**
 * Created by admin on 02/10/15.
 */

import java.util.{Scanner, Hashtable}

import com.pubnub.api._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.json.{JSONObject, JSONArray}

object HackMain {

  var pub_key = "pub-c-6546afcf-2977-4f18-9ab4-68543a990569"
  var sub_key = "sub-c-9759ad36-68e5-11e5-9e44-02ee2ddab7fe"
  var sec_key = "sec-c-YWJmMDdmMTItYTBlMC00Y2I3LWI0NDctZTM0ODdlNDhiYjdk"

  var channel ="spark-streaming"



  var pubnub: Pubnub = new Pubnub(pub_key,sub_key,sec_key)

  def notifyUser(msg: String) {
    println(msg)
  }
  def subscribe(channelName: String) {

    try {
      pubnub.subscribe(channelName, new Callback() {
        override def connectCallback(channel: String, message: Object) {
          notifyUser("SUBSCRIBE : CONNECT on channel:" + channel
            + " : " + message.getClass() + " : "
            + message.toString())
        }

        override def disconnectCallback(channel: String, message: Object) {
          notifyUser("SUBSCRIBE : DISCONNECT on channel:" + channel
            + " : " + message.getClass() + " : "
            + message.toString())
        }

        override def reconnectCallback(channel: String, message: Object) {
          notifyUser("SUBSCRIBE : RECONNECT on channel:" + channel
            + " : " + message.getClass() + " : "
            + message.toString())
        }

        override def successCallback(channel: String, message: Object) {
          notifyUser("SUBSCRIBE : " + channel + " : "
            + message.getClass() + " : " + message.toString())
        }

        override def errorCallback(channel: String, error: PubnubError) {
          notifyUser("SUBSCRIBE : ERROR on channel " + channel
            + " : " + error.toString())
          error.errorCode match {
            case PubnubError.PNERR_FORBIDDEN => { pubnub.unsubscribe(channel);}
            case PubnubError.PNERR_UNAUTHORIZED => {pubnub.unsubscribe(channel);}
            case _ => {}
          }
        }
      })

    } catch {
      case e: Exception => {}

    }
  }
  def publish(channel: String,message:String) {
    var callback = new Callback() {

      override def successCallback(channel: String, message: Object) {
        notifyUser("PUBLISH : " + message);
      }

      override def errorCallback(channel: String, error: PubnubError) {
        notifyUser("PUBLISH : " + error);
      }
    }
      var parsed = false

      if (!parsed) {
        try {
          var i: Integer = message.toInt
          pubnub.publish(channel, i, callback)
          parsed = true
        } catch {
          case e: Exception => {}
        }
      }
      if (!parsed) {
        try {
          var d = message.toDouble
          parsed = true
          pubnub.publish(channel, d, callback)
        } catch {
          case e: Exception => {}

        }
      }
      if (!parsed) {
        try {
          var js: JSONArray = new JSONArray(message)
          parsed = true
          pubnub.publish(channel, js, callback)
        } catch {
          case e: Exception => {}

        }
      }
      if (!parsed) {
        try {
          var js: JSONObject = new JSONObject(message);
          pubnub.publish(channel, js, callback)
          parsed = true
        } catch {
          case e: Exception => {}

        }
      }
      if (!parsed) {
        pubnub.publish(channel, message, callback)
      }
    }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    /*
      generate risk score here and use the below line to send to pubnub
     */
    lines.foreach( x => {if(x.count()>0) { println(x.first());publish(channel,x.first())}})
    ssc.start()

    ssc.awaitTermination()

  }

}

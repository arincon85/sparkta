/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.sparkta.plugin.input.twitter

import java.io.{Serializable => JSerializable}
import com.stratio.sparkta.sdk.Input._
import com.stratio.sparkta.sdk.ValidatingPropertyMap._
import com.stratio.sparkta.sdk.{Event, Input}

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.{Status, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

/**
 * Created by ajnavarro on 22/10/14.
 */
class TwitterInput(properties: Map[String, JSerializable]) extends Input(properties) {


  System.setProperty("twitter4j.oauth.consumerKey",properties.getString("consumerKey"))
  System.setProperty("twitter4j.oauth.consumerSecret", properties.getString("consumerSecret"))
  System.setProperty("twitter4j.oauth.accessToken",  properties.getString("accessToken"))
  System.setProperty("twitter4j.oauth.accessTokenSecret", properties.getString("accessTokenSecret"))

  val cb = new ConfigurationBuilder().setUseSSL(true)
  val tf = new TwitterFactory(cb.build())
  val twitterApi = tf.getInstance()
  val trends = twitterApi.getPlaceTrends(1).getTrends.map(trend => trend.getName)

  override def setUp(ssc: StreamingContext): DStream[Event] = {
    val stream=TwitterUtils.createStream(ssc, None, trends)

    stream.map(data => new Event(Map("status" -> Array(data),
      "wordsN" -> data.getText.split(" ").toArray,
      "timestamp" ->  Array(data.getCreatedAt),
      "geolocation" -> Array(data.getGeoLocation match {
        case null => None
        case _ => Some((data.getGeoLocation.getLatitude , data.getGeoLocation.getLongitude))
      })
    )))
  }
}

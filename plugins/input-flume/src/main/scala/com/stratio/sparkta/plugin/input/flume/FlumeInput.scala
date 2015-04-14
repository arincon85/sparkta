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
package com.stratio.sparkta.plugin.input.flume

import java.io.Serializable
import java.net.InetSocketAddress

import com.stratio.sparkta.sdk.Input._
import com.stratio.sparkta.sdk.ValidatingPropertyMap._
import com.stratio.sparkta.sdk.{Event, Input}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils

import scala.collection.JavaConverters._


class FlumeInput(properties: Map[String, Serializable]) extends Input(properties) {

  val DEFAULT_STORAGE_LEVEL = "MEMORY_AND_DISK_SER_2"
  val DEFAULT_FLUME_PORT = 11999
  val DEFAULT_ENABLE_DECOMPRESSION = false
  val DEFAULT_MAXBATCHSIZE = 1000
  val DEFAULT_PARALLELISM = 5

  override def setUp(ssc: StreamingContext): DStream[Event] = {

    if (properties.getString("type").equalsIgnoreCase("pull")) {
      FlumeUtils.createPollingStream(
        ssc,
        getAddresses,
        storageLevel,
        maxBatchSize,
        parallelism
      ).map(data => new Event(Map(RAW_DATA_KEY -> data.event.getBody.array.asInstanceOf[Array[Any]]) ++
        data.event.getHeaders.asScala.map(h =>
          (h._1.toString -> h._2.toString.split(SEPARATOR).asInstanceOf[Array[Any]]))
      ))
    } else {
      // push
      FlumeUtils.createStream(
        ssc, properties.getString("hostname"),
        properties.getInt("port"),
        storageLevel,
        enableDecompression
      ).map(data => new Event(Map(RAW_DATA_KEY -> data.event.getBody.array.asInstanceOf[Array[Any]])++
        data.event.getHeaders.asScala.map(h =>
          h._1.toString -> h._2.toString.split (SEPARATOR).asInstanceOf[Array[Any]])
      ))
    }

  }

  private def getAddresses(): Seq[InetSocketAddress] = {
    properties.getString("addresses").split(",").toSeq.map(str => str.split(":").toSeq match {
      case Seq(address) => new InetSocketAddress(address, DEFAULT_FLUME_PORT)
      case Seq(address, port) => new InetSocketAddress(address, port.toInt)
      case _ =>
        throw new IllegalStateException(s"Invalid conf value for addresses : $str")
    })
  }

  private def storageLevel(): StorageLevel =
    properties.hasKey("storageLevel") match {
      case true => StorageLevel.fromString(properties.getString("storageLevel"))
      case false => StorageLevel.fromString(DEFAULT_STORAGE_LEVEL)
    }

  private def enableDecompression(): Boolean =
    properties.hasKey("enableDecompression") match {
      case true => properties.getBoolean("enableDecompression")
      case false => DEFAULT_ENABLE_DECOMPRESSION
    }

  private def parallelism(): Int = {
    properties.hasKey("parallelism") match {
      case true => properties.getInt("parallelism")
      case false => DEFAULT_PARALLELISM
    }
  }

  private def maxBatchSize(): Int =
    properties.hasKey("maxBatchSize") match {
      case true => properties.getInt("maxBatchSize")
      case false => DEFAULT_MAXBATCHSIZE
    }
}


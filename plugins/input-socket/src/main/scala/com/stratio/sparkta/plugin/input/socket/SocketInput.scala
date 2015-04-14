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
package com.stratio.sparkta.plugin.input.socket

import java.io.{Serializable => JSerializable}
import com.stratio.sparkta.sdk.Input._
import com.stratio.sparkta.sdk.ValidatingPropertyMap._
import com.stratio.sparkta.sdk.{Event, Input}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ajnavarro on 22/10/14.
 */
class SocketInput(properties: Map[String, JSerializable]) extends Input(properties) {

  private val hostname : String = properties.getString("hostname")
  private val port : Int = properties.getInt("port")
  private val storageLevel : StorageLevel = StorageLevel
    .fromString(properties.getString("storageLevel", "MEMORY_AND_DISK_SER_2"))

  override def setUp(ssc: StreamingContext): DStream[Event] = {
    ssc.socketTextStream(
      hostname,
      port,
      storageLevel)
      .map(data => new Event(Map(RAW_DATA_KEY -> data.split(SEPARATOR).toArray )))
  }
}

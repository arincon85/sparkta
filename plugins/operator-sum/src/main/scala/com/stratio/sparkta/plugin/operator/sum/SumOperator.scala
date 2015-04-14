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
package com.stratio.sparkta.plugin.operator.sum

import java.io.{Serializable => JSerializable}
import org.apache.spark.sql.Row

import com.stratio.sparkta.sdk._
import com.stratio.sparkta.sdk.ValidatingPropertyMap._

import scala.util.Try

class SumOperator(properties: Map[String, JSerializable]) extends Operator(properties) {

  private val inputField = if(properties.contains("inputField")) properties.getString("inputField") else ""

  override val key : String = "sum_" + inputField

  override val writeOperation = WriteOp.Inc

  override def processMap(inputFields: Map[String, Row]) :Option[Number]=
    inputFields.contains(inputField) match {
      case false => Some(0L)
      case true => Some(inputFields.get(inputField).get.asInstanceOf[Number])
    }

  override def processReduce(values : Iterable[Option[_>:AnyVal]]):Option[Long] = {
    Try(Some(values.map(_.get.asInstanceOf[Number].longValue()).reduce(_ + _)))
      .getOrElse(Some(0L))
  }

}

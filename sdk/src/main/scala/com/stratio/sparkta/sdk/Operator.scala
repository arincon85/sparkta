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
package com.stratio.sparkta.sdk

import java.io.{Serializable => JSerializable}

import org.apache.spark.sql.Row

import com.stratio.sparkta.sdk.WriteOp.WriteOp

abstract class Operator(properties: Map[String, JSerializable]) extends Parameterizable(properties) {
  def key : String
  def writeOperation : WriteOp
  def processMap(inputFields: Map[String, Row]) : Option[_>:AnyVal]
  def processReduce(values : Iterable[Option[_>:AnyVal]]) : Option[_>:AnyVal]
}

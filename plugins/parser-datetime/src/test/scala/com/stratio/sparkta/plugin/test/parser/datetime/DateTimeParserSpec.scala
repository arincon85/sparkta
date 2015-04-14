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
package com.stratio.sparkta.plugin.test.parser.datetime

import java.util.Date

import com.stratio.sparkta.plugin.parser.datetime.DateTimeParser
import com.stratio.sparkta.sdk.Event
import org.joda.time.DateTime
import org.joda.time.chrono.ISOChronology
import org.scalatest.WordSpecLike

class DateTimeParserSpec extends WordSpecLike {

  "A DateTimeParser" should {
    "parse unixMillis" in {
      val e1 = new Event(Map("ts" -> Array(1416330788000L).asInstanceOf[Array[Any]]))
      val e2 = new Event(Map("ts" -> Array(new Date(1416330788000L)).asInstanceOf[Array[Any]]))
      assertResult(e2)(new DateTimeParser(Map("ts" -> "unixMillis")).parse(e1))
    }
    "parse unixMillis string" in {
      val e1 = new Event(Map("ts" -> Array("1416330788000").asInstanceOf[Array[Any]]))
      val e2 = new Event(Map("ts" -> Array(new Date(1416330788000L)).asInstanceOf[Array[Any]]))
      assertResult(e2)(new DateTimeParser(Map("ts" -> "unixMillis")).parse(e1))
    }
    "parse unix" in {
      val e1 = new Event(Map("ts" -> Array("1416330788").asInstanceOf[Array[Any]]))
      val e2 = new Event(Map("ts" -> Array(new Date(1416330788000L)).asInstanceOf[Array[Any]]))
      println(new Date(1416330788L))
      assertResult(e2)(new DateTimeParser(Map("ts" -> "unix")).parse(e1))
    }
    "parse unix string" in {
      val e1 = new Event(Map("ts" -> Array("1416330788").asInstanceOf[Array[Any]]))
      val e2 = new Event(Map("ts" -> Array(new Date(1416330788000L)).asInstanceOf[Array[Any]]))
      assertResult(e2)(new DateTimeParser(Map("ts" -> "unix")).parse(e1))
    }
    "parse dateTime" in {
      val e1 = new Event(Map("ts" ->Array( "2014-05-23T21:22:23.250Z").asInstanceOf[Array[Any]]))
      val e2 = new Event(Map("ts" ->Array( new DateTime(ISOChronology.getInstanceUTC)
        .withYear(2014).withMonthOfYear(5).withDayOfMonth(23)
        .withHourOfDay(21).withMinuteOfHour(22).withSecondOfMinute(23).withMillisOfSecond(250)
        .toDate).asInstanceOf[Array[Any]])
      )
      assertResult(e2)(new DateTimeParser(Map("ts" -> "dateTime")).parse(e1))
    }
  }
}

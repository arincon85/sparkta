/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
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
package com.stratio.sparkta.aggregator
import org.apache.spark.streaming._
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import com.stratio.sparkta.plugin.bucketer.passthrough.PassthroughBucketer
import com.stratio.sparkta.plugin.operator.count.CountOperator
import com.stratio.sparkta.sdk._


import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.streaming.receiver.Receiver

//noinspection ScalaStyle
class DataCubeSpec extends TestSuiteBase {
  val myConf = new SparkConf()
    .setAppName(this.getClass.getSimpleName + "" + System.currentTimeMillis())
    .setMaster("local[2]")

  val ssc: StreamingContext = new StreamingContext(myConf,Seconds(2))

  val inputStream: DStream[Event] =new MyDstream[Event](sec = 2000)(ssc)

  def getDimensions: Seq[Dimension] = (0 until 10 )map ( i=> new Dimension("myKey"+i, PassthroughBucketer
    .asInstanceOf[Bucketer]) )

  def getComponents  : Seq[(Dimension, BucketType)] = (0 until 2) map (i=> ((new Dimension("myKey"+i,
    PassthroughBucketer
    .asInstanceOf[Bucketer])),Bucketer.identity))

  def getOperators : Seq[Operator] = (0 until 1) map (i=> new CountOperator(Map("prop"->"propValue"
    .asInstanceOf[Serializable])))

    val myRollups: Seq[Rollup] = (0 until 2) map (i => new Rollup(components = getComponents, operators = getOperators))



  val myDimensions: Seq[Dimension] = getDimensions

  test("DataCube setUp"){
    val dc :DataCube =new DataCube(myDimensions ,myRollups)
      dc.setUp(inputStream)
  }


}

//noinspection ScalaStyle
case class MyDstream[Event] (sec:Long)(@transient ssc_ : StreamingContext)
  extends ReceiverInputDStream[Event] (ssc_) {
  override def slideDuration(): Duration = {
    return Seconds(sec)
  }
  def newEvent(l: Long):Event = {
    new Event(){

      val myKeyMap: Map[String, Array[Any]] = Map("myKey"->Array(System.currentTimeMillis(),2,3,4).asInstanceOf[Array[Any]])

      Event(myKeyMap)
    }
  }

  override def getReceiver(): Receiver[Event] = {
    new Receiver[Event](StorageLevel.MEMORY_AND_DISK) {
      @volatile var killed=false;
      override def onStart() = {
        new Thread(
          new Runnable() {
            override def run(): Unit = {
              while(!killed) {
                System.out.println("-")
                store(newEvent(System.currentTimeMillis()))
                Thread.sleep(1000);
              }
            }
          }).start()
      }

      override def onStop() = {
        System.out.println("Stop requested.");
        killed=true;
      }
    }

  }
}
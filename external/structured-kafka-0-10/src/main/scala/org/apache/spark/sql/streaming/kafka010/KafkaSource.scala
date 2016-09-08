/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming.kafka010

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.execution.streaming.{ Offset, Source }
import org.apache.spark.sql.types._

object KafkaSource {
  // XXX TODO parameterize key and value types according to deserializer
  def stringSchema: StructType = {
    val key = StringType
    val value = StringType
    StructType(Seq(
      StructField("checksum", LongType),
      StructField("key", key),
      StructField("offset", LongType),
      StructField("partition", IntegerType),
      StructField("serializedKeySize", IntegerType),
      StructField("serializedValueSize", IntegerType),
      StructField("timestamp", LongType),
      StructField("timestampType", IntegerType),
      StructField("topic", StringType),
      StructField("value", value)
    ))
  }
}

class KafkaSource(sqlContext: SQLContext) extends Source {
  var bogusOffset = 0

  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    import sqlContext.implicits._

    sqlContext.sparkContext.parallelize(Seq(
      ("bogusTopic", 0, bogusOffset, "a_key", "a_value"),
      ("bogusValue", 0, bogusOffset + 1, "another_key", "another_value")
    )).map { t =>
      val cr = new ConsumerRecord[String, String](t._1, t._2, t._3, t._4, t._5)
      (cr.checksum, cr.key, cr.offset, cr.partition, cr.serializedKeySize, cr.serializedValueSize,
        cr.timestamp, cr.timestampType.id, cr.topic, cr.value)
    }.toDF("checksum", "key", "offset", "partition", "serializedKeySize", "serializedValueSize",
      "timestamp", "timestampType", "topic", "value")
  }

  def getOffset: Option[Offset] = {
    bogusOffset += 2
    Some(KafkaOffsets(Map(new TopicPartition("bogusTopic", 0) -> bogusOffset)))
  }

  val schema = KafkaSource.stringSchema

  def stop(): Unit = {

  }
}

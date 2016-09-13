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

package org.apache.spark.streaming.kafka010

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.execution.streaming.{ Offset, Source }
import org.apache.spark.sql.types._

object KafkaSource {
  // TODO parameterize key and value types according to deserializer
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

/**
 * Structured Streaming Source for Kafka
 */
class KafkaSource[K, V](
    sqlContext: SQLContext,
    val locationStrategy: LocationStrategy,
    val consumerStrategy: ConsumerStrategy[K, V]
) extends Source with DriverConsumer[K, V] {
  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    import sqlContext.implicits._

    // TODO is this the right semantics - docs say empty start should be "first available record",
    // but that's kind of up to ConsumerStrategy (earliest, latest, somewhere in middle)
    val fromOffsets = start.map(_.asInstanceOf[KafkaOffsets].offsets).getOrElse(Map())
    val untilOffsets = end.asInstanceOf[KafkaOffsets].offsets
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = fromOffsets.getOrElse(tp, uo)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }

    val rdd = new KafkaRDD[K, V](
      sqlContext.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, true
    ).map { cr =>
      Row(cr.checksum, cr.key, cr.offset, cr.partition, cr.serializedKeySize,
        cr.serializedValueSize, cr.timestamp, cr.timestampType.id, cr.topic, cr.value)
    }

    sqlContext.sparkSession.createDataFrame(rdd, schema)
  }

  def getOffset: Option[Offset] = {
    // TODO how to do rate limiting, without a rate estimator or regular batch intervals
    val latest = latestOffsets
    if (latest == currentOffsets) {
      None
    } else {
      currentOffsets = latestOffsets
      Some(KafkaOffsets(latestOffsets))
    }
  }

  val schema = KafkaSource.stringSchema

  def stop(): Unit = {
    stopConsumer()
  }

  // Startup code
  {
    val c = consumer
    c.poll(0)
    if (currentOffsets.isEmpty) {
      currentOffsets = c.assignment().asScala.map { tp =>
        tp -> c.position(tp)
      }.toMap
    }

    // don't actually want to consume any messages, so pause all partitions
    c.pause(currentOffsets.keySet.asJava)
  }
}

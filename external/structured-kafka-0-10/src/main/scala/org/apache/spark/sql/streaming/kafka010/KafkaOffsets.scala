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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.execution.streaming.Offset

/**
 * Offset for Kafka Sources, compares all Kafka offsets for a given group of TopicPartitions
 */
case class KafkaOffsets(offsets: Map[TopicPartition, Long]) extends Offset {

  override def compareTo(other: Offset): Int = {
    var result: Option[Int] = None
    val that: KafkaOffsets = other match {
      case x: KafkaOffsets =>
        x
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid comparison of $getClass with ${other.getClass}")
    }

    this.offsets.foreach { kv =>
      val (k, v) = kv
      that.offsets.get(k) match {
        case Some(o: Long) if v > o =>
          assert(result.isEmpty || result == Some(1),
            s"$this is ahead of $that for $k, but is behind for other partitions")
          result = Some(1)
        case Some(o: Long) if v < o =>
          assert(result.isEmpty || result == Some(-1),
            s"$this is behind $that for $k, but is ahead for other partitions")
          result = Some(-1)
        case _ =>
          // other equal or missing partitions don't tell us anything about order, no change
      }
    }

    if (result.isEmpty) {
      if (this.offsets == that.offsets) {
        result = Some(0)
      } else {
        // This is possible if partitions have changed and are completely divergent.
        // In that case, order doesn't matter, but returning equal could violate hashcode contract.
        // So use some defined ordering.
        result = Some(this.hashCode.compareTo(that.hashCode))
      }
    }

    assert(result.isDefined, s"Cannot compare $this and $that")
    result.get
  }
}
